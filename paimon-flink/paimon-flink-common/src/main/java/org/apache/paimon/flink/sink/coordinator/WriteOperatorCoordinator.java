/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.sink.coordinator;

import org.apache.paimon.flink.sink.TableWriteOperator;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SegmentsCache;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_EXPIRE_AFTER_ACCESS;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_PAGE_SIZE;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_SOFT_VALUES;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;

/**
 * {@link OperatorCoordinator} for {@link TableWriteOperator}, to use a single point to obtain the
 * list of initialization files required for write operators.
 */
public class WriteOperatorCoordinator implements OperatorCoordinator, CoordinationRequestHandler {

    private static final Logger LOG = LoggerFactory.getLogger(WriteOperatorCoordinator.class);
    private static final long SLOW_REQUEST_MILLIS = 1_000;

    private final FileStoreTable table;
    @Nullable private final MetricGroup metricGroup;

    private ThreadPoolExecutor executor;
    private TableWriteCoordinator coordinator;

    public WriteOperatorCoordinator(FileStoreTable table) {
        this(table, null);
    }

    WriteOperatorCoordinator(FileStoreTable table, @Nullable MetricGroup metricGroup) {
        this.table = table;
        this.metricGroup = metricGroup;
    }

    @Override
    public void start() throws Exception {
        executor = createCachedThreadPool(1, "WriteCoordinator");
        Options tableOptions = table.coreOptions().toConfiguration();
        MemorySize cacheMemory = tableOptions.get(SINK_WRITER_COORDINATOR_CACHE_MEMORY);
        int cachePageSize =
                (int) tableOptions.get(SINK_WRITER_COORDINATOR_CACHE_PAGE_SIZE).getBytes();
        Duration cacheExpireAfterAccess =
                tableOptions.get(SINK_WRITER_COORDINATOR_CACHE_EXPIRE_AFTER_ACCESS);
        boolean cacheSoftValues = tableOptions.get(SINK_WRITER_COORDINATOR_CACHE_SOFT_VALUES);
        SegmentsCache<Path> manifestCache =
                SegmentsCache.create(
                        cachePageSize,
                        cacheMemory,
                        Long.MAX_VALUE,
                        cacheExpireAfterAccess,
                        cacheSoftValues);
        table.setManifestCache(manifestCache);
        coordinator = new TableWriteCoordinator(table);
        if (metricGroup != null) {
            metricGroup.gauge("manifestCacheBytes", manifestCache::totalCacheBytes);
            metricGroup.gauge("manifestCacheEntries", manifestCache::estimatedSize);
            metricGroup.gauge("pagedCoordinationCacheBytes", coordinator::pagedCoordinationBytes);
            metricGroup.gauge(
                    "pagedCoordinationSpillBytes", coordinator::pagedCoordinationSpillBytes);
            metricGroup.gauge(
                    "pagedCoordinationCacheEntries", coordinator::pagedCoordinationEntries);
            metricGroup.gauge(
                    "lastSerializedScanResponseBytes",
                    coordinator::lastSerializedScanResponseBytes);
            metricGroup.gauge(
                    "maxSerializedScanResponseBytes", coordinator::maxSerializedScanResponseBytes);
            metricGroup.gauge(
                    "pagedCoordinationRecomputations",
                    coordinator::pagedCoordinationRecomputations);
        }
    }

    @Override
    public void close() throws Exception {
        if (coordinator != null) {
            coordinator.clearPagedCoordination();
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        long receivedNanos = System.nanoTime();
        int queueSizeAtSubmission = executor.getQueue().size();
        CompletableFuture<CoordinationResponse> future = new CompletableFuture<>();
        executor.execute(
                () -> {
                    long startedNanos = System.nanoTime();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                                "Writer coordinator request started: table={}, requestType={}, "
                                        + "requestId={}, pageToken={}, queueWaitMs={}, queueSize={}",
                                table.name(),
                                request.getClass().getSimpleName(),
                                requestId(request),
                                pageToken(request),
                                TimeUnit.NANOSECONDS.toMillis(startedNanos - receivedNanos),
                                executor.getQueue().size());
                    }
                    try {
                        CoordinationResponse response;
                        if (request instanceof PagedCoordinationRequest) {
                            response = coordinator.scan((PagedCoordinationRequest) request);
                        } else if (request instanceof LatestIdentifierRequest) {
                            response =
                                    new LatestIdentifierResponse(
                                            coordinator.latestCommittedIdentifier(
                                                    ((LatestIdentifierRequest) request).user()));
                        } else {
                            throw new UnsupportedOperationException(
                                    "Unsupported request type: " + request);
                        }
                        CoordinationResponse wrapped = CoordinationResponseUtils.wrap(response);
                        logRequestDuration(
                                request,
                                receivedNanos,
                                startedNanos,
                                queueSizeAtSubmission,
                                response,
                                null);
                        future.complete(wrapped);
                    } catch (Exception e) {
                        if (request instanceof PagedCoordinationRequest) {
                            coordinator.invalidatePagedCoordination(
                                    (PagedCoordinationRequest) request);
                        }
                        logRequestDuration(
                                request,
                                receivedNanos,
                                startedNanos,
                                queueSizeAtSubmission,
                                null,
                                e);
                        future.completeExceptionally(e);
                    }
                });
        return future;
    }

    private void logRequestDuration(
            CoordinationRequest request,
            long receivedNanos,
            long startedNanos,
            int queueSizeAtSubmission,
            @Nullable CoordinationResponse response,
            @Nullable Exception failure) {
        long finishedNanos = System.nanoTime();
        long totalMs = TimeUnit.NANOSECONDS.toMillis(finishedNanos - receivedNanos);
        boolean firstPage =
                request instanceof PagedCoordinationRequest && pageToken(request) == null;
        // Fast continuation pages can be very numerous. Keep them at DEBUG, while first pages,
        // slow requests (including queue wait), and failures remain visible by default.
        boolean info = firstPage || totalMs >= SLOW_REQUEST_MILLIS;
        if (failure == null && !info && !LOG.isDebugEnabled()) {
            return;
        }
        PagedCoordinationResponse page =
                response instanceof PagedCoordinationResponse
                        ? (PagedCoordinationResponse) response
                        : null;
        String format =
                "Writer coordinator request finished: table={}, requestType={}, "
                        + "requestId={}, pageToken={}, success={}, queueWaitMs={}, executionMs={}, "
                        + "totalMs={}, queueSizeAtSubmission={}, queueSizeAtFinish={}, "
                        + "responseBytes={}, nextPageToken={}";
        Object[] arguments = {
            table.name(),
            request.getClass().getSimpleName(),
            requestId(request),
            pageToken(request),
            failure == null,
            TimeUnit.NANOSECONDS.toMillis(startedNanos - receivedNanos),
            TimeUnit.NANOSECONDS.toMillis(finishedNanos - startedNanos),
            totalMs,
            queueSizeAtSubmission,
            executor.getQueue().size(),
            page == null ? -1 : page.content().length,
            page == null ? null : page.nextPageToken(),
            failure
        };
        if (failure != null) {
            LOG.warn(format, arguments);
        } else if (info) {
            LOG.info(format, arguments);
        } else {
            LOG.debug(format, arguments);
        }
    }

    private static String requestId(CoordinationRequest request) {
        return request instanceof PagedCoordinationRequest
                ? ((PagedCoordinationRequest) request).requestId()
                : "n/a";
    }

    @Nullable
    private static Integer pageToken(CoordinationRequest request) {
        return request instanceof PagedCoordinationRequest
                ? ((PagedCoordinationRequest) request).pageToken()
                : null;
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
        coordinator.checkpoint();
        executor.execute(
                () -> {
                    try {
                        result.complete(new byte[0]);
                    } catch (Throwable throwable) {
                        result.completeExceptionally(new CompletionException(throwable));
                    }
                });
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {
        clearPagedCoordination();
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        clearPagedCoordination();
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, Throwable reason) {
        clearPagedCoordination();
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {}

    private void clearPagedCoordination() {
        if (coordinator != null) {
            coordinator.clearPagedCoordination();
        }
    }

    /** Provider for {@link WriteOperatorCoordinator}. */
    public static class Provider implements OperatorCoordinator.Provider {

        private final OperatorID operatorId;
        private final FileStoreTable table;

        public Provider(OperatorID operatorId, FileStoreTable table) {
            this.operatorId = operatorId;
            this.table = table;
        }

        @Override
        public OperatorID getOperatorId() {
            return this.operatorId;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new WriteOperatorCoordinator(table, context.metricGroup());
        }
    }
}
