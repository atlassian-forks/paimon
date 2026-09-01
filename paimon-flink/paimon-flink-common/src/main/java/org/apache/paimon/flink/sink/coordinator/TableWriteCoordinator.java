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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.utils.InstantiationUtil.deserializeObject;
import static org.apache.paimon.utils.InstantiationUtil.serializeObject;
import static org.apache.paimon.utils.Preconditions.checkNotNull;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * Coordinator for a table, to use a single point to obtain the list of initialization files
 * required for write operators.
 */
public class TableWriteCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(TableWriteCoordinator.class);
    private static final long MAX_TRACKED_PAGED_REQUESTS = 10_000;

    private final FileStoreTable table;
    private final Map<String, Long> latestCommittedIdentifiers;
    private final FileStoreScan scan;
    private final IndexFileHandler indexFileHandler;
    private final int pageSize;
    private final boolean prefetchManifests;
    private final Cache<CoordinationKey, byte[]> pagedCoordination;
    private final Cache<CoordinationKey, Snapshot> pagedCoordinationSnapshots;
    private final long pagedCoordinationMaxBytes;
    private final AtomicLong pagedCoordinationRecomputations = new AtomicLong();

    private volatile long lastSerializedScanResponseBytes;
    private volatile long maxSerializedScanResponseBytes;

    private volatile Snapshot snapshot;

    public TableWriteCoordinator(FileStoreTable table) {
        this.table = table;
        checkNotNull(table.getManifestCache());
        this.latestCommittedIdentifiers = new ConcurrentHashMap<>();
        this.scan = table.store().newScan();
        if (table.coreOptions().manifestDeleteFileDropStats()) {
            scan.dropStats();
        }
        this.indexFileHandler = table.store().newIndexFileHandler();
        Options options = table.coreOptions().toConfiguration();
        this.pageSize =
                (int)
                        options.get(FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PAGE_SIZE)
                                .getBytes();
        this.prefetchManifests =
                options.get(FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PREFETCH_MANIFESTS);
        String pagedCacheMemory =
                options.get(FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PAGED_CACHE_MEMORY);
        Duration pagedCacheTtl =
                options.get(
                        FlinkConnectorOptions
                                .SINK_WRITER_COORDINATOR_PAGED_CACHE_EXPIRE_AFTER_ACCESS);
        this.pagedCoordinationMaxBytes = parsePagedCacheMemory(pagedCacheMemory);
        if (pagedCoordinationMaxBytes >= 0) {
            this.pagedCoordination =
                    Caffeine.newBuilder()
                            .executor(Runnable::run)
                            .weigher(TableWriteCoordinator::pagedResponseWeight)
                            .maximumWeight(pagedCoordinationMaxBytes)
                            .expireAfterAccess(pagedCacheTtl)
                            .build();
        } else {
            this.pagedCoordination =
                    Caffeine.newBuilder()
                            .executor(Runnable::run)
                            .expireAfterAccess(pagedCacheTtl)
                            .build();
        }
        this.pagedCoordinationSnapshots =
                Caffeine.newBuilder()
                        .executor(Runnable::run)
                        .maximumSize(MAX_TRACKED_PAGED_REQUESTS)
                        .expireAfterAccess(pagedCacheTtl)
                        .build();
        refresh();
    }

    private synchronized void refresh() {
        Optional<Snapshot> latestSnapshot = table.latestSnapshot();
        if (!latestSnapshot.isPresent()) {
            return;
        }
        this.snapshot = latestSnapshot.get();
        this.scan.withSnapshot(snapshot);
        if (prefetchManifests) {
            warmManifestCache();
        }
    }

    /**
     * Eagerly read all data manifests of the current snapshot once to warm the table's {@link
     * org.apache.paimon.utils.SegmentsCache} (the byte-level manifest cache attached to the table
     * inside the Job Manager). This reuses the same threaded {@code plan()} read path that per-task
     * {@link #scan} requests use, so subsequent concurrent requests hit warm bytes instead of each
     * performing a cold manifest read. A failure here must never break {@link #refresh()}, so any
     * exception is swallowed and logged.
     */
    private void warmManifestCache() {
        try {
            long startTime = System.currentTimeMillis();
            // Clear any leftover partition/bucket filter from a previous scan() so the whole
            // snapshot is read; each scan() call re-applies its own withPartitionBucket afterward.
            scan.withPartitionFilter((List<BinaryRow>) null).withBucket((Integer) null).plan();
            LOG.info(
                    "Warmed writer coordinator manifest cache for snapshot {}, duration: {}ms",
                    snapshot.id(),
                    System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to warm writer coordinator manifest cache for snapshot {}. "
                            + "Falling back to cold per-request manifest reads.",
                    snapshot == null ? null : snapshot.id(),
                    e);
        }
    }

    public synchronized PagedCoordinationResponse scan(PagedCoordinationRequest request)
            throws IOException {
        Integer pageToken = request.pageToken();
        CoordinationKey requestKey = new CoordinationKey(request.content(), request.requestId());
        byte[] full;
        if (pageToken != null) {
            full = pagedCoordination.getIfPresent(requestKey);
            if (full == null) {
                Snapshot requestSnapshot = pagedCoordinationSnapshots.getIfPresent(requestKey);
                if (requestSnapshot == null) {
                    throw new IllegalStateException(
                            "Paged writer coordinator request expired before completion. "
                                    + "Increase sink.writer-coordinator.paged-cache-expire-after-access "
                                    + "or sink.writer-coordinator.paged-cache-memory.");
                }
                pagedCoordinationRecomputations.incrementAndGet();
                full = serializeScanResponse(request.content(), requestSnapshot);
                cachePagedResponse(requestKey, full);
            }
        } else {
            Snapshot requestSnapshot = snapshot;
            full = serializeScanResponse(request.content(), requestSnapshot);
            if (full.length > pageSize && requestSnapshot != null) {
                pagedCoordinationSnapshots.put(requestKey, requestSnapshot);
                cachePagedResponse(requestKey, full);
                if (pagedCoordinationMaxBytes >= 0
                        && pagedCoordination.getIfPresent(requestKey) == null) {
                    LOG.warn(
                            "Writer coordinator response ({} bytes) exceeds or was evicted from "
                                    + "the paged response cache ({} bytes). Later pages will be "
                                    + "rebuilt against snapshot {}.",
                            full.length,
                            pagedCoordinationMaxBytes,
                            requestSnapshot.id());
                }
            }
        }

        int offset = pageToken == null ? 0 : pageToken;
        if (offset < 0 || offset >= full.length) {
            invalidatePagedCoordination(request);
            throw new IllegalArgumentException(
                    "Invalid writer coordinator page token "
                            + offset
                            + " for response of "
                            + full.length
                            + " bytes.");
        }
        int len = Math.min(full.length - offset, pageSize);
        byte[] content = Arrays.copyOfRange(full, offset, offset + len);
        Integer nextPageToken = offset + len;
        if (nextPageToken >= full.length) {
            nextPageToken = null;
            invalidatePagedCoordination(request);
        }
        return new PagedCoordinationResponse(content, nextPageToken);
    }

    private byte[] serializeScanResponse(byte[] requestContent, Snapshot requestSnapshot)
            throws IOException {
        ScanCoordinationRequest coordination;
        try {
            coordination = deserializeObject(requestContent, getClass().getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        byte[] full = serializeObject(scan(coordination, requestSnapshot));
        lastSerializedScanResponseBytes = full.length;
        maxSerializedScanResponseBytes = Math.max(maxSerializedScanResponseBytes, full.length);
        return full;
    }

    public synchronized ScanCoordinationResponse scan(ScanCoordinationRequest request)
            throws IOException {
        return scan(request, snapshot);
    }

    private ScanCoordinationResponse scan(ScanCoordinationRequest request, Snapshot requestSnapshot)
            throws IOException {
        if (requestSnapshot == null) {
            return new ScanCoordinationResponse(null, null, null, null, null);
        }

        BinaryRow partition = deserializeBinaryRow(request.partition());
        int bucket = request.bucket();

        List<DataFileMeta> restoreFiles = new ArrayList<>();
        FileStoreScan requestScan = table.store().newScan().withSnapshot(requestSnapshot);
        if (table.coreOptions().manifestDeleteFileDropStats()) {
            requestScan.dropStats();
        }
        List<ManifestEntry> entries =
                requestScan.withPartitionBucket(partition, bucket).plan().files();
        Integer totalBuckets = WriteRestore.extractDataFiles(entries, restoreFiles);

        IndexFileMeta dynamicBucketIndex = null;
        if (request.scanDynamicBucketIndex()) {
            dynamicBucketIndex =
                    indexFileHandler.scanHashIndex(requestSnapshot, partition, bucket).orElse(null);
        }

        List<IndexFileMeta> deleteVectorsIndex = null;
        if (request.scanDeleteVectorsIndex()) {
            deleteVectorsIndex =
                    indexFileHandler.scan(
                            requestSnapshot, DELETION_VECTORS_INDEX, partition, bucket);
        }

        return new ScanCoordinationResponse(
                requestSnapshot,
                totalBuckets,
                restoreFiles,
                dynamicBucketIndex,
                deleteVectorsIndex);
    }

    public void invalidatePagedCoordination(PagedCoordinationRequest request) {
        CoordinationKey requestKey = new CoordinationKey(request.content(), request.requestId());
        pagedCoordination.invalidate(requestKey);
        pagedCoordinationSnapshots.invalidate(requestKey);
    }

    public void clearPagedCoordination() {
        pagedCoordination.invalidateAll();
        pagedCoordinationSnapshots.invalidateAll();
        pagedCoordination.cleanUp();
        pagedCoordinationSnapshots.cleanUp();
    }

    long pagedCoordinationBytes() {
        if (pagedCoordinationMaxBytes < 0) {
            return pagedCoordination.asMap().entrySet().stream()
                    .mapToLong(entry -> pagedResponseWeight(entry.getKey(), entry.getValue()))
                    .sum();
        }
        return pagedCoordination
                .policy()
                .eviction()
                .map(eviction -> eviction.weightedSize().orElse(0L))
                .orElse(0L);
    }

    long pagedCoordinationEntries() {
        return pagedCoordination.estimatedSize();
    }

    long lastSerializedScanResponseBytes() {
        return lastSerializedScanResponseBytes;
    }

    long maxSerializedScanResponseBytes() {
        return maxSerializedScanResponseBytes;
    }

    long pagedCoordinationRecomputations() {
        return pagedCoordinationRecomputations.get();
    }

    private void cachePagedResponse(CoordinationKey requestKey, byte[] full) {
        pagedCoordination.put(requestKey, full);
    }

    private static int pagedResponseWeight(CoordinationKey key, byte[] response) {
        long weight = 128L + key.content.length + (long) key.uuid.length() * 2 + response.length;
        return (int) Math.min(Integer.MAX_VALUE, weight);
    }

    private static long parsePagedCacheMemory(String configuredMemory) {
        String trimmed = configuredMemory.trim();
        if ("-1".equals(trimmed)) {
            return -1L;
        }
        return MemorySize.parse(trimmed).getBytes();
    }

    public synchronized long latestCommittedIdentifier(String user) {
        return latestCommittedIdentifiers.computeIfAbsent(user, this::computeLatestIdentifier);
    }

    private synchronized long computeLatestIdentifier(String user) {
        Optional<Snapshot> snapshotOptional = table.snapshotManager().latestSnapshotOfUser(user);
        if (!snapshotOptional.isPresent()) {
            return Long.MIN_VALUE;
        }

        Snapshot latestSnapshotOfUser = snapshotOptional.get();
        if (snapshot == null || latestSnapshotOfUser.id() > snapshot.id()) {
            snapshot = latestSnapshotOfUser;
            scan.withSnapshot(snapshot);
        }
        return latestSnapshotOfUser.commitIdentifier();
    }

    public void checkpoint() {
        // refresh latest snapshot for data & index files scan
        refresh();
        // refresh latest committed identifiers for all users
        latestCommittedIdentifiers.clear();
    }

    private static class CoordinationKey {

        private final byte[] content;
        private final String uuid;

        private CoordinationKey(byte[] content, String uuid) {
            this.content = content;
            this.uuid = uuid;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CoordinationKey that = (CoordinationKey) o;
            return Objects.deepEquals(content, that.content) && Objects.equals(uuid, that.uuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(content), uuid);
        }
    }
}
