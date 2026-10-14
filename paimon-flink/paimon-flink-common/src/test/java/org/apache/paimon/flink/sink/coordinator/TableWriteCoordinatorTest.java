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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.utils.InstantiationUtil.serializeObject;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableWriteCoordinatorTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLatestIdentifierAndScan(boolean initSnapshot) throws Exception {
        Identifier identifier = new Identifier("db", "table");
        Schema schema = Schema.newBuilder().column("f0", DataTypes.INT()).build();
        catalog.createDatabase("db", false);
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        // initial with snapshot 1
        if (initSnapshot) {
            write(table, GenericRow.of(1));
        }
        TableWriteCoordinator coordinator = new TableWriteCoordinator(table);

        // latest snapshot get snapshot 2
        write(table, GenericRow.of(1));
        Snapshot latest = table.latestSnapshot().get();
        String commitUser = latest.commitUser();
        coordinator.latestCommittedIdentifier(commitUser);

        // scan should scan snapshot 2
        ScanCoordinationRequest request =
                new ScanCoordinationRequest(serializeBinaryRow(EMPTY_ROW), 0, false, false);
        ScanCoordinationResponse scan = coordinator.scan(request);
        assertThat(scan.snapshot().id()).isEqualTo(latest.id());
        assertThat(scan.extractDataFiles().size()).isEqualTo(initSnapshot ? 2 : 1);
    }

    @Test
    public void testPrefetchManifestsWarmsCache() throws Exception {
        Identifier identifier = new Identifier("db", "table");
        Schema schema =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .option(
                                FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PREFETCH_MANIFESTS
                                        .key(),
                                "true")
                        .build();
        catalog.createDatabase("db", false);
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        write(table, GenericRow.of(1));
        write(table, GenericRow.of(2));

        // Writing might already touch manifest files, so replace the table cache with a fresh one
        // to make this test focus only on coordinator prefetch behavior.
        table.setManifestCache(
                new SegmentsCache<Path>(1024, MemorySize.ofMebiBytes(64), Long.MAX_VALUE));
        assertThat(table.getManifestCache().totalCacheBytes()).isZero();

        // Constructing the coordinator runs refresh() which warms the manifest cache when the
        // prefetch option is enabled
        TableWriteCoordinator coordinator = new TableWriteCoordinator(table);
        assertThat(table.getManifestCache().totalCacheBytes()).isGreaterThan(0);

        // scan results remain correct after warming
        ScanCoordinationRequest request =
                new ScanCoordinationRequest(serializeBinaryRow(EMPTY_ROW), 0, false, false);
        ScanCoordinationResponse scan = coordinator.scan(request);
        assertThat(scan.snapshot().id()).isEqualTo(table.latestSnapshot().get().id());
        assertThat(scan.extractDataFiles().size()).isEqualTo(2);
    }

    @Test
    public void testNoManifestCache() throws Exception {
        Identifier identifier = new Identifier("db", "table");
        catalog.createDatabase("db", false);
        createTable(identifier);
        FileStoreTable table = getTable(identifier);
        table.setManifestCache(null);
        assertThatThrownBy(() -> new TableWriteCoordinator(table))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testPagedResponseCacheReleasedAfterCompletion() throws Exception {
        TableWriteCoordinator coordinator = createPagedCoordinator("1 mb");
        byte[] requestContent = serializeObject(scanRequest());
        PagedCoordinationRequest request =
                new PagedCoordinationRequest(requestContent, "request-1", null);

        PagedCoordinationResponse response = coordinator.scan(request);
        assertThat(response.nextPageToken()).isNotNull();
        assertThat(coordinator.pagedCoordinationEntries()).isEqualTo(1);
        assertThat(coordinator.pagedCoordinationBytes()).isGreaterThan(0);

        while (response.nextPageToken() != null) {
            response =
                    coordinator.scan(
                            new PagedCoordinationRequest(
                                    requestContent, "request-1", response.nextPageToken()));
        }

        assertThat(coordinator.pagedCoordinationEntries()).isZero();
        assertThat(coordinator.pagedCoordinationBytes()).isZero();
        assertThat(coordinator.lastSerializedScanResponseBytes()).isGreaterThan(0);
        assertThat(coordinator.maxSerializedScanResponseBytes())
                .isEqualTo(coordinator.lastSerializedScanResponseBytes());
    }

    @Test
    public void testPagedResponseCacheIsUnboundedByDefault() throws Exception {
        TableWriteCoordinator coordinator = createPagedCoordinator(null);
        byte[] requestContent = serializeObject(scanRequest());

        PagedCoordinationResponse response =
                coordinator.scan(
                        new PagedCoordinationRequest(requestContent, "unbounded-request", null));

        assertThat(response.nextPageToken()).isNotNull();
        assertThat(coordinator.pagedCoordinationEntries()).isEqualTo(1);
        assertThat(coordinator.pagedCoordinationBytes()).isGreaterThan(0);
    }

    @Test
    public void testPagedResponseRebuiltWhenOverMemoryLimit() throws Exception {
        TableWriteCoordinator coordinator = createPagedCoordinator("1 bytes");
        byte[] requestContent = serializeObject(scanRequest());

        PagedCoordinationResponse first =
                coordinator.scan(new PagedCoordinationRequest(requestContent, "request-2", null));
        assertThat(first.nextPageToken()).isNotNull();
        assertThat(coordinator.pagedCoordinationEntries()).isZero();
        assertThat(coordinator.pagedCoordinationBytes()).isZero();

        PagedCoordinationResponse second =
                coordinator.scan(
                        new PagedCoordinationRequest(
                                requestContent, "request-2", first.nextPageToken()));
        assertThat(second.content()).isNotEmpty();
        assertThat(coordinator.pagedCoordinationRecomputations()).isEqualTo(1);
    }

    @Test
    public void testClearPagedResponseCacheInvalidatesRequest() throws Exception {
        TableWriteCoordinator coordinator = createPagedCoordinator("1 mb");
        byte[] requestContent = serializeObject(scanRequest());
        PagedCoordinationResponse first =
                coordinator.scan(new PagedCoordinationRequest(requestContent, "request-3", null));
        assertThat(first.nextPageToken()).isNotNull();

        coordinator.clearPagedCoordination();

        assertThatThrownBy(
                        () ->
                                coordinator.scan(
                                        new PagedCoordinationRequest(
                                                requestContent,
                                                "request-3",
                                                first.nextPageToken())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("expired before completion");
    }

    private TableWriteCoordinator createPagedCoordinator(String cacheMemory) throws Exception {
        Identifier identifier =
                new Identifier(
                        "db",
                        cacheMemory == null
                                ? "paged_default"
                                : "paged_" + cacheMemory.replace(' ', '_'));
        Schema.Builder schemaBuilder =
                Schema.newBuilder()
                        .column("f0", DataTypes.INT())
                        .option(
                                FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PAGE_SIZE.key(),
                                "16 bytes");
        if (cacheMemory != null) {
            schemaBuilder.option(
                    FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PAGED_CACHE_MEMORY.key(),
                    cacheMemory);
        }
        Schema schema = schemaBuilder.build();
        catalog.createDatabase("db", true);
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);
        write(table, GenericRow.of(1));
        return new TableWriteCoordinator(table);
    }

    private ScanCoordinationRequest scanRequest() {
        return new ScanCoordinationRequest(serializeBinaryRow(EMPTY_ROW), 0, false, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEmptyBucketUsesPartitionBucketCount(boolean prefetch) throws Exception {
        // Table default: 32 buckets; partition A was rescaled to 2
        int tableBuckets = 32;
        int partitionBuckets = 2;

        Identifier identifier = new Identifier("db2", "table");
        catalog.createDatabase("db2", false);
        Schema schema =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("pt", "k")
                        .partitionKeys("pt")
                        .option(CoreOptions.BUCKET.key(), String.valueOf(tableBuckets))
                        .option(
                                FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PREFETCH_MANIFESTS
                                        .key(),
                                String.valueOf(prefetch))
                        .build();
        catalog.createTable(identifier, schema, false);
        FileStoreTable table = getTable(identifier);

        // Write to partition A, bucket=0 with totalBuckets=2 (rescaled partition)
        String commitUser = UUID.randomUUID().toString();
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        List<CommitMessage> messages;
        try (StreamTableWrite write = writeBuilder.newWrite()) {
            write.write(GenericRow.of(1, 1, 100), 0);
            messages = write.prepareCommit(false, 0);
        }

        // Override totalBuckets to simulate partition rescale to 2
        CommitMessageImpl original = (CommitMessageImpl) messages.get(0);
        CommitMessageImpl rescaled =
                new CommitMessageImpl(
                        original.partition(),
                        original.bucket(),
                        partitionBuckets,
                        original.newFilesIncrement(),
                        original.compactIncrement());
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            commit.commit(0, Collections.<CommitMessage>singletonList(rescaled));
        }

        // Reload the table and create coordinator
        FileStoreTable freshTable = getTable(identifier);
        TableWriteCoordinator coordinator = new TableWriteCoordinator(freshTable);

        BinaryRow partitionA = partitionRow(1);

        // Scan bucket=0 (has files): totalBuckets should be 2
        ScanCoordinationRequest requestBucket0 =
                new ScanCoordinationRequest(serializeBinaryRow(partitionA), 0, false, false);
        ScanCoordinationResponse responseBucket0 = coordinator.scan(requestBucket0);
        assertThat(responseBucket0.totalBuckets())
                .as("bucket=0 (has files) should use per-partition bucket count 2")
                .isEqualTo(partitionBuckets);
        assertThat(responseBucket0.extractDataFiles()).isNotEmpty();

        // Scan bucket=1 (empty): totalBuckets must be 2, not the table default 32
        ScanCoordinationRequest requestBucket1 =
                new ScanCoordinationRequest(serializeBinaryRow(partitionA), 1, false, false);
        ScanCoordinationResponse responseBucket1 = coordinator.scan(requestBucket1);
        assertThat(responseBucket1.totalBuckets())
                .as(
                        "bucket=1 (empty bucket) must use per-partition bucket count 2, not table default 32")
                .isEqualTo(partitionBuckets);
        assertThat(responseBucket1.extractDataFiles()).isEmpty();

        ScanCoordinationResponse newPartition =
                coordinator.scan(
                        new ScanCoordinationRequest(
                                serializeBinaryRow(partitionRow(2)), 1, false, false));
        assertThat(newPartition.totalBuckets()).isEqualTo(tableBuckets);
        assertThat(newPartition.extractDataFiles()).isEmpty();

        // A checkpoint must invalidate the cached layout after a partition overwrite.
        try (StreamTableWrite write = writeBuilder.newWrite()) {
            write.write(GenericRow.of(1, 2, 200), 0);
            original = (CommitMessageImpl) write.prepareCommit(false, 1).get(0);
        }
        CommitMessageImpl resizedAgain =
                new CommitMessageImpl(
                        original.partition(),
                        original.bucket(),
                        4,
                        original.newFilesIncrement(),
                        original.compactIncrement());
        try (TableCommitImpl commit = table.newCommit(UUID.randomUUID().toString())) {
            commit.withOverwrite(Collections.singletonMap("pt", "1"))
                    .commit(0, Collections.<CommitMessage>singletonList(resizedAgain));
        }
        coordinator.checkpoint();
        assertThat(coordinator.scan(requestBucket1).totalBuckets()).isEqualTo(4);
    }

    private BinaryRow partitionRow(int partitionValue) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, partitionValue);
        writer.complete();
        return row;
    }
}
