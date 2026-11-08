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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.utils.InstantiationUtil.serializeObject;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Manual benchmark for comparing in-memory and disk-spilled coordinator responses. */
class TableWriteCoordinatorSpillMemoryBenchmark extends TableTestBase {

    private static final String PROPERTY_PREFIX = "paimon.coordinator.spill.memory.";

    @Test
    void coldScan() throws Exception {
        boolean spillEnabled = booleanProperty("spill-enabled", false);
        int syntheticManifestMb = intProperty("synthetic-manifest-mb", 100);
        int syntheticEntryBytes = intProperty("synthetic-entry-bytes", 512);
        int manifestParallelism = intProperty("manifest-parallelism", 4);
        java.nio.file.Path spillDirectory = tempPath.resolve("coordinator-spill");

        FileStoreTable table = createTable(spillEnabled, spillDirectory, manifestParallelism);
        write(table, GenericRow.of(1));
        replaceWithSyntheticManifest(table, syntheticManifestMb, syntheticEntryBytes);
        long manifestBytes = currentSnapshotManifestBytes(table);
        int manifestFiles = currentSnapshotManifestFiles(table);
        resetManifestCache(table);

        forceGc();
        long baselineHeap = currentHeap();
        TableWriteCoordinator coordinator = new TableWriteCoordinator(table);
        byte[] requestContent =
                serializeObject(
                        new ScanCoordinationRequest(
                                serializeBinaryRow(EMPTY_ROW), 0, false, false));

        resetPoolPeaks();
        long startNanos = System.nanoTime();
        PagedCoordinationResponse firstPage =
                coordinator.scan(
                        new PagedCoordinationRequest(requestContent, "benchmark-request", null));
        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
        long peakHeap = poolPeakUsed();
        long heapAfterRequest = currentHeap();
        assertThat(firstPage.nextPageToken()).isNotNull();

        long responseBytes = coordinator.lastSerializedScanResponseBytes();
        long responseHeapCacheBytes = coordinator.pagedCoordinationBytes();
        long responseSpillBytes = coordinator.pagedCoordinationSpillBytes();
        long spillFilesBytes = directoryBytes(spillDirectory);
        long manifestCacheBytes = table.getManifestCache().totalCacheBytes();

        forceGc();
        long heapAfterRequestGc = currentHeap();
        coordinator.clearPagedCoordination();
        forceGc();
        long heapAfterClearGc = currentHeap();
        long spillFilesAfterClear = directoryBytes(spillDirectory);

        System.out.printf(
                "SPILL_MEMORY_RESULT spillEnabled=%s manifestBytes=%,d manifestFiles=%d "
                        + "responseBytes=%,d responseHeapCacheBytes=%,d responseSpillBytes=%,d "
                        + "spillFilesBytes=%,d manifestCacheBytes=%,d elapsedMillis=%,d "
                        + "baselineHeap=%,d peakHeap=%,d heapAfterRequest=%,d "
                        + "heapAfterRequestGc=%,d heapAfterClearGc=%,d spillFilesAfterClear=%,d%n",
                spillEnabled,
                manifestBytes,
                manifestFiles,
                responseBytes,
                responseHeapCacheBytes,
                responseSpillBytes,
                spillFilesBytes,
                manifestCacheBytes,
                elapsedMillis,
                baselineHeap,
                peakHeap,
                heapAfterRequest,
                heapAfterRequestGc,
                heapAfterClearGc,
                spillFilesAfterClear);

        assertThat(manifestBytes).isGreaterThan(syntheticManifestMb * 1024L * 1024L);
        assertThat(responseBytes).isPositive();
        assertThat(responseSpillBytes > 0).isEqualTo(spillEnabled);
        assertThat(spillFilesAfterClear).isZero();
    }

    private FileStoreTable createTable(
            boolean spillEnabled, java.nio.file.Path spillDirectory, int manifestParallelism)
            throws Exception {
        Identifier identifier = new Identifier("default", "spill_memory_" + UUID.randomUUID());
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "k")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option(CoreOptions.MANIFEST_TARGET_FILE_SIZE.key(), "512 kb")
                        .option(
                                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(),
                                String.valueOf(manifestParallelism))
                        .option(
                                FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PAGE_SIZE.key(),
                                "4 kb")
                        .option(
                                FlinkConnectorOptions.SINK_WRITER_COORDINATOR_PAGED_CACHE_MEMORY
                                        .key(),
                                "-1")
                        .option(
                                FlinkConnectorOptions.SINK_WRITER_COORDINATOR_SPILL_ENABLED.key(),
                                String.valueOf(spillEnabled))
                        .option(
                                FlinkConnectorOptions.SINK_WRITER_COORDINATOR_SPILL_BUFFER_SIZE
                                        .key(),
                                "256 kb")
                        .option(
                                FlinkConnectorOptions.SINK_WRITER_COORDINATOR_SPILL_DIRECTORY.key(),
                                spillDirectory.toAbsolutePath().toString())
                        .build();
        catalog.createTable(identifier, schema, false);
        return getTable(identifier);
    }

    private static void replaceWithSyntheticManifest(
            FileStoreTable table, int targetManifestMb, int entryPayloadBytes) throws Exception {
        long targetBytes = targetManifestMb * 1024L * 1024L;
        int entryCount = Math.toIntExact((targetBytes + entryPayloadBytes - 1) / entryPayloadBytes);
        Random random = new Random(987654321L);
        List<ManifestEntry> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            byte[] payload = new byte[entryPayloadBytes];
            random.nextBytes(payload);
            DataFileMeta file =
                    DataFileMeta.forAppend(
                            "synthetic-" + i + ".parquet",
                            1024L,
                            1L,
                            SimpleStats.EMPTY_STATS,
                            i,
                            i,
                            table.schema().id(),
                            Collections.emptyList(),
                            payload,
                            null,
                            null,
                            null,
                            null,
                            null);
            entries.add(ManifestEntry.create(FileKind.ADD, EMPTY_ROW, 0, 1, file));
        }

        ManifestFile manifestFile = table.store().manifestFileFactory().create();
        List<ManifestFileMeta> manifests = manifestFile.write(entries);
        ManifestList manifestList = table.store().manifestListFactory().create();
        Pair<String, Long> base = manifestList.write(manifests);
        Pair<String, Long> delta = manifestList.write(Collections.emptyList());
        Snapshot previous = table.latestSnapshot().get();
        Snapshot synthetic =
                new Snapshot(
                        previous.id(),
                        previous.schemaId(),
                        base.getLeft(),
                        base.getRight(),
                        delta.getLeft(),
                        delta.getRight(),
                        null,
                        null,
                        null,
                        previous.commitUser(),
                        previous.commitIdentifier(),
                        previous.commitKind(),
                        previous.timeMillis(),
                        previous.logOffsets(),
                        (long) entryCount,
                        (long) entryCount,
                        0L,
                        previous.watermark(),
                        null,
                        previous.properties(),
                        previous.nextRowId());
        table.snapshotManager()
                .fileIO()
                .overwriteFileUtf8(
                        table.snapshotManager().snapshotPath(previous.id()), synthetic.toJson());
        table.snapshotManager().invalidateCache();
    }

    private static void resetManifestCache(FileStoreTable table) {
        SegmentsCache<Path> original = table.getManifestCache();
        table.setManifestCache(
                SegmentsCache.create(
                        original.pageSize(),
                        original.maxMemorySize(),
                        original.maxElementSize(),
                        original.ttl(),
                        original.softValues()));
    }

    private static long currentSnapshotManifestBytes(FileStoreTable table) {
        return table.store().manifestListFactory().create()
                .readDataManifests(table.latestSnapshot().get()).stream()
                .mapToLong(ManifestFileMeta::fileSize)
                .sum();
    }

    private static int currentSnapshotManifestFiles(FileStoreTable table) {
        return table.store()
                .manifestListFactory()
                .create()
                .readDataManifests(table.latestSnapshot().get())
                .size();
    }

    private static long directoryBytes(java.nio.file.Path directory) throws Exception {
        if (!Files.exists(directory)) {
            return 0;
        }
        try (java.util.stream.Stream<java.nio.file.Path> files = Files.list(directory)) {
            return files.filter(Files::isRegularFile)
                    .mapToLong(
                            path -> {
                                try {
                                    return Files.size(path);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            })
                    .sum();
        }
    }

    private static void forceGc() throws InterruptedException {
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        long previous = Long.MAX_VALUE;
        for (int i = 0; i < 5; i++) {
            System.gc();
            System.runFinalization();
            Thread.sleep(200);
            long used = memory.getHeapMemoryUsage().getUsed();
            if (used >= previous) {
                return;
            }
            previous = used;
        }
    }

    private static long currentHeap() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    private static void resetPoolPeaks() {
        ManagementFactory.getMemoryPoolMXBeans().forEach(MemoryPoolMXBean::resetPeakUsage);
    }

    private static long poolPeakUsed() {
        return ManagementFactory.getMemoryPoolMXBeans().stream()
                .filter(pool -> pool.getType() == MemoryType.HEAP)
                .map(MemoryPoolMXBean::getPeakUsage)
                .filter(usage -> usage != null)
                .mapToLong(MemoryUsage::getUsed)
                .sum();
    }

    private static int intProperty(String name, int defaultValue) {
        return Integer.parseInt(
                System.getProperty(PROPERTY_PREFIX + name, String.valueOf(defaultValue)));
    }

    private static boolean booleanProperty(String name, boolean defaultValue) {
        return Boolean.parseBoolean(
                System.getProperty(PROPERTY_PREFIX + name, String.valueOf(defaultValue)));
    }
}
