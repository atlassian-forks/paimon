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

package org.apache.paimon.benchmark;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.operation.FileSystemWriteRestore;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SegmentsCache;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.GraphLayout;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Benchmark for the {@link FileSystemWriteRestore#restoreFiles} hot loop.
 *
 * <p>Builds a primary-key table with many partitions and a small number of rows per partition, then
 * enumerates every (partition, bucket) pair and invokes {@code restoreFiles} on each — the same
 * call pattern a writer pays during restore. Three arms isolate the contribution of the different
 * manifest-caching layers:
 *
 * <ul>
 *   <li>{@code segmentsCacheDisabled} — no {@code SegmentsCache}, no prefetch. Cold disk reads
 *       every iteration; upper bound.
 *   <li>{@code segmentsCacheEnabled} — default catalog cache on, prefetch off. Each {@code
 *       restoreFiles} call goes through {@code ManifestFile.read} which consults {@code
 *       SegmentsCache}; warm after the first iteration.
 *   <li>{@code prefetchEnabled} — prefetch on. Each iteration pays a single bulk scan plus N
 *       in-memory filters. The static {@code prefetchedManifestEntriesCache} is invalidated between
 *       iterations via reflection so we measure cold-prefetch cost, not steady-state reuse.
 * </ul>
 *
 * <p>After the latency table, each arm also prints a {@code Manifest cache footprint} block with:
 * total manifest directory bytes on disk (split by {@code manifest-list-} / {@code manifest-} /
 * {@code index-manifest-} prefixes), {@link SegmentsCache#totalCacheBytes()} for the table's
 * manifest cache (when attached), and the JOL {@link GraphLayout} deep retained size of the static
 * {@code prefetchedManifestEntriesCache} entry for this table (when prefetch is on). The block also
 * prints SegmentsCache/disk and Prefetch/disk ratios so the in-memory expansion of a manifest
 * relative to its on-disk footprint is directly comparable across arms.
 */
public class WriteRestoreScanBenchmark extends TableBenchmark {

    private static final int NUM_PARTITIONS = 5_000;
    private static final int ROWS_PER_PARTITION = 16;
    private static final int NUM_BUCKETS = 4;
    private static final int COMMIT_BATCH_PARTITIONS = 100;
    private static final int VALUE_COUNT = 5;

    private static final int NUM_WARMUP_ITERS = 1;
    private static final int NUM_MEASURED_ITERS = 3;

    @Test
    public void testRestoreFiles_segmentsCacheDisabled() throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.CACHE_ENABLED, false);
        Options tableOptions = new Options();
        tableOptions.set(CoreOptions.MANIFEST_PREFETCH_ENTRIES, false);
        innerTest("segmentsCacheDisabled", catalogOptions, tableOptions, false);
        /*
         * OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
         * Apple M4 Pro
         * Populated table: 1,000 partitions x 16 rows, bucket=4 -> 4,000 (partition, bucket) pairs
         * segmentsCacheDisabled:   Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * ----------------------------------------------------------------------------------------
         * restore                   12543 / 12605             0.3          3135816.3       1.0X
         *
         * Populated table: 5,000 partitions x 16 rows, bucket=4 -> 4,000 (partition, bucket) pairs
         * OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
         * Apple M4 Pro
         * segmentsCacheDisabled:                       Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * ------------------------------------------------------------------------------------------------------------
         * OPERATORTEST_segmentsCacheDisabled_restore    174845 / 177212              0.1        8742263.3       1.0X
         */
    }

    @Test
    public void testRestoreFiles_segmentsCacheEnabled() throws Exception {
        Options catalogOptions = new Options();
        Options tableOptions = new Options();
        tableOptions.set(CoreOptions.MANIFEST_PREFETCH_ENTRIES, false);
        catalogOptions.set(
                CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY, MemorySize.ofMebiBytes(2048));
        catalogOptions.set(CatalogOptions.CACHE_MANIFEST_MAX_MEMORY, MemorySize.ofMebiBytes(4096));
        innerTest("segmentsCacheEnabled", catalogOptions, tableOptions, false);
        /*
         * Populated table: 5,000 partitions x 16 rows, bucket=4 -> 4,000 (partition, bucket) pairs, default memory
         * OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
         * Apple M4 Pro
         * segmentsCacheEnabled:                         Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * -------------------------------------------------------------------------------------------------------------
         * OPERATORTEST_segmentsCacheEnabled_restore      142279 / 142700              0.1        7113951.4       1.0X
         *
         * Populated table: 5,000 partitions x 16 rows, bucket=4 -> 4,000 (partition, bucket) pairs, cache memory 2GB/4GB
         * OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
         * Apple M4 Pro
         * segmentsCacheEnabled:                       Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * -----------------------------------------------------------------------------------------------------------
         * OPERATORTEST_segmentsCacheEnabled_restore        2967 / 3001              6.7         148341.9       1.0X
         *
         * Manifest cache footprint (segmentsCacheEnabled):
         *  Disk          manifests=2,142,579 bytes (21 files), manifest-lists=24,755 bytes (20 files), index-manifests=0 bytes (0 files); total=2,167,334 bytes
         *  Segmen *tsCache bytes=33,158,618 (estimatedSize=151, maxMemory=4 gb, maxElementSize=9223372036854775807)
         *  Prefet *ch n/a (prefetch disabled or prefetchedManifestEntriesCache empty for this table)
         *  Ratios *        SegmentsCache/disk=15.30x, Prefetch/disk=n/a
         */
    }

    @Test
    public void testRestoreFiles_prefetchEnabled() throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.CACHE_ENABLED, false);
        Options tableOptions = new Options();
        tableOptions.set(CoreOptions.MANIFEST_PREFETCH_ENTRIES, true);
        innerTest("prefetchEnabled", catalogOptions, tableOptions, true);
        /*
         * Populated table: 5,000 partitions x 16 rows, bucket=4 -> 4,000 (partition, bucket) pairs
         * OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
         * Apple M4 Pro
         * prefetchEnabled:                         Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * --------------------------------------------------------------------------------------------------------
         * OPERATORTEST_prefetchEnabled_restore         9714 / 10265              2.1         485704.0       1.0X
         *
         * Manifest cache footprint (prefetchEnabled):
         * Disk          manifests=2,108,133 bytes (21 files), manifest-lists=24,796 bytes (20 files), index-manifests=0 bytes (0 files); total=2,132,929 bytes
         * SegmentsCache n/a (no manifest cache attached to table — cache disabled)
         * Prefetch bytes=53,833,096 (entries=20000, deep-size via JOL GraphLayout)
         * Ratios        SegmentsCache/disk=n/a, Prefetch/disk=25.24x
         */
    }

    private void innerTest(
            String name,
            Options catalogOptions,
            Options tableOptions,
            boolean clearStaticPrefetchCacheBetweenIters)
            throws Exception {
        Table table = createPartitionedTable(catalogOptions, tableOptions, "T");
        populateTable(table);

        FileStoreTable fst = (FileStoreTable) table;
        List<BucketEntry> bucketEntries = fst.newSnapshotReader().bucketEntries();
        System.out.println(
                "Populated table has "
                        + bucketEntries.size()
                        + " (partition, bucket) pairs across "
                        + NUM_PARTITIONS
                        + " partitions.");

        long valuesPerIteration = bucketEntries.size();
        Benchmark benchmark =
                new Benchmark(name, valuesPerIteration)
                        .setNumWarmupIters(NUM_WARMUP_ITERS)
                        .setOutputPerIteration(true);
        benchmark.addCase(
                "restore",
                NUM_MEASURED_ITERS,
                () -> {
                    if (clearStaticPrefetchCacheBetweenIters) {
                        try {
                            clearStaticPrefetchCache();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    FileSystemWriteRestore restore =
                            new FileSystemWriteRestore(
                                    fst.coreOptions(),
                                    fst.snapshotManager(),
                                    fst.store().newScan(),
                                    fst.store().newIndexFileHandler());
                    for (BucketEntry entry : bucketEntries) {
                        restore.restoreFiles(entry.partition(), entry.bucket(), false, false);
                    }
                });
        benchmark.run();

        printCacheFootprint(name, fst);
    }

    private Table createPartitionedTable(
            Options catalogOptions, Options tableOptions, String tableName) throws Exception {
        catalogOptions.set(CatalogOptions.WAREHOUSE, tempFile.toUri().toString());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        String database = "default";
        catalog.createDatabase(database, true);

        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "pt", new IntType()));
        fields.add(new DataField(1, "k", new IntType()));
        for (int i = 0; i < VALUE_COUNT; i++) {
            fields.add(new DataField(2 + i, "f" + i, DataTypes.STRING()));
        }

        tableOptions.set(CoreOptions.BUCKET, NUM_BUCKETS);
        tableOptions.set(CoreOptions.WRITE_ONLY, false);
        tableOptions.set(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX, 10);

        // Primary keys must include all partition keys, so PK = (pt, k).
        Schema schema =
                new Schema(
                        fields,
                        Collections.singletonList("pt"),
                        Arrays.asList("pt", "k"),
                        tableOptions.toMap(),
                        "");
        Identifier identifier = Identifier.create(database, tableName);
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    private void populateTable(Table table) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        RandomDataGenerator random = new RandomDataGenerator();
        for (int batchStart = 0;
                batchStart < NUM_PARTITIONS;
                batchStart += COMMIT_BATCH_PARTITIONS) {
            int batchEnd = Math.min(batchStart + COMMIT_BATCH_PARTITIONS, NUM_PARTITIONS);
            try (BatchTableWrite write = writeBuilder.newWrite();
                    BatchTableCommit commit = writeBuilder.newCommit()) {
                for (int pt = batchStart; pt < batchEnd; pt++) {
                    for (int k = 0; k < ROWS_PER_PARTITION; k++) {
                        write.write(makeRow(pt, k, random));
                    }
                }
                commit.commit(write.prepareCommit());
            }
        }
    }

    private InternalRow makeRow(int pt, int k, RandomDataGenerator random) {
        GenericRow row = new GenericRow(2 + VALUE_COUNT);
        row.setField(0, pt);
        row.setField(1, k);
        for (int i = 0; i < VALUE_COUNT; i++) {
            row.setField(2 + i, BinaryString.fromString(random.nextHexString(10)));
        }
        return row;
    }

    /**
     * Invalidate {@link FileSystemWriteRestore}'s private static {@code
     * prefetchedManifestEntriesCache} via reflection. Without this, every iteration of the prefetch
     * arm after the first would just re-filter the cached entries — a real production steady-state,
     * but it would make the 3-iteration Best/Avg block misleading. Clearing per iteration measures
     * the cold-prefetch cost (one bulk scan + N filters) the way the writer pays it on a
     * freshly-scheduled task.
     */
    private static void clearStaticPrefetchCache() throws Exception {
        Field f = FileSystemWriteRestore.class.getDeclaredField("prefetchedManifestEntriesCache");
        f.setAccessible(true);
        Cache<?, ?> c = (Cache<?, ?>) f.get(null);
        if (c != null) {
            c.invalidateAll();
        }
    }

    /**
     * Print a compact footprint summary for one benchmark arm: total manifest directory bytes on
     * disk (split by file-name prefix), the table's {@link SegmentsCache} accounting bytes, and the
     * JOL deep retained size of the prefetched manifest entries for this table — plus
     * memory-to-disk ratios so each arm's in-memory expansion is directly comparable.
     *
     * <p>Caveats:
     *
     * <ul>
     *   <li>{@link SegmentsCache#totalCacheBytes()} walks {@code cache.asMap()} and re-applies the
     *       weigher per entry — it's an O(N) snapshot, fine here but not a free read.
     *   <li>{@link GraphLayout#parseInstance} walks the entire reachable object graph from the
     *       {@code PrefetchedManifestEntries} root, so it includes everything transitively retained
     *       (snapshot, partitionType, every {@code BinaryRow} partition, the {@code
     *       PartitionBucketMapping}, etc.). That's the right number for "what's actually held in
     *       heap" but it is not what a Caffeine weigher would report.
     *   <li>The prefetch reading uses {@link #lookupPrefetchedForTable} which filters by this
     *       table's path — the static cache may contain entries from prior {@code @Test} methods in
     *       the same JVM.
     * </ul>
     */
    private void printCacheFootprint(String name, FileStoreTable fst) throws Exception {
        Path manifestDir = new Path(fst.snapshotManager().tablePath(), "manifest");
        FileStatus[] statuses = fst.snapshotManager().fileIO().listStatus(manifestDir);
        long manifestBytes = 0;
        long manifestListBytes = 0;
        long indexManifestBytes = 0;
        int manifestCount = 0;
        int manifestListCount = 0;
        int indexManifestCount = 0;
        for (FileStatus s : statuses) {
            String fileName = s.getPath().getName();
            // INDEX_MANIFEST_PREFIX and MANIFEST_LIST_PREFIX both start with "manifest-",
            // so the more specific prefixes must be checked first.
            if (fileName.startsWith(FileStorePathFactory.INDEX_MANIFEST_PREFIX)) {
                indexManifestBytes += s.getLen();
                indexManifestCount++;
            } else if (fileName.startsWith(FileStorePathFactory.MANIFEST_LIST_PREFIX)) {
                manifestListBytes += s.getLen();
                manifestListCount++;
            } else if (fileName.startsWith(FileStorePathFactory.MANIFEST_PREFIX)) {
                manifestBytes += s.getLen();
                manifestCount++;
            }
        }
        long diskTotal = manifestBytes + manifestListBytes + indexManifestBytes;

        SegmentsCache<Path> sc = fst.getManifestCache();
        Long segmentsCacheBytes = sc == null ? null : sc.totalCacheBytes();
        String segmentsCacheLine;
        if (sc == null) {
            segmentsCacheLine =
                    "SegmentsCache n/a (no manifest cache attached to table — cache disabled)";
        } else {
            segmentsCacheLine =
                    String.format(
                            "SegmentsCache bytes=%,d (estimatedSize=%d, maxMemory=%s, maxElementSize=%d)",
                            segmentsCacheBytes,
                            sc.estimatedSize(),
                            sc.maxMemorySize(),
                            sc.maxElementSize());
        }

        String tableKey = fst.snapshotManager().tablePath().toString();
        FileSystemWriteRestore.PrefetchedManifestEntries prefetched =
                lookupPrefetchedForTable(tableKey);
        Long prefetchBytes = null;
        String prefetchLine;
        if (prefetched == null) {
            prefetchLine =
                    "Prefetch n/a (prefetch disabled or prefetchedManifestEntriesCache empty for this table)";
        } else {
            prefetchBytes = GraphLayout.parseInstance(prefetched).totalSize();
            prefetchLine =
                    String.format(
                            "Prefetch bytes=%,d (entries=%d, deep-size via JOL GraphLayout)",
                            prefetchBytes, prefetched.manifestEntries().size());
        }

        System.out.println();
        System.out.println("Manifest cache footprint (" + name + "):");
        System.out.printf(
                "  Disk          manifests=%,d bytes (%d files), manifest-lists=%,d bytes (%d files), index-manifests=%,d bytes (%d files); total=%,d bytes%n",
                manifestBytes,
                manifestCount,
                manifestListBytes,
                manifestListCount,
                indexManifestBytes,
                indexManifestCount,
                diskTotal);
        System.out.println("  " + segmentsCacheLine);
        System.out.println("  " + prefetchLine);
        if (diskTotal > 0) {
            String scRatio =
                    segmentsCacheBytes == null
                            ? "n/a"
                            : String.format("%.2fx", (double) segmentsCacheBytes / diskTotal);
            String pfRatio =
                    prefetchBytes == null
                            ? "n/a"
                            : String.format("%.2fx", (double) prefetchBytes / diskTotal);
            System.out.printf(
                    "  Ratios        SegmentsCache/disk=%s, Prefetch/disk=%s%n", scRatio, pfRatio);
        }
        System.out.println();
    }

    /**
     * Reflectively read {@link FileSystemWriteRestore}'s private static {@code
     * prefetchedManifestEntriesCache} and return the entry keyed by {@code tableKey}, or {@code
     * null} if absent. The static cache is shared across {@code @Test} methods in the same JVM, so
     * filtering by this table's path is required.
     */
    private static FileSystemWriteRestore.PrefetchedManifestEntries lookupPrefetchedForTable(
            String tableKey) throws Exception {
        Field f = FileSystemWriteRestore.class.getDeclaredField("prefetchedManifestEntriesCache");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        Cache<String, FileSystemWriteRestore.PrefetchedManifestEntries> c =
                (Cache<String, FileSystemWriteRestore.PrefetchedManifestEntries>) f.get(null);
        if (c == null) {
            return null;
        }
        return c.asMap().get(tableKey);
    }
}
