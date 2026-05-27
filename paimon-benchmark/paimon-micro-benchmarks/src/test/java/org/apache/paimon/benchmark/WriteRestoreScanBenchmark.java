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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Benchmark for the {@link FileSystemWriteRestore#restoreFiles} hot loop, instrumented to surface
 * the manifest-cache memory spike that writers can pay during cold cache population.
 *
 * <p>Builds a primary-key table with many partitions and a small number of rows per partition, then
 * enumerates every (partition, bucket) pair and invokes {@code restoreFiles} on each — the same
 * call pattern a writer pays during restore. Three arms isolate the contribution of the different
 * manifest-caching layers:
 *
 * <ul>
 *   <li>{@code segmentsCacheDisabled} — no {@code SegmentsCache}, no prefetch. Cold disk reads
 *       every iteration; upper bound.
 *   <li>{@code segmentsCacheEnabled} — catalog cache on, prefetch off. Each {@code restoreFiles}
 *       call goes through {@code ManifestFile.read} which consults {@code SegmentsCache}. With
 *       per-iteration cache resets (see below) every measured iteration pays cold-populate cost.
 *   <li>{@code prefetchEnabled} — prefetch on. Each iteration pays a single bulk scan plus N
 *       in-memory filters. With per-iteration cache resets the static {@code
 *       prefetchedManifestEntriesCache} is invalidated between iterations so we measure
 *       cold-prefetch cost, not steady-state reuse.
 * </ul>
 *
 * <p>Spike-reproduction characteristics, applied uniformly to all arms:
 *
 * <ul>
 *   <li>Caches that are in play (per the test's options) are <b>always</b> reset between
 *       iterations. See {@link #resetCachesForIteration(FileStoreTable)} — the decision is derived
 *       from {@code fst.getManifestCache() != null} and {@code
 *       fst.coreOptions().prefetchManifestEntries()}, so no extra config flags are carried.
 *   <li>Restore is driven by an {@link ExecutorService} with {@link #NUM_RESTORE_THREADS} threads,
 *       each holding its own {@link FileSystemWriteRestore}. This is required because {@code
 *       AbstractFileStoreScan} is stateful, and it matches a real Flink TM packed with multiple
 *       writer subtasks restoring concurrently.
 *   <li>Manifests are intentionally fragmented (small commit batches) and rows are widened (many
 *       columns × bigger values) to make per-manifest stats sizes realistic.
 *   <li>Per iteration we sample heap peak via {@link MemoryPoolMXBean#getPeakUsage()} and
 *       post-{@code System.gc()} steady-state via {@link
 *       java.lang.management.MemoryMXBean#getHeapMemoryUsage()}, and print the full {@code Manifest
 *       cache footprint} block — including {@code Peak/Steady} (the "spike multiplier") — at the
 *       end of each iteration. A one-line aggregate over the measured iterations is printed after
 *       the benchmark completes.
 * </ul>
 */
public class WriteRestoreScanBenchmark extends TableBenchmark {

    /**
     * Default parallelism for the restore worker pool. Bumping this approximates packing more Flink
     * writer subtasks onto a single TM.
     */
    private static final int NUM_RESTORE_THREADS = 8;

    /** All tunables for one benchmark run. Defaults match the pre-spike-knobs version. */
    private static final class BenchParams {
        int numPartitions = 1_000;
        int rowsPerPartition = 16;
        int numBuckets = 4;

        /** Smaller -> more, smaller manifest files (fragmentation). */
        int commitBatchPartitions = 10;

        /** Number of value columns; widens DataFileMeta stats per manifest entry. */
        int valueCount = 10;

        /** Length of each random hex value string; widens per-stat min/max blobs. */
        int valueCharCount = 64;

        /** Parallelism for the restore worker pool. */
        int numRestoreThreads = NUM_RESTORE_THREADS;

        int numWarmupIters = 1;
        int numMeasuredIters = 3;
    }

    @Test
    public void testRestoreFiles_segmentsCacheDisabled() throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.CACHE_ENABLED, false);
        Options tableOptions = new Options();
        tableOptions.set(CoreOptions.MANIFEST_PREFETCH_ENTRIES, false);

        BenchParams p = new BenchParams();
        innerTest("segmentsCacheDisabled", catalogOptions, tableOptions, p);
        /*
         * HISTORICAL REFERENCE (pre-spike-knobs values; current runs will differ):
         *
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

        BenchParams p = new BenchParams();
        innerTest("segmentsCacheEnabled", catalogOptions, tableOptions, p);
        /*
         * HISTORICAL REFERENCE (pre-spike-knobs values; the new arm pays cold-populate cost every
         * iteration, so the new numbers will be substantially higher):
         *
         * OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
         * Apple M4 Pro
         * Populated table: 1,000 partitions x 16 rows, bucket=4 -> 4,000 (partition, bucket) pairs
         * segmentsCacheEnabled:    Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * ----------------------------------------------------------------------------------------
         * restore                     584 /  601             6.9           145985.1       1.0X
         *
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
         */
    }

    @Test
    public void testRestoreFiles_prefetchEnabled() throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.CACHE_ENABLED, false);
        Options tableOptions = new Options();
        tableOptions.set(CoreOptions.MANIFEST_PREFETCH_ENTRIES, true);

        BenchParams p = new BenchParams();
        innerTest("prefetchEnabled", catalogOptions, tableOptions, p);
        /*
         * HISTORICAL REFERENCE (pre-spike-knobs values; current runs will differ):
         *
         * OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
         * Apple M4 Pro
         * Populated table: 1,000 partitions x 16 rows, bucket=4 -> 4,000 (partition, bucket) pairs
         * prefetchEnabled:         Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * ----------------------------------------------------------------------------------------
         * restore                     619 /  627             6.5           154853.5       1.0X
         *
         * Populated table: 5,000 partitions x 16 rows, bucket=4 -> 4,000 (partition, bucket) pairs
         * OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
         * Apple M4 Pro
         * prefetchEnabled:                         Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
         * --------------------------------------------------------------------------------------------------------
         * OPERATORTEST_prefetchEnabled_restore         9714 / 10265              2.1         485704.0       1.0X
         */
    }

    private void innerTest(String name, Options catalogOptions, Options tableOptions, BenchParams p)
            throws Exception {
        Table table = createPartitionedTable(catalogOptions, tableOptions, "T", p);
        populateTable(table, p);

        FileStoreTable fst = (FileStoreTable) table;
        List<BucketEntry> bucketEntries = fst.newSnapshotReader().bucketEntries();
        System.out.println(
                "Populated table has "
                        + bucketEntries.size()
                        + " (partition, bucket) pairs across "
                        + p.numPartitions
                        + " partitions ("
                        + p.numRestoreThreads
                        + " restore threads, "
                        + p.valueCount
                        + "x value cols, "
                        + p.valueCharCount
                        + "-char values, commit batch="
                        + p.commitBatchPartitions
                        + ").");

        long valuesPerIteration = bucketEntries.size();
        ExecutorService executor = Executors.newFixedThreadPool(p.numRestoreThreads);
        AtomicInteger iterCounter = new AtomicInteger(0);
        List<long[]> perIterHeap = new ArrayList<>(p.numWarmupIters + p.numMeasuredIters);

        try {
            Benchmark benchmark =
                    new Benchmark(name, valuesPerIteration)
                            .setNumWarmupIters(p.numWarmupIters)
                            .setOutputPerIteration(true);
            benchmark.addCase(
                    "restore",
                    p.numMeasuredIters,
                    () -> {
                        int iter = iterCounter.getAndIncrement();
                        String iterLabel =
                                iter < p.numWarmupIters
                                        ? "warmup-" + iter
                                        : "iter-" + (iter - p.numWarmupIters);

                        try {
                            resetCachesForIteration(fst);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                        // Fresh ThreadLocal each iteration so the first worker access constructs a
                        // fresh FileSystemWriteRestore + scan that observes the just-reset cache.
                        // (AbstractFileStoreScan is stateful, so one FSWR per thread is required.)
                        ThreadLocal<FileSystemWriteRestore> threadLocalRestore =
                                ThreadLocal.withInitial(
                                        () ->
                                                new FileSystemWriteRestore(
                                                        fst.coreOptions(),
                                                        fst.snapshotManager(),
                                                        fst.store().newScan(),
                                                        fst.store().newIndexFileHandler()));

                        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
                            if (pool.getType() == MemoryType.HEAP) {
                                pool.resetPeakUsage();
                            }
                        }

                        List<Future<?>> futures = new ArrayList<>(bucketEntries.size());
                        for (BucketEntry entry : bucketEntries) {
                            futures.add(
                                    executor.submit(
                                            () ->
                                                    threadLocalRestore
                                                            .get()
                                                            .restoreFiles(
                                                                    entry.partition(),
                                                                    entry.bucket(),
                                                                    false,
                                                                    false)));
                        }
                        for (Future<?> f : futures) {
                            try {
                                f.get();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }

                        long peak = 0;
                        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
                            if (pool.getType() == MemoryType.HEAP) {
                                peak += pool.getPeakUsage().getUsed();
                            }
                        }
                        System.gc();
                        System.runFinalization();
                        System.gc();
                        long steady =
                                ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
                        perIterHeap.add(new long[] {peak, steady});

                        try {
                            printCacheFootprint(name + " " + iterLabel, fst, peak, steady);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            benchmark.run();
        } finally {
            executor.shutdownNow();
        }

        printAggregateHeap(name, p, perIterHeap);
    }

    private Table createPartitionedTable(
            Options catalogOptions, Options tableOptions, String tableName, BenchParams p)
            throws Exception {
        catalogOptions.set(CatalogOptions.WAREHOUSE, tempFile.toUri().toString());
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(catalogOptions));
        String database = "default";
        catalog.createDatabase(database, true);

        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "pt", new IntType()));
        fields.add(new DataField(1, "k", new IntType()));
        for (int i = 0; i < p.valueCount; i++) {
            fields.add(new DataField(2 + i, "f" + i, DataTypes.STRING()));
        }

        tableOptions.set(CoreOptions.BUCKET, p.numBuckets);
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

    private void populateTable(Table table, BenchParams p) throws Exception {
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        RandomDataGenerator random = new RandomDataGenerator();
        for (int batchStart = 0;
                batchStart < p.numPartitions;
                batchStart += p.commitBatchPartitions) {
            int batchEnd = Math.min(batchStart + p.commitBatchPartitions, p.numPartitions);
            try (BatchTableWrite write = writeBuilder.newWrite();
                    BatchTableCommit commit = writeBuilder.newCommit()) {
                for (int pt = batchStart; pt < batchEnd; pt++) {
                    for (int k = 0; k < p.rowsPerPartition; k++) {
                        write.write(makeRow(pt, k, random, p));
                    }
                }
                commit.commit(write.prepareCommit());
            }
        }
    }

    private InternalRow makeRow(int pt, int k, RandomDataGenerator random, BenchParams p) {
        GenericRow row = new GenericRow(2 + p.valueCount);
        row.setField(0, pt);
        row.setField(1, k);
        for (int i = 0; i < p.valueCount; i++) {
            row.setField(2 + i, BinaryString.fromString(random.nextHexString(p.valueCharCount)));
        }
        return row;
    }

    /**
     * Invalidate {@link FileSystemWriteRestore}'s private static {@code
     * prefetchedManifestEntriesCache} via reflection. Used by {@link
     * #resetCachesForIteration(FileStoreTable)} when the table opts into prefetch.
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
     * Reset whichever manifest caches are in play for this table, derived from the options the test
     * already configured. Always called at the start of every iteration so each measured iteration
     * pays the cold-populate cost (the production-onset condition we're trying to reproduce).
     *
     * <ul>
     *   <li>SegmentsCache (per-table, attached by {@code CachingCatalog.putTableCache} when {@code
     *       CACHE_ENABLED=true}): if attached, replace with a fresh instance preserving {@code
     *       maxMemorySize} / {@code maxElementSize}. Replacing (rather than {@code
     *       invalidateAll()}-ing) sidesteps Caffeine's asynchronous eviction so the cold state is
     *       deterministic.
     *   <li>Prefetch cache (static in {@link FileSystemWriteRestore}, populated when {@code
     *       CoreOptions.MANIFEST_PREFETCH_ENTRIES} is true): invalidate all entries.
     * </ul>
     *
     * <p>Both branches are no-ops when the corresponding cache isn't enabled, so this single call
     * site works for all benchmark arms — no per-test config flags required.
     */
    private static void resetCachesForIteration(FileStoreTable fst) throws Exception {
        SegmentsCache<Path> original = fst.getManifestCache();
        if (original != null) {
            fst.setManifestCache(
                    SegmentsCache.create(original.maxMemorySize(), original.maxElementSize()));
        }
        if (fst.coreOptions().prefetchManifestEntries()) {
            clearStaticPrefetchCache();
        }
    }

    /**
     * Print a per-iteration footprint summary: total manifest directory bytes on disk (split by
     * file-name prefix), the table's {@link SegmentsCache} accounting bytes, the JOL deep retained
     * size of the prefetched manifest entries for this table, the just-sampled heap peak and
     * post-GC steady-state, and memory-to-disk plus {@code Peak/Steady} (spike multiplier) ratios.
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
     *   <li>Peak is per-pool sum: {@code MemoryPoolMXBean.getPeakUsage()} is per-pool and peaks
     *       don't necessarily coincide across pools; summing slightly overcounts. Accurate enough
     *       for order-of-magnitude spike comparison.
     *   <li>The prefetch reading uses {@link #lookupPrefetchedForTable} which filters by this
     *       table's path — the static cache may contain entries from prior {@code @Test} methods in
     *       the same JVM.
     * </ul>
     */
    private void printCacheFootprint(String label, FileStoreTable fst, long peak, long steady)
            throws Exception {
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
        System.out.println("Manifest cache footprint (" + label + "):");
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
        System.out.printf("  Heap          peak=%,d bytes, steady=%,d bytes%n", peak, steady);
        if (diskTotal > 0) {
            System.out.printf(
                    "  Ratios        SegmentsCache/disk=%s, Prefetch/disk=%s, Peak/disk=%s, Steady/disk=%s, Peak/Steady=%s%n",
                    ratio(segmentsCacheBytes, diskTotal),
                    ratio(prefetchBytes, diskTotal),
                    ratio(peak, diskTotal),
                    ratio(steady, diskTotal),
                    ratio(peak, steady));
        }
        System.out.println();
    }

    /**
     * Print a one-block aggregate over the <b>measured</b> iterations (the warmup iterations are
     * skipped). The {@code Peak/Steady} max is the worst-case "spike multiplier" observed during
     * this run.
     */
    private void printAggregateHeap(String name, BenchParams p, List<long[]> perIterHeap) {
        int start = p.numWarmupIters;
        int end = perIterHeap.size();
        if (start >= end) {
            return;
        }

        long peakSum = 0;
        long peakMin = Long.MAX_VALUE;
        long peakMax = Long.MIN_VALUE;
        long steadySum = 0;
        long steadyMin = Long.MAX_VALUE;
        long steadyMax = Long.MIN_VALUE;
        double ratioSum = 0;
        double ratioMax = 0;
        int n = end - start;
        for (int i = start; i < end; i++) {
            long peak = perIterHeap.get(i)[0];
            long steady = perIterHeap.get(i)[1];
            peakSum += peak;
            peakMin = Math.min(peakMin, peak);
            peakMax = Math.max(peakMax, peak);
            steadySum += steady;
            steadyMin = Math.min(steadyMin, steady);
            steadyMax = Math.max(steadyMax, steady);
            double r = steady == 0 ? 0 : (double) peak / steady;
            ratioSum += r;
            ratioMax = Math.max(ratioMax, r);
        }

        System.out.println();
        System.out.println(
                "Manifest cache footprint aggregate (" + name + ", " + n + " measured iters):");
        System.out.printf(
                "  Heap          peak   avg=%,d bytes, min=%,d, max=%,d%n",
                peakSum / n, peakMin, peakMax);
        System.out.printf(
                "  Heap          steady avg=%,d bytes, min=%,d, max=%,d%n",
                steadySum / n, steadyMin, steadyMax);
        System.out.printf(
                "  Heap          peak/steady avg=%.2fx (max spike multiplier=%.2fx)%n",
                ratioSum / n, ratioMax);
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

    private static String ratio(Long num, long denom) {
        if (num == null || denom == 0) {
            return "n/a";
        }
        return String.format("%.2fx", (double) num / denom);
    }

    private static String ratio(long num, long denom) {
        if (denom == 0) {
            return "n/a";
        }
        return String.format("%.2fx", (double) num / denom);
    }
}
