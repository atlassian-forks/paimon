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
 *   <li>Per iteration we sample heap usage before the restore task without forcing GC, heap peak
 *       via {@link MemoryPoolMXBean#getPeakUsage()} after the restore task, and post-{@code
 *       System.gc()} heap usage via {@link java.lang.management.MemoryMXBean#getHeapMemoryUsage()}.
 *       The full {@code Manifest cache footprint} block — including {@code Peak/After-GC} (the
 *       "spike multiplier") — is printed at the end of each iteration. A one-line aggregate over
 *       the measured iterations is printed after the benchmark completes.
 * </ul>
 */
public class WriteRestoreScanBenchmark extends TableBenchmark {

    /**
     * Default parallelism for the restore worker pool. Bumping this approximates packing more Flink
     * writer subtasks onto a single TM.
     */
    private static final int NUM_RESTORE_THREADS = 4;

    /** All tunables for one benchmark run. Defaults match the pre-spike-knobs version. */
    private static final class BenchParams {
        int numPartitions = 2_000;
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

    /**
     * All numbers captured during a single iteration's footprint print. The aggregate at the end of
     * {@link #innerTest} consumes one of these per iteration so it can report SegmentsCache,
     * Prefetch and Heap dimensions side by side.
     */
    private static final class FootprintSample {
        // Disk fields are derived from the manifest directory listing and are constant across
        // iterations (the table is populated once). Captured per-iteration anyway so the aggregate
        // is self-contained.
        final long manifestBytes;
        final int manifestCount;
        final long manifestListBytes;
        final int manifestListCount;
        final long indexManifestBytes;
        final int indexManifestCount;
        final long diskTotal;

        /** {@code null} when no {@link SegmentsCache} is attached to the table. */
        final Long segmentsCacheBytes;

        /** {@code null} when prefetch is disabled or has no entry for this table. */
        final Long prefetchBytes;

        /** {@code null} when prefetch is disabled or has no entry for this table. */
        final Integer prefetchEntries;

        final long beforeHeap;
        final long peakHeap;
        final long afterGcHeap;

        FootprintSample(
                long manifestBytes,
                int manifestCount,
                long manifestListBytes,
                int manifestListCount,
                long indexManifestBytes,
                int indexManifestCount,
                Long segmentsCacheBytes,
                Long prefetchBytes,
                Integer prefetchEntries,
                long beforeHeap,
                long peakHeap,
                long afterGcHeap) {
            this.manifestBytes = manifestBytes;
            this.manifestCount = manifestCount;
            this.manifestListBytes = manifestListBytes;
            this.manifestListCount = manifestListCount;
            this.indexManifestBytes = indexManifestBytes;
            this.indexManifestCount = indexManifestCount;
            this.diskTotal = manifestBytes + manifestListBytes + indexManifestBytes;
            this.segmentsCacheBytes = segmentsCacheBytes;
            this.prefetchBytes = prefetchBytes;
            this.prefetchEntries = prefetchEntries;
            this.beforeHeap = beforeHeap;
            this.peakHeap = peakHeap;
            this.afterGcHeap = afterGcHeap;
        }
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
        Populated table has 8000 (partition, bucket) pairs across 2000 partitions (4 restore threads, 10x value cols, 64-char values, commit batch=10).

        OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
        Apple M4 Pro
        segmentsCacheDisabled:                                                                               Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------
        OPERATORTEST_segmentsCacheDisabled_restore                                                              18915 / 19044              0.4        2364354.1       1.0X

        Manifest cache footprint aggregate (segmentsCacheDisabled, 3 measured iters):
          Disk          manifests=1,707,536 bytes (26 files), manifest-lists=25,915 bytes (20 files), index-manifests=0 bytes (0 files); total=1,733,451 bytes
          SegmentsCache n/a (no manifest cache attached to table — cache disabled)
          Prefetch      n/a (prefetch disabled or prefetchedManifestEntriesCache empty for this table)
          Heap          before  avg=30,596,917 bytes, min=30,584,480, max=30,621,648 (avg 17.65x of disk)
          Heap          peak   avg=384,227,248 bytes, min=380,394,312, max=389,768,056 (avg 221.65x of disk, max 224.85x of disk)
          Heap          after-gc avg=30,605,949 bytes, min=30,584,480, max=30,621,552 (avg 17.66x of disk)
          Heap          peak/after-gc avg=12.55x (max spike multiplier=12.73x)
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
        catalogOptions.set(CatalogOptions.CACHE_MANIFEST_SOFT_VALUES, false);

        BenchParams p = new BenchParams();
        innerTest("segmentsCacheEnabled", catalogOptions, tableOptions, p);
        /*
        Populated table has 8000 (partition, bucket) pairs across 2000 partitions (4 restore threads, 10x value cols, 64-char values, commit batch=10).

        OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
        Apple M4 Pro
        segmentsCacheEnabled:                                                                                Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------
        OPERATORTEST_segmentsCacheEnabled_restore                                                                  613 /  622             13.1          76585.7       1.0X

        Manifest cache footprint aggregate (segmentsCacheEnabled, 3 measured iters):
          Disk          manifests=1,785,338 bytes (26 files), manifest-lists=25,910 bytes (20 files), index-manifests=0 bytes (0 files); total=1,811,248 bytes
          SegmentsCache bytes  avg=16,422,240, min=16,422,240, max=16,422,240 (avg 9.07x of disk)
          Prefetch      n/a (prefetch disabled or prefetchedManifestEntriesCache empty for this table)
          Heap          before  avg=49,020,690 bytes, min=47,396,328, max=52,228,256 (avg 27.06x of disk)
          Heap          peak   avg=400,259,984 bytes, min=376,393,272, max=426,200,520 (avg 220.99x of disk, max 235.31x of disk)
          Heap          after-gc avg=50,887,970 bytes, min=47,394,736, max=53,042,464 (avg 28.10x of disk)
          Heap          peak/after-gc avg=7.88x (max spike multiplier=8.40x)
         */
    }

    @Test
    public void testRestoreFiles_segmentsCacheEnabled_smallPage() throws Exception {
        Options catalogOptions = new Options();
        Options tableOptions = new Options();
        tableOptions.set(CoreOptions.MANIFEST_PREFETCH_ENTRIES, false);
        catalogOptions.set(
                CatalogOptions.CACHE_MANIFEST_SMALL_FILE_MEMORY, MemorySize.ofMebiBytes(2048));
        catalogOptions.set(CatalogOptions.CACHE_MANIFEST_MAX_MEMORY, MemorySize.ofMebiBytes(4096));
        catalogOptions.set(CatalogOptions.CACHE_MANIFEST_PAGE_SIZE, MemorySize.ofKibiBytes(2));
        catalogOptions.set(CatalogOptions.CACHE_MANIFEST_SOFT_VALUES, false);

        BenchParams p = new BenchParams();
        innerTest("segmentsCacheEnabled_smallPage", catalogOptions, tableOptions, p);
        /*
        Populated table has 8000 (partition, bucket) pairs across 2000 partitions (4 restore threads, 10x value cols, 64-char values, commit batch=10).

        OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
        Apple M4 Pro
        segmentsCacheEnabled_smallPage:                                                                      Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------
        OPERATORTEST_segmentsCacheEnabled_smallPage_restore                                                        605 /  617             13.2          75663.6       1.0X

        Manifest cache footprint aggregate (segmentsCacheEnabled_smallPage, 3 measured iters):
          Disk          manifests=1,783,321 bytes (26 files), manifest-lists=25,925 bytes (20 files), index-manifests=0 bytes (0 files); total=1,809,246 bytes
          SegmentsCache bytes  avg=16,422,432, min=16,422,432, max=16,422,432 (avg 9.08x of disk)
          Prefetch      n/a (prefetch disabled or prefetchedManifestEntriesCache empty for this table)
          Heap          before  avg=47,634,520 bytes, min=46,992,936, max=48,558,552 (avg 26.33x of disk)
          Heap          peak   avg=397,549,856 bytes, min=372,505,400, max=432,364,032 (avg 219.73x of disk, max 238.97x of disk)
          Heap          after-gc avg=48,709,448 bytes, min=46,991,392, max=51,786,472 (avg 26.92x of disk)
          Heap          peak/after-gc avg=8.19x (max spike multiplier=9.20x)
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
        Populated table has 8000 (partition, bucket) pairs across 2000 partitions (4 restore threads, 10x value cols, 64-char values, commit batch=10).

        OpenJDK 64-Bit Server VM 11.0.28+0 on Mac OS X 26.5
        Apple M4 Pro
        prefetchEnabled:                                                                                     Best/Avg Time(ms)    Row Rate(K/s)      Per Row(ns)   Relative
        --------------------------------------------------------------------------------------------------------------------------------------------------------------------
        OPERATORTEST_prefetchEnabled_restore                                                                      1001 / 1024              8.0         125069.8       1.0X

        Manifest cache footprint aggregate (prefetchEnabled, 3 measured iters):
          Disk          manifests=1,708,606 bytes (26 files), manifest-lists=25,872 bytes (20 files), index-manifests=0 bytes (0 files); total=1,734,478 bytes
          SegmentsCache n/a (no manifest cache attached to table — cache disabled)
          Prefetch      bytes  avg=31,363,282, min=31,327,744, max=31,434,360 (entries=8000, avg 18.08x of disk)
          Heap          before  avg=26,287,618 bytes, min=26,138,816, max=26,378,816 (avg 15.16x of disk)
          Heap          peak   avg=210,689,234 bytes, min=193,303,240, max=224,128,704 (avg 121.47x of disk, max 129.22x of disk)
          Heap          after-gc avg=46,754,616 bytes, min=43,163,384, max=48,592,160 (avg 26.96x of disk)
          Heap          peak/after-gc avg=4.52x (max spike multiplier=4.97x)
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
        List<FootprintSample> perIterSamples =
                new ArrayList<>(p.numWarmupIters + p.numMeasuredIters);

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
                            // also fully run gc before starting iterations
                            System.gc();
                            System.runFinalization();
                            System.gc();
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
                        long before = currentHeapUsage();

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
                        long afterGc = currentHeapUsage();

                        try {
                            FootprintSample sample =
                                    printCacheFootprint(
                                            name + " " + iterLabel, fst, before, peak, afterGc);
                            perIterSamples.add(sample);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            benchmark.run();
        } finally {
            executor.shutdownNow();
        }

        printAggregateFootprint(name, p, perIterSamples);
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
                    SegmentsCache.create(
                            original.pageSize(),
                            original.maxMemorySize(),
                            original.maxElementSize(),
                            original.ttl(),
                            original.softValues()));
        }
        clearStaticPrefetchCache();
    }

    /**
     * Print a per-iteration footprint summary: total manifest directory bytes on disk (split by
     * file-name prefix), the table's {@link SegmentsCache} accounting bytes, the JOL deep retained
     * size of the prefetched manifest entries for this table, the just-sampled heap before/peak and
     * post-GC usage, and memory-to-disk plus {@code Peak/After-GC} (spike multiplier) ratios.
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
    private FootprintSample printCacheFootprint(
            String label, FileStoreTable fst, long before, long peak, long afterGc)
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
        Integer prefetchEntries = null;
        String prefetchLine;
        if (prefetched == null) {
            prefetchLine =
                    "Prefetch n/a (prefetch disabled or prefetchedManifestEntriesCache empty for this table)";
        } else {
            prefetchBytes = GraphLayout.parseInstance(prefetched).totalSize();
            prefetchEntries = prefetched.manifestEntries().size();
            prefetchLine =
                    String.format(
                            "Prefetch bytes=%,d (entries=%d, deep-size via JOL GraphLayout)",
                            prefetchBytes, prefetchEntries);
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
        System.out.printf(
                "  Heap          before=%,d bytes, peak=%,d bytes, after-gc=%,d bytes%n",
                before, peak, afterGc);
        if (diskTotal > 0) {
            System.out.printf(
                    "  Ratios        SegmentsCache/disk=%s, Prefetch/disk=%s, Before/disk=%s, Peak/disk=%s, After-GC/disk=%s, Peak/After-GC=%s%n",
                    ratio(segmentsCacheBytes, diskTotal),
                    ratio(prefetchBytes, diskTotal),
                    ratio(before, diskTotal),
                    ratio(peak, diskTotal),
                    ratio(afterGc, diskTotal),
                    ratio(peak, afterGc));
        }
        System.out.println();

        return new FootprintSample(
                manifestBytes,
                manifestCount,
                manifestListBytes,
                manifestListCount,
                indexManifestBytes,
                indexManifestCount,
                segmentsCacheBytes,
                prefetchBytes,
                prefetchEntries,
                before,
                peak,
                afterGc);
    }

    /**
     * Print one-block aggregate over the <b>measured</b> iterations (warmup iterations skipped).
     * Reports Disk (constant — printed once), {@link SegmentsCache} bytes (avg/min/max + avg ratio
     * to disk), Prefetch bytes (avg/min/max + entries + avg ratio to disk), and Heap
     * before/peak/after-GC (avg/min/max + Peak/After-GC spike multiplier + heap/disk ratios).
     */
    private void printAggregateFootprint(
            String name, BenchParams p, List<FootprintSample> samples) {
        int start = p.numWarmupIters;
        int end = samples.size();
        if (start >= end) {
            return;
        }
        int n = end - start;
        FootprintSample first = samples.get(start);

        // SegmentsCache: aggregate non-null sample bytes; treat as absent if every sample is null.
        boolean anySegmentsCache = false;
        long scSum = 0;
        long scMin = Long.MAX_VALUE;
        long scMax = Long.MIN_VALUE;
        int scN = 0;
        // Prefetch: same pattern.
        boolean anyPrefetch = false;
        long pfSum = 0;
        long pfMin = Long.MAX_VALUE;
        long pfMax = Long.MIN_VALUE;
        int pfEntries = -1;
        int pfN = 0;

        long beforeSum = 0;
        long beforeMin = Long.MAX_VALUE;
        long beforeMax = Long.MIN_VALUE;
        long peakSum = 0;
        long peakMin = Long.MAX_VALUE;
        long peakMax = Long.MIN_VALUE;
        long afterGcSum = 0;
        long afterGcMin = Long.MAX_VALUE;
        long afterGcMax = Long.MIN_VALUE;
        double psRatioSum = 0;
        double psRatioMax = 0;

        for (int i = start; i < end; i++) {
            FootprintSample s = samples.get(i);
            if (s.segmentsCacheBytes != null) {
                anySegmentsCache = true;
                scSum += s.segmentsCacheBytes;
                scMin = Math.min(scMin, s.segmentsCacheBytes);
                scMax = Math.max(scMax, s.segmentsCacheBytes);
                scN++;
            }
            if (s.prefetchBytes != null) {
                anyPrefetch = true;
                pfSum += s.prefetchBytes;
                pfMin = Math.min(pfMin, s.prefetchBytes);
                pfMax = Math.max(pfMax, s.prefetchBytes);
                pfEntries = s.prefetchEntries == null ? pfEntries : s.prefetchEntries;
                pfN++;
            }
            beforeSum += s.beforeHeap;
            beforeMin = Math.min(beforeMin, s.beforeHeap);
            beforeMax = Math.max(beforeMax, s.beforeHeap);
            peakSum += s.peakHeap;
            peakMin = Math.min(peakMin, s.peakHeap);
            peakMax = Math.max(peakMax, s.peakHeap);
            afterGcSum += s.afterGcHeap;
            afterGcMin = Math.min(afterGcMin, s.afterGcHeap);
            afterGcMax = Math.max(afterGcMax, s.afterGcHeap);
            double psRatio = s.afterGcHeap == 0 ? 0 : (double) s.peakHeap / s.afterGcHeap;
            psRatioSum += psRatio;
            psRatioMax = Math.max(psRatioMax, psRatio);
        }

        System.out.println();
        System.out.println(
                "Manifest cache footprint aggregate (" + name + ", " + n + " measured iters):");
        System.out.printf(
                "  Disk          manifests=%,d bytes (%d files), manifest-lists=%,d bytes (%d files), index-manifests=%,d bytes (%d files); total=%,d bytes%n",
                first.manifestBytes,
                first.manifestCount,
                first.manifestListBytes,
                first.manifestListCount,
                first.indexManifestBytes,
                first.indexManifestCount,
                first.diskTotal);
        if (anySegmentsCache) {
            long scAvg = scSum / scN;
            System.out.printf(
                    "  SegmentsCache bytes  avg=%,d, min=%,d, max=%,d (avg %s of disk)%n",
                    scAvg, scMin, scMax, ratio(scAvg, first.diskTotal));
        } else {
            System.out.println(
                    "  SegmentsCache n/a (no manifest cache attached to table — cache disabled)");
        }
        if (anyPrefetch) {
            long pfAvg = pfSum / pfN;
            System.out.printf(
                    "  Prefetch      bytes  avg=%,d, min=%,d, max=%,d (entries=%d, avg %s of disk)%n",
                    pfAvg, pfMin, pfMax, pfEntries, ratio(pfAvg, first.diskTotal));
        } else {
            System.out.println(
                    "  Prefetch      n/a (prefetch disabled or prefetchedManifestEntriesCache empty for this table)");
        }
        System.out.printf(
                "  Heap          before  avg=%,d bytes, min=%,d, max=%,d (avg %s of disk)%n",
                beforeSum / n, beforeMin, beforeMax, ratio(beforeSum / n, first.diskTotal));
        System.out.printf(
                "  Heap          peak   avg=%,d bytes, min=%,d, max=%,d (avg %s of disk, max %s of disk)%n",
                peakSum / n,
                peakMin,
                peakMax,
                ratio(peakSum / n, first.diskTotal),
                ratio(peakMax, first.diskTotal));
        System.out.printf(
                "  Heap          after-gc avg=%,d bytes, min=%,d, max=%,d (avg %s of disk)%n",
                afterGcSum / n, afterGcMin, afterGcMax, ratio(afterGcSum / n, first.diskTotal));
        System.out.printf(
                "  Heap          peak/after-gc avg=%.2fx (max spike multiplier=%.2fx)%n",
                psRatioSum / n, psRatioMax);
        System.out.println();
    }

    private static long currentHeapUsage() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
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
