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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.BucketFilter;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.PartitionBucketMapping;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/** {@link WriteRestore} to restore files directly from file system. */
public class FileSystemWriteRestore implements WriteRestore {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemWriteRestore.class);

    private final SnapshotManager snapshotManager;
    private final FileStoreScan scan;
    private final IndexFileHandler indexFileHandler;

    private final Boolean usePrefetchManifestEntries;

    @Nullable
    private static Cache<String, PrefetchedManifestEntries> prefetchedManifestEntriesCache;

    public FileSystemWriteRestore(
            CoreOptions options,
            SnapshotManager snapshotManager,
            FileStoreScan scan,
            IndexFileHandler indexFileHandler) {
        this.snapshotManager = snapshotManager;
        this.scan = scan;
        this.indexFileHandler = indexFileHandler;
        if (options.manifestDeleteFileDropStats()) {
            if (this.scan != null) {
                this.scan.dropStats();
            }
        }
        this.usePrefetchManifestEntries = options.prefetchManifestEntries();
        initializeCacheIfNeeded();
    }

    private static synchronized void initializeCacheIfNeeded() {
        if (prefetchedManifestEntriesCache == null) {
            prefetchedManifestEntriesCache =
                    Caffeine.newBuilder()
                            .expireAfterAccess(Duration.ofMinutes(30))
                            .executor(Runnable::run)
                            .build();
            // .softValues() - not used as we want to hold onto a copy of manifest for each table we
            // are writing to
            // .maximumSize(...) - not used as number of keys is static = number of tables being
            // written to
            // .maximumWeight(...).weigher(...) - not used as there isn't a convenient way of
            // measuring memory size
            //    of a ManifestEntry object
        }
    }

    @Override
    public long latestCommittedIdentifier(String user) {
        return snapshotManager
                .latestSnapshotOfUserFromFilesystem(user)
                .map(Snapshot::commitIdentifier)
                .orElse(Long.MIN_VALUE);
    }

    private String getPrefetchManifestEntriesCacheKey() {
        return snapshotManager.tablePath().toString();
    }

    private List<ManifestEntry> fetchManifestEntries(
            Snapshot snapshot, @Nullable BinaryRow partition, @Nullable Integer bucket) {
        // The scan instance is shared across calls. Always pass through the (possibly-null)
        // partition and bucket so that any filter previously set by another invocation is
        // explicitly cleared when this call wants no such filter. Otherwise leftover state would
        // silently scope the plan to the previous (partition, bucket).
        return scan.withSnapshot(snapshot)
                .withPartitionFilter(
                        partition == null ? null : Collections.singletonList(partition))
                .withBucket(bucket)
                .plan()
                .files();
    }

    private Integer getDefaultTotalBuckets(Snapshot snapshot) {
        SchemaManager schemaManager =
                new SchemaManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        TableSchema tableSchema = schemaManager.schema(snapshot.schemaId());
        return tableSchema.numBuckets();
    }

    public PrefetchedManifestEntries prefetchManifestEntries(Snapshot snapshot) {
        synchronized (this.getClass()) {
            if (prefetchedManifestEntriesCache == null) {
                initializeCacheIfNeeded();
            }

            // check if fetch is needed - if it was done by another thread then we can skip
            // altogether
            PrefetchedManifestEntries prefetch =
                    prefetchedManifestEntriesCache.getIfPresent(
                            getPrefetchManifestEntriesCacheKey());
            if (prefetch != null && prefetch.snapshot.id() == snapshot.id()) {
                LOG.info(
                        "FileSystemWriteRestore skipping prefetching manifestEntries for table {}, snapshot {} as it was fetched by another thread",
                        snapshotManager.tablePath(),
                        snapshot.id());
                return prefetch;
            }

            LOG.info(
                    "FileSystemWriteRestore started prefetching manifestEntries for table {}, snapshot {}",
                    snapshotManager.tablePath(),
                    snapshot.id());
            List<ManifestEntry> manifestEntries = fetchManifestEntries(snapshot, null, null);
            LOG.info(
                    "FileSystemWriteRestore prefetched manifestEntries for table {}, snapshot {}: {} entries",
                    snapshotManager.tablePath(),
                    snapshot.id(),
                    manifestEntries.size());

            RowType partitionType = scan.manifestsReader().partitionType();
            Integer defaultTotalBuckets = getDefaultTotalBuckets(snapshot);

            PrefetchedManifestEntries prefetchedManifestEntries =
                    new PrefetchedManifestEntries(
                            snapshot, partitionType, manifestEntries, defaultTotalBuckets);

            prefetchedManifestEntriesCache.put(
                    getPrefetchManifestEntriesCacheKey(), prefetchedManifestEntries);
            return prefetchedManifestEntries;
        }
    }

    @Override
    public RestoreFiles restoreFiles(
            BinaryRow partition,
            int bucket,
            boolean scanDynamicBucketIndex,
            boolean scanDeleteVectorsIndex) {
        // NOTE: don't use snapshotManager.latestSnapshot() here,
        // because we don't want to flood the catalog with high concurrency
        Snapshot snapshot = snapshotManager.latestSnapshotFromFileSystem();
        if (snapshot == null) {
            return RestoreFiles.empty();
        }

        List<ManifestEntry> entries;
        if (usePrefetchManifestEntries) {
            PrefetchedManifestEntries prefetch =
                    prefetchedManifestEntriesCache.getIfPresent(
                            getPrefetchManifestEntriesCacheKey());
            if (prefetch == null || prefetch.snapshot.id() != snapshot.id()) {
                // manifest entries if snapshot ids don't match
                prefetch = prefetchManifestEntries(snapshot);
            }

            entries = prefetch.filter(partition, bucket);
        } else {
            entries = fetchManifestEntries(snapshot, partition, bucket);
        }
        LOG.info(
                "FileSystemWriteRestore filtered manifestEntries for {}, {}, {}: {} entries",
                snapshotManager.tablePath(),
                partition,
                bucket,
                entries.size());

        List<DataFileMeta> restoreFiles = new ArrayList<>();
        Integer totalBuckets = WriteRestore.extractDataFiles(entries, restoreFiles);
        if (totalBuckets == null) {
            // totalBuckets can be null if `entries` is an empty list
            // i.e. there are no data files in this (partition, bucket)
            // in this case, WriteRestore cannot infer the totalBuckets value, as there are no
            // matching manifest entries
            // fallback to using PartitionBucketMapping for this
            totalBuckets = resolveTotalBuckets(snapshot, partition);
            LOG.info(
                    "FileSystemWriteRestore no manifest entries found for table {}, partition {}, bucket {} - falling back to PartitionBucketMapping and resolved totalBuckets={}",
                    snapshotManager.tablePath(),
                    partition,
                    bucket,
                    totalBuckets);
        }

        IndexFileMeta dynamicBucketIndex = null;
        if (scanDynamicBucketIndex) {
            dynamicBucketIndex =
                    indexFileHandler.scanHashIndex(snapshot, partition, bucket).orElse(null);
        }

        List<IndexFileMeta> deleteVectorsIndex = null;
        if (scanDeleteVectorsIndex) {
            deleteVectorsIndex =
                    indexFileHandler.scan(snapshot, DELETION_VECTORS_INDEX, partition, bucket);
        }

        return new RestoreFiles(
                snapshot, totalBuckets, restoreFiles, dynamicBucketIndex, deleteVectorsIndex);
    }

    private Integer resolveTotalBuckets(Snapshot snapshot, BinaryRow partition) {
        PartitionBucketMapping partitionBucketMapping;

        if (usePrefetchManifestEntries) {
            LOG.debug(
                    "FileSystemWriteRestore resolveTotalBuckets using prefetched PartitionBucketMapping for table {}, partition {}",
                    snapshotManager.tablePath(),
                    partition);
            PrefetchedManifestEntries prefetch =
                    prefetchedManifestEntriesCache.getIfPresent(
                            getPrefetchManifestEntriesCacheKey());
            partitionBucketMapping = prefetch.partitionBucketMapping();
        } else {
            Integer defaultTotalBuckets = getDefaultTotalBuckets(snapshot);
            List<ManifestEntry> entries = fetchManifestEntries(snapshot, partition, null);
            LOG.debug(
                    "FileSystemWriteRestore resolveTotalBuckets fetching manifest entries for table {}, partition {}, defaultTotalBuckets={}, entries.size()={}",
                    snapshotManager.tablePath(),
                    partition,
                    defaultTotalBuckets,
                    entries.size());

            partitionBucketMapping =
                    PartitionBucketMapping.loadFromEntries(entries, defaultTotalBuckets);
        }

        return partitionBucketMapping.resolveNumBuckets(partition);
    }

    /**
     * Container for a {@link Snapshot}'s manifest entries, used by {@link FileSystemWriteRestore}
     * to broker thread-safe access to cached results.
     */
    public static class PrefetchedManifestEntries {

        private final Snapshot snapshot;
        private final RowType partitionType;
        private final List<ManifestEntry> manifestEntries;
        private final PartitionBucketMapping partitionBucketMapping;

        public PrefetchedManifestEntries(
                Snapshot snapshot,
                RowType partitionType,
                List<ManifestEntry> manifestEntries,
                Integer defaultTotalBuckets) {
            this.snapshot = snapshot;
            this.partitionType = partitionType;
            this.manifestEntries = manifestEntries;
            this.partitionBucketMapping =
                    PartitionBucketMapping.loadFromEntries(manifestEntries, defaultTotalBuckets);
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        public RowType partitionType() {
            return partitionType;
        }

        public List<ManifestEntry> manifestEntries() {
            return manifestEntries;
        }

        public PartitionBucketMapping partitionBucketMapping() {
            return partitionBucketMapping;
        }

        public List<ManifestEntry> filter(BinaryRow partition, int bucket) {
            PartitionPredicate partitionPredicate =
                    PartitionPredicate.fromMultiple(
                            partitionType, Collections.singletonList(partition));

            BucketFilter bucketFilter = BucketFilter.create(false, bucket, null, null);
            return manifestEntries.stream()
                    .filter(
                            m ->
                                    (partitionPredicate == null
                                                    || partitionPredicate.test(m.partition()))
                                            && bucketFilter.test(m.bucket(), m.totalBuckets()))
                    .collect(Collectors.toList());
        }
    }
}
