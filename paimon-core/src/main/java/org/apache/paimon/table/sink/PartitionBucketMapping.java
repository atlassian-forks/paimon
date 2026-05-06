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

package org.apache.paimon.table.sink;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A mapping that resolves the number of buckets for each partition in a table.
 *
 * <p>Different partitions may have different bucket counts (e.g., after a rescale operation). This
 * class maintains a per-partition bucket count mapping and falls back to a default bucket count for
 * partitions that are not explicitly mapped.
 *
 * <p>This is used by components such as {@link FixedBucketRowKeyExtractor} and {@link
 * FixedBucketWriteSelector} to correctly determine the bucket assignment for rows in tables where
 * partitions may have been rescaled independently.
 *
 * @see #loadFromTable(FileStoreTable)
 * @see #resolveNumBuckets(BinaryRow)
 */
public class PartitionBucketMapping implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(PartitionBucketMapping.class);

    /** The default number of buckets, used when a partition has no explicit mapping. */
    private final int defaultBucketCount;

    /** A map from partition to its specific bucket count. May be empty but never {@code null}. */
    private final Map<BinaryRow, Integer> partitionBucketMap;

    /**
     * Renders a {@link BinaryRow} partition into a human-readable {@code key=value/key=value}
     * string. Built once at load time from the table's partition schema so that
     * {@link #resolveNumBuckets(BinaryRow)} doesn't have to repeatedly convert types per record.
     * May be {@code null} for legacy code paths that constructed this object without schema
     * info, in which case logging falls back to {@link BinaryRow#toString()}.
     */
    private final transient PartitionRenderer partitionRenderer;

    /**
     * Tracks partitions for which we've already emitted the "first resolution" log line in this
     * JVM/instance, so that the per-record {@link #resolveNumBuckets(BinaryRow)} call only logs
     * once per partition, not on every record. Marked {@code transient} so it does not get
     * shipped with the serialized state.
     */
    private transient Set<BinaryRow> firstResolutionLogged;

    /**
     * Functional interface used to render a partition {@link BinaryRow} as a human-readable
     * string. Holding a tiny wrapper here (rather than the full converter + key list) lets
     * unit tests stub the rendering without depending on {@code RowDataToObjectArrayConverter}.
     */
    @FunctionalInterface
    public interface PartitionRenderer extends Serializable {
        String render(BinaryRow partition);
    }

    /**
     * Creates a mapping with only a default bucket count and no per-partition overrides.
     *
     * @param defaultBucketCount the default number of buckets for all partitions
     */
    public PartitionBucketMapping(int defaultBucketCount) {
        this(defaultBucketCount, Collections.emptyMap());
    }

    /**
     * Creates a mapping with a default bucket count and an explicit per-partition bucket map.
     *
     * @param defaultBucketCount the default number of buckets, used as a fallback
     * @param partitionBucketMap a map from partition (as {@link BinaryRow}) to its bucket count
     */
    public PartitionBucketMapping(
            int defaultBucketCount, Map<BinaryRow, Integer> partitionBucketMap) {
        this(defaultBucketCount, partitionBucketMap, null);
    }

    /**
     * Same as {@link #PartitionBucketMapping(int, Map)} but additionally accepts a
     * {@link PartitionRenderer} used to format partition values in log output. When the renderer
     * is {@code null}, logging falls back to {@link BinaryRow#toString()} (which prints
     * {@code BinaryRow@hex}).
     */
    public PartitionBucketMapping(
            int defaultBucketCount,
            Map<BinaryRow, Integer> partitionBucketMap,
            PartitionRenderer partitionRenderer) {
        this.defaultBucketCount = defaultBucketCount;
        this.partitionBucketMap = partitionBucketMap;
        this.partitionRenderer = partitionRenderer;
    }

    /**
     * Loads a {@link PartitionBucketMapping} by scanning the manifest entries of the given table.
     *
     * <p>For non-partitioned tables, this returns a mapping with only the schema-defined default
     * bucket count and an empty partition map.
     *
     * <p>For partitioned tables, the method scans all manifest entries and records the {@code
     * totalBuckets} value for each partition. If the scan fails for any reason, a fallback mapping
     * with only the default bucket count is returned.
     *
     * @param table the {@link FileStoreTable} to load the mapping from
     * @return a {@link PartitionBucketMapping} reflecting the current bucket layout of the table
     */
    public static PartitionBucketMapping loadFromTable(FileStoreTable table) {
        long startNanos = System.nanoTime();
        int defaultBuckets = table.schema().numBuckets();
        String tableName;
        try {
            tableName = table.name();
        } catch (Throwable t) {
            tableName = "<unknown>";
        }
        if (table.partitionKeys().isEmpty()) {
            LOG.info(
                    "PartitionBucketMapping.loadFromTable: table={} is non-partitioned, "
                            + "using default bucket count = {}",
                    tableName,
                    defaultBuckets);
            return new PartitionBucketMapping(defaultBuckets, Collections.emptyMap());
        }

        try {
            List<SimpleFileEntry> entries = table.store().newScan().readSimpleEntries();
            Map<BinaryRow, Integer> partitionBucketMap = new HashMap<>();
            // Track *all* total_buckets values per partition along with a sample of the file
            // names that contributed each value, so we can detect on-disk divergence (e.g. an
            // unfinished rescale or a residual file from a prior failed commit) AND tell the
            // operator exactly which parquet files belong to which layout. The file lists are
            // capped per (partition, total_buckets) at MAX_FILES_PER_LAYOUT to avoid producing
            // gigantic log entries when a partition has thousands of files.
            final int MAX_FILES_PER_LAYOUT = 20;
            Map<BinaryRow, Map<Integer, List<String>>> divergenceTracker = new HashMap<>();
            Map<BinaryRow, Map<Integer, Integer>> divergenceFileCount = new HashMap<>();
            for (SimpleFileEntry entry : entries) {
                int totalBuckets = entry.totalBuckets();
                if (totalBuckets <= 0) {
                    continue;
                }
                String fileName;
                try {
                    fileName = entry.fileName();
                } catch (Throwable t) {
                    fileName = "<unknown:" + t.getClass().getSimpleName() + ">";
                }
                Map<Integer, List<String>> perTb =
                        divergenceTracker.computeIfAbsent(
                                entry.partition(), k -> new TreeMap<>());
                List<String> fileList =
                        perTb.computeIfAbsent(totalBuckets, k -> new ArrayList<>());
                if (fileList.size() < MAX_FILES_PER_LAYOUT) {
                    fileList.add(fileName);
                }
                divergenceFileCount
                        .computeIfAbsent(entry.partition(), k -> new HashMap<>())
                        .merge(totalBuckets, 1, Integer::sum);
                // Only store partitions whose bucket count differs from the default.
                // This keeps the map empty for partitions that have never been rescaled,
                // avoiding per-partition BinaryRow copies and Integer allocations entirely.
                if (totalBuckets != defaultBuckets) {
                    BinaryRow partition = entry.partition();
                    partitionBucketMap.putIfAbsent(partition.copy(), totalBuckets);
                }
            }

            // Render partition keys to a human-readable string for log output.
            RowDataToObjectArrayConverter partitionConverter =
                    new RowDataToObjectArrayConverter(table.schema().logicalPartitionType());
            List<String> partitionKeyNames = table.partitionKeys();
            // Renderer used by resolveNumBuckets() so per-record logs print "key=value/..."
            // instead of the opaque BinaryRow@hex form. We pre-bind the converter and key list
            // here at load time, so the per-record path doesn't have to recreate them.
            PartitionRenderer renderer =
                    new ConverterPartitionRenderer(partitionConverter, partitionKeyNames);

            // Detect and log divergence (multiple total_buckets values for one partition).
            // For each divergent partition we emit:
            //   1. A summary line listing all total_buckets values seen with their file counts.
            //   2. A separate WARN line per total_buckets layout, listing up to
            //      MAX_FILES_PER_LAYOUT representative file names. This gives the operator the
            //      evidence needed to identify which files belong to the stale layout vs the
            //      current layout, so they can be removed/rewritten.
            int divergent = 0;
            for (Map.Entry<BinaryRow, Map<Integer, List<String>>> e :
                    divergenceTracker.entrySet()) {
                if (e.getValue().size() > 1) {
                    divergent++;
                    Map<Integer, Integer> counts = divergenceFileCount.get(e.getKey());
                    String partitionStr =
                            renderPartition(
                                    e.getKey(), partitionConverter, partitionKeyNames);
                    LOG.warn(
                            "PartitionBucketMapping.loadFromTable: table={} partition {} has "
                                    + "MULTIPLE total_buckets layouts on disk: {} (file counts "
                                    + "per layout: {}). This is a pre-existing on-disk "
                                    + "inconsistency that will trigger a bucket-count conflict "
                                    + "on commit. See per-layout file lists below.",
                            tableName,
                            partitionStr,
                            e.getValue().keySet(),
                            counts);
                    for (Map.Entry<Integer, List<String>> tbAndFiles :
                            e.getValue().entrySet()) {
                        int tb = tbAndFiles.getKey();
                        List<String> files = tbAndFiles.getValue();
                        int totalCount =
                                counts == null
                                        ? files.size()
                                        : counts.getOrDefault(tb, files.size());
                        LOG.warn(
                                "PartitionBucketMapping.loadFromTable: table={} partition {} "
                                        + "total_buckets={} -> {} file(s) "
                                        + "(showing first {}): {}",
                                tableName,
                                partitionStr,
                                tb,
                                totalCount,
                                Math.min(MAX_FILES_PER_LAYOUT, totalCount),
                                files);
                    }
                }
            }

            // Per-partition INFO log for each non-default mapping (the "interesting" subset).
            // For tables with thousands of partitions, this is small because only RESCALED
            // partitions appear here.
            if (LOG.isInfoEnabled() && !partitionBucketMap.isEmpty()) {
                int sample = Math.min(50, partitionBucketMap.size());
                int idx = 0;
                for (Map.Entry<BinaryRow, Integer> e : partitionBucketMap.entrySet()) {
                    if (idx++ >= sample) {
                        break;
                    }
                    LOG.info(
                            "PartitionBucketMapping.loadFromTable: table={} partition {} -> "
                                    + "total_buckets={} (overrides default {})",
                            tableName,
                            renderPartition(e.getKey(), partitionConverter, partitionKeyNames),
                            e.getValue(),
                            defaultBuckets);
                }
                if (partitionBucketMap.size() > sample) {
                    LOG.info(
                            "PartitionBucketMapping.loadFromTable: table={} (... {} more partition "
                                    + "overrides not logged; enable DEBUG to see all)",
                            tableName,
                            partitionBucketMap.size() - sample);
                }
            }
            if (LOG.isDebugEnabled()) {
                for (Map.Entry<BinaryRow, Integer> e : partitionBucketMap.entrySet()) {
                    LOG.debug(
                            "PartitionBucketMapping.loadFromTable: table={} partition {} -> "
                                    + "total_buckets={}",
                            tableName,
                            renderPartition(e.getKey(), partitionConverter, partitionKeyNames),
                            e.getValue());
                }
            }

            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
            LOG.info(
                    "PartitionBucketMapping.loadFromTable: table={} default_buckets={}, "
                            + "scanned {} live entries, found {} partition-bucket overrides, "
                            + "{} divergent partition(s). Took {} ms.",
                    tableName,
                    defaultBuckets,
                    entries.size(),
                    partitionBucketMap.size(),
                    divergent,
                    elapsedMs);

            return new PartitionBucketMapping(defaultBuckets, partitionBucketMap, renderer);
        } catch (Exception e) {
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
            LOG.warn(
                    "PartitionBucketMapping.loadFromTable: table={} FAILED to scan manifests "
                            + "after {} ms; falling back to default_buckets={}. Cause: {}",
                    tableName,
                    elapsedMs,
                    defaultBuckets,
                    e.toString());
            return new PartitionBucketMapping(defaultBuckets, Collections.emptyMap());
        }
    }

    /**
     * {@link PartitionRenderer} backed by a {@link RowDataToObjectArrayConverter} and the table's
     * partition key names. Reused for the per-record path in
     * {@link #resolveNumBuckets(BinaryRow)} so we render at most once per distinct partition.
     */
    private static final class ConverterPartitionRenderer implements PartitionRenderer {
        private static final long serialVersionUID = 1L;

        private final RowDataToObjectArrayConverter converter;
        private final List<String> partitionKeys;

        ConverterPartitionRenderer(
                RowDataToObjectArrayConverter converter, List<String> partitionKeys) {
            this.converter = converter;
            this.partitionKeys = partitionKeys;
        }

        @Override
        public String render(BinaryRow partition) {
            return renderPartition(partition, converter, partitionKeys);
        }
    }

    /** Render a partition's BinaryRow as "key1=value1/key2=value2" for log output. */
    private static String renderPartition(
            BinaryRow partition,
            RowDataToObjectArrayConverter converter,
            List<String> partitionKeys) {
        if (partition == null) {
            return "<null>";
        }
        try {
            Object[] vals = converter.convert(partition);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < vals.length; i++) {
                if (i > 0) {
                    sb.append("/");
                }
                String key = (i < partitionKeys.size()) ? partitionKeys.get(i) : ("col" + i);
                sb.append(key).append("=").append(vals[i]);
            }
            return sb.toString();
        } catch (Throwable t) {
            return "<renderPartition failed: " + t.getClass().getSimpleName() + ">";
        }
    }

    /**
     * Resolves the number of buckets for the given partition.
     *
     * <p>If the partition has an explicit entry in the partition-to-bucket map, that value is
     * returned. Otherwise, the default bucket count is returned.
     *
     * @param partition the partition key as a {@link BinaryRow}
     * @return the number of buckets for the given partition
     */
    public int resolveNumBuckets(BinaryRow partition) {
        int resolved;
        boolean fromOverride = false;
        if (partitionBucketMap != null) {
            Integer partitionBucketCount = partitionBucketMap.get(partition);
            if (partitionBucketCount != null) {
                resolved = partitionBucketCount;
                fromOverride = true;
            } else {
                resolved = defaultBucketCount;
            }
        } else {
            resolved = defaultBucketCount;
        }

        // Emit a one-time INFO log per distinct partition this writer instance sees, so the
        // operator can confirm exactly which (partition -> total_buckets) the writer chose
        // when the conflict happens. Subsequent records for the same partition stay silent.
        if (LOG.isInfoEnabled()) {
            if (firstResolutionLogged == null) {
                synchronized (this) {
                    if (firstResolutionLogged == null) {
                        firstResolutionLogged = ConcurrentHashMap.newKeySet();
                    }
                }
            }
            if (firstResolutionLogged.add(partition.copy())) {
                String partitionStr;
                if (partitionRenderer != null) {
                    try {
                        partitionStr = partitionRenderer.render(partition);
                    } catch (Throwable t) {
                        partitionStr =
                                org.apache.paimon.utils.PartitionLogFormatter.format(partition);
                    }
                } else {
                    // Renderer was lost (transient field, e.g. after deserialization) or never
                    // set — best-effort untyped formatting so the log is still readable instead
                    // of showing "BinaryRow@<hex>".
                    partitionStr = org.apache.paimon.utils.PartitionLogFormatter.format(partition);
                }
                LOG.info(
                        "PartitionBucketMapping.resolveNumBuckets: writer first sees partition {} -> "
                                + "total_buckets={} (source={}, default={}, override_map_size={})",
                        partitionStr,
                        resolved,
                        fromOverride ? "PER_PARTITION_OVERRIDE" : "DEFAULT",
                        defaultBucketCount,
                        partitionBucketMap == null ? 0 : partitionBucketMap.size());
            }
        }

        return resolved;
    }
}
