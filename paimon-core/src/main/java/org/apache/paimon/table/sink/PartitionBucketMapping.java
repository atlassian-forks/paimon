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
import org.apache.paimon.manifest.FileEntry;
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

        List<SimpleFileEntry> entries = table.store().newScan().readSimpleEntries();
        return loadFromEntries(entries, defaultBuckets);
    }

    public static PartitionBucketMapping loadFromEntries(
            List<? extends FileEntry> entries, int defaultBuckets) {
        Map<BinaryRow, Integer> partitionBucketMap = new HashMap<>();
        for (FileEntry entry : entries) {
            int totalBuckets = entry.totalBuckets();
            // Only store partitions whose bucket count differs from the default.
            // This keeps the map empty for partitions that have never been rescaled,
            // avoiding per-partition BinaryRow copies and Integer allocations entirely.
            if (totalBuckets > 0 && totalBuckets != defaultBuckets) {
                BinaryRow partition = entry.partition();
                partitionBucketMap.putIfAbsent(partition.copy(), totalBuckets);
            }
        }
        return new PartitionBucketMapping(defaultBuckets, partitionBucketMap);
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
