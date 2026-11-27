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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link FileSystemWriteRestore}, with a particular focus on the empty-bucket fallback
 * path (see commit "FileSystemWriteRestore: infer totalBuckets from PartitionBucketMapping for
 * empty buckets").
 *
 * <p>The bug: when restoring files for a {@code (partition, bucket)} that has no existing data
 * files, {@link WriteRestore#extractDataFiles(java.util.List, java.util.List)} returns {@code null}
 * for {@code totalBuckets} because there are no manifest entries to derive it from. Previously this
 * {@code null} was propagated, causing the writer to fall back to the table-level default bucket
 * count even when the partition had been rescaled to a different bucket count. New writes to the
 * empty bucket would be stamped with the wrong {@code totalBuckets}, corrupting the partition's
 * bucket layout.
 */
public class FileSystemWriteRestoreTest {

    @TempDir java.nio.file.Path tempDir;

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                    new String[] {"pt", "k", "v"});

    @ParameterizedTest(name = "prefetch={0}")
    @ValueSource(booleans = {false, true})
    public void testRestoreFiles_emptyBucketUsesPartitionBucketMapping(boolean prefetch)
            throws Exception {
        // Build a table with default bucket=2 and write data into partition 1.
        // Some buckets within partition 1 will end up with files (bucket 0 OR
        // bucket 1, depending on hash); the OTHER bucket will be empty. Then
        // "rescale" the table-level default to 32 (without rewriting partition 1)
        // and ask the WriteRestore for an empty bucket. It must return
        // totalBuckets=2 (the partition's actual bucket count), NOT 32 (the new
        // table default).
        FileStoreTable table = createPartitionedPkTable(4);

        // Write enough rows to populate at least one bucket within partition 1.
        // We'll then probe the OTHER bucket (which is necessarily empty for that
        // partition under fixed-bucket hashing if no row hashes to it). If both
        // buckets happen to be populated, we still cover the bug for the bucket
        // that we probe — but the test prefers the empty case.
        commitOneRow(table, /* pt */ 1, /* k */ 1);
        commitOneRow(table, /* pt */ 1, /* k */ 2);

        // Find an empty bucket in partition 1 by inspecting the existing files.
        int emptyBucket = findEmptyBucket(table, 1, /* totalBuckets */ 4);

        // Simulate a rescale by raising the table-level default bucket count
        // (without rewriting existing files). Existing manifest entries still
        // carry totalBuckets=2.
        table = withBucket(table, 32);

        WriteRestore restore = newWriteRestore(table, prefetch);
        if (prefetch) {
            ((FileSystemWriteRestore) restore)
                    .prefetchManifestEntries(table.snapshotManager().latestSnapshot());
        }

        RestoreFiles restored = restore.restoreFiles(binaryRow(1), emptyBucket, false, false);

        assertThat(restored.totalBuckets())
                .as(
                        "Empty (partition 1, bucket %d): totalBuckets must be inferred from "
                                + "PartitionBucketMapping (4), not the new table default (32). "
                                + "If null/32, FileSystemWriteRestore failed to fall back to "
                                + "PartitionBucketMapping for the empty bucket.",
                        emptyBucket)
                .isEqualTo(4);
        assertThat(restored.dataFiles()).isNullOrEmpty();
    }

    @Test
    public void testRestoreFiles_emptyBucketInUnseenPartitionUsesDefault() throws Exception {
        // For an entirely unseen partition (no files anywhere), no per-partition
        // mapping exists and PartitionBucketMapping.resolveNumBuckets falls back to
        // the table's default bucket count.
        FileStoreTable table = createPartitionedPkTable(8);
        commitOneRow(table, 1, 100); // ensures the snapshot exists

        WriteRestore restore = newWriteRestore(table, false);
        RestoreFiles restored = restore.restoreFiles(binaryRow(/* unseen */ 999), 0, false, false);

        assertThat(restored.totalBuckets()).isEqualTo(8);
        assertThat(restored.dataFiles()).isNullOrEmpty();
    }

    @Test
    public void testRestoreFiles_nonEmptyBucketReportsManifestTotalBuckets() throws Exception {
        // Sanity test: when a bucket has files, totalBuckets must come from the
        // manifest entries (not from the fallback path). This guards against
        // accidentally always overriding totalBuckets via PartitionBucketMapping.
        FileStoreTable table = createPartitionedPkTable(2);
        commitOneRow(table, 1, 1);
        commitOneRow(table, 1, 2);

        // Locate a non-empty bucket within partition 1.
        int nonEmptyBucket = findNonEmptyBucket(table, 1, 2);

        // Change the table default to ensure the returned totalBuckets is from the
        // manifest entry, not the schema.
        table = withBucket(table, 32);

        WriteRestore restore = newWriteRestore(table, false);
        RestoreFiles restored = restore.restoreFiles(binaryRow(1), nonEmptyBucket, false, false);

        assertThat(restored.totalBuckets()).isEqualTo(2);
        assertThat(restored.dataFiles()).isNotEmpty();
    }

    // ------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------

    private FileStoreTable createPartitionedPkTable(int bucket) throws Exception {
        Path path = new Path(tempDir.toString());
        Options options = new Options();
        options.set(CoreOptions.PATH, path.toString());
        options.set(CoreOptions.BUCKET, bucket);

        TableSchema tableSchema =
                SchemaUtils.forceCommit(
                        new SchemaManager(LocalFileIO.create(), path),
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.singletonList("pt"),
                                Arrays.asList("pt", "k"),
                                options.toMap(),
                                ""));

        return FileStoreTableFactory.create(
                LocalFileIO.create(), path, tableSchema, CatalogEnvironment.empty());
    }

    private FileStoreTable withBucket(FileStoreTable table, int newBucket) {
        Options options = new Options(table.options());
        options.set(CoreOptions.BUCKET, newBucket);
        return table.copy(table.schema().copy(options.toMap()));
    }

    private WriteRestore newWriteRestore(FileStoreTable table, boolean prefetch) {
        Options options = new Options(table.options());
        options.set(CoreOptions.MANIFEST_PREFETCH_ENTRIES, prefetch);
        return new FileSystemWriteRestore(
                new CoreOptions(options),
                table.snapshotManager(),
                table.store().newScan(),
                table.store().newIndexFileHandler());
    }

    private void commitOneRow(FileStoreTable table, int pt, int k) throws Exception {
        String user = UUID.randomUUID().toString();
        Long latest = table.snapshotManager().latestSnapshotId();
        long id = latest == null ? 0L : latest;
        try (StreamTableWrite write = table.newWrite(user);
                StreamTableCommit commit = table.newCommit(user)) {
            write.write(GenericRow.of(pt, k, (long) k));
            commit.commit(id, write.prepareCommit(true, id));
        }
    }

    /** Returns a bucket id (0..totalBuckets-1) that has no data files within the partition. */
    private int findEmptyBucket(FileStoreTable table, int pt, int totalBuckets) throws Exception {
        BinaryRow partition = binaryRow(pt);
        for (int b = 0; b < totalBuckets; b++) {
            int bucket = b;
            boolean nonEmpty =
                    table.newSnapshotReader()
                            .withPartitionFilter(Collections.singletonList(partition))
                            .withBucket(bucket).read().dataSplits().stream()
                            .anyMatch(s -> !s.dataFiles().isEmpty());
            if (!nonEmpty) {
                return bucket;
            }
        }
        throw new IllegalStateException(
                "Could not find an empty bucket in partition "
                        + pt
                        + " (every bucket has files); test scenario could not be set up.");
    }

    /** Returns a bucket id (0..totalBuckets-1) that has at least one data file. */
    private int findNonEmptyBucket(FileStoreTable table, int pt, int totalBuckets)
            throws Exception {
        BinaryRow partition = binaryRow(pt);
        for (int b = 0; b < totalBuckets; b++) {
            int bucket = b;
            boolean nonEmpty =
                    table.newSnapshotReader()
                            .withPartitionFilter(Collections.singletonList(partition))
                            .withBucket(bucket).read().dataSplits().stream()
                            .anyMatch(s -> !s.dataFiles().isEmpty());
            if (nonEmpty) {
                return bucket;
            }
        }
        throw new IllegalStateException("Could not find a non-empty bucket in partition " + pt);
    }

    private static BinaryRow binaryRow(int pt) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter writer = new BinaryRowWriter(row);
        writer.writeInt(0, pt);
        writer.complete();
        return row;
    }
}
