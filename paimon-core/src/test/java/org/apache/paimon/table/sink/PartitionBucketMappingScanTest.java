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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies incremental mapping scans against real snapshots, including deleted files. */
class PartitionBucketMappingScanTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void testMappingAfterPartitionDeletion(int parallelism) throws Exception {
        Identifier identifier = new Identifier(database, "mapping");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("k", DataTypes.INT())
                        .partitionKeys("pt")
                        .option(CoreOptions.BUCKET.key(), "2")
                        .option(CoreOptions.BUCKET_KEY.key(), "k")
                        .option(CoreOptions.FILE_FORMAT.key(), "avro")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), "false")
                        .option(CoreOptions.MANIFEST_TARGET_FILE_SIZE.key(), "1 kb")
                        .option(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "" + parallelism)
                        .build(),
                false);
        FileStoreTable table = getTable(identifier);
        for (int i = 0; i < 12; i++) {
            write(table, GenericRow.of(1, i), GenericRow.of(2, i));
        }
        assertMatchesListScan(table);

        // Delete partition 1 using an empty overwrite, leaving ADDs and DELETEs in manifests.
        try (StreamTableWrite write = table.newWrite(commitUser).withIgnorePreviousFiles(true);
                InnerTableCommit commit = table.newCommit(commitUser)) {
            commit.withOverwrite(Collections.singletonMap("pt", "1"))
                    .commit(100, write.prepareCommit(true, 100));
        }
        Options rescaledOptions = new Options(table.options());
        rescaledOptions.set(CoreOptions.BUCKET, 32);
        table = table.copy(table.schema().copy(rescaledOptions.toMap()));
        List<SimpleFileEntry> remaining = assertMatchesListScan(table);
        assertThat(remaining).isNotEmpty();
        assertThat(remaining).allMatch(e -> e.partition().equals(BinaryRow.singleColumn(2)));

        // Existing partition retains its original count; deleted/unseen partitions use default.
        PartitionBucketMapping mapping = PartitionBucketMapping.loadFromTableWithoutCache(table);
        assertThat(mapping.resolveNumBuckets(BinaryRow.singleColumn(1))).isEqualTo(32);
        assertThat(mapping.resolveNumBuckets(BinaryRow.singleColumn(2))).isEqualTo(2);
        assertThat(mapping.resolveNumBuckets(BinaryRow.singleColumn(99))).isEqualTo(32);
    }

    private List<SimpleFileEntry> assertMatchesListScan(FileStoreTable table) {
        List<SimpleFileEntry> expected = table.store().newScan().readSimpleEntries();
        List<SimpleFileEntry> actual = new ArrayList<>();
        table.store().newScan().readSimpleEntryIterator().forEachRemaining(actual::add);
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        return actual;
    }
}
