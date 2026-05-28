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

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.SegmentsCache;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WriteOperatorCoordinator}. */
class WriteOperatorCoordinatorTest extends TableTestBase {

    @Test
    public void testManifestCachePageSizeDefault() throws Exception {
        FileStoreTable table = createTableForCoordinator(null);

        WriteOperatorCoordinator coordinator = new WriteOperatorCoordinator(table);
        try {
            coordinator.start();
            SegmentsCache<Path> manifestCache = table.getManifestCache();
            assertThat(manifestCache).isNotNull();
            assertThat(manifestCache.pageSize())
                    .isEqualTo(
                            (int)
                                    FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_PAGE_SIZE
                                            .defaultValue()
                                            .getBytes());
        } finally {
            coordinator.close();
        }
    }

    @Test
    public void testManifestCachePageSizeOverride() throws Exception {
        MemorySize pageSize = MemorySize.ofKibiBytes(2);
        FileStoreTable table = createTableForCoordinator(pageSize);

        WriteOperatorCoordinator coordinator = new WriteOperatorCoordinator(table);
        try {
            coordinator.start();
            SegmentsCache<Path> manifestCache = table.getManifestCache();
            assertThat(manifestCache).isNotNull();
            assertThat(manifestCache.pageSize()).isEqualTo((int) pageSize.getBytes());
        } finally {
            coordinator.close();
        }
    }

    private FileStoreTable createTableForCoordinator(MemorySize pageSizeOverride) throws Exception {
        Identifier identifier = new Identifier(database, "wocoord_t");
        Schema.Builder schemaBuilder = Schema.newBuilder().column("f0", DataTypes.INT());
        if (pageSizeOverride != null) {
            schemaBuilder.option(
                    FlinkConnectorOptions.SINK_WRITER_COORDINATOR_CACHE_PAGE_SIZE.key(),
                    pageSizeOverride.toString());
        }
        catalog.createTable(identifier, schemaBuilder.build(), false);
        return getTable(identifier);
    }
}
