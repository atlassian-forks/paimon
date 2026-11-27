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

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.operation.RestoreFiles;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamingScanResponseTest extends TableTestBase {

    @Test
    void testWritesRecordsIncrementallyAndRoundTripsIndexes() throws Exception {
        Identifier identifier = Identifier.create("db", "streaming");
        catalog.createDatabase("db", false);
        catalog.createTable(
                identifier, Schema.newBuilder().column("v", DataTypes.INT()).build(), false);
        FileStoreTable table = getTable(identifier);
        write(table, GenericRow.of(1));
        Snapshot snapshot = table.latestSnapshot().get();
        DataFileMeta file = table.store().newScan().plan().files().get(0).file();
        IndexFileMeta dynamic = new IndexFileMeta("HASH", "hash-index", 42, 1, null, null);
        IndexFileMeta deletion =
                new IndexFileMeta("DELETION_VECTORS", "dv-index", 43, 1, null, null);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        StreamingScanResponse.Writer writer = new StreamingScanResponse.Writer(output, snapshot);
        int headerBytes = output.size();
        for (int i = 0; i < 100; i++) {
            int before = output.size();
            writer.writeFile(file);
            assertThat(output.size()).isGreaterThan(before);
        }
        ScanCoordinationResponse stats =
                writer.finish(-1, dynamic, Collections.singletonList(deletion));
        assertThat(stats.dataFileCount()).isEqualTo(100);
        assertThat(stats.serializedDataFilesBytes())
                .isEqualTo(
                        output.size()
                                - headerBytes
                                - stats.serializedIndexBytes()
                                - 100 * Integer.BYTES
                                - 21);
        assertThat(stats.extractDataFiles())
                .as("writer retains counters, not serialized file lists")
                .isNull();
        RestoreFiles restored = read(output.toByteArray());
        assertThat(restored.snapshot().toJson()).isEqualTo(snapshot.toJson());
        assertThat(restored.totalBuckets()).isEqualTo(-1);
        assertThat(restored.dataFiles()).hasSize(100).allMatch(file::equals);
        assertThat(restored.dynamicBucketIndex()).isEqualTo(dynamic);
        assertThat(restored.deleteVectorsIndex()).containsExactly(deletion);
    }

    @Test
    void testNoSnapshotPreservesNullFiles() throws Exception {
        RestoreFiles restored = read(emptyResponse());
        assertThat(restored.snapshot()).isNull();
        assertThat(restored.totalBuckets()).isNull();
        assertThat(restored.dataFiles()).isNull();
        assertThat(restored.dynamicBucketIndex()).isNull();
        assertThat(restored.deleteVectorsIndex()).isNull();
    }

    @Test
    void testRejectsTruncationAndCorruptFramingBeforeAllocating() throws Exception {
        byte[] valid = emptyResponse();
        for (int length = 0; length < valid.length; length++) {
            byte[] truncated = Arrays.copyOf(valid, length);
            assertThatThrownBy(() -> read(truncated)).isInstanceOf(IOException.class);
        }
        byte[] oversized = valid.clone();
        ByteBuffer.wrap(oversized).putInt(12, Integer.MAX_VALUE);
        assertThatThrownBy(() -> read(oversized)).isInstanceOf(IOException.class);
        byte[] negativeLength = valid.clone();
        ByteBuffer.wrap(negativeLength).putInt(12, -2);
        assertThatThrownBy(() -> read(negativeLength)).isInstanceOf(IOException.class);
        byte[] badVersion = valid.clone();
        ByteBuffer.wrap(badVersion).putInt(4, 999);
        assertThatThrownBy(() -> read(badVersion))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
        byte[] badCount = valid.clone();
        ByteBuffer.wrap(badCount).putInt(valid.length - 4, -2);
        assertThatThrownBy(() -> read(badCount)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> read(Arrays.copyOf(valid, valid.length + 1)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Trailing");
    }

    @Test
    void testStreamingWriteFailureDeletesPartialSpill() throws Exception {
        java.nio.file.Path directory = Files.createDirectory(tempPath.resolve("failed-stream"));
        assertThatThrownBy(
                        () ->
                                SerializedScanResponse.spilled(
                                        output -> {
                                            new StreamingScanResponse.Writer(output, null);
                                            throw new IOException("injected manifest scan failure");
                                        },
                                        64,
                                        directory))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("injected");
        try (java.util.stream.Stream<java.nio.file.Path> files = Files.list(directory)) {
            assertThat(files.count()).isZero();
        }
    }

    @Test
    void testRequestPreservesLegacySerializationUid() {
        assertThat(ObjectStreamClass.lookup(PagedCoordinationRequest.class).getSerialVersionUID())
                .isEqualTo(7230230030925988974L);
        assertThat(new PagedCoordinationRequest(new byte[0], "legacy", null).streamingResponse())
                .isFalse();
    }

    @Test
    void testDeserializesActualLegacyRequestWithoutCapability() throws Exception {
        // Produced by the original three-field request class, without an explicit UID.
        String fixture =
                "rO0ABXNyAEFvcmcuYXBhY2hlLnBhaW1vbi5mbGluay5zaW5rLmNvb3JkaW5hdG9yLlBhZ2VkQ29vcmRpbmF0aW9uUmVxdWVzdGRW7+fdtIRuAgADWwAHY29udGVudHQAAltCTAAJcGFnZVRva2VudAATTGphdmEvbGFuZy9JbnRlZ2VyO0wACXJlcXVlc3RJZHQAEkxqYXZhL2xhbmcvU3RyaW5nO3hwdXIAAltCrPMX+AYIVOACAAB4cAAAAAMBAgNzcgARamF2YS5sYW5nLkludGVnZXIS4qCk94GHOAIAAUkABXZhbHVleHIAEGphdmEubGFuZy5OdW1iZXKGrJUdC5TgiwIAAHhwAAAAKnQADmxlZ2FjeS1maXh0dXJl";
        try (ObjectInputStream input =
                new ObjectInputStream(
                        new ByteArrayInputStream(Base64.getDecoder().decode(fixture)))) {
            PagedCoordinationRequest request = (PagedCoordinationRequest) input.readObject();
            assertThat(request.content()).containsExactly(1, 2, 3);
            assertThat(request.requestId()).isEqualTo("legacy-fixture");
            assertThat(request.pageToken()).isEqualTo(42);
            assertThat(request.streamingResponse()).isFalse();
        }
    }

    private static byte[] emptyResponse() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        new StreamingScanResponse.Writer(output, null).finish(null, null, null);
        return output.toByteArray();
    }

    private static RestoreFiles read(byte[] bytes) throws IOException {
        return StreamingScanResponse.read(new ByteArrayInputStream(bytes), bytes.length);
    }
}
