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
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexFileMetaSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.operation.RestoreFiles;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Versioned, length-delimited spill format. Only one serialized file record is held at a time;
 * neither side builds a list of serialized records or an outer Java object stream. Paging remains
 * unchanged. A capability on the request prevents sending this format to legacy TaskManagers.
 */
final class StreamingScanResponse {

    static final int MAGIC = 0x50414D33; // PAM3, distinct from the Java serialization header.
    private static final int VERSION = 1;

    private StreamingScanResponse() {}

    static final class Writer {

        private final DataOutputStream output;
        private final DataFileMetaSerializer fileSerializer = new DataFileMetaSerializer();
        private final ScanCoordinationResponse statistics;

        Writer(OutputStream output, @Nullable Snapshot snapshot) throws IOException {
            this.output = new DataOutputStream(output);
            // Diagnostics only: no serialized file/index collections are stored here.
            this.statistics = new ScanCoordinationResponse(snapshot, null, null, null, null);
            this.output.writeInt(MAGIC);
            this.output.writeInt(VERSION);
            writeBytes(
                    snapshot == null ? null : snapshot.toJson().getBytes(StandardCharsets.UTF_8));
        }

        void writeFile(DataFileMeta file) throws IOException {
            byte[] bytes = fileSerializer.serializeToBytes(file);
            writeBytes(bytes);
            statistics.recordDataFile(file, bytes.length);
        }

        ScanCoordinationResponse finish(
                @Nullable Integer totalBuckets,
                @Nullable IndexFileMeta dynamicBucketIndex,
                @Nullable List<IndexFileMeta> deleteVectorsIndex)
                throws IOException {
            output.writeInt(-1); // End of files: the count is unknown until the scan completes.
            output.writeBoolean(totalBuckets != null);
            if (totalBuckets != null) {
                output.writeInt(totalBuckets);
            }
            IndexFileMetaSerializer indexSerializer = new IndexFileMetaSerializer();
            writeIndex(dynamicBucketIndex, indexSerializer);
            output.writeInt(deleteVectorsIndex == null ? -1 : deleteVectorsIndex.size());
            if (deleteVectorsIndex != null) {
                for (IndexFileMeta index : deleteVectorsIndex) {
                    writeIndex(index, indexSerializer);
                }
            }
            output.flush();
            return statistics;
        }

        private void writeIndex(@Nullable IndexFileMeta index, IndexFileMetaSerializer serializer)
                throws IOException {
            byte[] bytes = index == null ? null : serializer.serializeToBytes(index);
            writeBytes(bytes);
            statistics.recordSerializedIndex(bytes == null ? 0 : bytes.length);
        }

        private void writeBytes(@Nullable byte[] bytes) throws IOException {
            output.writeInt(bytes == null ? -1 : bytes.length);
            if (bytes != null) {
                output.write(bytes);
            }
        }
    }

    static RestoreFiles read(InputStream input, long size) throws IOException {
        Reader reader = new Reader(input, size);
        if (reader.readInt() != MAGIC) {
            throw new IOException("Invalid streaming writer coordinator response header.");
        }
        int version = reader.readInt();
        if (version != VERSION) {
            throw new IOException(
                    "Unsupported streaming writer coordinator response version: " + version);
        }
        byte[] snapshotBytes = reader.readBytes();
        Snapshot snapshot =
                snapshotBytes == null
                        ? null
                        : Snapshot.fromJson(new String(snapshotBytes, StandardCharsets.UTF_8));
        List<DataFileMeta> files = new ArrayList<>();
        DataFileMetaSerializer fileSerializer = new DataFileMetaSerializer();
        byte[] bytes;
        while ((bytes = reader.readBytes()) != null) {
            files.add(fileSerializer.deserializeFromBytes(bytes));
        }
        Integer totalBuckets = reader.readBoolean() ? reader.readInt() : null;
        IndexFileMetaSerializer indexSerializer = new IndexFileMetaSerializer();
        bytes = reader.readBytes();
        IndexFileMeta dynamicIndex =
                bytes == null ? null : indexSerializer.deserializeFromBytes(bytes);
        int indexCount = reader.readInt();
        if (indexCount < -1 || indexCount > reader.remaining / Integer.BYTES) {
            throw new IOException("Invalid deletion vector count: " + indexCount);
        }
        List<IndexFileMeta> deleteVectors = indexCount == -1 ? null : new ArrayList<>();
        for (int i = 0; i < indexCount; i++) {
            bytes = reader.readBytes();
            if (bytes == null) {
                throw new IOException(
                        "Null deletion vector in streaming writer coordinator response.");
            }
            deleteVectors.add(indexSerializer.deserializeFromBytes(bytes));
        }
        if (reader.remaining != 0) {
            throw new IOException("Trailing bytes in streaming writer coordinator response.");
        }
        if (snapshot == null
                && (!files.isEmpty()
                        || totalBuckets != null
                        || dynamicIndex != null
                        || deleteVectors != null)) {
            throw new IOException("Unexpected metadata without a snapshot.");
        }
        return new RestoreFiles(
                snapshot,
                totalBuckets,
                snapshot == null ? null : files,
                dynamicIndex,
                deleteVectors);
    }

    /** Validate lengths against the actual spill file before allocating a record buffer. */
    private static final class Reader {

        private final DataInputStream input;
        private long remaining;

        private Reader(InputStream input, long size) {
            this.input = new DataInputStream(input);
            this.remaining = size;
        }

        private void require(long bytes) throws IOException {
            if (bytes < 0 || bytes > remaining) {
                throw new EOFException(
                        "Truncated or invalid streaming writer coordinator response.");
            }
            remaining -= bytes;
        }

        private int readInt() throws IOException {
            require(Integer.BYTES);
            return input.readInt();
        }

        private boolean readBoolean() throws IOException {
            require(1);
            int value = input.readUnsignedByte();
            if (value > 1) {
                throw new IOException("Invalid nullable bucket marker: " + value);
            }
            return value == 1;
        }

        @Nullable
        private byte[] readBytes() throws IOException {
            int length = readInt();
            if (length == -1) {
                return null;
            }
            require(length);
            byte[] bytes = new byte[length];
            input.readFully(bytes);
            return bytes;
        }
    }
}
