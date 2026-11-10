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

import org.apache.paimon.utils.InstantiationUtil;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;

/** A serialized scan response stored either in heap or in a local spill file. */
final class SerializedScanResponse implements AutoCloseable {

    private final byte[] inMemory;
    private final WriterCoordinatorSpillFile spillFile;
    private final int length;

    private SerializedScanResponse(
            byte[] inMemory, WriterCoordinatorSpillFile spillFile, int length) {
        this.inMemory = inMemory;
        this.spillFile = spillFile;
        this.length = length;
    }

    static SerializedScanResponse inMemory(Object response) throws IOException {
        byte[] serialized = InstantiationUtil.serializeObject(response);
        return new SerializedScanResponse(serialized, null, serialized.length);
    }

    static SerializedScanResponse spilled(
            Object response, int bufferSize, @Nullable Path spillDirectory) throws IOException {
        return spilled(
                output -> InstantiationUtil.serializeObject(output, response),
                bufferSize,
                spillDirectory);
    }

    interface SpillWriter {
        void write(OutputStream output) throws IOException;
    }

    static SerializedScanResponse spilled(
            SpillWriter writer, int bufferSize, @Nullable Path spillDirectory) throws IOException {
        WriterCoordinatorSpillFile spillFile = WriterCoordinatorSpillFile.create(spillDirectory);
        try {
            try (OutputStream output = spillFile.outputStream(bufferSize)) {
                writer.write(output);
            }
            long length = spillFile.size();
            if (length > Integer.MAX_VALUE) {
                throw new IOException(
                        "Serialized writer coordinator response is too large for the existing "
                                + "integer page-token protocol: "
                                + length
                                + " bytes.");
            }
            return new SerializedScanResponse(null, spillFile, (int) length);
        } catch (IOException | RuntimeException | Error e) {
            spillFile.close();
            throw e;
        }
    }

    int length() {
        return length;
    }

    int heapBytes() {
        return inMemory == null ? 0 : inMemory.length;
    }

    long spillBytes() {
        return spillFile == null ? 0 : length;
    }

    byte[] readPage(int offset, int pageSize) throws IOException {
        if (spillFile != null) {
            return spillFile.readPage(offset, pageSize);
        }
        return Arrays.copyOfRange(inMemory, offset, Math.min(length, offset + pageSize));
    }

    @Override
    public void close() {
        if (spillFile != null) {
            spillFile.close();
        }
    }
}
