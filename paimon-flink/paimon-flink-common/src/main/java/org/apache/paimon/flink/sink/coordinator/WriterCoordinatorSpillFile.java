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

import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_SPILL_BUFFER_SIZE;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_WRITER_COORDINATOR_SPILL_DIRECTORY;

/** A temporary local file used to spill a writer coordinator response. */
final class WriterCoordinatorSpillFile implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(WriterCoordinatorSpillFile.class);
    private static final String FILE_PREFIX = "paimon-writer-coordinator-";
    private static final String FILE_SUFFIX = ".spill";

    private final Path path;
    private boolean closed;

    private WriterCoordinatorSpillFile(Path path) {
        this.path = path;
    }

    static WriterCoordinatorSpillFile create(@Nullable Path directory) throws IOException {
        Path path =
                directory == null
                        ? Files.createTempFile(FILE_PREFIX, FILE_SUFFIX)
                        : Files.createTempFile(directory, FILE_PREFIX, FILE_SUFFIX);
        return new WriterCoordinatorSpillFile(path.toAbsolutePath());
    }

    static int configuredBufferSize(Options options) {
        MemorySize configured = options.get(SINK_WRITER_COORDINATOR_SPILL_BUFFER_SIZE);
        long bytes = configured.getBytes();
        if (bytes <= 0 || bytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    SINK_WRITER_COORDINATOR_SPILL_BUFFER_SIZE.key()
                            + " must be between 1 byte and "
                            + Integer.MAX_VALUE
                            + " bytes, but was "
                            + configured
                            + '.');
        }
        return (int) bytes;
    }

    @Nullable
    static Path configuredDirectory(Options options) {
        String configured = options.getOptional(SINK_WRITER_COORDINATOR_SPILL_DIRECTORY).orElse("");
        if (configured.trim().isEmpty()) {
            return null;
        }

        Path directory = Paths.get(configured.trim());
        if (!directory.isAbsolute()) {
            throw new IllegalArgumentException(
                    "Writer coordinator spill directory must be absolute: " + directory + '.');
        }
        directory = directory.normalize();
        try {
            Files.createDirectories(directory);
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException(
                    "Cannot create writer coordinator spill directory " + directory + '.', e);
        }
        if (!Files.isDirectory(directory) || !Files.isWritable(directory)) {
            throw new IllegalArgumentException(
                    "Writer coordinator spill directory is not a writable directory: "
                            + directory
                            + '.');
        }
        return directory;
    }

    void writeObject(Object object, int bufferSize) throws IOException {
        try (OutputStream output = outputStream(bufferSize)) {
            InstantiationUtil.serializeObject(output, object);
        }
    }

    OutputStream outputStream(int bufferSize) throws IOException {
        return new BufferedOutputStream(
                Files.newOutputStream(
                        path, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING),
                bufferSize);
    }

    InputStream inputStream(int bufferSize) throws IOException {
        return new BufferedInputStream(Files.newInputStream(path), bufferSize);
    }

    byte[] readPage(int offset, int pageSize) throws IOException {
        long fileSize = size();
        int length = (int) Math.min(fileSize - offset, pageSize);
        byte[] page = new byte[length];
        ByteBuffer buffer = ByteBuffer.wrap(page);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.position(offset);
            while (buffer.hasRemaining()) {
                if (channel.read(buffer) < 0) {
                    throw new IOException(
                            "Unexpected end of writer coordinator spill file " + path + '.');
                }
            }
        }
        return page;
    }

    long size() throws IOException {
        return Files.size(path);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            LOG.warn("Failed to delete writer coordinator spill file {}.", path, e);
        }
    }
}
