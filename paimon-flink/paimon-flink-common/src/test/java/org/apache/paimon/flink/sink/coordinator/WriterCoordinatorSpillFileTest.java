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

import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WriterCoordinatorSpillFileTest {

    @TempDir private Path tempDir;

    @Test
    void testConcurrentCreationUsesUniqueFilesAndCleansUp() throws Exception {
        int fileCount = 64;
        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<WriterCoordinatorSpillFile> spillFiles = new ArrayList<>();
        try {
            List<Callable<WriterCoordinatorSpillFile>> tasks = new ArrayList<>();
            for (int i = 0; i < fileCount; i++) {
                tasks.add(() -> WriterCoordinatorSpillFile.create(tempDir));
            }
            for (Future<WriterCoordinatorSpillFile> future : executor.invokeAll(tasks)) {
                spillFiles.add(future.get());
            }

            assertThat(fileCount(tempDir)).isEqualTo(fileCount);
        } finally {
            spillFiles.forEach(WriterCoordinatorSpillFile::close);
            // Closing a spill file more than once must remain safe.
            spillFiles.forEach(WriterCoordinatorSpillFile::close);
            executor.shutdownNow();
        }

        assertThat(fileCount(tempDir)).isZero();
    }

    @Test
    void testConfiguredDirectoryRejectsRegularFile() throws Exception {
        Path regularFile = Files.createFile(tempDir.resolve("not-a-directory"));
        Options options = new Options();
        options.set(
                FlinkConnectorOptions.SINK_WRITER_COORDINATOR_SPILL_DIRECTORY,
                regularFile.toString());

        assertThatThrownBy(() -> WriterCoordinatorSpillFile.configuredDirectory(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot create writer coordinator spill directory");
    }

    private static long fileCount(Path directory) throws Exception {
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            return files.count();
        }
    }
}
