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

package org.apache.paimon.utils;

import org.slf4j.Logger;

/**
 * Lightweight helper that periodically emits a sampled "units / bytes / rate" log line.
 *
 * <p>Used by the disk-spill, external-sort and merge-tree write paths to expose progress when the
 * Flink subtask appears stuck inside file-channel read/write or external merge-sort. Designed so
 * the per-record path is just an increment + cheap modulo; clock reads only happen on sample
 * boundaries / close.
 *
 * <p>NOT thread-safe. Each instance is intended to be used by a single writer/reader thread.
 */
public final class RateLogger {

    private final Logger log;
    private final String name;
    private final long sampleEvery;

    private long count;
    private long bytes;

    private final long startNanos;
    private long lastLogNanos;
    private long lastLogCount;
    private long lastLogBytes;

    public RateLogger(Logger log, String name, long sampleEvery) {
        this.log = log;
        this.name = name;
        this.sampleEvery = Math.max(sampleEvery, 1L);
        long now = System.nanoTime();
        this.startNanos = now;
        this.lastLogNanos = now;
    }

    /** Record a single unit of work (e.g. one block written, one record serialized). */
    public void onUnit(long byteSize) {
        count++;
        bytes += byteSize;
        if (count % sampleEvery == 0 && log.isInfoEnabled()) {
            flush(false);
        }
    }

    /** Force a final summary line to be emitted (e.g. on close). */
    public void close() {
        if (count > 0 && log.isInfoEnabled()) {
            flush(true);
        }
    }

    public long count() {
        return count;
    }

    public long bytes() {
        return bytes;
    }

    private void flush(boolean closing) {
        long now = System.nanoTime();
        double dtSec = Math.max((now - lastLogNanos) / 1e9, 1e-9);
        double totalSec = Math.max((now - startNanos) / 1e9, 1e-9);
        long dCount = count - lastLogCount;
        long dBytes = bytes - lastLogBytes;

        log.info(
                "[{}] {} units={} bytes={} window: {}/s {}MB/s | total: {}/s {}MB/s elapsedMs={}",
                name,
                closing ? "CLOSE" : "TICK",
                count,
                bytes,
                String.format("%.0f", dCount / dtSec),
                String.format("%.2f", dBytes / 1024.0 / 1024.0 / dtSec),
                String.format("%.0f", count / totalSec),
                String.format("%.2f", bytes / 1024.0 / 1024.0 / totalSec),
                (long) (totalSec * 1000));

        lastLogNanos = now;
        lastLogCount = count;
        lastLogBytes = bytes;
    }
}
