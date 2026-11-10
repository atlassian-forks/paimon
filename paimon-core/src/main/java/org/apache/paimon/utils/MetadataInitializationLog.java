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

import java.util.concurrent.TimeUnit;

/**
 * Timing and JVM-wide heap observations around metadata initialization.
 *
 * <p>Heap deltas include concurrent allocations and GC, can be negative, and do not measure the
 * retained size of manifest objects. No GC or background sampler is triggered by this logger.
 */
public final class MetadataInitializationLog implements AutoCloseable {

    private final Logger logger;
    private final String operation;
    private final String context;
    private final long startNanos;
    private final long heapUsedBefore;
    private boolean success;
    private long entryCount = -1;
    private long serializedBytes = -1;

    private MetadataInitializationLog(Logger logger, String operation, String context) {
        this.logger = logger;
        this.operation = operation;
        this.context = context;
        this.startNanos = System.nanoTime();
        this.heapUsedBefore = usedHeapBytes();
        logger.info(
                "Metadata initialization started: operation={}, context={}, jvmHeapUsedBeforeBytes={}",
                operation,
                context,
                heapUsedBefore);
    }

    public static MetadataInitializationLog start(Logger logger, String operation, String context) {
        return new MetadataInitializationLog(logger, operation, context);
    }

    /** Mark success; use -1 for counts/sizes unavailable for this operation. */
    public void complete(long entryCount, long serializedBytes) {
        this.entryCount = entryCount;
        this.serializedBytes = serializedBytes;
        this.success = true;
    }

    @Override
    public void close() {
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        long heapUsedAfter = usedHeapBytes();
        Runtime runtime = Runtime.getRuntime();
        logger.info(
                "Metadata initialization finished: operation={}, context={}, success={}, "
                        + "durationMs={}, entryCount={}, serializedBytes={}, "
                        + "jvmHeapUsedBeforeBytes={}, jvmHeapUsedAfterBytes={}, "
                        + "jvmHeapUsedDeltaBytes={}, jvmHeapCommittedBytes={}, jvmHeapMaxBytes={}",
                operation,
                context,
                success,
                elapsedMs,
                entryCount,
                serializedBytes,
                heapUsedBefore,
                heapUsedAfter,
                heapUsedAfter - heapUsedBefore,
                runtime.totalMemory(),
                runtime.maxMemory());
    }

    private static long usedHeapBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
