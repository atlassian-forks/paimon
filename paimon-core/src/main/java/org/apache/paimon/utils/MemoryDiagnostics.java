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

import com.sun.management.GarbageCollectorMXBean;
import com.sun.management.GcInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Event-driven, rate-limited JVM observations. Never requests GC or walks the heap. */
public final class MemoryDiagnostics {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryDiagnostics.class);
    private static final long INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final AtomicLong LAST_SAMPLE_NANOS = new AtomicLong();
    private static final AtomicInteger ACTIVE_METADATA_OPERATIONS = new AtomicInteger();
    private static final Map<String, AtomicInteger> ACTIVE_BY_OPERATION = new ConcurrentHashMap<>();
    private static final Map<String, Long> LAST_GC_IDS = new ConcurrentHashMap<>();

    private MemoryDiagnostics() {}

    static int metadataStarted(String operation) {
        ACTIVE_BY_OPERATION
                .computeIfAbsent(operation, ignored -> new AtomicInteger())
                .incrementAndGet();
        return ACTIVE_METADATA_OPERATIONS.incrementAndGet();
    }

    static int metadataFinished(String operation) {
        ACTIVE_BY_OPERATION.get(operation).decrementAndGet();
        return ACTIVE_METADATA_OPERATIONS.decrementAndGet();
    }

    /** At most one sample per minute per classloader, on the caller's existing thread. */
    public static void logJvmMemory(String context) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        long now = System.nanoTime();
        long previous = LAST_SAMPLE_NANOS.get();
        if ((previous != 0 && now - previous < INTERVAL_NANOS)
                || !LAST_SAMPLE_NANOS.compareAndSet(previous, now)) {
            return;
        }
        try {
            MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            MemoryUsage nonHeap = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
            long directBytes = 0;
            long mappedBytes = 0;
            for (BufferPoolMXBean pool :
                    ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
                if ("direct".equals(pool.getName())) {
                    directBytes = pool.getMemoryUsed();
                } else if ("mapped".equals(pool.getName())) {
                    mappedBytes = pool.getMemoryUsed();
                }
            }
            LOG.info(
                    "JVM memory observation: context={}, heapUsedBytes={}, heapCommittedBytes={}, "
                            + "heapMaxBytes={}, nonHeapUsedBytes={}, directBufferUsedBytes={}, "
                            + "mappedBufferUsedBytes={}, activeMetadataOperations={}, activeMetadataOperationsByType={}",
                    context,
                    heap.getUsed(),
                    heap.getCommitted(),
                    heap.getMax(),
                    nonHeap.getUsed(),
                    directBytes,
                    mappedBytes,
                    ACTIVE_METADATA_OPERATIONS.get(),
                    ACTIVE_BY_OPERATION);
            logLastGc();
        } catch (RuntimeException | LinkageError e) {
            // Diagnostics must not fail a writer, including on JVMs without HotSpot GC extensions.
            LOG.debug("Unable to sample JVM memory for context {}", context, e);
        }
    }

    private static void logLastGc() {
        Set<String> heapPools = new HashSet<>();
        ManagementFactory.getMemoryPoolMXBeans().stream()
                .filter(pool -> pool.getType() == MemoryType.HEAP)
                .forEach(pool -> heapPools.add(pool.getName()));
        for (java.lang.management.GarbageCollectorMXBean collector :
                ManagementFactory.getGarbageCollectorMXBeans()) {
            if (!(collector instanceof GarbageCollectorMXBean)) {
                continue;
            }
            GcInfo gc = ((GarbageCollectorMXBean) collector).getLastGcInfo();
            if (gc == null
                    || Long.valueOf(gc.getId()).equals(LAST_GC_IDS.get(collector.getName()))) {
                continue;
            }
            LAST_GC_IDS.put(collector.getName(), gc.getId());
            long before = 0;
            long after = 0;
            Map<String, Long> afterByPool = new LinkedHashMap<>();
            for (String pool : heapPools) {
                MemoryUsage beforeUsage = gc.getMemoryUsageBeforeGc().get(pool);
                MemoryUsage afterUsage = gc.getMemoryUsageAfterGc().get(pool);
                if (beforeUsage != null) {
                    before += beforeUsage.getUsed();
                }
                if (afterUsage != null) {
                    after += afterUsage.getUsed();
                    afterByPool.put(pool, afterUsage.getUsed());
                }
            }
            LOG.info(
                    "Last GC observation: collector={}, gcId={}, gcDurationMs={}, "
                            + "gcEndUptimeMs={}, observationAgeMs={}, heapUsedBeforeGcBytes={}, "
                            + "heapUsedAfterGcBytes={}, heapPoolsAfterGcBytes={}, collectionCount={}, "
                            + "collectionTimeMs={}",
                    collector.getName(),
                    gc.getId(),
                    gc.getDuration(),
                    gc.getEndTime(),
                    ManagementFactory.getRuntimeMXBean().getUptime() - gc.getEndTime(),
                    before,
                    after,
                    afterByPool,
                    collector.getCollectionCount(),
                    collector.getCollectionTime());
        }
    }
}
