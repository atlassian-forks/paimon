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

import org.apache.paimon.data.MultiSegments;
import org.apache.paimon.data.Segments;
import org.apache.paimon.options.MemorySize;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;

import static org.apache.paimon.CoreOptions.PAGE_SIZE;

/** Cache {@link Segments}. */
public class SegmentsCache<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentsCache.class);

    private static final int OBJECT_MEMORY_SIZE = 1000;

    /**
     * Approximate per-{@code MemorySegment} bookkeeping overhead (object header + array header +
     * references). With strong references this term turns {@link #weigh} into a real OOM safety net
     * rather than just a hint, which matters most when pages are small and a cached manifest holds
     * many segments.
     */
    private static final long PER_SEGMENT_OVERHEAD = 64;

    private final int pageSize;
    private final Cache<T, Segments> cache;
    private final MemorySize maxMemorySize;
    private final long maxElementSize;
    @Nullable private final Duration expireAfterAccess;
    private final boolean softValues;

    public SegmentsCache(int pageSize, MemorySize maxMemorySize, long maxElementSize) {
        this(pageSize, maxMemorySize, maxElementSize, null, true);
    }

    public SegmentsCache(
            int pageSize,
            MemorySize maxMemorySize,
            long maxElementSize,
            @Nullable Duration expireAfterAccess,
            boolean softValues) {
        this.pageSize = pageSize;
        Caffeine<T, Segments> builder =
                Caffeine.newBuilder()
                        .weigher(this::weigh)
                        .maximumWeight(maxMemorySize.getBytes())
                        .removalListener(this::onRemoval)
                        .executor(Runnable::run);
        // No idle TTL is applied unless one is explicitly supplied, preserving the original
        // behaviour where entries are only evicted by weight (or GC, when soft values are on).
        if (expireAfterAccess != null) {
            builder.expireAfterAccess(expireAfterAccess);
        }
        // When soft values are enabled, entries may be reclaimed by the GC under memory pressure,
        // which can trigger a cache-thrash spiral. Disabling them pins the working set with strong
        // references, breaking the spiral at the cost of deterministic heap occupancy.
        if (softValues) {
            builder.softValues();
        }
        this.cache = builder.build();
        this.maxMemorySize = maxMemorySize;
        this.maxElementSize = maxElementSize;
        this.expireAfterAccess = expireAfterAccess;
        this.softValues = softValues;
    }

    public int pageSize() {
        return pageSize;
    }

    public MemorySize maxMemorySize() {
        return maxMemorySize;
    }

    public long maxElementSize() {
        return maxElementSize;
    }

    @Nullable
    public Duration ttl() {
        return expireAfterAccess;
    }

    public boolean softValues() {
        return softValues;
    }

    @Nullable
    public Segments getIfPresents(T key) {
        return cache.getIfPresent(key);
    }

    public void put(T key, Segments segments) {
        cache.put(key, segments);
    }

    private int weigh(T cacheKey, Segments segments) {
        int n =
                segments instanceof MultiSegments
                        ? ((MultiSegments) segments).segments().size()
                        : 1;
        return (int)
                (OBJECT_MEMORY_SIZE + (long) n * PER_SEGMENT_OVERHEAD + segments.totalMemorySize());
    }

    private void onRemoval(@Nullable T key, @Nullable Segments value, RemovalCause cause) {
        if (LOG.isInfoEnabled()) {
            LOG.info("SegmentsCache entry removed: key={}, cause={}", key, cause);
        }
    }

    @Nullable
    public static <T> SegmentsCache<T> create(MemorySize maxMemorySize, long maxElementSize) {
        return create((int) PAGE_SIZE.defaultValue().getBytes(), maxMemorySize, maxElementSize);
    }

    @Nullable
    public static <T> SegmentsCache<T> create(
            int pageSize, MemorySize maxMemorySize, long maxElementSize) {
        return create(pageSize, maxMemorySize, maxElementSize, null, true);
    }

    @Nullable
    public static <T> SegmentsCache<T> create(
            int pageSize,
            MemorySize maxMemorySize,
            long maxElementSize,
            @Nullable Duration expireAfterAccess,
            boolean softValues) {
        if (maxMemorySize.getBytes() == 0) {
            return null;
        }

        return new SegmentsCache<>(
                pageSize, maxMemorySize, maxElementSize, expireAfterAccess, softValues);
    }

    public long estimatedSize() {
        return cache.estimatedSize();
    }

    public long totalCacheBytes() {
        return cache.asMap().entrySet().stream()
                .mapToLong(entry -> weigh(entry.getKey(), entry.getValue()))
                .sum();
    }
}
