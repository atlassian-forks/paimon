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
import org.apache.paimon.stats.SimpleStats;

import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Write response to initial data files for partition and bucket. */
public class ScanCoordinationResponse implements CoordinationResponse {

    private static final long serialVersionUID = 1L;

    @Nullable private final Snapshot snapshot;
    @Nullable private final Integer totalBuckets;
    @Nullable private final List<byte[]> dataFiles;
    @Nullable private final byte[] dynamicBucketIndex;
    @Nullable private final List<byte[]> deleteVectorsIndex;

    // Diagnostic-only values derived while the response is built. They are deliberately transient
    // so adding observability does not increase the coordinator response sent to TaskManagers.
    private transient long serializedDataFilesBytes;
    private transient long rawStatsPayloadBytes;
    private transient long rawKeyBoundsPayloadBytes;
    private transient long embeddedIndexPayloadBytes;
    private transient long serializedIndexBytes;

    public ScanCoordinationResponse(
            @Nullable Snapshot snapshot,
            @Nullable Integer totalBuckets,
            @Nullable List<DataFileMeta> dataFiles,
            @Nullable IndexFileMeta dynamicBucketIndex,
            @Nullable List<IndexFileMeta> deleteVectorsIndex)
            throws IOException {
        this.snapshot = snapshot;
        this.totalBuckets = totalBuckets;

        DataFileMetaSerializer serializer = new DataFileMetaSerializer();
        if (dataFiles != null) {
            this.dataFiles = new ArrayList<>(dataFiles.size());
            for (DataFileMeta dataFile : dataFiles) {
                byte[] serializedDataFile = serializer.serializeToBytes(dataFile);
                this.dataFiles.add(serializedDataFile);
                serializedDataFilesBytes += serializedDataFile.length;
                rawStatsPayloadBytes += statsPayloadBytes(dataFile);
                rawKeyBoundsPayloadBytes +=
                        (long) dataFile.minKey().getSizeInBytes()
                                + dataFile.maxKey().getSizeInBytes();
                byte[] embeddedIndex = dataFile.embeddedIndex();
                embeddedIndexPayloadBytes += embeddedIndex == null ? 0 : embeddedIndex.length;
            }
        } else {
            this.dataFiles = null;
        }

        IndexFileMetaSerializer indexSerializer = new IndexFileMetaSerializer();
        if (dynamicBucketIndex != null) {
            this.dynamicBucketIndex = indexSerializer.serializeToBytes(dynamicBucketIndex);
            serializedIndexBytes += this.dynamicBucketIndex.length;
        } else {
            this.dynamicBucketIndex = null;
        }

        if (deleteVectorsIndex != null) {
            this.deleteVectorsIndex = new ArrayList<>(deleteVectorsIndex.size());
            for (IndexFileMeta indexFile : deleteVectorsIndex) {
                byte[] serializedIndex = indexSerializer.serializeToBytes(indexFile);
                this.deleteVectorsIndex.add(serializedIndex);
                serializedIndexBytes += serializedIndex.length;
            }
        } else {
            this.deleteVectorsIndex = null;
        }
    }

    int dataFileCount() {
        return dataFiles == null ? 0 : dataFiles.size();
    }

    long serializedDataFilesBytes() {
        return serializedDataFilesBytes;
    }

    long rawStatsPayloadBytes() {
        return rawStatsPayloadBytes;
    }

    long rawKeyBoundsPayloadBytes() {
        return rawKeyBoundsPayloadBytes;
    }

    long embeddedIndexPayloadBytes() {
        return embeddedIndexPayloadBytes;
    }

    long serializedIndexBytes() {
        return serializedIndexBytes;
    }

    private static long statsPayloadBytes(DataFileMeta dataFile) {
        return statsPayloadBytes(dataFile.keyStats()) + statsPayloadBytes(dataFile.valueStats());
    }

    private static long statsPayloadBytes(SimpleStats stats) {
        return (long) stats.minValues().getSizeInBytes()
                + stats.maxValues().getSizeInBytes()
                + stats.nullCounts().getSizeInBytes();
    }

    @Nullable
    public Snapshot snapshot() {
        return snapshot;
    }

    @Nullable
    public Integer totalBuckets() {
        return totalBuckets;
    }

    @Nullable
    public List<DataFileMeta> extractDataFiles() throws IOException {
        if (dataFiles == null) {
            return null;
        }
        DataFileMetaSerializer serializer = new DataFileMetaSerializer();
        List<DataFileMeta> dataFileMetas = new ArrayList<>(dataFiles.size());
        for (byte[] file : dataFiles) {
            dataFileMetas.add(serializer.deserializeFromBytes(file));
        }
        return dataFileMetas;
    }

    @Nullable
    public IndexFileMeta extractDynamicBucketIndex() throws IOException {
        if (dynamicBucketIndex == null) {
            return null;
        }
        return new IndexFileMetaSerializer().deserializeFromBytes(dynamicBucketIndex);
    }

    @Nullable
    public List<IndexFileMeta> extractDeletionVectorsIndex() throws IOException {
        if (deleteVectorsIndex == null) {
            return null;
        }
        IndexFileMetaSerializer serializer = new IndexFileMetaSerializer();
        List<IndexFileMeta> metas = new ArrayList<>(deleteVectorsIndex.size());
        for (byte[] file : deleteVectorsIndex) {
            metas.add(serializer.deserializeFromBytes(file));
        }
        return metas;
    }
}
