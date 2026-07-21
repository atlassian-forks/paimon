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

package org.apache.paimon.flink.source.assigners;

import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.FlinkRowData;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.utils.BinPacking;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.connector.source.DynamicFilteringData;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.utils.TableScanUtils.getSnapshotId;

/**
 * Pre-calculate which splits each task should process according to the weight or given
 * DynamicFilteringData, and then distribute the splits fairly.
 */
public class PreAssignSplitAssigner implements SplitAssigner {

    /** Default batch splits size to avoid exceed `akka.framesize`. */
    private final int splitBatchSize;

    private final int parallelism;

    private final Map<Integer, LinkedList<FileStoreSourceSplit>> pendingSplitAssignment;

    private final AtomicInteger numberOfPendingSplits;
    private final Collection<FileStoreSourceSplit> splits;
    private final SerializableFunction<FileStoreSourceSplit, Long> weightFunc;

    @Nullable private final SerializableFunction<FileStoreSourceSplit, ?> groupFunc;
    @Nullable private final SerializableFunction<FileStoreSourceSplit, ?> spreadGroupFunc;
    private final int maxReadersPerSpreadGroup;

    public PreAssignSplitAssigner(
            int splitBatchSize,
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> splits) {
        this(splitBatchSize, context.currentParallelism(), splits);
    }

    public PreAssignSplitAssigner(
            int splitBatchSize,
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> splits,
            SerializableFunction<FileStoreSourceSplit, Long> weightFunc) {
        this(splitBatchSize, context.currentParallelism(), splits, weightFunc);
    }

    public PreAssignSplitAssigner(
            int splitBatchSize,
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> splits,
            SerializableFunction<FileStoreSourceSplit, Long> weightFunc,
            @Nullable SerializableFunction<FileStoreSourceSplit, ?> groupFunc) {
        this(splitBatchSize, context.currentParallelism(), splits, weightFunc, groupFunc);
    }

    public PreAssignSplitAssigner(
            int splitBatchSize,
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> splits,
            SerializableFunction<FileStoreSourceSplit, Long> weightFunc,
            @Nullable SerializableFunction<FileStoreSourceSplit, ?> groupFunc,
            @Nullable SerializableFunction<FileStoreSourceSplit, ?> spreadGroupFunc,
            int maxReadersPerSpreadGroup) {
        this(
                splitBatchSize,
                context.currentParallelism(),
                splits,
                weightFunc,
                groupFunc,
                spreadGroupFunc,
                maxReadersPerSpreadGroup);
    }

    public PreAssignSplitAssigner(
            int splitBatchSize,
            int parallelism,
            Collection<FileStoreSourceSplit> splits,
            Projection partitionRowProjection,
            DynamicFilteringData dynamicFilteringData) {
        this(
                splitBatchSize,
                parallelism,
                splits,
                partitionRowProjection,
                dynamicFilteringData,
                split -> split.split().rowCount());
    }

    public PreAssignSplitAssigner(
            int splitBatchSize,
            int parallelism,
            Collection<FileStoreSourceSplit> splits,
            Projection partitionRowProjection,
            DynamicFilteringData dynamicFilteringData,
            SerializableFunction<FileStoreSourceSplit, Long> weightFunc) {
        this(
                splitBatchSize,
                parallelism,
                splits.stream()
                        .filter(s -> filter(partitionRowProjection, dynamicFilteringData, s))
                        .collect(Collectors.toList()),
                weightFunc);
    }

    public PreAssignSplitAssigner(
            int splitBatchSize, int parallelism, Collection<FileStoreSourceSplit> splits) {
        this(splitBatchSize, parallelism, splits, split -> split.split().rowCount());
    }

    public PreAssignSplitAssigner(
            int splitBatchSize,
            int parallelism,
            Collection<FileStoreSourceSplit> splits,
            SerializableFunction<FileStoreSourceSplit, Long> weightFunc) {
        this(splitBatchSize, parallelism, splits, weightFunc, null);
    }

    public PreAssignSplitAssigner(
            int splitBatchSize,
            int parallelism,
            Collection<FileStoreSourceSplit> splits,
            SerializableFunction<FileStoreSourceSplit, Long> weightFunc,
            @Nullable SerializableFunction<FileStoreSourceSplit, ?> groupFunc) {
        this(splitBatchSize, parallelism, splits, weightFunc, groupFunc, null, -1);
    }

    public PreAssignSplitAssigner(
            int splitBatchSize,
            int parallelism,
            Collection<FileStoreSourceSplit> splits,
            SerializableFunction<FileStoreSourceSplit, Long> weightFunc,
            @Nullable SerializableFunction<FileStoreSourceSplit, ?> groupFunc,
            @Nullable SerializableFunction<FileStoreSourceSplit, ?> spreadGroupFunc,
            int maxReadersPerSpreadGroup) {
        this.splitBatchSize = splitBatchSize;
        this.parallelism = parallelism;
        this.splits = splits;
        this.weightFunc = weightFunc;
        this.groupFunc = groupFunc;
        this.spreadGroupFunc = spreadGroupFunc;
        this.maxReadersPerSpreadGroup = maxReadersPerSpreadGroup;
        this.pendingSplitAssignment =
                createBatchFairSplitAssignment(
                        splits,
                        parallelism,
                        weightFunc,
                        groupFunc,
                        spreadGroupFunc,
                        maxReadersPerSpreadGroup);
        this.numberOfPendingSplits = new AtomicInteger(splits.size());
    }

    @Override
    public List<FileStoreSourceSplit> getNext(int subtask, @Nullable String hostname) {
        // The following batch assignment operation is for two purposes:
        // To distribute splits evenly when batch reading to prevent a few tasks from reading all
        // the data (for example, the current resource can only schedule part of the tasks).
        Queue<FileStoreSourceSplit> taskSplits = pendingSplitAssignment.get(subtask);
        List<FileStoreSourceSplit> assignment = new ArrayList<>();
        while (taskSplits != null && !taskSplits.isEmpty() && assignment.size() < splitBatchSize) {
            assignment.add(taskSplits.poll());
        }
        numberOfPendingSplits.getAndAdd(-assignment.size());
        return assignment;
    }

    @Override
    public void addSplit(int suggestedTask, FileStoreSourceSplit split) {
        pendingSplitAssignment.computeIfAbsent(suggestedTask, k -> new LinkedList<>()).add(split);
        numberOfPendingSplits.incrementAndGet();
    }

    @Override
    public void addSplitsBack(int subtask, List<FileStoreSourceSplit> splits) {
        LinkedList<FileStoreSourceSplit> remainingSplits =
                pendingSplitAssignment.computeIfAbsent(subtask, k -> new LinkedList<>());
        ListIterator<FileStoreSourceSplit> iterator = splits.listIterator(splits.size());
        while (iterator.hasPrevious()) {
            remainingSplits.addFirst(iterator.previous());
        }
        numberOfPendingSplits.getAndAdd(splits.size());
    }

    @Override
    public Collection<FileStoreSourceSplit> remainingSplits() {
        List<FileStoreSourceSplit> splits = new ArrayList<>();
        pendingSplitAssignment.values().forEach(splits::addAll);
        return splits;
    }

    /**
     * this method only reload restore for batch execute, because in streaming mode, we need to
     * assign certain bucket to certain task.
     */
    private static Map<Integer, LinkedList<FileStoreSourceSplit>> createBatchFairSplitAssignment(
            Collection<FileStoreSourceSplit> splits,
            int numReaders,
            SerializableFunction<FileStoreSourceSplit, Long> weightFunc,
            @Nullable SerializableFunction<FileStoreSourceSplit, ?> groupFunc,
            @Nullable SerializableFunction<FileStoreSourceSplit, ?> spreadGroupFunc,
            int maxReadersPerSpreadGroup) {
        Map<Integer, LinkedList<FileStoreSourceSplit>> assignment = new HashMap<>();
        if (groupFunc == null) {
            List<List<FileStoreSourceSplit>> assignmentList =
                    BinPacking.packForFixedBinNumber(splits, weightFunc, numReaders);
            for (int i = 0; i < assignmentList.size(); i++) {
                assignment.put(i, new LinkedList<>(assignmentList.get(i)));
            }
        } else {
            List<SplitGroup> splitGroups = createSplitGroups(splits, weightFunc, groupFunc);
            if (spreadGroupFunc != null && maxReadersPerSpreadGroup > 0) {
                assignment =
                        createSpreadLimitedAssignment(
                                splitGroups, numReaders, spreadGroupFunc, maxReadersPerSpreadGroup);
            } else {
                List<List<SplitGroup>> assignmentList =
                        BinPacking.packForFixedBinNumber(
                                splitGroups, SplitGroup::weight, numReaders);
                for (int i = 0; i < assignmentList.size(); i++) {
                    LinkedList<FileStoreSourceSplit> assignedSplits = new LinkedList<>();
                    assignmentList.get(i).forEach(group -> assignedSplits.addAll(group.splits()));
                    assignment.put(i, assignedSplits);
                }
            }
        }
        return assignment;
    }

    private static List<SplitGroup> createSplitGroups(
            Collection<FileStoreSourceSplit> splits,
            SerializableFunction<FileStoreSourceSplit, Long> weightFunc,
            SerializableFunction<FileStoreSourceSplit, ?> groupFunc) {
        Map<Object, SplitGroup> groups = new HashMap<>();
        for (FileStoreSourceSplit split : splits) {
            Object key = groupFunc.apply(split);
            groups.computeIfAbsent(key, ignored -> new SplitGroup())
                    .add(split, weightFunc.apply(split));
        }
        return new ArrayList<>(groups.values());
    }

    private static Map<Integer, LinkedList<FileStoreSourceSplit>> createSpreadLimitedAssignment(
            List<SplitGroup> splitGroups,
            int numReaders,
            SerializableFunction<FileStoreSourceSplit, ?> spreadGroupFunc,
            int maxReadersPerSpreadGroup) {
        Map<Integer, LinkedList<FileStoreSourceSplit>> assignment = new HashMap<>();
        long[] readerWeights = new long[numReaders];

        // Step 1: bucket groups have already been created by groupFunc, for example one
        // SplitGroup per (partition, bucket). Group those bucket groups again by spread key,
        // for example by partition, so we can limit how many readers receive buckets from the
        // same partition.
        Map<Object, List<SplitGroup>> groupsBySpreadKey = new HashMap<>();
        for (SplitGroup splitGroup : splitGroups) {
            Object spreadKey = spreadGroupFunc.apply(splitGroup.splits().get(0));
            groupsBySpreadKey
                    .computeIfAbsent(spreadKey, ignored -> new ArrayList<>())
                    .add(splitGroup);
        }

        for (List<SplitGroup> groups : groupsBySpreadKey.values()) {
            // Step 2: for each spread group, pick the currently lightest N readers as candidates.
            // This keeps global load roughly balanced while preventing a single partition from
            // spreading its buckets across too many readers at the same time.
            List<Integer> candidateReaders =
                    selectLightestReaders(
                            readerWeights, Math.min(maxReadersPerSpreadGroup, numReaders));
            long[] candidateWeights = new long[candidateReaders.size()];
            for (int i = 0; i < candidateReaders.size(); i++) {
                candidateWeights[i] = readerWeights[candidateReaders.get(i)];
            }
            // Step 3: assign this spread group's bucket groups only within the selected readers.
            // Use LPT-style greedy assignment: process heavier bucket groups first and always put
            // the next bucket group onto the lightest candidate reader.
            groups.sort((left, right) -> Long.compare(right.weight(), left.weight()));
            for (SplitGroup group : groups) {
                int candidateIndex = lightestReaderIndex(candidateWeights);
                int reader = candidateReaders.get(candidateIndex);
                assignment
                        .computeIfAbsent(reader, ignored -> new LinkedList<>())
                        .addAll(group.splits());
                readerWeights[reader] += group.weight();
                candidateWeights[candidateIndex] += group.weight();
            }
        }
        for (int reader = 0; reader < numReaders; reader++) {
            assignment.computeIfAbsent(reader, ignored -> new LinkedList<>());
        }
        return assignment;
    }

    private static List<Integer> selectLightestReaders(long[] readerWeights, int count) {
        List<Integer> readers = new ArrayList<>();
        boolean[] selected = new boolean[readerWeights.length];
        for (int i = 0; i < count; i++) {
            int selectedReader = -1;
            for (int reader = 0; reader < readerWeights.length; reader++) {
                if (!selected[reader]
                        && (selectedReader < 0
                                || readerWeights[reader] < readerWeights[selectedReader])) {
                    selectedReader = reader;
                }
            }
            selected[selectedReader] = true;
            readers.add(selectedReader);
        }
        return readers;
    }

    private static int lightestReaderIndex(long[] readerWeights) {
        int selectedReader = 0;
        for (int reader = 1; reader < readerWeights.length; reader++) {
            if (readerWeights[reader] < readerWeights[selectedReader]) {
                selectedReader = reader;
            }
        }
        return selectedReader;
    }

    private static class SplitGroup {

        private final List<FileStoreSourceSplit> splits = new ArrayList<>();
        private long weight;

        private void add(FileStoreSourceSplit split, long splitWeight) {
            splits.add(split);
            weight += splitWeight;
        }

        private List<FileStoreSourceSplit> splits() {
            return splits;
        }

        private long weight() {
            return weight;
        }
    }

    @Override
    public Optional<Long> getNextSnapshotId(int subtask) {
        LinkedList<FileStoreSourceSplit> pendingSplits = pendingSplitAssignment.get(subtask);
        return (pendingSplits == null || pendingSplits.isEmpty())
                ? Optional.empty()
                : getSnapshotId(pendingSplits.peekFirst());
    }

    @Override
    public int numberOfRemainingSplits() {
        return numberOfPendingSplits.get();
    }

    public SplitAssigner ofDynamicPartitionPruning(
            Projection partitionRowProjection, DynamicFilteringData dynamicFilteringData) {
        return new PreAssignSplitAssigner(
                splitBatchSize,
                parallelism,
                splits,
                partitionRowProjection,
                dynamicFilteringData,
                weightFunc);
    }

    private static boolean filter(
            Projection partitionRowProjection,
            DynamicFilteringData dynamicFilteringData,
            FileStoreSourceSplit sourceSplit) {
        DataSplit dataSplit = (DataSplit) sourceSplit.split();
        BinaryRow partition = dataSplit.partition();
        FlinkRowData projected = new FlinkRowData(partitionRowProjection.apply(partition));
        return dynamicFilteringData.contains(projected);
    }
}
