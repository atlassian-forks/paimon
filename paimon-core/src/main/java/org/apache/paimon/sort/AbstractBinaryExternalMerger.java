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

package org.apache.paimon.sort;

import org.apache.paimon.compression.BlockCompressionFactory;
import org.apache.paimon.data.AbstractPagedOutputView;
import org.apache.paimon.disk.ChannelReaderInputView;
import org.apache.paimon.disk.ChannelWithMeta;
import org.apache.paimon.disk.ChannelWriterOutputView;
import org.apache.paimon.disk.FileChannelUtil;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.utils.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Spilled files Merger of {@link BinaryExternalSortBuffer}. It merges {@link #maxFanIn} spilled
 * files at most once.
 *
 * @param <Entry> Type of Entry to Merge sort.
 */
public abstract class AbstractBinaryExternalMerger<Entry> implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractBinaryExternalMerger.class);

    private volatile boolean closed;

    private final int maxFanIn;
    private final SpillChannelManager channelManager;
    private final BlockCompressionFactory compressionCodecFactory;
    private final int compressionBlockSize;

    /**
     * Best-effort identifier (e.g. "t=<table> p=<partitionString> b=<bucket>") used as a prefix on
     * all log lines so they can be attributed to a particular Paimon writer / partition / bucket
     * even when several writers share the same TaskManager. Empty string when unknown.
     */
    protected final String identifier;

    protected final int pageSize;
    protected final IOManager ioManager;

    public AbstractBinaryExternalMerger(
            IOManager ioManager,
            int pageSize,
            int maxFanIn,
            SpillChannelManager channelManager,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize) {
        this(
                ioManager,
                pageSize,
                maxFanIn,
                channelManager,
                compressionCodecFactory,
                compressionBlockSize,
                "");
    }

    public AbstractBinaryExternalMerger(
            IOManager ioManager,
            int pageSize,
            int maxFanIn,
            SpillChannelManager channelManager,
            BlockCompressionFactory compressionCodecFactory,
            int compressionBlockSize,
            String identifier) {
        this.ioManager = ioManager;
        this.pageSize = pageSize;
        this.maxFanIn = maxFanIn;
        this.channelManager = channelManager;
        this.compressionCodecFactory = compressionCodecFactory;
        this.compressionBlockSize = compressionBlockSize;
        this.identifier = identifier == null ? "" : identifier;
    }

    @Override
    public void close() {
        this.closed = true;
    }

    /**
     * Returns an iterator that iterates over the merged result from all given channels.
     *
     * @param channelIDs The channels that are to be merged and returned.
     * @return An iterator over the merged records of the input channels.
     * @throws IOException Thrown, if the readers encounter an I/O problem.
     */
    public BinaryMergeIterator<Entry> getMergingIterator(
            List<ChannelWithMeta> channelIDs, List<FileIOChannel> openChannels) throws IOException {
        // create one iterator per channel id
        if (LOG.isDebugEnabled()) {
            LOG.debug("Performing merge of " + channelIDs.size() + " sorted streams.");
        }

        final List<MutableObjectIterator<Entry>> iterators = new ArrayList<>(channelIDs.size() + 1);

        for (ChannelWithMeta channel : channelIDs) {
            ChannelReaderInputView view =
                    FileChannelUtil.createInputView(
                            ioManager,
                            channel,
                            openChannels,
                            compressionCodecFactory,
                            compressionBlockSize);
            iterators.add(channelReaderInputViewIterator(view));
        }

        return new BinaryMergeIterator<>(
                iterators, mergeReusedEntries(channelIDs.size()), mergeComparator());
    }

    /**
     * Merges the given sorted runs to a smaller number of sorted runs.
     *
     * @param channelIDs The IDs of the sorted runs that need to be merged.
     * @return A list of the IDs of the merged channels.
     * @throws IOException Thrown, if the readers or writers encountered an I/O problem.
     */
    public List<ChannelWithMeta> mergeChannelList(List<ChannelWithMeta> channelIDs)
            throws IOException {
        long mergeRoundStart = System.nanoTime();
        long totalIn = 0L;
        for (ChannelWithMeta m : channelIDs) {
            totalIn += m.getNumBytes();
        }
        LOG.info(
                "[{}] mergeChannelList ROUND start inputChannels={} totalInputBytes={} maxFanIn={}",
                identifier,
                channelIDs.size(),
                totalIn,
                maxFanIn);
        try {
            return mergeChannelListInternal(channelIDs);
        } finally {
            LOG.info(
                    "[{}] mergeChannelList ROUND end   inputChannels={} elapsedMs={}",
                    identifier,
                    channelIDs.size(),
                    (System.nanoTime() - mergeRoundStart) / 1_000_000);
        }
    }

    private List<ChannelWithMeta> mergeChannelListInternal(List<ChannelWithMeta> channelIDs)
            throws IOException {
        // A channel list with length maxFanIn<sup>i</sup> can be merged to maxFanIn files in i-1
        // rounds where every merge
        // is a full merge with maxFanIn input channels. A partial round includes merges with fewer
        // than maxFanIn
        // inputs. It is most efficient to perform the partial round first.
        final double scale = Math.ceil(Math.log(channelIDs.size()) / Math.log(maxFanIn)) - 1;

        final int numStart = channelIDs.size();
        final int numEnd = (int) Math.pow(maxFanIn, scale);

        final int numMerges = (int) Math.ceil((numStart - numEnd) / (double) (maxFanIn - 1));

        final int numNotMerged = numEnd - numMerges;
        final int numToMerge = numStart - numNotMerged;

        // unmerged channel IDs are copied directly to the result list
        final List<ChannelWithMeta> mergedChannelIDs = new ArrayList<>(numEnd);
        mergedChannelIDs.addAll(channelIDs.subList(0, numNotMerged));

        final int channelsToMergePerStep = (int) Math.ceil(numToMerge / (double) numMerges);

        final List<ChannelWithMeta> channelsToMergeThisStep =
                new ArrayList<>(channelsToMergePerStep);
        int channelNum = numNotMerged;
        while (!closed && channelNum < channelIDs.size()) {
            channelsToMergeThisStep.clear();

            for (int i = 0;
                    i < channelsToMergePerStep && channelNum < channelIDs.size();
                    i++, channelNum++) {
                channelsToMergeThisStep.add(channelIDs.get(channelNum));
            }

            mergedChannelIDs.add(mergeChannels(channelsToMergeThisStep));
        }

        return mergedChannelIDs;
    }

    /**
     * Merges the sorted runs described by the given Channel IDs into a single sorted run.
     *
     * @param channelIDs The IDs of the runs' channels.
     * @return The ID and number of blocks of the channel that describes the merged run.
     */
    private ChannelWithMeta mergeChannels(List<ChannelWithMeta> channelIDs) throws IOException {
        // the list with the target iterators
        List<FileIOChannel> openChannels = new ArrayList<>(channelIDs.size());
        final BinaryMergeIterator<Entry> mergeIterator =
                getMergingIterator(channelIDs, openChannels);

        // create a new channel writer
        final FileIOChannel.ID mergedChannelID = ioManager.createChannel();
        channelManager.addChannel(mergedChannelID);
        ChannelWriterOutputView output = null;

        long inputBytesEstimate = 0L;
        long inputBlocks = 0L;
        for (ChannelWithMeta meta : channelIDs) {
            inputBytesEstimate += meta.getNumBytes();
            inputBlocks += meta.getBlockCount();
        }
        long t0 = System.nanoTime();
        LOG.info(
                "[{}] External merge START inputChannels={} inputBlocks={} inputBytes~={} -> outputPath={}",
                identifier,
                channelIDs.size(),
                inputBlocks,
                inputBytesEstimate,
                mergedChannelID.getPath());

        int numBlocksWritten;
        try {
            output =
                    FileChannelUtil.createOutputView(
                            ioManager,
                            mergedChannelID,
                            compressionCodecFactory,
                            compressionBlockSize);
            writeMergingOutput(mergeIterator, output);
            output.close();
            numBlocksWritten = output.getBlockCount();
        } catch (IOException e) {
            if (output != null) {
                output.close();
                output.getChannel().deleteChannel();
            }
            LOG.warn(
                    "[{}] External merge FAILED outputPath={} after elapsedMs={}",
                    identifier,
                    mergedChannelID.getPath(),
                    (System.nanoTime() - t0) / 1_000_000,
                    e);
            throw e;
        } finally {
            // remove, close and delete channels
            for (FileIOChannel channel : openChannels) {
                channelManager.removeChannel(channel.getChannelID());
                try {
                    channel.closeAndDelete();
                } catch (Throwable ignored) {
                }
            }
        }

        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        long outBytes = output.getWriteBytes();
        LOG.info(
                "[{}] External merge END   inputChannels={} outputPath={} outputBlocks={} outputBytes={} elapsedMs={} writeMBps={}",
                identifier,
                channelIDs.size(),
                mergedChannelID.getPath(),
                numBlocksWritten,
                outBytes,
                elapsedMs,
                String.format(
                        "%.2f", outBytes / 1024.0 / 1024.0 / Math.max(elapsedMs / 1000.0, 1e-9)));

        return new ChannelWithMeta(mergedChannelID, numBlocksWritten, output.getWriteBytes());
    }

    // -------------------------------------------------------------------------------------------

    /** @return entry iterator reading from inView. */
    protected abstract MutableObjectIterator<Entry> channelReaderInputViewIterator(
            ChannelReaderInputView inView);

    /** @return merging comparator used in merging. */
    protected abstract Comparator<Entry> mergeComparator();

    /** @return reused entry object used in merging. */
    protected abstract List<Entry> mergeReusedEntries(int size);

    /** read the merged stream and write the data back. */
    protected abstract void writeMergingOutput(
            MutableObjectIterator<Entry> mergeIterator, AbstractPagedOutputView output)
            throws IOException;
}
