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

package org.apache.paimon.disk;

import org.apache.paimon.memory.Buffer;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.RateLogger;

import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/** A synchronous {@link BufferFileWriter} implementation. */
public class BufferFileWriterImpl extends AbstractFileIOChannel implements BufferFileWriter {

    // Sampled per-block rate logger.  Logs every N blocks + a final CLOSE line so that you can
    // tell what a stuck Paimon writer thread is doing on disk even when the rest of the operator
    // is silent (see thread-dump cases where the writer is blocked in FileChannel.write).
    private static final long SPILL_LOG_EVERY_BLOCKS =
            Long.getLong("paimon.spill.log.every.blocks", 1024L);

    private final RateLogger rate;

    protected BufferFileWriterImpl(ID channelID) throws IOException {
        super(channelID, true);
        this.rate =
                new RateLogger(
                        LoggerFactory.getLogger(BufferFileWriterImpl.class),
                        "spill-write " + channelID.getPath(),
                        SPILL_LOG_EVERY_BLOCKS);
        LOG.info("Spill writer OPEN path={}", channelID.getPath());
    }

    @Override
    public void writeBlock(Buffer buffer) throws IOException {
        int payload = buffer.getSize();
        ByteBuffer nioBufferReadable = buffer.getMemorySegment().wrap(0, payload).slice();
        ByteBuffer header = ByteBuffer.allocateDirect(4);
        header.putInt(nioBufferReadable.remaining());
        header.flip();

        FileIOUtils.writeCompletely(fileChannel, header);
        FileIOUtils.writeCompletely(fileChannel, nioBufferReadable);

        rate.onUnit(payload + 4L);
    }

    @Override
    public void close() throws IOException {
        try {
            rate.close();
            LOG.info(
                    "Spill writer CLOSE path={} blocks={} bytes={}",
                    id.getPath(),
                    rate.count(),
                    rate.bytes());
        } finally {
            super.close();
        }
    }
}
