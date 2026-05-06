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
import org.apache.paimon.utils.RateLogger;

import org.slf4j.LoggerFactory;

import java.io.IOException;

/** A synchronous {@link BufferFileReader} implementation. */
public class BufferFileReaderImpl extends AbstractFileIOChannel implements BufferFileReader {

    // Sampled per-block rate logger for spill reads. See BufferFileWriterImpl for rationale.
    private static final long SPILL_LOG_EVERY_BLOCKS =
            Long.getLong("paimon.spill.log.every.blocks", 1024L);

    private final BufferFileChannelReader reader;
    private final RateLogger rate;

    private boolean hasReachedEndOfFile;

    public BufferFileReaderImpl(ID channelID) throws IOException {
        super(channelID, false);
        this.reader = new BufferFileChannelReader(fileChannel);
        this.rate =
                new RateLogger(
                        LoggerFactory.getLogger(BufferFileReaderImpl.class),
                        "spill-read " + channelID.getPath(),
                        SPILL_LOG_EVERY_BLOCKS);
        LOG.info("Spill reader OPEN path={}", channelID.getPath());
    }

    @Override
    public void readInto(Buffer buffer) throws IOException {
        hasReachedEndOfFile = reader.readBufferFromFileChannel(buffer);
        rate.onUnit(buffer.getSize() + 4L);
    }

    @Override
    public boolean hasReachedEndOfFile() {
        return hasReachedEndOfFile;
    }

    @Override
    public void close() throws IOException {
        try {
            rate.close();
            LOG.info(
                    "Spill reader CLOSE path={} blocks={} bytes={} reachedEnd={}",
                    id.getPath(),
                    rate.count(),
                    rate.bytes(),
                    hasReachedEndOfFile);
        } finally {
            super.close();
        }
    }
}
