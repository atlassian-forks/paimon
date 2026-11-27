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

package org.apache.paimon.flink.sink;

import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.options.Options;

import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that {@link PrepareCommitOperator} reports the checkpoint flush as idle time. */
public class PrepareCommitOperatorIdleTimeTest {

    private static final long FLUSH_MILLIS = 100;

    @Test
    public void testFlushCountedAsIdleWhenEnabled() throws Exception {
        try (Harness h = new Harness(true, FlushAction.SLEEP)) {
            TimerGauge idle = h.idleTimer();
            assertThat(idle.getAccumulatedCount()).isEqualTo(0);

            h.harness.prepareSnapshotPreBarrier(1L);

            assertThat(h.operator().timerRunningDuringFlush).isTrue();
            assertThat(idle.isMeasuring()).isFalse();
            // allow a little clock granularity slack
            assertThat(idle.getAccumulatedCount()).isGreaterThanOrEqualTo(FLUSH_MILLIS - 20);
        }
    }

    @Test
    public void testIdleTimerUntouchedWhenDisabled() throws Exception {
        try (Harness h = new Harness(false, FlushAction.SLEEP)) {
            TimerGauge idle = h.idleTimer();

            h.harness.prepareSnapshotPreBarrier(1L);

            assertThat(h.operator().timerRunningDuringFlush).isFalse();
            assertThat(idle.isMeasuring()).isFalse();
            assertThat(idle.getAccumulatedCount()).isEqualTo(0);
        }
    }

    @Test
    public void testIdleTimerStoppedWhenFlushThrows() throws Exception {
        try (Harness h = new Harness(true, FlushAction.THROW)) {
            TimerGauge idle = h.idleTimer();

            assertThatThrownBy(() -> h.harness.prepareSnapshotPreBarrier(1L))
                    .isInstanceOf(IOException.class)
                    .hasMessage("flush failed");

            assertThat(h.operator().timerRunningDuringFlush).isTrue();
            assertThat(idle.isMeasuring()).isFalse();
        }
    }

    @Test
    public void testAlreadyRunningIdleTimerIsNotStopped() throws Exception {
        try (Harness h = new Harness(true, FlushAction.SLEEP)) {
            TimerGauge idle = h.idleTimer();
            // Simulate the (not expected) case that the mailbox's own idle measurement is active
            // when the flush starts: the operator must not end a measurement it did not start.
            idle.markStart();

            h.harness.prepareSnapshotPreBarrier(1L);

            assertThat(idle.isMeasuring()).isTrue();
            idle.markEnd();
        }
    }

    @Test
    public void testEndInputFlushCountedAsIdleWhenEnabled() throws Exception {
        try (Harness h = new Harness(true, FlushAction.SLEEP)) {
            TimerGauge idle = h.idleTimer();

            h.harness.endInput();

            assertThat(h.operator().timerRunningDuringFlush).isTrue();
            assertThat(idle.isMeasuring()).isFalse();
            assertThat(idle.getAccumulatedCount()).isGreaterThanOrEqualTo(FLUSH_MILLIS - 20);
        }
    }

    private enum FlushAction {
        SLEEP,
        THROW
    }

    /** Operator whose {@code prepareCommit} blocks (like an S3 flush) or fails. */
    private static class BlockingFlushOperator extends PrepareCommitOperator<String, String> {

        private final FlushAction action;
        volatile boolean timerRunningDuringFlush;

        BlockingFlushOperator(
                StreamOperatorParameters<String> parameters, Options options, FlushAction action) {
            super(parameters, options);
            this.action = action;
        }

        @Override
        protected List<String> prepareCommit(boolean waitCompaction, long checkpointId)
                throws IOException {
            timerRunningDuringFlush =
                    getContainingTask()
                            .getEnvironment()
                            .getMetricGroup()
                            .getIOMetricGroup()
                            .getIdleTimeMsPerSecond()
                            .isMeasuring();
            if (action == FlushAction.THROW) {
                throw new IOException("flush failed");
            }
            try {
                Thread.sleep(FLUSH_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            return Collections.singletonList("committable");
        }

        @Override
        public void processElement(StreamRecord<String> element) {}
    }

    private static class BlockingFlushFactory
            extends PrepareCommitOperator.Factory<String, String> {

        private final FlushAction action;

        BlockingFlushFactory(Options options, FlushAction action) {
            super(options);
            this.action = action;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends StreamOperator<String>> T createStreamOperator(
                StreamOperatorParameters<String> parameters) {
            return (T) new BlockingFlushOperator(parameters, options, action);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return BlockingFlushOperator.class;
        }
    }

    private static class Harness implements AutoCloseable {

        final OneInputStreamOperatorTestHarness<String, String> harness;

        Harness(boolean reportIoWaitAsIdle, FlushAction action) throws Exception {
            Options options = new Options();
            options.set(FlinkConnectorOptions.SINK_REPORT_IO_WAIT_AS_IDLE, reportIoWaitAsIdle);
            harness =
                    new OneInputStreamOperatorTestHarness<>(
                            new BlockingFlushFactory(options, action));
            harness.open();
        }

        BlockingFlushOperator operator() {
            return (BlockingFlushOperator) harness.getOperator();
        }

        TimerGauge idleTimer() {
            return harness.getEnvironment()
                    .getMetricGroup()
                    .getIOMetricGroup()
                    .getIdleTimeMsPerSecond();
        }

        @Override
        public void close() throws Exception {
            harness.close();
        }
    }
}
