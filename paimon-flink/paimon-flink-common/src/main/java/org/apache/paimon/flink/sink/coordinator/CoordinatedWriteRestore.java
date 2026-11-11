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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.operation.RestoreFiles;
import org.apache.paimon.operation.WriteRestore;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.ArrayUtils;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.MetadataInitializationLog;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.apache.paimon.utils.InstantiationUtil.serializeObject;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/**
 * {@link WriteRestore} to restore files from write coordinator, there is cache in coordinator, this
 * is to avoid a large number of duplicate scans of manifest files.
 */
public class CoordinatedWriteRestore implements WriteRestore {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatedWriteRestore.class);

    private final TaskOperatorEventGateway gateway;
    private final OperatorID operatorID;
    private final boolean spillEnabled;
    private final int spillBufferSize;
    @Nullable private final Path spillDirectory;

    public CoordinatedWriteRestore(TaskOperatorEventGateway gateway, OperatorID operatorID) {
        this(gateway, operatorID, false, 64 * 1024, null);
    }

    public CoordinatedWriteRestore(
            TaskOperatorEventGateway gateway,
            OperatorID operatorID,
            boolean spillEnabled,
            int spillBufferSize) {
        this(gateway, operatorID, spillEnabled, spillBufferSize, null);
    }

    public CoordinatedWriteRestore(
            TaskOperatorEventGateway gateway,
            OperatorID operatorID,
            boolean spillEnabled,
            int spillBufferSize,
            @Nullable Path spillDirectory) {
        if (spillEnabled && spillBufferSize <= 0) {
            throw new IllegalArgumentException("spillBufferSize must be greater than 0.");
        }
        this.gateway = gateway;
        this.operatorID = operatorID;
        this.spillEnabled = spillEnabled;
        this.spillBufferSize = spillBufferSize;
        this.spillDirectory = spillDirectory;
    }

    public static CoordinatedWriteRestore fromTableOptions(
            TaskOperatorEventGateway gateway, OperatorID operatorID, Options options) {
        boolean spillEnabled =
                options.get(
                        org.apache.paimon.flink.FlinkConnectorOptions
                                .SINK_WRITER_COORDINATOR_SPILL_ENABLED);
        return new CoordinatedWriteRestore(
                gateway,
                operatorID,
                spillEnabled,
                spillEnabled ? WriterCoordinatorSpillFile.configuredBufferSize(options) : 0,
                spillEnabled ? WriterCoordinatorSpillFile.configuredDirectory(options) : null);
    }

    @Override
    public long latestCommittedIdentifier(String user) {
        LatestIdentifierRequest request = new LatestIdentifierRequest(user);
        try {
            SerializedValue<CoordinationRequest> serializedRequest = new SerializedValue<>(request);
            LatestIdentifierResponse response =
                    CoordinationResponseUtils.unwrap(
                            gateway.sendRequestToCoordinator(operatorID, serializedRequest).get());
            return response.latestIdentifier();
        } catch (IOException | ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RestoreFiles restoreFiles(
            BinaryRow partition,
            int bucket,
            boolean scanDynamicBucketIndex,
            boolean scanDeleteVectorsIndex) {
        try (MetadataInitializationLog initialization =
                MetadataInitializationLog.start(
                        LOG,
                        "coordinated-restore-rpc-and-decode",
                        "operator="
                                + operatorID
                                + ",bucket="
                                + bucket
                                + ",spill="
                                + spillEnabled)) {
            ScanCoordinationRequest coordinationRequest =
                    new ScanCoordinationRequest(
                            serializeBinaryRow(partition),
                            bucket,
                            scanDynamicBucketIndex,
                            scanDeleteVectorsIndex);
            try {
                byte[] requestContent = serializeObject(coordinationRequest);
                RestoreFiles restored =
                        spillEnabled
                                ? requestAndDeserializeSpilled(requestContent)
                                : extractRestoreFiles(
                                        requestAndDeserializeInMemory(requestContent));
                initialization.complete(
                        restored.dataFiles() == null ? 0 : restored.dataFiles().size(), -1);
                return restored;
            } catch (IOException
                    | ExecutionException
                    | InterruptedException
                    | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static RestoreFiles extractRestoreFiles(ScanCoordinationResponse response)
            throws IOException {
        return new RestoreFiles(
                response.snapshot(),
                response.totalBuckets(),
                response.extractDataFiles(),
                response.extractDynamicBucketIndex(),
                response.extractDeletionVectorsIndex());
    }

    private ScanCoordinationResponse requestAndDeserializeInMemory(byte[] requestContent)
            throws IOException, ExecutionException, InterruptedException, ClassNotFoundException {
        Integer nextPageToken = null;
        List<byte[]> result = new ArrayList<>();
        String uuid = UUID.randomUUID().toString();

        do {
            PagedCoordinationResponse response = requestPage(requestContent, uuid, nextPageToken);
            result.add(response.content());
            nextPageToken = response.nextPageToken();
        } while (nextPageToken != null);

        byte[] responseContent = ArrayUtils.mergeByteArrays(result);
        return InstantiationUtil.deserializeObject(responseContent, getClass().getClassLoader());
    }

    private RestoreFiles requestAndDeserializeSpilled(byte[] requestContent)
            throws IOException, ExecutionException, InterruptedException, ClassNotFoundException {
        WriterCoordinatorSpillFile spillFile = WriterCoordinatorSpillFile.create(spillDirectory);
        try {
            Integer nextPageToken = null;
            String uuid = UUID.randomUUID().toString();
            try (OutputStream output = spillFile.outputStream(spillBufferSize)) {
                do {
                    PagedCoordinationResponse response =
                            requestPage(requestContent, uuid, nextPageToken);
                    output.write(response.content());
                    nextPageToken = response.nextPageToken();
                } while (nextPageToken != null);
            }
            try (InputStream input = spillFile.inputStream(spillBufferSize)) {
                input.mark(Integer.BYTES);
                int magic = new DataInputStream(input).readInt();
                input.reset();
                if (magic == StreamingScanResponse.MAGIC) {
                    return StreamingScanResponse.read(input, spillFile.size());
                }
                // An older JM ignores the capability and sends a Java-serialized response.
                return extractRestoreFiles(
                        InstantiationUtil.deserializeObject(input, getClass().getClassLoader()));
            }
        } finally {
            spillFile.close();
        }
    }

    private PagedCoordinationResponse requestPage(
            byte[] requestContent, String uuid, Integer nextPageToken)
            throws IOException, ExecutionException, InterruptedException {
        PagedCoordinationRequest request =
                new PagedCoordinationRequest(requestContent, uuid, nextPageToken, spillEnabled);
        SerializedValue<CoordinationRequest> serializedRequest = new SerializedValue<>(request);
        return CoordinationResponseUtils.unwrap(
                gateway.sendRequestToCoordinator(operatorID, serializedRequest).get());
    }
}
