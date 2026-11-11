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

import org.apache.flink.runtime.operators.coordination.CoordinationRequest;

import javax.annotation.Nullable;

/** Paged {@link CoordinationRequest} with page token. */
public class PagedCoordinationRequest implements CoordinationRequest {

    // Preserve the computed UID of the original three-field request for rolling upgrades.
    private static final long serialVersionUID = 7230230030925988974L;

    private final byte[] content;
    private final String requestId;
    private final @Nullable Integer pageToken;
    private final boolean streamingResponse;

    public PagedCoordinationRequest(byte[] content, String requestId, @Nullable Integer pageToken) {
        this(content, requestId, pageToken, false);
    }

    public PagedCoordinationRequest(
            byte[] content,
            String requestId,
            @Nullable Integer pageToken,
            boolean streamingResponse) {
        this.content = content;
        this.requestId = requestId;
        this.pageToken = pageToken;
        this.streamingResponse = streamingResponse;
    }

    public boolean streamingResponse() {
        return streamingResponse;
    }

    public byte[] content() {
        return content;
    }

    public String requestId() {
        return requestId;
    }

    @Nullable
    public Integer pageToken() {
        return pageToken;
    }
}
