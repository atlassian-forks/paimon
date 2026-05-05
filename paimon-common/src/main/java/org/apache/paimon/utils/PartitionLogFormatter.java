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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

/**
 * Best-effort formatter for partition {@link BinaryRow} values used solely for human-readable
 * trace/debug logging. NOT for serialization or business logic.
 *
 * <p>Two strategies are provided:
 *
 * <ul>
 *   <li>{@link #format(RowType, BinaryRow)} — typed: use the known {@link RowType} to render each
 *       field with its native value (preferred when partition type is in scope).
 *   <li>{@link #format(BinaryRow)} — untyped fallback: when only a {@link BinaryRow} is available
 *       (e.g. inside {@code SimpleFileEntry}/{@code PojoManifestEntry}), best-effort renders each
 *       field by trying STRING, then INT, then LONG. Unparseable fields are shown as {@code <?>}.
 * </ul>
 */
public final class PartitionLogFormatter {

    private PartitionLogFormatter() {}

    /** Typed formatter — preferred. */
    public static String format(@Nullable RowType partitionType, @Nullable BinaryRow partition) {
        if (partition == null) {
            return "null";
        }
        if (partitionType == null) {
            return format(partition);
        }
        try {
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < partitionType.getFieldCount(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(partitionType.getFieldNames().get(i)).append('=');
                if (partition.isNullAt(i)) {
                    sb.append("null");
                } else {
                    DataType type = partitionType.getTypeAt(i);
                    Object v =
                            InternalRow.createFieldGetter(type, i).getFieldOrNull(partition);
                    sb.append(v);
                }
            }
            sb.append('}');
            return sb.toString();
        } catch (Throwable t) {
            return "<typedFormatFailed: " + t.getClass().getSimpleName() + ">"
                    + format(partition);
        }
    }

    /**
     * Untyped best-effort formatter. Tries STRING/INT/LONG for each field. Always safe to call.
     */
    public static String format(@Nullable BinaryRow partition) {
        if (partition == null) {
            return "null";
        }
        int n = partition.getFieldCount();
        StringBuilder sb = new StringBuilder("BinaryRow[fields=").append(n).append(", values=[");
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            try {
                if (partition.isNullAt(i)) {
                    sb.append("null");
                    continue;
                }
            } catch (Throwable t) {
                sb.append("<isNullAtFailed>");
                continue;
            }
            sb.append(bestEffortField(partition, i));
        }
        sb.append("]]");
        return sb.toString();
    }

    private static String bestEffortField(BinaryRow partition, int i) {
        // Try STRING first (most common for partition columns in many tables).
        try {
            BinaryString s = partition.getString(i);
            if (s != null) {
                String str = s.toString();
                if (isPrintable(str)) {
                    return "\"" + str + "\"";
                }
            }
        } catch (Throwable ignored) {
        }
        // Try INT.
        try {
            return Integer.toString(partition.getInt(i));
        } catch (Throwable ignored) {
        }
        // Try LONG.
        try {
            return Long.toString(partition.getLong(i));
        } catch (Throwable ignored) {
        }
        return "<?>";
    }

    private static boolean isPrintable(String s) {
        if (s == null || s.isEmpty()) {
            return true;
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            // Allow ASCII printable + common whitespace + non-ASCII.
            if (c < 0x20 && c != '\t' && c != '\n' && c != '\r') {
                return false;
            }
        }
        return true;
    }
}
