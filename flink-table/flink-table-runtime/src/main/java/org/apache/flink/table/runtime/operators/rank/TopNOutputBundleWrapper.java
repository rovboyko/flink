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

package org.apache.flink.table.runtime.operators.rank;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTriggerCallback;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/** The wrapper for topN output collector. */
public class TopNOutputBundleWrapper implements BundleTriggerCallback {

    // key -> Map(rank -> BuffedRecord)
    Map<RowData, Map<Integer, BuffedRecord>> buffer = new HashMap<>();

    Collector<RowData> out;

    BundleTrigger<RowData> bundleTrigger;

    private final JoinedRowData outputRow;
    private final GenericRowData rankRow = new GenericRowData(1);

    public TopNOutputBundleWrapper(long bundleSize) {
        bundleTrigger = new CountBundleTrigger<>(bundleSize);
        bundleTrigger.registerCallback(this);
        outputRow = new JoinedRowData();
    }

    public void collect(
            Collector<RowData> out, RowData key, int rank, RowData outputRow, RowKind rowKind)
            throws Exception {
        if (this.out == null) {
            this.out = out;
        }

        Map<Integer, BuffedRecord> rankMap = buffer.computeIfAbsent(key, k -> new HashMap<>());

        BuffedRecord buffedRecord = rankMap.computeIfAbsent(rank, r -> new BuffedRecord());

        if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
            // accumulate message should be simply provided to output
            buffedRecord.accumulateRow = outputRow;
            buffedRecord.accumulateRowKind = rowKind;
        } else {
            // retract record can be collapsed with previous acc record
            if (buffedRecord.accumulateRow != null) {
                // we can fold -U/-D record with +I/+U which is already stored in buffer
                if (buffedRecord.accumulateRowKind == RowKind.INSERT) {
                    buffedRecord.insertWasRemoved = true;
                }
                buffedRecord.accumulateRow = null;
            } else {
                buffedRecord.retractRow = outputRow;
                buffedRecord.retractRowKind = rowKind;
            }
        }
    }

    public void finishElementProcessing(RowData inputRow) throws Exception {
        bundleTrigger.onElement(inputRow);
    }

    @Override
    public void finishBundle() throws Exception {
        int i = 0;
        for (Map.Entry<RowData, Map<Integer, BuffedRecord>> keyToRankList : buffer.entrySet()) {
            for (Map.Entry<Integer, BuffedRecord> rankToValues :
                    keyToRankList.getValue().entrySet()) {
                Integer rank = rankToValues.getKey();
                RowData retractRow = rankToValues.getValue().retractRow;
                RowKind retractRowKind = rankToValues.getValue().retractRowKind;
                RowData accumulateRow = rankToValues.getValue().accumulateRow;
                RowKind accumulateRowKind = rankToValues.getValue().accumulateRowKind;
                boolean isInsertFolded = rankToValues.getValue().insertWasRemoved;
                i++;

                if (retractRow != null) {
                    out.collect(createOutputRow(retractRow, rank, retractRowKind));
                }
                if (accumulateRow != null) {
                    RowKind accRowKind = isInsertFolded ? RowKind.INSERT : accumulateRowKind;
                    out.collect(createOutputRow(accumulateRow, rank, accRowKind));
                }
            }
        }
        buffer.clear();
        bundleTrigger.reset();
    }

    private RowData createOutputRow(RowData inputRow, long rank, RowKind rowKind) {
        rankRow.setField(0, rank);

        outputRow.replace(inputRow, rankRow);
        outputRow.setRowKind(rowKind);
        return outputRow;
    }

    private static class BuffedRecord {
        private RowData retractRow;
        private RowKind retractRowKind;
        private RowData accumulateRow;
        private RowKind accumulateRowKind;
        private boolean insertWasRemoved;
    }
}
