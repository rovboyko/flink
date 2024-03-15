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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.JoinConditionWithNullFilters;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.IterableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract implementation for streaming unbounded Join operator which defines some member fields
 * can be shared between different implementations.
 */
public abstract class AbstractStreamingJoinOperator extends AbstractStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData> {

    private static final long serialVersionUID = -376944622236540545L;

    protected static final String LEFT_RECORDS_STATE_NAME = "left-records";
    protected static final String RIGHT_RECORDS_STATE_NAME = "right-records";

    private final GeneratedJoinCondition generatedJoinCondition;
    protected final InternalTypeInfo<RowData> leftType;
    protected final InternalTypeInfo<RowData> rightType;

    protected final JoinInputSideSpec leftInputSideSpec;
    protected final JoinInputSideSpec rightInputSideSpec;

    private final boolean[] filterNullKeys;

    protected final long leftStateRetentionTime;
    protected final long rightStateRetentionTime;

    protected transient JoinConditionWithNullFilters joinCondition;
    protected transient TimestampedCollector<RowData> collector;

    public AbstractStreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean[] filterNullKeys,
            long leftStateRetentionTime,
            long rightStateRetentionTime) {
        this.leftType = leftType;
        this.rightType = rightType;
        this.generatedJoinCondition = generatedJoinCondition;
        this.leftInputSideSpec = leftInputSideSpec;
        this.rightInputSideSpec = rightInputSideSpec;
        this.leftStateRetentionTime = leftStateRetentionTime;
        this.rightStateRetentionTime = rightStateRetentionTime;
        this.filterNullKeys = filterNullKeys;
    }

    @Override
    public void open() throws Exception {
        super.open();
        JoinCondition condition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.joinCondition = new JoinConditionWithNullFilters(condition, filterNullKeys, this);
        this.joinCondition.setRuntimeContext(getRuntimeContext());
        this.joinCondition.open(DefaultOpenContext.INSTANCE);

        this.collector = new TimestampedCollector<>(output);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (joinCondition != null) {
            joinCondition.close();
        }
    }

    protected boolean otherRecordHasNoAssociationsInInputSide(
            Iterator<RowData> inputSideIterator, RowData otherRecord, boolean inputIsLeft) {
        while (inputSideIterator.hasNext()) {
            if (inputIsLeft
                    ? joinCondition.apply(inputSideIterator.next(), otherRecord)
                    : joinCondition.apply(otherRecord, inputSideIterator.next())) {
                return false;
            }
        }
        return true;
    }

    /**
     * The {@link AssociatedRecords} is the records associated to the input row. It is a wrapper of
     * {@code List<OuterRecord>} which provides two helpful methods {@link #getRecords()} and {@link
     * #getOuterRecords()}. See the method Javadoc for more details.
     */
    protected static final class AssociatedRecords {
        private final List<RowData> records;

        private AssociatedRecords(List<RowData> records) {
            checkNotNull(records);
            this.records = records;
        }

        public boolean isEmpty() {
            return records.isEmpty();
        }

        public int size() {
            return records.size();
        }

        /**
         * Gets the iterable of records. This is usually be called when the {@link
         * AssociatedRecords} is from inner side.
         */
        public Iterable<RowData> getRecords() {
            return new RecordsIterable(records);
        }

        /**
         * Gets the iterable of {@link RowData} which composites record and numOfAssociations. This
         * is usually be called when the {@link AssociatedRecords} is from outer side.
         */
        public Iterable<RowData> getOuterRecords() {
            return records;
        }

        /**
         * Creates an {@link AssociatedRecords} which represents the records associated to the input
         * row.
         */
        public static AssociatedRecords of(
                RowData input,
                boolean inputIsLeft,
                JoinRecordStateView otherSideStateView,
                JoinCondition condition)
                throws Exception {
            List<RowData> associations = new ArrayList<>();
            Iterable<RowData> records = otherSideStateView.getRecords();
            for (RowData record : records) {
                boolean matched =
                        inputIsLeft
                                ? condition.apply(input, record)
                                : condition.apply(record, input);
                if (matched) {
                    associations.add(record);
                }
            }
            return new AssociatedRecords(associations);
        }
    }

    /** A lazy Iterable which transform {@code List<OuterReocord>} to {@code Iterable<RowData>}. */
    private static final class RecordsIterable implements IterableIterator<RowData> {
        private final List<RowData> records;
        private int index = 0;

        private RecordsIterable(List<RowData> records) {
            this.records = records;
        }

        @Override
        public Iterator<RowData> iterator() {
            index = 0;
            return this;
        }

        @Override
        public boolean hasNext() {
            return index < records.size();
        }

        @Override
        public RowData next() {
            RowData row = records.get(index);
            index++;
            return row;
        }
    }
}
