/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.operators.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.FlushEvent;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

/**
 * An operator that processes records to be written into a {@link
 * org.apache.flink.api.connector.sink2.Sink}.
 *
 * <p>The operator is a proxy of {@link
 * org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator}.
 *
 * <p>The operator is always part of a sink pipeline and is the first operator.
 *
 * @param <CommT> the type of the committable (to send to downstream operators)
 */
@Internal
public class DataSinkWriterOperator<CommT> extends AbstractStreamOperator<CommittableMessage<CommT>>
        implements OneInputStreamOperator<Event, CommittableMessage<CommT>>, BoundedOneInput {

    private SchemaEvolutionClient schemaEvolutionClient;

    private final OperatorID schemaOperatorID;

    Sink<Event> sink;

    ProcessingTimeService processingTimeService;

    MailboxExecutor mailboxExecutor;

    /** Operator that actually execute sink logic. */
    Object flinkWriterOperator;

    /**
     * The internal {@link SinkWriter} of flinkWriterOperator, obtained it through reflection to
     * deal with {@link FlushEvent}.
     */
    private SinkWriter<Event> copySinkWriter;

    public DataSinkWriterOperator(
            Sink<Event> sink,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor,
            OperatorID schemaOperatorID) {
        this.sink = sink;
        this.processingTimeService = processingTimeService;
        this.mailboxExecutor = mailboxExecutor;
        this.schemaOperatorID = schemaOperatorID;
    }

    @Override
    public void setup(StreamTask containingTask, StreamConfig config, Output output) {
        super.setup(containingTask, config, output);
        try {
            Class<?> flinkWriterClass =
                    getRuntimeContext()
                            .getUserCodeClassLoader()
                            .loadClass(
                                    "org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator");
            Constructor<?> constructor =
                    flinkWriterClass.getDeclaredConstructor(
                            Sink.class, ProcessingTimeService.class, MailboxExecutor.class);
            constructor.setAccessible(true);
            flinkWriterOperator =
                    constructor.newInstance(sink, processingTimeService, mailboxExecutor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ((AbstractStreamOperator) flinkWriterOperator).setup(containingTask, config, output);
        schemaEvolutionClient =
                new SchemaEvolutionClient(
                        containingTask.getEnvironment().getOperatorCoordinatorEventGateway(),
                        schemaOperatorID);
    }

    @Override
    public void open() throws Exception {
        ((AbstractStreamOperator) flinkWriterOperator).open();
        copySinkWriter = (SinkWriter) getFieldValue("sinkWriter");
    }

    /**
     * Finds a field by name from its declaring class. This also searches for the field in super
     * classes.
     *
     * @param fieldName the name of the field to find.
     * @return the Object value of this field.
     */
    private Object getFieldValue(String fieldName) throws IllegalAccessException {
        Class clazz = flinkWriterOperator.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(flinkWriterOperator);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new RuntimeException("failed to get sinkWriter");
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        schemaEvolutionClient.registerSubtask(getRuntimeContext().getIndexOfThisSubtask());
        ((AbstractStreamOperator) flinkWriterOperator).initializeState(context);
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();
        if (event instanceof FlushEvent) {
            copySinkWriter.flush(false);
            schemaEvolutionClient.notifyFlushSuccess(
                    getRuntimeContext().getIndexOfThisSubtask(), ((FlushEvent) event).getTableId());
        } else {
            ((OneInputStreamOperator) flinkWriterOperator).processElement(element);
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        ((AbstractStreamOperator) flinkWriterOperator).prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void close() throws Exception {
        ((OneInputStreamOperator) flinkWriterOperator).close();
    }

    @Override
    public void endInput() throws Exception {
        ((BoundedOneInput) flinkWriterOperator).endInput();
    }
}
