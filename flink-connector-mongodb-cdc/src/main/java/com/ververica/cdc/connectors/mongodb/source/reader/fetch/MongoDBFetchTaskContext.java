/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.mongodb.source.reader.fetch;

import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.dialect.MongoDBDialect;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamDescriptor;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

import java.util.concurrent.BlockingQueue;

/** The context for fetch task that fetching data of snapshot split from MongoDB data source. */
public class MongoDBFetchTaskContext implements FetchTask.Context {

    private final MongoDBDialect mongoDBDialect;
    private final MongoDBSourceConfig sourceConfig;
    private final ChangeStreamDescriptor changeStreamDescriptor;
    @Nullable private BlockingQueue<SourceRecord> copyExistingQueue;

    public MongoDBFetchTaskContext(
            MongoDBDialect mongoDBDialect,
            MongoDBSourceConfig sourceConfig,
            ChangeStreamDescriptor changeStreamDescriptor) {
        this.mongoDBDialect = mongoDBDialect;
        this.sourceConfig = sourceConfig;
        this.changeStreamDescriptor = changeStreamDescriptor;
    }

    public MongoDBDialect getMongoDBDialect() {
        return mongoDBDialect;
    }

    public MongoDBSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public ChangeStreamDescriptor getChangeStreamDescriptor() {
        return changeStreamDescriptor;
    }

    @Nullable
    public BlockingQueue<SourceRecord> getCopyExistingQueue() {
        return copyExistingQueue;
    }

    public void setCopyExistingQueue(BlockingQueue<SourceRecord> copyExistingQueue) {
        this.copyExistingQueue = copyExistingQueue;
    }
}
