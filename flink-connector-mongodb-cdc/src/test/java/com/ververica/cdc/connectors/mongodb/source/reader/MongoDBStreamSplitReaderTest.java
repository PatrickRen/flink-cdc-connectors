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

package com.ververica.cdc.connectors.mongodb.source.reader;

import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.OperationType;
import com.ververica.cdc.connectors.base.source.meta.split.ChangeEventRecords;
import com.ververica.cdc.connectors.base.source.meta.split.SourceRecords;
import com.ververica.cdc.connectors.base.source.meta.split.StreamSplit;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSourceTestBase;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import com.ververica.cdc.connectors.mongodb.source.dialect.MongoDBDialect;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamDescriptor;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamOffset;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamOffsetFactory;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static com.ververica.cdc.connectors.base.source.assigner.HybridSplitAssigner.BINLOG_SPLIT_ID;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.FULL_DOCUMENT_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.OPERATION_TYPE_FIELD;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.collectionNames;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.collectionsFilter;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.databaseFilter;
import static com.ververica.cdc.connectors.mongodb.source.utils.CollectionDiscoveryUtils.databaseNames;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.getChangeStreamDescriptor;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.getChangeStreamIterable;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.getResumeToken;
import static com.ververica.cdc.connectors.mongodb.source.utils.RecordUtils.currentBsonTimestamp;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** MongoDB stream split reader test case. */
public class MongoDBStreamSplitReaderTest extends MongoDBSourceTestBase {

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);

    private String database;

    private MongoDBDialect dialect;

    private MongoDBSourceConfig sourceConfig;

    private ChangeStreamOffsetFactory changeStreamOffsetFactory;

    private ChangeStreamDescriptor changeStreamDescriptor;

    private BsonDocument startupResumeToken;

    @Before
    public void before() {
        database = ROUTER.executeCommandFileInSeparateDatabase("chunk_test");

        MongoDBSourceConfigFactory configFactory =
                new MongoDBSourceConfigFactory()
                        .hosts(ROUTER.getHostAndPort())
                        .databaseList(database)
                        .collectionList(database + ".shopping_cart")
                        .username(FLINK_USER)
                        .password(FLINK_USER_PASSWORD)
                        .chunkSizeMB(1)
                        .pollAwaitTimeMillis(500);

        dialect = new MongoDBDialect();

        sourceConfig = configFactory.create(0);

        changeStreamOffsetFactory = new ChangeStreamOffsetFactory();

        List<String> discoveredDatabases =
                databaseNames(mongodbClient, databaseFilter(sourceConfig.getDatabaseList()));
        List<String> discoveredCollections =
                collectionNames(
                        mongodbClient,
                        discoveredDatabases,
                        collectionsFilter(sourceConfig.getCollectionList()));

        changeStreamDescriptor =
                getChangeStreamDescriptor(sourceConfig, discoveredDatabases, discoveredCollections);

        startupResumeToken =
                getResumeToken(getChangeStreamIterable(sourceConfig, changeStreamDescriptor));
    }

    @Test
    public void testStreamSplitReader() throws IOException {
        MongoDBSourceReaderContext sourceReaderContext =
                new MongoDBSourceReaderContext(new TestingReaderContext());
        MongoDBSourceSplitReader streamSplitReader =
                new MongoDBSourceSplitReader(dialect, sourceConfig, 0, sourceReaderContext);

        try {
            ChangeStreamOffset startOffset =
                    new ChangeStreamOffset(startupResumeToken, currentBsonTimestamp());

            StreamSplit streamSplit =
                    new StreamSplit(
                            BINLOG_SPLIT_ID,
                            startOffset,
                            changeStreamOffsetFactory.createNoStoppingOffset(),
                            new ArrayList<>(),
                            new HashMap<>(),
                            0);

            assertTrue(streamSplitReader.canAssignNextSplit());
            streamSplitReader.handleSplitsChanges(new SplitsAddition<>(singletonList(streamSplit)));

            MongoCollection<Document> collection =
                    mongodbClient.getDatabase(database).getCollection("shopping_cart");

            long now = System.currentTimeMillis();
            List<Document> inserts =
                    Arrays.asList(
                            shoppingCartDoc(now),
                            shoppingCartDoc(now + 1),
                            shoppingCartDoc(now + 2),
                            shoppingCartDoc(now + 3));
            collection.insertMany(inserts);

            while (true) {
                ChangeEventRecords records = (ChangeEventRecords) streamSplitReader.fetch();
                if (records.nextSplit() != null) {
                    SourceRecords sourceRecords;
                    int count = 0;
                    while ((sourceRecords = records.nextRecordFromSplit()) != null) {
                        Iterator<SourceRecord> iterator = sourceRecords.iterator();
                        while (iterator.hasNext()) {
                            Struct value = (Struct) iterator.next().value();
                            OperationType operationType =
                                    OperationType.fromString(value.getString(OPERATION_TYPE_FIELD));

                            assertEquals(OperationType.INSERT, operationType);
                            BsonDocument fullDocument =
                                    BsonDocument.parse(value.getString(FULL_DOCUMENT_FIELD));
                            long productNo = fullDocument.getInt64("product_no").longValue();
                            String productKind = fullDocument.getString("product_kind").getValue();
                            String userId = fullDocument.getString("user_id").getValue();
                            String description = fullDocument.getString("description").getValue();

                            assertEquals("KIND_" + productNo, productKind);
                            assertEquals("user_" + productNo, userId);
                            assertEquals("my shopping cart " + productNo, description);

                            if (++count >= inserts.size()) {
                                streamSplitReader.close();
                                break;
                            }
                        }
                    }
                } else {
                    break;
                }
            }
        } finally {
            streamSplitReader.close();
        }
    }

    private Document shoppingCartDoc(long productNo) {
        Document document = new Document();
        document.put("product_no", productNo);
        document.put("product_kind", "KIND_" + productNo);
        document.put("user_id", "user_" + productNo);
        document.put("description", "my shopping cart " + productNo);
        return document;
    }
}
