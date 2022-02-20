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

package com.ververica.cdc.connectors.mongodb.source.config;

import com.ververica.cdc.connectors.base.config.SourceConfig;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.ververica.cdc.connectors.mongodb.source.utils.MongoUtils.buildConnectionString;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A MongoDB Source configuration which is used by {@link MongoDBSource}. */
public class MongoDBSourceConfig implements SourceConfig {

    private static final long serialVersionUID = 1L;

    private final String hosts;
    @Nullable private final String username;
    @Nullable private final String password;
    @Nullable private final List<String> databaseList;
    @Nullable private final List<String> collectionList;

    private final String connectionString;

    private final int batchSize;

    private final int pollAwaitTimeMillis;

    private final int pollMaxBatchSize;

    private final boolean copyExisting;

    private final int copyExistingQueueSize;

    private final int heartbeatIntervalMillis;

    private final int chunkSizeMB;

    MongoDBSourceConfig(
            String hosts,
            @Nullable String username,
            @Nullable String password,
            @Nullable List<String> databaseList,
            @Nullable List<String> collectionList,
            @Nullable String connectionOptions,
            int batchSize,
            int pollAwaitTimeMillis,
            int pollMaxBatchSize,
            boolean copyExisting,
            int copyExistingQueueSize,
            int heartbeatIntervalMillis,
            int chunkSizeMB) {
        this.hosts = checkNotNull(hosts);
        this.username = username;
        this.password = password;
        this.databaseList = databaseList;
        this.collectionList = collectionList;
        this.connectionString =
                buildConnectionString(username, password, hosts, connectionOptions)
                        .getConnectionString();
        this.batchSize = batchSize;
        this.pollAwaitTimeMillis = pollAwaitTimeMillis;
        this.pollMaxBatchSize = pollMaxBatchSize;
        this.copyExisting = copyExisting;
        this.copyExistingQueueSize = copyExistingQueueSize;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.chunkSizeMB = chunkSizeMB;
    }

    public String getHosts() {
        return hosts;
    }

    public String getConnectionString() {
        return connectionString;
    }

    @Nullable
    public String getUsername() {
        return username;
    }

    @Nullable
    public String getPassword() {
        return password;
    }

    @Nullable
    public List<String> getDatabaseList() {
        return databaseList;
    }

    @Nullable
    public List<String> getCollectionList() {
        return collectionList;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getPollAwaitTimeMillis() {
        return pollAwaitTimeMillis;
    }

    public int getPollMaxBatchSize() {
        return pollMaxBatchSize;
    }

    public int getCopyExistingQueueSize() {
        return copyExistingQueueSize;
    }

    public int getHeartbeatIntervalMillis() {
        return heartbeatIntervalMillis;
    }

    public int getChunkSizeMB() {
        return chunkSizeMB;
    }

    @Override
    public StartupOptions getStartupOptions() {
        return copyExisting ? StartupOptions.initial() : StartupOptions.latest();
    }

    @Override
    public int getSplitSize() {
        return chunkSizeMB;
    }

    @Override
    public int getSplitMetaGroupSize() {
        return 0;
    }

    @Override
    public double getDistributionFactorUpper() {
        return 0;
    }

    @Override
    public double getDistributionFactorLower() {
        return 0;
    }

    @Override
    public boolean isIncludeSchemaChanges() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoDBSourceConfig that = (MongoDBSourceConfig) o;
        return batchSize == that.batchSize
                && pollAwaitTimeMillis == that.pollAwaitTimeMillis
                && pollMaxBatchSize == that.pollMaxBatchSize
                && copyExisting == that.copyExisting
                && copyExistingQueueSize == that.copyExistingQueueSize
                && heartbeatIntervalMillis == that.heartbeatIntervalMillis
                && chunkSizeMB == that.chunkSizeMB
                && Objects.equals(hosts, that.hosts)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(databaseList, that.databaseList)
                && Objects.equals(collectionList, that.collectionList)
                && Objects.equals(connectionString, that.connectionString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                hosts,
                username,
                password,
                databaseList,
                collectionList,
                connectionString,
                batchSize,
                pollAwaitTimeMillis,
                pollMaxBatchSize,
                copyExisting,
                copyExistingQueueSize,
                heartbeatIntervalMillis,
                chunkSizeMB);
    }
}
