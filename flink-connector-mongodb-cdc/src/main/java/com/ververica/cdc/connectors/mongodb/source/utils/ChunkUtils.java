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

package com.ververica.cdc.connectors.mongodb.source.utils;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.BSON_MAX_KEY;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.BSON_MIN_KEY;

/** Utilities to split chunks of collection. */
public class ChunkUtils {

    private ChunkUtils() {}

    public static Object[] boundOf(String splitKey, BsonValue bound) {
        return new Object[] {new BsonDocument(splitKey, bound)};
    }

    public static Object[] minLowerBound(String splitKey) {
        return boundOf(splitKey, BSON_MIN_KEY);
    }

    public static Object[] maxUpperBound(String splitKey) {
        return boundOf(splitKey, BSON_MAX_KEY);
    }
}
