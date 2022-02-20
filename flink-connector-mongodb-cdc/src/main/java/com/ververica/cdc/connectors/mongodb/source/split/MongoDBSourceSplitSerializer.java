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

package com.ververica.cdc.connectors.mongodb.source.split;

import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamOffsetFactory;

/** A serializer for the {@link SourceSplitBase}. */
public class MongoDBSourceSplitSerializer extends SourceSplitSerializer {

    private final ChangeStreamOffsetFactory offsetFactory;

    public MongoDBSourceSplitSerializer(ChangeStreamOffsetFactory offsetFactory) {
        this.offsetFactory = offsetFactory;
    }

    @Override
    public OffsetFactory getOffsetFactory() {
        return offsetFactory;
    }
}
