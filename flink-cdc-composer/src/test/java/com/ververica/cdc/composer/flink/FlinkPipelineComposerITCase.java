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

package com.ververica.cdc.composer.flink;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.pipeline.PipelineOptions;
import com.ververica.cdc.composer.PipelineExecution;
import com.ververica.cdc.composer.definition.PipelineDef;
import com.ververica.cdc.composer.definition.SinkDef;
import com.ververica.cdc.composer.definition.SourceDef;
import com.ververica.cdc.connectors.values.factory.ValuesDataFactory;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class FlinkPipelineComposerITCase {
    @Test
    void test() throws Exception {
        FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();
        SourceDef sourceDef =
                new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", new Configuration());
        SinkDef sinkDef =
                new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", new Configuration());
        Configuration pipelineConfig = new Configuration();
        pipelineConfig.set(PipelineOptions.GLOBAL_PARALLELISM, 3);
        PipelineDef pipelineDef =
                new PipelineDef(
                        sourceDef,
                        sinkDef,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        pipelineConfig);

        PipelineExecution execution = composer.compose(pipelineDef);
        PipelineExecution.ExecutionInfo executionInfo = execution.execute();
    }
}
