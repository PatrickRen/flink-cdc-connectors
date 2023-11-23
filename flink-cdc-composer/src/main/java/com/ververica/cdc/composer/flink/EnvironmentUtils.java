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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.List;

/** Utilities for {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment}. */
public class EnvironmentUtils {
    public static void addJar(StreamExecutionEnvironment env, URL jarUrl) throws Exception {
        Class<StreamExecutionEnvironment> envClass = StreamExecutionEnvironment.class;
        Field field = envClass.getDeclaredField("configuration");
        field.setAccessible(true);
        Configuration configuration = ((Configuration) field.get(env));
        List<String> jars = configuration.get(PipelineOptions.JARS);
        jars.add(jarUrl.toString());
        configuration.set(PipelineOptions.JARS, jars);
    }
}
