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

package com.ververica.cdc.common.utils;

import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/** Utils for {@link Schema} to perform the ability of evolution. */
public class SchemaUtils {

    /** apply SchemaChangeEvent to the old schema and return the schema after changing. */
    public static Schema applySchemaChangeEvent(SchemaChangeEvent event, final Schema oldSchema) {
        Schema newSchema = oldSchema;
        if (event instanceof AddColumnEvent) {
            newSchema = applyAddColumnEvent((AddColumnEvent) event, oldSchema);
        } else if (event instanceof DropColumnEvent) {
            newSchema = applyDropColumnEvent((DropColumnEvent) event, oldSchema);
        } else if (event instanceof RenameColumnEvent) {
            newSchema = applyRenameColumnEvent((RenameColumnEvent) event, oldSchema);
        } else if (event instanceof AlterColumnTypeEvent) {
            newSchema = applyAlterColumnTypeEvent((AlterColumnTypeEvent) event, oldSchema);
        }
        return newSchema;
    }

    private static Schema applyAddColumnEvent(AddColumnEvent event, Schema oldSchema) {
        LinkedList<Column> columns = new LinkedList<>(oldSchema.getColumns());
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            if (columns.contains(columnWithPosition.getAddColumn())) {
                throw new IllegalArgumentException(
                        columnWithPosition.getAddColumn().getName()
                                + " of AddColumnEvent is already existed");
            }
            switch (columnWithPosition.getPosition()) {
                case FIRST:
                    {
                        columns.addFirst(columnWithPosition.getAddColumn());
                        break;
                    }
                case LAST:
                    {
                        columns.addLast(columnWithPosition.getAddColumn());
                        break;
                    }
                case BEFORE:
                    {
                        Preconditions.checkNotNull(
                                columnWithPosition.getExistingColumn(),
                                "existingColumn could not be null in BEFORE type AddColumnEvent");
                        int index = columns.indexOf(columnWithPosition.getExistingColumn());
                        if (index < 0) {
                            throw new IllegalArgumentException(
                                    columnWithPosition.getExistingColumn().getName()
                                            + " of AddColumnEvent is not existed");
                        }
                        columns.add(index, columnWithPosition.getAddColumn());
                        break;
                    }
                case AFTER:
                    {
                        Preconditions.checkNotNull(
                                columnWithPosition.getExistingColumn(),
                                "existingColumn could not be null in AFTER type AddColumnEvent");
                        int index = columns.indexOf(columnWithPosition.getExistingColumn());
                        if (index < 0) {
                            throw new IllegalArgumentException(
                                    columnWithPosition.getExistingColumn().getName()
                                            + " of AddColumnEvent is not existed");
                        }
                        columns.add(index + 1, columnWithPosition.getAddColumn());
                        break;
                    }
            }
        }
        return oldSchema.copy(columns);
    }

    private static Schema applyDropColumnEvent(DropColumnEvent event, Schema oldSchema) {
        event.getDroppedColumns()
                .forEach(
                        column -> {
                            if (!oldSchema.getColumn(column.getName()).isPresent()) {
                                throw new IllegalArgumentException(
                                        column.getName() + " of DropColumnEvent is not existed");
                            }
                        });
        List<Column> columns =
                oldSchema.getColumns().stream()
                        .filter((column -> !event.getDroppedColumns().contains(column)))
                        .collect(Collectors.toList());
        return oldSchema.copy(columns);
    }

    private static Schema applyRenameColumnEvent(RenameColumnEvent event, Schema oldSchema) {
        event.getNameMapping()
                .forEach(
                        (name, newName) -> {
                            if (!oldSchema.getColumn(name).isPresent()) {
                                throw new IllegalArgumentException(
                                        name + " of RenameColumnEvent is not existed");
                            }
                        });
        List<Column> columns = new ArrayList<>();
        oldSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (event.getNameMapping().containsKey(column.getName())) {
                                columns.add(
                                        column.copy(event.getNameMapping().get(column.getName())));
                            } else {
                                columns.add(column);
                            }
                        });
        return oldSchema.copy(columns);
    }

    private static Schema applyAlterColumnTypeEvent(AlterColumnTypeEvent event, Schema oldSchema) {
        event.getTypeMapping()
                .forEach(
                        (name, newType) -> {
                            if (!oldSchema.getColumn(name).isPresent()) {
                                throw new IllegalArgumentException(
                                        name + " of AlterColumnTypeEvent is not existed");
                            }
                        });
        List<Column> columns = new ArrayList<>();
        oldSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (event.getTypeMapping().containsKey(column.getName())) {
                                columns.add(
                                        column.copy(event.getTypeMapping().get(column.getName())));
                            } else {
                                columns.add(column);
                            }
                        });
        return oldSchema.copy(columns);
    }
}
