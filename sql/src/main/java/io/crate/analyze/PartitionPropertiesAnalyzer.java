/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.analyze.expressions.ExpressionToObjectVisitor;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.data.Row;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.Assignment;
import io.crate.sql.tree.Expression;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class PartitionPropertiesAnalyzer {

    public static Map<ColumnIdent, Object> assignmentsToMap(List<Assignment<Expression>> assignments,
                                                            Row parameters) {
        Map<ColumnIdent, Object> map = new HashMap<>(assignments.size());
        for (Assignment<Expression> assignment : assignments) {
            map.put(
                ColumnIdent.fromPath(ExpressionToStringVisitor.convert(assignment.columnName(), parameters)),
                ExpressionToObjectVisitor.convert(assignment.expression(), parameters)
            );
        }
        return map;
    }

    public static PartitionName toPartitionName(DocTableInfo tableInfo,
                                                List<Assignment<Expression>> partitionProperties,
                                                Row parameters) {
        Preconditions.checkArgument(tableInfo.isPartitioned(), "table '%s' is not partitioned", tableInfo.ident().fqn());
        Preconditions.checkArgument(partitionProperties.size() == tableInfo.partitionedBy().size(),
            "The table \"%s\" is partitioned by %s columns but the PARTITION clause contains %s columns",
            tableInfo.ident().fqn(),
            tableInfo.partitionedBy().size(),
            partitionProperties.size()
        );
        Map<ColumnIdent, Object> properties = assignmentsToMap(partitionProperties, parameters);
        String[] values = new String[properties.size()];

        for (Map.Entry<ColumnIdent, Object> entry : properties.entrySet()) {
            Object value = entry.getValue();

            int idx = tableInfo.partitionedBy().indexOf(entry.getKey());
            try {
                Reference reference = tableInfo.partitionedByColumns().get(idx);
                Object converted = reference.valueType().value(value);
                values[idx] = DataTypes.STRING.value(converted);
            } catch (IndexOutOfBoundsException ex) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "\"%s\" is no known partition column", entry.getKey().sqlFqn()));
            }
        }
        return new PartitionName(tableInfo.ident(), Arrays.asList(values));
    }

    public static PartitionName toPartitionName(RelationName relationName,
                                                @Nullable DocTableInfo docTableInfo,
                                                List<Assignment<Expression>> partitionProperties,
                                                Row parameters) {
        if (docTableInfo != null) {
            return toPartitionName(docTableInfo, partitionProperties, parameters);
        }

        // Because only RelationName is available, types of partitioned columns must be guessed
        Map<ColumnIdent, Object> properties = assignmentsToMap(partitionProperties, parameters);
        String[] values = new String[properties.size()];

        int idx = 0;
        for (Object o : properties.values()) {
            values[idx++] = DataTypes.STRING.value(o);
        }
        return new PartitionName(relationName, Arrays.asList(values));
    }

    public static String toPartitionIdent(DocTableInfo tableInfo,
                                          List<Assignment<Expression>> partitionProperties,
                                          Row parameters) {
        return toPartitionName(tableInfo, partitionProperties, parameters).ident();
    }
}
