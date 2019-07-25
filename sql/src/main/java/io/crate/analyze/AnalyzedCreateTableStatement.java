/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.data.Row;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.SubQueryResults;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class AnalyzedCreateTableStatement implements DDLStatement {

    private final RelationName name;
    private final boolean ifNotExists;
    private final AnalyzedColumns analyzedColumns;
    private final Symbol clusteredByColumn;
    private final Symbol numberOfShards;
    private final List<Symbol> partitionByColumns;
    private final Map<String, Symbol> analyzedProperties;

    public AnalyzedCreateTableStatement(RelationName name,
                                        boolean ifNotExists,
                                        AnalyzedColumns analyzedColumns,
                                        @Nullable Symbol clusteredByColumn,
                                        Symbol numberOfShards,
                                        List<Symbol> partitionByColumns,
                                        Map<String, Symbol> analyzedProperties) {
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.analyzedColumns = analyzedColumns;
        this.clusteredByColumn = clusteredByColumn;
        this.numberOfShards = numberOfShards;
        this.partitionByColumns = partitionByColumns;
        this.analyzedProperties = analyzedProperties;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitCreateTable(this, context);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public boolean isUnboundPlanningSupported() {
        return true;
    }

    public RelationName relationName() {
        return name;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public List<Symbol> partitionByColumns() {
        return partitionByColumns;
    }

    public Settings createSettings(TransactionContext txnCtx, Functions functions, Row params, SubQueryResults subQueryResults) {
        Settings.Builder builder = Settings.builder();
        for (var entry : TableParameters.CREATE_TABLE_PARAMETERS.supportedSettings().entrySet()) {
            builder.put(entry.getKey(), entry.getValue().getDefault(Settings.EMPTY).toString());
        }
        for (var entry : analyzedProperties.entrySet()) {
            builder.put(
                entry.getKey(),
                SymbolEvaluator.evaluate(txnCtx, functions, entry.getValue(), params, subQueryResults).toString()
            );
        }
        return builder.build();
    }

    public Map<String, Object> createMapping(TransactionContext txnCtx,
                                             Functions functions,
                                             Row params,
                                             SubQueryResults subQueryResults) {
        Map<String, Object> meta = new HashMap<>();
        if (!partitionByColumns.isEmpty()) {
            // TODO:
        }
        Map<String, Object> properties = new HashMap<>(analyzedColumns.size());
        return Map.of(
            "_meta", meta,
            "properties", properties
        );
    }
}
