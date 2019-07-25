/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.PartitionedBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class CreateTableStatementAnalyzer {

    private static final String CLUSTERED_BY_IN_PARTITIONED_ERROR = "Cannot use CLUSTERED BY column in PARTITIONED BY clause";
    private final Schemas schemas;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;
    private final Functions functions;
    private final NumberOfShards numberOfShards;

    public CreateTableStatementAnalyzer(Schemas schemas,
                                        FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                        Functions functions,
                                        NumberOfShards numberOfShards) {
        this.schemas = schemas;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        this.functions = functions;
        this.numberOfShards = numberOfShards;
    }

    public AnalyzedCreateTableStatement analyze(CreateTable createTable,
                                                ParamTypeHints paramTypeHints,
                                                CoordinatorTxnCtx coordinatorTxnCtx) {
        RelationName relationName = RelationName.of(
            createTable.name().getName(),
            coordinatorTxnCtx.sessionContext().searchPath().currentSchema()
        );
        relationName.ensureValidForRelationCreation();
        var noColumnExprAnalyzer = new ExpressionAnalyzer(
            functions, coordinatorTxnCtx, paramTypeHints, FieldProvider.UNSUPPORTED, null);
        var expressionAnalysisContext = new ExpressionAnalysisContext();
        Map<String, Symbol> analyzedProperties = createTable.properties().properties().entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> noColumnExprAnalyzer.convert(e.getValue(), expressionAnalysisContext)));

        TableElementsAnalyzer tableElementsAnalyzer = new TableElementsAnalyzer(
            noColumnExprAnalyzer, fulltextAnalyzerResolver, relationName, null);
        AnalyzedColumns analyzedColumns = tableElementsAnalyzer.analyze(createTable.tableElements());

        var withColumnExprAnalyzer = new ExpressionAnalyzer(
            functions, coordinatorTxnCtx, paramTypeHints, analyzedColumns, null);

        List<Symbol> partitionByColumns = createTable.partitionedBy()
            .map(PartitionedBy::columns)
            .map(xs -> Lists2.map(xs, x -> withColumnExprAnalyzer.convert(x, expressionAnalysisContext)))
            .orElse(List.of());
        Optional<ClusteredBy> clusteredBy = createTable.clusteredBy();
        Symbol clusteredByColumn = clusteredBy.flatMap(ClusteredBy::column)
            .map(e -> withColumnExprAnalyzer.convert(e, expressionAnalysisContext))
            .orElse(null);
        Symbol numShards = clusteredBy.flatMap(ClusteredBy::numberOfShards)
            .map(e -> noColumnExprAnalyzer.convert(e, expressionAnalysisContext))
            .orElse(Literal.of(numberOfShards.defaultNumberOfShards()));
        return new AnalyzedCreateTableStatement(
            relationName,
            createTable.ifNotExists(),
            analyzedColumns,
            clusteredByColumn,
            numShards,
            partitionByColumns,
            analyzedProperties
        );
    }
}
