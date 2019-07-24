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

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnConstraint;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnStorageDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.IndexColumnConstraint;
import io.crate.sql.tree.NotNullColumnConstraint;
import io.crate.sql.tree.ObjectColumnType;
import io.crate.sql.tree.PrimaryKeyColumnConstraint;
import io.crate.sql.tree.TableElement;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class TableElementsAnalyzer {

    private static final String COLUMN_STORE_PROPERTY = "columnstore";

    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;
    private final RelationName relationName;
    private final ExpressionAnalyzer expressionAnalyzer;

    @Nullable
    private final TableInfo tableInfo;
    private final ExpressionAnalysisContext context;

    public TableElementsAnalyzer(ExpressionAnalyzer expressionAnalyzer,
                                 FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                 RelationName relationName,
                                 @Nullable TableInfo tableInfo) {
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        this.expressionAnalyzer = expressionAnalyzer;
        this.relationName = relationName;
        this.tableInfo = tableInfo;
        this.context = new ExpressionAnalysisContext();
    }

    public AnalyzedTableElements analyze(List<TableElement> tableElements) {
        AnalyzedTableElements analyzedTableElements = new AnalyzedTableElements();
        int positionOffset = tableInfo == null ? 0 : tableInfo.columns().size();
        for (int i = 0; i < tableElements.size(); i++) {
            TableElement tableElement = tableElements.get(i);
            int position = positionOffset + i + 1;

            if (tableElement instanceof ColumnDefinition) {
                AnalyzedColumn analyzedColumn = analyze(((ColumnDefinition) tableElement), position);
            }

        }
        return analyzedTableElements;
    }

    private AnalyzedColumn analyze(ColumnDefinition column, int position) {
        Expression defaultExpression = column.defaultExpression();
        Expression generatedExpression = column.generatedExpression();
        Symbol defaultExpr = defaultExpression == null ? null : expressionAnalyzer.convert(defaultExpression, context);
        Symbol generatedExpr = generatedExpression == null ? null : expressionAnalyzer.convert(generatedExpression, context);
        DataType<?> type = getDataType(column.columnName(), generatedExpr, column.type());

        boolean isNullable = true;
        boolean isPrimaryKey = false;
        for (ColumnConstraint constraint : column.constraints()) {
            if (constraint instanceof NotNullColumnConstraint) {
                isNullable = false;
            } else if (constraint instanceof IndexColumnConstraint) {
                IndexColumnConstraint index = (IndexColumnConstraint) constraint;
                String indexMethod = index.indexMethod();
                Map<String, Expression> indexProperties = index.properties().properties();

            } else if (constraint instanceof PrimaryKeyColumnConstraint) {
                isPrimaryKey = true;
            } else if (constraint instanceof ColumnStorageDefinition) {
                Map<String, Expression> columnStorageProperties = ((ColumnStorageDefinition) constraint).properties().properties();
            } else {
                throw new UnsupportedOperationException("Constraint is not supported: " + constraint);
            }
        }
        return new AnalyzedColumn(
            column.columnName(),
            type,
            defaultExpr,
            generatedExpr,
            isNullable,
            isPrimaryKey
        );
    }

    private DataType<?> getDataType(String columnName, Symbol generatedExpr, @Nullable ColumnType type) {
        if (type == null) {
            if (generatedExpr == null) {
                throw new IllegalArgumentException(
                    "The type of a column can only be omitted if there is a generated expression from which the type can be inferred. " +
                    "Both are missing for column `" + columnName + "`");
            }
            return generatedExpr.valueType();
        } else {
            return analyze(type);
        }
    }

    private DataType<?> analyze(ColumnType columnType) {
        if (columnType instanceof ObjectColumnType) {
            ObjectColumnType objectColumn = (ObjectColumnType) columnType;
            ObjectType.Builder objTypeBuilder = ObjectType.builder();
            for (ColumnDefinition nestedColumn : objectColumn.nestedColumns()) {
                AnalyzedColumn analyzedColumn = analyze(nestedColumn, 0);
                objTypeBuilder.setInnerType(
                    analyzedColumn.columnName(),
                    analyzedColumn.type()
                );
            }
            return objTypeBuilder.build();
        } else if (columnType instanceof CollectionColumnType) {
            DataType<?> innerType = analyze(((CollectionColumnType) columnType).innerType());
            return new ArrayType<>(innerType);
        } else {
            return DataTypes.ofName(columnType.name());
        }
    }
}
