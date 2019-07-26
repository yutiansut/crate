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

import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

public class AnalyzedColumns implements FieldProvider<Literal>, Iterable<AnalyzedColumn>  {

    private final List<AnalyzedColumn> columns;

    public AnalyzedColumns(List<AnalyzedColumn> columns) {
        this.columns = columns;
    }

    @Override
    public Literal resolveField(QualifiedName qualifiedName, @Nullable List<String> path, Operation operation) {
        List<String> parts = qualifiedName.getParts();
        // TODO: fix this
        for (AnalyzedColumn column : columns) {
            if (column.columnName().equals(parts.get(0))) {
                return Literal.of(column.columnName());
            }
        }
        throw new IllegalArgumentException("Column " + qualifiedName + " not present in CREATE TABLE statement");
    }

    @Override
    @Nonnull
    public Iterator<AnalyzedColumn> iterator() {
        return columns.iterator();
    }

    public int size() {
        return columns.size();
    }

    public AnalyzedColumn getSafe(ColumnIdent column) {
        for (AnalyzedColumn analyzedColumn : columns) {
            return analyzedColumn;
        }
        throw new IllegalArgumentException("Column not found: " + column);
    }
}
