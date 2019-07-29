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

package io.crate.sql.tree;

public abstract class DefaultTraversalVisitor<T, R, C> extends AstVisitor<T, R, C> {

    private final ElementVisitor<T, R, C> elementVisitor;

    protected DefaultTraversalVisitor(ElementVisitor<T, R, C> elementVisitor) {
        this.elementVisitor = elementVisitor;
    }

    @Override
    protected R visitQuery(Query<T> node, C context) {
        process(node.getQueryBody(), context);
        for (SortItem<T> sortItem : node.getOrderBy()) {
            process(sortItem, context);
        }
        return null;
    }

    @Override
    protected R visitSelect(Select<T> node, C context) {
        for (SelectItem<T> item : node.getSelectItems()) {
            process(item, context);
        }
        return null;
    }

    @Override
    protected R visitSingleColumn(SingleColumn<T> node, C context) {
        return elementVisitor.visit(node.getExpression(), context);
    }

    @Override
    protected R visitQuerySpecification(QuerySpecification<T> node, C context) {

        // visit the from first, since this qualifies the select
        for (Relation<T> relation : node.getFrom()) {
            process(relation, context);
        }

        process(node.getSelect(), context);
        if (node.getWhere().isPresent()) {
            process(node.getWhere().get(), context);
        }
        for (T expression : node.getGroupBy()) {
            process(expression, context);
        }
        if (node.getHaving().isPresent()) {
            process(node.getHaving().get(), context);
        }
        for (SortItem<T> sortItem : node.getOrderBy()) {
            process(sortItem, context);
        }
        return null;
    }

    @Override
    protected R visitUnion(Union<T> node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);
        return null;
    }

    @Override
    protected R visitIntersect(Intersect<T> node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);
        return null;
    }

    @Override
    protected R visitExcept(Except<T> node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);
        return null;
    }

    @Override
    protected R visitTableSubquery(TableSubquery<T> node, C context) {
        return process(node.getQuery(), context);
    }

    @Override
    protected R visitAliasedRelation(AliasedRelation<T> node, C context) {
        return process(node.getRelation(), context);
    }

    @Override
    protected R visitJoin(Join<T> node, C context) {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        if (node.getCriteria().isPresent() && node.getCriteria().get() instanceof JoinOn) {
            process(((JoinOn<T>) node.getCriteria().get()).getExpression(), context);
        }

        return null;
    }

    @Override
    public R visitInsertFromValues(InsertFromValues<T> node, C context) {
        process(node.table(), context);
        for (ValuesList<T> valuesList : node.valuesLists()) {
            process(valuesList, context);
        }
        return null;
    }

    @Override
    public R visitValuesList(ValuesList<T> node, C context) {
        for (T value : node.values()) {
            process(value, context);
        }
        return null;
    }

    @Override
    public R visitUpdate(Update<T> node, C context) {
        process(node.relation(), context);
        for (Assignment<T> assignment : node.assignements()) {
            process(assignment, context);
        }
        if (node.whereClause().isPresent()) {
            process(node.whereClause().get(), context);
        }
        return null;
    }

    @Override
    public R visitDelete(Delete<T> node, C context) {
        process(node.getRelation(), context);
        return null;
    }

    @Override
    public R visitCopyFrom(CopyFrom<T> node, C context) {
        process(node.table(), context);
        return null;
    }

    @Override
    public R visitCopyTo(CopyTo<T> node, C context) {
        process(node.table(), context);
        return null;
    }

    @Override
    public R visitAlterTable(AlterTable<T> node, C context) {
        process(node.table(), context);
        return null;
    }

    @Override
    public R visitInsertFromSubquery(InsertFromSubquery<T> node, C context) {
        process(node.table(), context);
        process(node.subQuery(), context);
        return null;
    }

    @Override
    public R visitDropTable(DropTable<T> node, C context) {
        process(node.table(), context);
        return super.visitDropTable(node, context);
    }

    @Override
    public R visitCreateTable(CreateTable<T> node, C context) {
        process(node.name(), context);
        return null;
    }

    @Override
    public R visitShowCreateTable(ShowCreateTable<T> node, C context) {
        process(node.table(), context);
        return null;
    }

    @Override
    public R visitRefreshStatement(RefreshStatement<T> node, C context) {
        for (Table<T> nodeTable : node.tables()) {
            process(nodeTable, context);
        }
        return null;
    }

    @Override
    public R visitMatchPredicate(MatchPredicate<T> node, C context) {
        for (MatchPredicateColumnIdent<T> columnIdent : node.idents()) {
            process(columnIdent.columnIdent(), context);
            process(columnIdent.boost(), context);
        }
        process(node.value(), context);

        return null;
    }
}
