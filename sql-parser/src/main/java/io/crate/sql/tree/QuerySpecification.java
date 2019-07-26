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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class QuerySpecification<T> extends QueryBody<T> {

    private final Select<T> select;
    private final List<Relation<T>> from;
    private final Optional<T> where;
    private final List<T> groupBy;
    private final Optional<T> having;
    private final List<SortItem<T>> orderBy;
    private final Optional<T> limit;
    private final Optional<T> offset;
    private final Map<String, Window<T>> windows;

    public QuerySpecification(
        Select<T> select,
        List<Relation<T>> from,
        Optional<T> where,
        List<T> groupBy,
        Optional<T> having,
        Map<String, Window<T>> windows,
        List<SortItem<T>> orderBy,
        Optional<T> limit,
        Optional<T> offset) {
        checkNotNull(select, "select is null");
        checkNotNull(where, "where is null");
        checkNotNull(groupBy, "groupBy is null");
        checkNotNull(having, "having is null");
        checkNotNull(orderBy, "orderBy is null");
        checkNotNull(limit, "limit is null");
        checkNotNull(offset, "offset is null");

        this.select = select;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.windows = windows;
        this.orderBy = orderBy;
        this.limit = limit;
        this.offset = offset;
    }

    public Select getSelect() {
        return select;
    }

    public List<Relation<T>> getFrom() {
        return from;
    }

    public Optional<T> getWhere() {
        return where;
    }

    public List<T> getGroupBy() {
        return groupBy;
    }

    public Optional<T> getHaving() {
        return having;
    }

    public List<SortItem<T>> getOrderBy() {
        return orderBy;
    }

    public Optional<T> getLimit() {
        return limit;
    }

    public Optional<T> getOffset() {
        return offset;
    }

    public Map<String, Window<T>> getWindows() {
        return windows;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQuerySpecification(this, context);
    }

    @Override
    public String toString() {
        return "QuerySpecification{" +
               "select=" + select +
               ", from=" + from +
               ", where=" + where +
               ", groupBy=" + groupBy +
               ", having=" + having +
               ", orderBy=" + orderBy +
               ", limit=" + limit +
               ", offset=" + offset +
               ", windows=" + windows +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QuerySpecification that = (QuerySpecification) o;
        return Objects.equals(select, that.select) &&
               Objects.equals(from, that.from) &&
               Objects.equals(where, that.where) &&
               Objects.equals(groupBy, that.groupBy) &&
               Objects.equals(having, that.having) &&
               Objects.equals(orderBy, that.orderBy) &&
               Objects.equals(limit, that.limit) &&
               Objects.equals(offset, that.offset) &&
               Objects.equals(windows, that.windows);
    }

    @Override
    public int hashCode() {
        return Objects.hash(select, from, where, groupBy, having, orderBy, limit, offset, windows);
    }
}
