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

package io.crate.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.List;

public class MatchPredicate<T> extends Expression {

    private final List<MatchPredicateColumnIdent<T>> idents;
    private final T value;
    private final GenericProperties<T> properties;
    private final String matchType;

    public MatchPredicate(List<MatchPredicateColumnIdent<T>> idents,
                          T value,
                          @Nullable String matchType,
                          GenericProperties<T> properties) {
        Preconditions.checkArgument(idents.size() > 0, "at least one ident must be given");
        Preconditions.checkNotNull(value, "query_term is null");
        this.idents = idents;
        this.value = value;
        this.matchType = matchType;
        this.properties = properties;
    }

    public List<MatchPredicateColumnIdent<T>> idents() {
        return idents;
    }

    public T value() {
        return value;
    }

    @Nullable
    public String matchType() {
        return matchType;
    }

    public GenericProperties<T> properties() {
        return properties;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(idents, value, matchType, properties);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MatchPredicate that = (MatchPredicate) o;

        if (!idents.equals(that.idents)) return false;
        if (!value.equals(that.value)) return false;
        if (matchType != null && that.matchType == null || matchType == null && that.matchType != null) {
            return false;
        } else if (matchType != null && !matchType.equals(that.matchType)) {
            return false;
        }
        if (!properties.equals(that.properties)) return false;

        return true;
    }


    @Override
    public <R, C> R accept(AstVisitor<T, R, C> visitor, C context) {
        return visitor.visitMatchPredicate(this, context);
    }

}
