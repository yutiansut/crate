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

package io.crate.planner.operators;

import io.crate.collections.Lists2;
import io.crate.common.collections.Maps;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.types.DataTypes;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FetchContext {

    private final Map<RelationName, FetchSource> fetchSourceByRelation;
    private final List<Symbol> outputs;

    public FetchContext(Map<RelationName, FetchSource> fetchSourceByRelation, List<Symbol> outputs) {
        this.fetchSourceByRelation = fetchSourceByRelation;
        this.outputs = outputs;
    }

    public static FetchContext empty() {
        return new FetchContext(Map.of(), List.of());
    }

    public FetchContext merge(FetchContext other) {
        return new FetchContext(
            Maps.concat(fetchSourceByRelation, other.fetchSourceByRelation),
            Lists2.concat(outputs, other.outputs)
        );
    }

    public Symbol createFetchReference(Symbol s) {
        int idx = outputs.indexOf(s);
        if (idx >= 0) {
            return new InputColumn(idx, outputs.get(idx).valueType());
        }
        return new FetchReference(new InputColumn(0, DataTypes.LONG), (Reference) s);
    }

    public Collection<Reference> allReferences() {
        return fetchSourceByRelation.values()
            .stream()
            .flatMap(x -> x.references().stream())
            .collect(Collectors.toList());
    }

    public Map<RelationName, FetchSource> fetchSourceByRelationName() {
        return fetchSourceByRelation;
    }
}
