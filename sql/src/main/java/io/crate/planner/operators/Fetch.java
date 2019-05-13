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

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.dsl.projection.FetchProjection;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.FetchIdStub;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.ReaderAllocations;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.fetch.FetchSource;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class Fetch extends ForwardingLogicalPlan {

    private final List<Symbol> outputs;

    public Fetch(LogicalPlan source, List<Symbol> outputs) {
        super(source);
        this.outputs = outputs;
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        ExecutionPlan sourcePlan = Merge.ensureOnHandler(
            source.build(
                plannerContext,
                projectionBuilder,
                limit,
                offset,
                order,
                pageSizeHint,
                params,
                subQueryResults
            ),
            plannerContext
        );
        Iterator<FetchIdStub> fetchIdStubs = source.outputs()
            .stream()
            .filter(p -> p instanceof FetchIdStub)
            .map(FetchIdStub.class::cast)
            .iterator();

        Map<RelationName, FetchSource> fetchSourceByTable = new HashMap<>();
        ArrayList<Reference> allFetchRefs = new ArrayList<>();
        while (fetchIdStubs.hasNext()) {
            FetchIdStub fetchIdStub = fetchIdStubs.next();
            DocTableRelation relation = fetchIdStub.relation();
            DocTableInfo table = relation.tableInfo();

            FetchSource fetchSource = fetchSourceByTable.computeIfAbsent(
                table.ident(), r -> new FetchSource(table.partitionedByColumns()));
            fetchIdStub.fetchCandidates().forEach(s -> RefVisitor.visitRefs(s, r -> {
                if (r.granularity() == RowGranularity.DOC) {
                    fetchSource.addRefToFetch(r);
                    allFetchRefs.add(r);
                }
            }));
        }
        ReaderAllocations readerAllocations = plannerContext.buildReaderAllocations();
        FetchPhase fetchPhase = new FetchPhase(
            plannerContext.nextExecutionPhaseId(),
            readerAllocations.nodeReaders().keySet(),
            readerAllocations.bases(),
            readerAllocations.tableIndices(),
            allFetchRefs
        );
        FetchProjection fetchProjection = new FetchProjection(
            fetchPhase.phaseId(),
            plannerContext.fetchSize(),
            fetchSourceByTable,
            outputs,
            readerAllocations.nodeReaders(),
            readerAllocations.indices(),
            readerAllocations.indicesToIdents()
        );
        sourcePlan.addProjection(fetchProjection);
        return new QueryThenFetch(sourcePlan, fetchPhase);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new Fetch(Lists2.getOnlyElement(sources), outputs);
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitFetch(this, context);
    }
}
