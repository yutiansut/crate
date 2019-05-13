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

import com.google.common.collect.Sets;
import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.FetchPhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.FetchProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.FetchIdStub;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ReaderAllocations;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.fetch.FetchSource;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.crate.planner.operators.LogicalPlanner.extractColumns;
import static io.crate.planner.operators.OperatorUtils.getUnusedColumns;


/**
 * The FetchOrEval operator is producing the values for all selected expressions.
 *
 * <p>
 * This can be a simple re-arranging of expressions, the evaluation of scalar functions or involve a fetch operation.
 * The first two are rather simple and are handled by a {@link EvalProjection}, the fetch operation is more complicated.
 * </p>
 *
 * <h2>Fetch</h2>:
 *
 * <p>
 * On user tables it's possible to collect a {@link DocSysColumns#FETCHID}, which is like a unique row-id, except that it
 * is only valid within an open index reader. This fetchId can be used to retrieve the values from a row.
 *
 * The idea of fetch is to avoid loading more values than required.
 * This is the case if the data is collected from several nodes and then merged on the coordinator while applying some
 * form of data reduction, like applying a limit.
 * </p>
 *
 * <pre>
 *     select a, b, c from t order by limit 500
 *
 *
 *       N1                 N2
 *        Collect 500        Collect 500
 *             \              /
 *              \            /
 *                Merge
 *              1000 -> 500
 *                  |
 *                Fetch 500
 * </pre>
 */
public class FetchOrEval implements LogicalPlan {

    private final FetchMode fetchMode;
    private final boolean doFetch;
    final LogicalPlan source;
    private final List<Symbol> outputs;

    static LogicalPlan.Builder create(LogicalPlan.Builder sourceBuilder,
                                      List<Symbol> outputs,
                                      FetchMode fetchMode,
                                      boolean isLastFetch,
                                      boolean childIsLimited) {
        return (tableStats, usedBeforeNextFetch) -> {
            final LogicalPlan source;

            // This avoids collecting scalars unnecessarily if their source-columns are already collected
            // Ex. cases like: select xx from (select x + x as xx, ... from t1 order by x ..)
            usedBeforeNextFetch = extractColumns(usedBeforeNextFetch);

            boolean doFetch = isLastFetch;
            if (fetchMode == FetchMode.NEVER_CLEAR) {
                source = sourceBuilder.build(tableStats, usedBeforeNextFetch);
            } else if (isLastFetch) {
                source = sourceBuilder.build(tableStats, Collections.emptySet());
            } else {
                /*
                 * In a case like
                 *      select sum(x) from (select x from t limit 10)
                 *
                 * It makes sense to do an intermediate fetch, to reduce the amount of rows.
                 * All columns are used, so a _fetchId propagation wouldn't work.
                 *
                 *
                 * But in a case like
                 *      select x, y from (select x, y from t order by x limit 10) order by x asc limit 5
                 *
                 * A _fetchId propagation makes sense because there are unusedColumns (y), which can be fetched
                 * at the end.
                 */
                List<Symbol> unusedColumns = getUnusedColumns(outputs, usedBeforeNextFetch);
                if (unusedColumns.isEmpty() && childIsLimited) {
                    source = sourceBuilder.build(tableStats, Collections.emptySet());
                    doFetch = true;
                } else {
                    source = sourceBuilder.build(tableStats, usedBeforeNextFetch);
                }
            }
            if (source.outputs().equals(outputs)) {
                return source;
            }
            if (!doFetch && Symbols.containsColumn(source.outputs(), DocSysColumns.FETCHID)) {
                if (usedBeforeNextFetch.isEmpty()) {
                    return new FetchOrEval(source, source.outputs(), fetchMode, false);
                } else {
                    return new FetchOrEval(source, generateOutputs(outputs, source.outputs()), fetchMode, false);
                }
            } else {
                return new FetchOrEval(source, outputs, fetchMode, true);
            }
        };
    }

    public FetchOrEval(LogicalPlan source, List<Symbol> outputs, FetchMode fetchMode, boolean doFetch) {
        this.source = source;
        this.outputs = outputs;
        this.fetchMode = fetchMode;
        this.doFetch = doFetch;
    }

    public LogicalPlan source() {
        return source;
    }

    /**
     * Returns the source outputs and if there are scalars in the wantedOutputs which can be evaluated
     * using the sourceOutputs these scalars are included as well.
     *
     * <pre>
     *     wantedOutputs: R.x + R.y, R.i
     *      -> wantedColumns: { R.x + R.y: {R.x, R.y} }
     *     sourceOutputs: R._fetchId, R.x, R.y
     *
     *     result: R._fetchId, R.x + R.x
     * </pre>
     */
    private static List<Symbol> generateOutputs(List<Symbol> wantedOutputs, List<Symbol> sourceOutputs) {
        ArrayList<Symbol> result = new ArrayList<>();
        Set<Symbol> sourceColumns = extractColumns(sourceOutputs);
        addFetchIdColumns(sourceOutputs, result);

        HashMap<Symbol, Set<Symbol>> wantedColumnsByScalar = new HashMap<>();
        for (int i = 0; i < wantedOutputs.size(); i++) {
            Symbol wantedOutput = wantedOutputs.get(i);
            if (wantedOutput instanceof Function) {
                wantedColumnsByScalar.put(wantedOutput, extractColumns(wantedOutput));
            } else {
                if (sourceColumns.contains(wantedOutput)) {
                    result.add(wantedOutput);
                }
            }
        }
        addScalarsWithAvailableSources(result, sourceColumns, wantedColumnsByScalar);
        return result;
    }

    private static void addScalarsWithAvailableSources(ArrayList<Symbol> result,
                                                       Set<Symbol> sourceColumns,
                                                       HashMap<Symbol, Set<Symbol>> wantedColumnsByScalar) {
        for (Map.Entry<Symbol, Set<Symbol>> wantedEntry : wantedColumnsByScalar.entrySet()) {
            Symbol wantedOutput = wantedEntry.getKey();
            Set<Symbol> columnsUsedInScalar = wantedEntry.getValue();
            if (Sets.difference(columnsUsedInScalar, sourceColumns).isEmpty()) {
                result.add(wantedOutput);
            }
        }
    }

    private static void addFetchIdColumns(List<Symbol> sourceOutputs, ArrayList<Symbol> result) {
        for (int i = 0; i < sourceOutputs.size(); i++) {
            Symbol sourceOutput = sourceOutputs.get(i);
            if (SymbolVisitors.any(s -> s instanceof FetchIdStub, sourceOutput)) {
                result.add(sourceOutput);
            }
        }
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

        ExecutionPlan executionPlan = source.build(
            plannerContext, projectionBuilder, limit, offset, null, pageSizeHint, params, subQueryResults);
        List<Symbol> sourceOutputs = source.outputs();
        if (doFetch && SymbolVisitors.any(sourceOutputs, s -> s instanceof FetchIdStub)) {
            return planWithFetch(plannerContext, executionPlan, sourceOutputs, params, subQueryResults);
        }
        return planWithEvalProjection(plannerContext, executionPlan, sourceOutputs, params, subQueryResults);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public Map<Symbol, Symbol> expressionMapping() {
        return source.expressionMapping();
    }

    @Override
    public List<AbstractTableRelation> baseTables() {
        return source.baseTables();
    }

    @Override
    public List<LogicalPlan> sources() {
        return List.of(source);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new FetchOrEval(Lists2.getOnlyElement(sources), outputs, fetchMode, doFetch);
    }

    @Override
    public Map<LogicalPlan, SelectSymbol> dependencies() {
        return source.dependencies();
    }

    @Override
    public long numExpectedRows() {
        return source.numExpectedRows();
    }

    @Override
    public long estimatedRowSize() {
        return source.estimatedRowSize();
    }

    private ExecutionPlan planWithFetch(PlannerContext plannerContext,
                                        ExecutionPlan executionPlan,
                                        List<Symbol> sourceOutputs,
                                        Row params,
                                        SubQueryResults subQueryResults) {
        executionPlan = Merge.ensureOnHandler(executionPlan, plannerContext);

        Map<RelationName, FetchSource> fetchSourceByRelName = new HashMap<>();
        LinkedHashSet<Reference> allFetchRefs = new LinkedHashSet<>();

        List<FetchIdStub> fetchIdStubs = sourceOutputs.stream()
            .filter(s -> s instanceof FetchIdStub)
            .map(FetchIdStub.class::cast)
            .collect(Collectors.toList());

        for (FetchIdStub fetchIdStub : fetchIdStubs) {
            DocTableRelation relation = fetchIdStub.relation();
            FetchSource fetchSource = fetchSourceByRelName.computeIfAbsent(
                relation.tableInfo().ident(),
                r -> new FetchSource(relation.tableInfo().partitionedByColumns()));

            fetchIdStub.fetchCandidates().forEach(s -> RefVisitor.visitRefs(s, r -> {
                if (r.granularity() == RowGranularity.DOC) {
                    allFetchRefs.add(r);
                    fetchSource.addRefToFetch(r);
                }
                fetchSource.addFetchIdColumn(new InputColumn(sourceOutputs.indexOf(fetchIdStub), fetchIdStub.valueType()));
            }));
        }

        // TODO: generate fetchOutputs

        if (fetchSourceByRelName.isEmpty()) {
            // TODO:
            // this can happen if the Collect operator adds a _fetchId, but it turns out
            // that all required columns are already provided.
            // This should be improved so that this case no longer occurs
            // `testNestedSimpleSelectWithJoin` is an example case
            return planWithEvalProjection(plannerContext, executionPlan, sourceOutputs, params, subQueryResults);
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
            fetchSourceByRelName,
            fetchOutputs,
            readerAllocations.nodeReaders(),
            readerAllocations.indices(),
            readerAllocations.indicesToIdents()
        );
        executionPlan.addProjection(fetchProjection);
        return new QueryThenFetch(executionPlan, fetchPhase);
    }


    private ExecutionPlan planWithEvalProjection(PlannerContext plannerContext,
                                                 ExecutionPlan executionPlan,
                                                 List<Symbol> sourceOutputs,
                                                 Row params,
                                                 SubQueryResults subQueryResults) {
        PositionalOrderBy orderBy = executionPlan.resultDescription().orderBy();
        PositionalOrderBy newOrderBy = null;
        SubQueryAndParamBinder binder = new SubQueryAndParamBinder(params, subQueryResults);
        List<Symbol> boundOutputs = Lists2.map(outputs, binder);
        if (orderBy != null) {
            newOrderBy = orderBy.tryMapToNewOutputs(sourceOutputs, boundOutputs);
            if (newOrderBy == null) {
                executionPlan = Merge.ensureOnHandler(executionPlan, plannerContext);
            }
        }
        InputColumns.SourceSymbols ctx = new InputColumns.SourceSymbols(Lists2.map(sourceOutputs, binder));
        executionPlan.addProjection(
            new EvalProjection(InputColumns.create(boundOutputs, ctx)),
            executionPlan.resultDescription().limit(),
            executionPlan.resultDescription().offset(),
            newOrderBy
        );
        return executionPlan;
    }

    @Override
    public String toString() {
        return "FetchOrEval{" +
               "src=" + source +
               ", out=" + outputs +
               '}';
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitFetchOrEval(this, context);
    }
}
