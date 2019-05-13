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
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.Merge;
import io.crate.planner.PlannerContext;
import io.crate.planner.PositionalOrderBy;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;


/**
 * The Eval operator is producing the values for all selected expressions.
 *
 * <p>
 * This can be a simple re-arranging of expressions or the evaluation of scalar functions
 * </p>
 */
public final class Eval implements LogicalPlan {

    final LogicalPlan source;
    private final List<Symbol> outputs;

    static LogicalPlan.Builder create(LogicalPlan.Builder sourceBuilder, List<Symbol> outputs) {
        return (tableStats) -> {
            final LogicalPlan source = sourceBuilder.build(tableStats);
            if (source.outputs().equals(outputs)) {
                return source;
            }
            return new Eval(source, outputs);
        };
    }

    public Eval(LogicalPlan source, List<Symbol> outputs) {
        this.source = source;
        this.outputs = outputs;
    }

    public LogicalPlan source() {
        return source;
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
        return planWithEvalProjection(plannerContext, executionPlan, source.outputs(), params, subQueryResults);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public Collection<Symbol> usedColumns() {
        return source.usedColumns();
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
        return new Eval(Lists2.getOnlyElement(sources), outputs);
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
        return "Eval{" +
               "src=" + source +
               ", out=" + outputs +
               '}';
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitEval(this, context);
    }
}
