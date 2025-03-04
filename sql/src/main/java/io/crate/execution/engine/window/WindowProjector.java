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

package io.crate.execution.engine.window;

import io.crate.analyze.OrderBy;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.WindowDefinition;
import io.crate.breaker.RamAccountingContext;
import io.crate.breaker.RowAccounting;
import io.crate.breaker.RowAccountingWithEstimators;
import io.crate.data.BatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.WindowAggProjection;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.ExpressionsInput;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.SubQueryResults;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static io.crate.execution.engine.sort.Comparators.createComparator;

public class WindowProjector implements Projector {

    private final Comparator<Object[]> cmpPartitionBy;
    private final Comparator<Object[]> cmpOrderBy;
    private final int cellOffset;
    private final ArrayList<WindowFunction> windowFunctions;
    private final List<CollectExpression<Row, ?>> argsExpressions;
    private final Input[][] args;
    private final IntSupplier numThreads;
    private final Executor executor;
    private final RowAccounting<Row> rowAccounting;
    private final WindowDefinition windowDefinition;
    @Nullable
    private final Object startFrameOffset;
    @Nullable
    private final Object endFrameOffset;

    public static WindowProjector fromProjection(WindowAggProjection projection,
                                                 Functions functions,
                                                 InputFactory inputFactory,
                                                 TransactionContext txnCtx,
                                                 RamAccountingContext ramAccountingContext,
                                                 BigArrays bigArrays,
                                                 Version indexVersionCreated,
                                                 IntSupplier numThreads,
                                                 Executor executor) {
        var windowFunctionContexts = projection.windowFunctionContexts();
        var numWindowFunctions = windowFunctionContexts.size();

        ArrayList<WindowFunction> windowFunctions = new ArrayList<>(numWindowFunctions);
        ArrayList<CollectExpression<Row, ?>> windowFuncArgsExpressions = new ArrayList<>(numWindowFunctions);
        Input[][] windowFuncArgsInputs = new Input[numWindowFunctions][];

        for (int idx = 0; idx < numWindowFunctions; idx++) {
            var windowFunctionContext = windowFunctionContexts.get(idx);

            InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(txnCtx);
            ctx.add(windowFunctionContext.inputs());

            FunctionImplementation impl = functions.getQualified(
                windowFunctionContext.function().info().ident());
            if (impl instanceof AggregationFunction) {

                var filterInputFactoryCtx = inputFactory.ctxForInputColumns(txnCtx);
                //noinspection unchecked
                Input<Boolean> filterInput =
                    (Input<Boolean>) filterInputFactoryCtx.add(windowFunctionContext.filter());

                ExpressionsInput<Row, Boolean> filter = new ExpressionsInput<>(
                    filterInput,
                    filterInputFactoryCtx.expressions());

                windowFunctions.add(
                    new AggregateToWindowFunctionAdapter(
                        (AggregationFunction) impl,
                        filter,
                        indexVersionCreated,
                        bigArrays,
                        ramAccountingContext)
                );
            } else if (impl instanceof WindowFunction) {
                windowFunctions.add((WindowFunction) impl);
            } else {
                throw new AssertionError("Function needs to be either a window or an aggregate function");
            }
            windowFuncArgsExpressions.addAll(ctx.expressions());
            windowFuncArgsInputs[idx] = ctx.topLevelInputs().toArray(new Input[0]);
        }
        var windowDefinition = projection.windowDefinition();
        var partitions = windowDefinition.partitions();
        Supplier<InputFactory.Context<CollectExpression<Row, ?>>> createInputFactoryContext =
            () -> inputFactory.ctxForInputColumns(txnCtx);
        int arrayListElementOverHead = 32;
        RowAccountingWithEstimators accounting = new RowAccountingWithEstimators(
            Symbols.typeView(projection.standalone()), ramAccountingContext, arrayListElementOverHead);
        return new WindowProjector(
            accounting,
            windowDefinition,
            SymbolEvaluator.evaluate(txnCtx, functions, windowDefinition.windowFrameDefinition().start().value(), Row.EMPTY, SubQueryResults.EMPTY),
            SymbolEvaluator.evaluate(txnCtx, functions, windowDefinition.windowFrameDefinition().end().value(), Row.EMPTY, SubQueryResults.EMPTY),
            partitions.isEmpty() ? null : createComparator(createInputFactoryContext, new OrderBy(windowDefinition.partitions())),
            createComparator(createInputFactoryContext, windowDefinition.orderBy()),
            projection.standalone().size(),
            windowFunctions,
            windowFuncArgsExpressions,
            windowFuncArgsInputs,
            numThreads,
            executor
        );
    }

    private WindowProjector(RowAccounting<Row> rowAccounting,
                            WindowDefinition windowDefinition,
                            @Nullable Object startFrameOffset,
                            @Nullable Object endFrameOffset,
                            Comparator<Object[]> cmpPartitionBy,
                            Comparator<Object[]> cmpOrderBy,
                            int cellOffset,
                            ArrayList<WindowFunction> windowFunctions,
                            ArrayList<CollectExpression<Row, ?>> argsExpressions,
                            Input[][] args,
                            IntSupplier numThreads,
                            Executor executor) {
        this.rowAccounting = rowAccounting;
        this.windowDefinition = windowDefinition;
        this.startFrameOffset = startFrameOffset;
        this.endFrameOffset = endFrameOffset;
        this.cmpPartitionBy = cmpPartitionBy;
        this.cmpOrderBy = cmpOrderBy;
        this.cellOffset = cellOffset;
        this.windowFunctions = windowFunctions;
        this.argsExpressions = argsExpressions;
        this.args = args;
        this.numThreads = numThreads;
        this.executor = executor;
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> source) {
        return WindowFunctionBatchIterator.of(
            source,
            rowAccounting,
            windowDefinition,
            startFrameOffset,
            endFrameOffset,
            cmpPartitionBy,
            cmpOrderBy,
            cellOffset,
            numThreads,
            executor,
            windowFunctions,
            argsExpressions,
            args
        );
    }
}
