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

package io.crate.execution.dml.upsert;

import io.crate.common.collections.Maps;
import io.crate.data.Input;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.types.ObjectType;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class GeneratedColumns<T> {

    public enum Validation {
        NONE,
        VALUE_MATCH
    }

    private static final GeneratedColumns EMPTY = new GeneratedColumns();

    public static <T> GeneratedColumns<T> empty() {
        //noinspection unchecked
        return EMPTY;
    }

    private final Map<Reference, Input<?>> toValidate;
    private final Map<Reference, Input<?>> toInject;
    private final Map<Reference, Input<?>> maybeInjectNested;
    private final List<CollectExpression<T, ?>> expressions;

    private GeneratedColumns() {
        toValidate = Collections.emptyMap();
        toInject = Collections.emptyMap();
        maybeInjectNested = Collections.emptyMap();
        expressions = Collections.emptyList();
    }

    public GeneratedColumns(InputFactory inputFactory,
                            TransactionContext txnCtx,
                            Validation validation,
                            ReferenceResolver<CollectExpression<T, ?>> refResolver,
                            Collection<Reference> presentColumns,
                            List<GeneratedReference> allGeneratedColumns,
                            List<Reference> allDefaultExpressionColumns) {
        toValidate = validation == Validation.NONE ? Collections.emptyMap() : new HashMap<>();
        InputFactory.Context<CollectExpression<T, ?>> ctx = inputFactory.ctxForRefs(txnCtx, refResolver);
        toInject = new HashMap<>();
        maybeInjectNested = new HashMap<>();
        Set<String> topLevelPresentColumns = presentColumns
            .stream()
            .map(Reference::column)
            .filter(ColumnIdent::isTopLevel)
            .map(ColumnIdent::name)
            .collect(Collectors.toSet());

        allDefaultExpressionColumns.forEach(
            r -> processGeneratedExpressionColumn(
                r,
                ctx.add(r.defaultExpression()),
                presentColumns,
                topLevelPresentColumns,
                false)
        );

        allGeneratedColumns.forEach(
            r -> processGeneratedExpressionColumn(
                r,
                ctx.add(r.generatedExpression()),
                presentColumns,
                topLevelPresentColumns,
                validation == Validation.VALUE_MATCH)
        );
        expressions = ctx.expressions();
    }

    private void processGeneratedExpressionColumn(Reference reference,
                                                 Input<?> expressionInput,
                                                 Collection<Reference> presentColumns,
                                                 Set<String> topLevelPresentColumns,
                                                 boolean doValidate) {
        final ColumnIdent ident = reference.column();
        final boolean isNestedColumn = !ident.isTopLevel();
        if (isNestedColumn) {
            if (topLevelPresentColumns.contains(ident.name())) {
                maybeInjectNested.put(reference, expressionInput);
            } else {
                toInject.put(reference, expressionInput);
            }
        } else {
            if (presentColumns.contains(reference)) {
                if (doValidate) {
                    toValidate.put(reference, expressionInput);
                }
            } else {
                toInject.put(reference, expressionInput);
            }
        }

    }

    public void setNextRow(T row) {
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).setNextRow(row);
        }
    }

    public void validateValue(Reference target, Object providedValue) {
        Input<?> input = toValidate.get(target);
        if (input != null) {
            doValidateValue(target, input.value(), providedValue);
        }
    }

    private void doValidateValue(Reference target, Object generatedValue, Object providedValue) {
        //noinspection unchecked
        if (target.valueType().compareValueTo(generatedValue, providedValue) != 0) {
            throw new IllegalArgumentException(
                "Given value " + providedValue +
                " for generated column " + target.column() +
                " does not match calculation " + ((GeneratedReference) target).formattedGeneratedExpression() + " = " +
                generatedValue
            );
        }
    }

    void checkNestedValues(Reference target, Object providedValue) {
        if (ObjectType.ID != target.valueType().id()) {
            return;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> objectMap = (Map<String, Object>) providedValue;

        for (var nestedGenerated : maybeInjectNested.entrySet()) {
            Reference genReference = nestedGenerated.getKey();
            ColumnIdent generatedColumnIdent = genReference.column();
            assert (generatedColumnIdent.isTopLevel() == false) :
                "We should be checking only nested columns at this point";

            if (!target.column().name().equals(generatedColumnIdent.name())) {
                continue;
            }
            Input<?> inputExpression = nestedGenerated.getValue();
            Object byPathValue = Maps.getByPath(objectMap, generatedColumnIdent.path());
            if (byPathValue == null) {
                toInject.put(genReference, inputExpression);
            } else if (genReference instanceof GeneratedReference) {
                // normally this should be done for validation == Validation.VALUE_MATCH
                // however, for nested values, validation should be performed always
                // as there is no validation happening in earlier stage (analyzer)
                doValidateValue(genReference, inputExpression.value(), byPathValue);
            }
        }
    }

    public Iterable<? extends Map.Entry<Reference, Input<?>>> toInject() {
        return toInject.entrySet();
    }
}
