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

package io.crate.sql;

import com.google.common.collect.ImmutableSet;
import io.crate.sql.tree.ArrayComparisonExpression;
import io.crate.sql.tree.ArraySubQueryExpression;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.CurrentTime;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.EscapedCharStringLiteral;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Extract;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.NullLiteral;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubscriptExpression;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public final class ExpressionFormatter {

    private static final Formatter DEFAULT_FORMATTER = new Formatter();

    private static final Collector<CharSequence, ?, String> COMMA_JOINER = Collectors.joining(", ");

    private static final Set<String> FUNCTION_CALLS_WITHOUT_PARENTHESIS = ImmutableSet.of(
        "current_catalog", "current_schema", "current_user", "session_user", "user");

    private ExpressionFormatter() {
    }

    /**
     * Formats the given expression and removes the outer most parenthesis, which are optional from an expression
     * correctness perspective, but clutter for the user (in case of nested expressions the inner expression will still
     * be enclosed in parenthesis, as that is a requirement for correctness, but the outer most expression will not be
     * surrounded by parenthesis)
     */
    public static String formatStandaloneExpression(Expression expression, @Nullable List<Expression> parameters) {
        return formatStandaloneExpression(expression, parameters, DEFAULT_FORMATTER);
    }

    public static String formatStandaloneExpression(Expression expression) {
        return formatStandaloneExpression(expression, null, DEFAULT_FORMATTER);
    }

    public static <T extends Formatter> String formatStandaloneExpression(Expression expression,
                                                                          @Nullable List<Expression> parameters,
                                                                          T formatter) {
        String formattedExpression = formatter.process(expression, parameters);
        if (formattedExpression.startsWith("(") && formattedExpression.endsWith(")")) {
            return formattedExpression.substring(1, formattedExpression.length() - 1);
        } else {
            return formattedExpression;
        }
    }

    public static String formatExpression(Expression<Expression<?>> expression) {
        return expression.accept(DEFAULT_FORMATTER, null);
    }

    public static class Formatter extends AstVisitor<Expression<?>, String, List<Expression>> {

        @Override
        protected String visitNode(Node<Expression<?>> node, @Nullable List<Expression> parameters) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "cannot handle node '%s'", node.toString()));
        }

        @Override
        protected String visitExpression(Expression<Expression<?>> node, @Nullable List<Expression> parameters) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "not yet implemented: %s.visit%s", getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        public String visitArrayComparisonExpression(ArrayComparisonExpression<Expression<?>> node, @Nullable List<Expression> parameters) {
            StringBuilder builder = new StringBuilder();

            String array = node.getRight().toString();
            String left = node.getLeft().toString();
            String type = node.getType().getValue();

            builder.append(left + " " + type + " ANY(" + array + ")");
            return builder.toString();
        }

        @Override
        protected String visitArraySubQueryExpression(ArraySubQueryExpression<Expression<?>> node, @Nullable List<Expression> parameters) {
            StringBuilder builder = new StringBuilder();
            String subqueryExpression = node.subqueryExpression().toString();

            return builder.append("ARRAY(").append(subqueryExpression).append(")").toString();
        }

        @Override
        protected String visitCurrentTime(CurrentTime<Expression<?>> node, @Nullable List<Expression> parameters) {
            StringBuilder builder = new StringBuilder();
            switch (node.getType()) {
                case TIME:
                    builder.append("current_time");
                    break;
                case DATE:
                    builder.append("current_date");
                    break;
                case TIMESTAMP:
                    builder.append("current_timestamp");
                    break;
                default:
                    throw new UnsupportedOperationException("not yet implemented: " + node.getType());
            }

            if (node.getPrecision().isPresent()) {
                builder.append('(')
                    .append(node.getPrecision().get())
                    .append(')');
            }

            return builder.toString();
        }

        @Override
        protected String visitExtract(Extract<Expression<?>> node, @Nullable List<Expression> parameters) {
            Expression<Expression<?>> expression = (Expression<Expression<?>>) node.getExpression();
            return "EXTRACT(" + node.getField() + " FROM " + process(expression, parameters) + ")";
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral<Expression<?>> node, @Nullable List<Expression> parameters) {
            return String.valueOf(node.getValue());
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression<Expression<?>> node, @Nullable List<Expression> parameters) {
            return node.name() + "[" + node.index() + "]";
        }

        @Override
        public String visitParameterExpression(ParameterExpression<Expression<?>> node, @Nullable List<Expression> parameters) {
            if (parameters == null) {
                return "$" + node.position();
            } else {
                int index = node.index();
                if (index >= parameters.size()) {
                    throw new IllegalArgumentException(
                        "Invalid parameter number " + node.position() +
                        ". Only " + parameters.size() + " parameters are available");
                }
                Expression<Expression<?>> expression = (Expression<Expression<?>>) parameters.get(index);
                return expression.accept(this, parameters);
            }
        }

        @Override
        protected String visitStringLiteral(StringLiteral<Expression<?>> node, @Nullable List<Expression> parameters) {
            return Literals.quoteStringLiteral(node.getValue());
        }

        @Override
        protected String visitEscapedCharStringLiteral(EscapedCharStringLiteral<Expression<?>> node, @Nullable List<Expression> parameters) {
            return Literals.quoteEscapedStringLiteral(node.getRawValue());
        }

        @Override
        protected String visitLongLiteral(LongLiteral<Expression<?>> node, @Nullable List<Expression> parameters) {
            return Long.toString(node.getValue());
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral<Expression<?>> node, @Nullable List<Expression> parameters) {
            return Double.toString(node.getValue());
        }

        @Override
        protected String visitNullLiteral(NullLiteral<Expression<?>> node, @Nullable List<Expression> parameters) {
            return "NULL";
        }


        private String formatBinaryExpression(String operator,
                                              Expression left,
                                              Expression right,
                                              @Nullable List<Expression> parameters) {
            return '(' + process(left, parameters) + ' ' + operator + ' ' + process(right, parameters) + ')';
        }

        private String joinExpressions(List<Expression> expressions) {
            return expressions.stream()
                .map(expression -> process(expression, null))
                .collect(COMMA_JOINER);
        }

        private static String formatIdentifier(String s) {
            return Identifiers.quote(s);
        }
    }
}
