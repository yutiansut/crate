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

package io.crate.sql.parser;

import io.crate.sql.parser.antlr.v4.SqlBaseBaseVisitor;
import io.crate.sql.parser.antlr.v4.SqlBaseParser;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.StringLiteral;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ExpressionBuilder extends SqlBaseBaseVisitor<Expression> {

    private int parameterPosition = 1;

    @Override
    public Expression visitParameterPlaceholder(SqlBaseParser.ParameterPlaceholderContext context) {
        return new ParameterExpression(parameterPosition++);
    }

    @Override
    public Expression visitPositionalParameter(SqlBaseParser.PositionalParameterContext context) {
        return new ParameterExpression(Integer.parseInt(context.integerLiteral().getText()));
    }

    @Override
    public StringLiteral<Expression> visitIdent(SqlBaseParser.IdentContext ctx) {
        return ctx.q
    }

    public Optional<Expression> visitIfPresent(@Nullable ParseTree tree) {
        if (tree == null) {
            return Optional.empty();
        } else {
            return Optional.of(visit(tree));
        }
    }

    public List<Expression> visitCollection(List<SqlBaseParser.PrimaryExpressionContext> primaryExpression) {
        ArrayList<Expression> result = new ArrayList<>(primaryExpression.size());
        for (var ctx : primaryExpression) {
            result.add(visit(ctx));
        }
        return result;
    }
}
