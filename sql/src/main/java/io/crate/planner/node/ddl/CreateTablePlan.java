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

package io.crate.planner.node.ddl;

import io.crate.Constants;
import io.crate.analyze.AnalyzedCreateTableStatement;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.SQLExceptions;
import io.crate.execution.ddl.tables.CreateTableRequest;
import io.crate.execution.support.OneRowActionListener;
import io.crate.execution.support.Transports;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

public class CreateTablePlan implements Plan {

    private static final Logger LOGGER = LogManager.getLogger(CreateTablePlan.class);

    private final AnalyzedCreateTableStatement createTable;

    public CreateTablePlan(AnalyzedCreateTableStatement createTable) {
        this.createTable = createTable;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {
        CoordinatorTxnCtx txnCtx = plannerContext.transactionContext();
        Functions functions = plannerContext.functions();
        Settings settings = createTable.createSettings(txnCtx, functions, params, subQueryResults);
        Map<String, Object> mapping = createTable.createMapping(txnCtx, functions, params, subQueryResults);
        var partitionByColumns = createTable.partitionByColumns();
        var relationName = createTable.relationName();
        final CreateTableRequest createTableRequest;
        final String templateName;
        if (partitionByColumns.isEmpty()) {
            templateName = null;
            createTableRequest = new CreateTableRequest(
                new CreateIndexRequest(relationName.indexNameOrAlias(), settings)
                    .mapping(Constants.DEFAULT_MAPPING_TYPE, mapping)
            );
        } else {
            templateName = PartitionName.templateName(relationName.schema(), relationName.name());
            String templatePrefix = PartitionName.templatePrefix(relationName.schema(), relationName.name());
            createTableRequest = new CreateTableRequest(
                new PutIndexTemplateRequest(templateName)
                    .mapping(Constants.DEFAULT_MAPPING_TYPE, mapping)
                    .create(true)
                    .settings(settings)
                    .patterns(Collections.singletonList(templatePrefix))
                    .order(100)
                    .alias(new Alias(relationName.indexNameOrAlias()))
            );
        }
        OneRowActionListener<Long> listener = new OneRowActionListener<>(consumer, Row1::new);
        Transports.execute(
            dependencies.createTableTransport(),
            createTableRequest,
            resp -> {
                if (!resp.isAllShardsAcked() &&
                    LOGGER.isWarnEnabled()) {
                    LOGGER.warn(
                        "CREATE TABLE `{}` was not acknowledged. This could lead to inconsistent state.",
                        relationName.fqn());
                }
                return 1L;
            }).exceptionally(error -> {
                Throwable t = SQLExceptions.unwrap(error);
                String message = t.getMessage();
                Throwable cause = t.getCause();
                if ("mapping [default]".equals(message) && cause != null) {
                    // this is a generic mapping parse exception,
                    // the cause has usually a better more detailed error message
                    return Exceptions.rethrowRuntimeException(cause);
                } else if (createTable.ifNotExists() && isTableExistsError(t, templateName)) {
                    return 0L;
                } else {
                    return Exceptions.rethrowRuntimeException(t);
                }
        }).whenComplete(listener);
    }


    private static boolean isTableExistsError(Throwable e, @Nullable String templateName) {
        return e instanceof ResourceAlreadyExistsException
               || (templateName != null && isTemplateAlreadyExistsException(e));
    }

    private static boolean isTemplateAlreadyExistsException(Throwable e) {
        return e instanceof IllegalArgumentException
               && e.getMessage() != null && e.getMessage().endsWith("already exists");
    }
}
