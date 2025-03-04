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

package io.crate.execution.ddl.tables;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.annotations.VisibleForTesting;
import io.crate.Constants;
import io.crate.action.FutureActionListener;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.action.sql.Session;
import io.crate.analyze.AddColumnAnalyzedStatement;
import io.crate.analyze.AlterTableAnalyzedStatement;
import io.crate.analyze.AlterTableOpenCloseAnalyzedStatement;
import io.crate.analyze.AlterTableRenameAnalyzedStatement;
import io.crate.analyze.TableParameter;
import io.crate.analyze.TableParameters;
import io.crate.concurrent.MultiBiConsumer;
import io.crate.data.Row;
import io.crate.execution.ddl.index.SwapAndDropIndexRequest;
import io.crate.execution.ddl.index.TransportSwapAndDropIndexNameAction;
import io.crate.execution.support.ChainableAction;
import io.crate.execution.support.ChainableActions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_BLOCKS_WRITE_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.AbstractScopedSettings.ARCHIVED_SETTINGS_PREFIX;

@Singleton
public class AlterTableOperation {

    public static final String RESIZE_PREFIX = ".resized.";

    private final ClusterService clusterService;
    private final TransportPutIndexTemplateAction transportPutIndexTemplateAction;
    private final TransportPutMappingAction transportPutMappingAction;
    private final TransportUpdateSettingsAction transportUpdateSettingsAction;
    private final TransportRenameTableAction transportRenameTableAction;
    private final TransportOpenCloseTableOrPartitionAction transportOpenCloseTableOrPartitionAction;
    private final TransportResizeAction transportResizeAction;
    private final TransportDeleteIndexAction transportDeleteIndexAction;
    private final TransportSwapAndDropIndexNameAction transportSwapAndDropIndexNameAction;
    private final IndexScopedSettings indexScopedSettings;
    private final SQLOperations sqlOperations;
    private Session session;

    @Inject
    public AlterTableOperation(ClusterService clusterService,
                               TransportPutIndexTemplateAction transportPutIndexTemplateAction,
                               TransportPutMappingAction transportPutMappingAction,
                               TransportUpdateSettingsAction transportUpdateSettingsAction,
                               TransportRenameTableAction transportRenameTableAction,
                               TransportOpenCloseTableOrPartitionAction transportOpenCloseTableOrPartitionAction,
                               TransportResizeAction transportResizeAction,
                               TransportDeleteIndexAction transportDeleteIndexAction,
                               TransportSwapAndDropIndexNameAction transportSwapAndDropIndexNameAction,
                               SQLOperations sqlOperations,
                               IndexScopedSettings indexScopedSettings) {

        this.clusterService = clusterService;
        this.transportPutIndexTemplateAction = transportPutIndexTemplateAction;
        this.transportPutMappingAction = transportPutMappingAction;
        this.transportUpdateSettingsAction = transportUpdateSettingsAction;
        this.transportRenameTableAction = transportRenameTableAction;
        this.transportResizeAction = transportResizeAction;
        this.transportDeleteIndexAction = transportDeleteIndexAction;
        this.transportSwapAndDropIndexNameAction = transportSwapAndDropIndexNameAction;
        this.transportOpenCloseTableOrPartitionAction = transportOpenCloseTableOrPartitionAction;
        this.indexScopedSettings = indexScopedSettings;
        this.sqlOperations = sqlOperations;
    }

    public CompletableFuture<Long> executeAlterTableAddColumn(final AddColumnAnalyzedStatement analysis) {
        final CompletableFuture<Long> result = new CompletableFuture<>();
        if (analysis.newPrimaryKeys() || analysis.hasNewGeneratedColumns()) {
            RelationName ident = analysis.table().ident();
            String stmt =
                String.format(Locale.ENGLISH, "SELECT COUNT(*) FROM \"%s\".\"%s\"", ident.schema(), ident.name());

            try {
                session().quickExec(stmt, new ResultSetReceiver(analysis, result), Row.EMPTY);
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        } else {
            addColumnToTable(analysis, result);
        }
        return result;
    }

    private Session session() {
        if (session == null) {
            this.session = sqlOperations.newSystemSession();
        }
        return session;
    }

    public CompletableFuture<Long> executeAlterTableOpenClose(final AlterTableOpenCloseAnalyzedStatement analysis) {
        FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(r -> -1L);
        String partitionIndexName = null;
        Optional<PartitionName> partitionName = analysis.partitionName();
        if (partitionName.isPresent()) {
            partitionIndexName = partitionName.get().asIndexName();
        }
        OpenCloseTableOrPartitionRequest request = new OpenCloseTableOrPartitionRequest(
            analysis.tableInfo().ident(), partitionIndexName, analysis.openTable());
        transportOpenCloseTableOrPartitionAction.execute(request, listener);
        return listener;
    }

    public CompletableFuture<Long> executeAlterTable(AlterTableAnalyzedStatement analysis) {
        final Settings settings = analysis.tableParameter().settings();
        final DocTableInfo table = analysis.table();
        final boolean includesNumberOfShardsSetting = settings.hasValue(SETTING_NUMBER_OF_SHARDS);
        final boolean isResizeOperationRequired = includesNumberOfShardsSetting &&
                                                  (!table.isPartitioned() || analysis.partitionName().isPresent());

        if (isResizeOperationRequired) {
            if (settings.size() > 1) {
                throw new IllegalArgumentException("Setting [number_of_shards] cannot be combined with other settings");
            }
            return executeAlterTableChangeNumberOfShards(analysis);
        }
        return executeAlterTableSetOrReset(analysis);
    }

    private CompletableFuture<Long> executeAlterTableSetOrReset(AlterTableAnalyzedStatement analysis) {
        DocTableInfo table = analysis.table();
        List<CompletableFuture<Long>> results = new ArrayList<>(3);
        if (table.isPartitioned()) {
            Optional<PartitionName> partitionName = analysis.partitionName();
            if (partitionName.isPresent()) {
                String index = partitionName.get().asIndexName();
                results.add(updateMapping(analysis.tableParameter().mappings(), index));
                results.add(updateSettings(analysis.tableParameter(), index));
            } else {
                // template gets all changes unfiltered
                results.add(updateTemplate(analysis.tableParameter(), table.ident()));

                if (!analysis.excludePartitions()) {
                    // create new filtered partition table settings
                    List<String> supportedSettings = TableParameters.PARTITIONED_TABLE_PARAMETER_INFO_FOR_TEMPLATE_UPDATE
                        .supportedSettings()
                        .values()
                        .stream()
                        .map(Setting::getKey)
                        .collect(Collectors.toList());
                    // auto_expand_replicas must be explicitly added as it is hidden under NumberOfReplicasSetting
                    supportedSettings.add(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS);
                    TableParameter parameterWithFilteredSettings = new TableParameter(
                        analysis.tableParameter().settings(),
                        supportedSettings);

                    String[] indices = Stream.of(table.concreteIndices()).toArray(String[]::new);
                    results.add(updateMapping(analysis.tableParameter().mappings(), indices));
                    results.add(updateSettings(parameterWithFilteredSettings, indices));
                }
            }
        } else {
            results.add(updateMapping(analysis.tableParameter().mappings(), table.ident().indexNameOrAlias()));
            results.add(updateSettings(analysis.tableParameter(), table.ident().indexNameOrAlias()));
        }

        final CompletableFuture<Long> result = new CompletableFuture<>();
        applyMultiFutureCallback(result, results);
        return result;
    }

    private CompletableFuture<Long> executeAlterTableChangeNumberOfShards(AlterTableAnalyzedStatement analysis) {
        final DocTableInfo table = analysis.table();
        final boolean isPartitioned = table.isPartitioned();
        String sourceIndexName;
        String sourceIndexAlias;
        if (isPartitioned) {
            Optional<PartitionName> partitionName = analysis.partitionName();
            assert partitionName.isPresent() : "Resizing operations for partitioned tables " +
                                               "are only supported at partition level";
            sourceIndexName = partitionName.get().asIndexName();
            sourceIndexAlias = table.ident().indexNameOrAlias();
        } else {
            sourceIndexName = table.ident().indexNameOrAlias();
            sourceIndexAlias = null;
        }

        final ClusterState currentState = clusterService.state();
        final IndexMetaData sourceIndexMetaData = currentState.metaData().index(sourceIndexName);
        final int targetNumberOfShards = getNumberOfShards(analysis.tableParameter().settings());
        validateForResizeRequest(sourceIndexMetaData, targetNumberOfShards);

        final List<ChainableAction<Long>> actions = new ArrayList<>();
        final String resizedIndex = RESIZE_PREFIX + sourceIndexName;
        deleteLeftOverFromPreviousOperations(currentState, actions, resizedIndex);

        actions.add(new ChainableAction<>(
            () -> resizeIndex(
                currentState.metaData().index(sourceIndexName),
                sourceIndexAlias,
                resizedIndex,
                targetNumberOfShards
            ),
            () -> CompletableFuture.completedFuture(-1L)
        ));
        actions.add(new ChainableAction<>(
            () -> swapAndDropIndex(resizedIndex, sourceIndexName)
                .exceptionally(error -> {
                    throw new IllegalStateException(
                        "The resize operation to change the number of shards completed partially but run into a failure. " +
                        "Please retry the operation or clean up the internal indices with ALTER CLUSTER GC DANGLING ARTIFACTS. "
                        + error.getMessage(), error
                    );
                }),
            () -> CompletableFuture.completedFuture(-1L)
        ));
        return ChainableActions.run(actions);
    }

    private void deleteLeftOverFromPreviousOperations(ClusterState currentState,
                                                      List<ChainableAction<Long>> actions,
                                                      String resizeIndex) {

        if (currentState.metaData().hasIndex(resizeIndex)) {
            actions.add(new ChainableAction<>(
                () -> deleteIndex(resizeIndex),
                () -> CompletableFuture.completedFuture(-1L)
            ));
        }
    }

    private static void validateForResizeRequest(IndexMetaData sourceIndex, int targetNumberOfShards) {
        validateNumberOfShardsForResize(sourceIndex, targetNumberOfShards);
        validateReadOnlyIndexForResize(sourceIndex);
    }

    @VisibleForTesting
    static int getNumberOfShards(final Settings settings) {
        return Objects.requireNonNull(
            settings.getAsInt(SETTING_NUMBER_OF_SHARDS, null),
            "Setting 'number_of_shards' is missing"
        );
    }

    @VisibleForTesting
    static void validateNumberOfShardsForResize(IndexMetaData indexMetaData, int targetNumberOfShards) {
        final int currentNumberOfShards = indexMetaData.getNumberOfShards();
        if (currentNumberOfShards == targetNumberOfShards) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH,
                    "Table/partition is already allocated <%d> shards",
                    currentNumberOfShards));
        }
        if (targetNumberOfShards < currentNumberOfShards && currentNumberOfShards % targetNumberOfShards != 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH,
                    "Requested number of shards: <%d> needs to be a factor of the current one: <%d>",
                    targetNumberOfShards,
                    currentNumberOfShards));
        }
    }

    @VisibleForTesting
    static void validateReadOnlyIndexForResize(IndexMetaData indexMetaData) {
        final Boolean readOnly = indexMetaData
            .getSettings()
            .getAsBoolean(INDEX_BLOCKS_WRITE_SETTING.getKey(), Boolean.FALSE);
        if (!readOnly) {
            throw new IllegalStateException("Table/Partition needs to be at a read-only state." +
                                               " Use 'ALTER table ... set (\"blocks.write\"=true)' and retry");
        }
    }

    private CompletableFuture<Long> resizeIndex(IndexMetaData sourceIndex,
                                                @Nullable String sourceIndexAlias,
                                                String targetIndexName,
                                                int targetNumberOfShards) {
        Settings targetIndexSettings = Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, targetNumberOfShards)
            .build();

        int currentNumShards = sourceIndex.getNumberOfShards();
        ResizeRequest request = new ResizeRequest(targetIndexName, sourceIndex.getIndex().getName());
        request.getTargetIndexRequest().settings(targetIndexSettings);
        if (sourceIndexAlias != null) {
            request.getTargetIndexRequest().alias(new Alias(sourceIndexAlias));
        }
        request.setResizeType(targetNumberOfShards > currentNumShards ? ResizeType.SPLIT : ResizeType.SHRINK);
        request.setCopySettings(Boolean.TRUE);
        request.setWaitForActiveShards(ActiveShardCount.ONE);
        FutureActionListener<ResizeResponse, Long> listener =
            new FutureActionListener<>(resp -> resp.isAcknowledged() ? 1L : 0L);

        transportResizeAction.execute(request, listener);
        return listener;
    }

    private CompletableFuture<Long> deleteIndex(String... indexNames) {
        DeleteIndexRequest request = new DeleteIndexRequest(indexNames);

        FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(r -> 0L);
        transportDeleteIndexAction.execute(request, listener);
        return listener;
    }

    private CompletableFuture<Long> swapAndDropIndex(String source, String target) {
        SwapAndDropIndexRequest request = new SwapAndDropIndexRequest(source, target);
        FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(response -> {
            if (!response.isAcknowledged()) {
                throw new RuntimeException("Publishing new cluster state during Shrink operation (rename phase) " +
                                           "has timed out");
            }
            return 0L;
        });
        transportSwapAndDropIndexNameAction.execute(request, listener);
        return listener;
    }

    public CompletableFuture<Long> executeAlterTableRenameTable(AlterTableRenameAnalyzedStatement statement) {
        DocTableInfo sourceTableInfo = statement.sourceTableInfo();
        RelationName sourceRelationName = sourceTableInfo.ident();
        RelationName targetRelationName = statement.targetTableIdent();

        return renameTable(sourceRelationName, targetRelationName, sourceTableInfo.isPartitioned());
    }

    private CompletableFuture<Long> renameTable(RelationName sourceRelationName,
                                                RelationName targetRelationName,
                                                boolean isPartitioned) {
        RenameTableRequest request = new RenameTableRequest(sourceRelationName, targetRelationName, isPartitioned);
        FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(r -> -1L);
        transportRenameTableAction.execute(request, listener);
        return listener;
    }

    private CompletableFuture<Long> updateTemplate(TableParameter tableParameter, RelationName relationName) {
        return updateTemplate(tableParameter.mappings(), tableParameter.settings(), relationName);
    }

    private CompletableFuture<Long> updateTemplate(Map<String, Object> newMappings,
                                                   Settings newSettings,
                                                   RelationName relationName) {
        return updateTemplate(newMappings, Collections.emptyMap(), newSettings, relationName);
    }

    private CompletableFuture<Long> updateTemplate(Map<String, Object> newMappings,
                                                   Map<String, Object> mappingsToRemove,
                                                   Settings newSettings,
                                                   RelationName relationName) {
        String templateName = PartitionName.templateName(relationName.schema(), relationName.name());
        IndexTemplateMetaData indexTemplateMetaData =
            clusterService.state().metaData().templates().get(templateName);
        if (indexTemplateMetaData == null) {
            return CompletableFuture.failedFuture(new RuntimeException("Template '" + templateName + "' for partitioned table is missing"));
        }

        PutIndexTemplateRequest request = preparePutIndexTemplateRequest(indexScopedSettings, indexTemplateMetaData,
            newMappings, mappingsToRemove, newSettings, relationName, templateName);
        FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(r -> -1L);
        transportPutIndexTemplateAction.execute(request, listener);
        return listener;
    }

    @VisibleForTesting
    static PutIndexTemplateRequest preparePutIndexTemplateRequest(IndexScopedSettings indexScopedSettings,
                                                                  IndexTemplateMetaData indexTemplateMetaData,
                                                                  Map<String, Object> newMappings,
                                                                  Map<String, Object> mappingsToRemove,
                                                                  Settings newSettings,
                                                                  RelationName relationName,
                                                                  String templateName) {
        // merge mappings
        Map<String, Object> mapping = mergeTemplateMapping(indexTemplateMetaData, newMappings);

        // remove mappings
        mapping = removeFromMapping(mapping, mappingsToRemove);

        // merge settings
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(indexTemplateMetaData.settings());
        settingsBuilder.put(newSettings);

        // Private settings must not be (over-)written as they are generated, remove them.
        // Validation will fail otherwise.
        Settings settings = settingsBuilder.build()
            .filter(k -> indexScopedSettings.isPrivateSetting(k) == false);

        PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName)
            .create(false)
            .mapping(Constants.DEFAULT_MAPPING_TYPE, mapping)
            .order(indexTemplateMetaData.order())
            .settings(settings)
            .patterns(indexTemplateMetaData.getPatterns())
            .alias(new Alias(relationName.indexNameOrAlias()));
        for (ObjectObjectCursor<String, AliasMetaData> container : indexTemplateMetaData.aliases()) {
            Alias alias = new Alias(container.key);
            request.alias(alias);
        }
        return request;
    }

    /**
     It is important to add the _meta field explicitly to the changed mapping here since ES updates
     the mapping and removes/overwrites the _meta field.
     Tested with PartitionedTableIntegrationTest#testAlterPartitionedTableKeepsMetadata()
     */
    @VisibleForTesting
    static PutMappingRequest preparePutMappingRequest(Map<String, Object> oldMapping, Map<String, Object> newMapping, String... indices) {

        // Only merge the _meta
        XContentHelper.update(oldMapping, newMapping, false);
        newMapping.put("_meta", oldMapping.get("_meta"));

        // update mapping of all indices
        PutMappingRequest request = new PutMappingRequest(indices);
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        request.type(Constants.DEFAULT_MAPPING_TYPE);
        request.source(newMapping);
        return request;
    }

    private CompletableFuture<Long> updateMapping(Map<String, Object> newMapping, String... indices) {
        if (newMapping.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        assert areAllMappingsEqual(clusterService.state().metaData(), indices) :
            "Trying to update mapping for indices with different existing mappings";

        Map<String, Object> mapping;
        try {
            MetaData metaData = clusterService.state().metaData();
            String index = indices[0];
            mapping = metaData.index(index).mapping(Constants.DEFAULT_MAPPING_TYPE).getSourceAsMap();
        } catch (ElasticsearchParseException e) {
            return CompletableFuture.failedFuture(e);
        }

        FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(r -> 0L);
        transportPutMappingAction.execute(preparePutMappingRequest(mapping, newMapping, indices), listener);
        return listener;
    }

    public static Map<String, Object> parseMapping(String mappingSource) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent
            .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, mappingSource)) {
            return parser.map();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to parse mapping");
        }
    }

    public static Map<String, Object> mergeTemplateMapping(IndexTemplateMetaData templateMetaData,
                                                           Map<String, Object> newMapping) {
        Map<String, Object> mergedMapping = new HashMap<>();
        for (ObjectObjectCursor<String, CompressedXContent> cursor : templateMetaData.mappings()) {
            try {
                Map<String, Object> mapping = parseMapping(cursor.value.toString());
                Object o = mapping.get(Constants.DEFAULT_MAPPING_TYPE);
                assert o != null && o instanceof Map :
                    "o must not be null and must be instance of Map";

                XContentHelper.update(mergedMapping, (Map) o, false);
            } catch (IOException e) {
                // pass
            }
        }
        XContentHelper.update(mergedMapping, newMapping, false);
        return mergedMapping;
    }

    public static Map<String, Object> removeFromMapping(Map<String, Object> mapping, Map<String, Object> mappingsToRemove) {
        for (String key : mappingsToRemove.keySet()) {
            if (mapping.containsKey(key)) {
                if (mapping.get(key) instanceof Map) {
                    mapping.put(key, removeFromMapping((Map<String, Object>) mapping.get(key),
                        (Map<String, Object>) mappingsToRemove.get(key)));
                } else {
                    mapping.remove(key);
                }
            }
        }

        return mapping;
    }

    private CompletableFuture<Long> updateSettings(TableParameter concreteTableParameter, String... indices) {
        return updateSettings(concreteTableParameter.settings(), indices);
    }

    private CompletableFuture<Long> updateSettings(Settings newSettings, String... indices) {
        if (newSettings.isEmpty() || indices.length == 0) {
            return CompletableFuture.completedFuture(null);
        }
        UpdateSettingsRequest request = new UpdateSettingsRequest(markArchivedSettings(newSettings), indices);
        request.indicesOptions(IndicesOptions.lenientExpandOpen());

        FutureActionListener<AcknowledgedResponse, Long> listener = new FutureActionListener<>(r -> 0L);
        transportUpdateSettingsAction.execute(request, listener);
        return listener;
    }

    /**
     * Mark possible archived settings to be removed, they are not allowed to be written.
     * (Private settings are already filtered out later at the meta data update service.)
     */
    @VisibleForTesting
    static Settings markArchivedSettings(Settings settings) {
        return Settings.builder()
            .put(settings)
            .putNull(ARCHIVED_SETTINGS_PREFIX + "*")
            .build();
    }

    private void addColumnToTable(AddColumnAnalyzedStatement analysis, final CompletableFuture<?> result) {
        boolean updateTemplate = analysis.table().isPartitioned();
        List<CompletableFuture<Long>> results = new ArrayList<>(2);
        final Map<String, Object> mapping = analysis.analyzedTableElements().toMapping();

        if (updateTemplate) {
            results.add(updateTemplate(mapping, Settings.EMPTY, analysis.table().ident()));
        }

        String[] indexNames = analysis.table().concreteIndices();
        if (indexNames.length > 0) {
            results.add(updateMapping(mapping, indexNames));
        }

        applyMultiFutureCallback(result, results);
    }

    private static void applyMultiFutureCallback(final CompletableFuture<?> result, List<CompletableFuture<Long>> futures) {
        BiConsumer<List<Long>, Throwable> finalConsumer = (List<Long> receivedResult, Throwable t) -> {
            if (t == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(t);
            }
        };

        MultiBiConsumer<Long> consumer = new MultiBiConsumer<>(futures.size(), finalConsumer);
        for (CompletableFuture<Long> future : futures) {
            future.whenComplete(consumer);
        }
    }

    private static boolean areAllMappingsEqual(MetaData metaData, String... indices) {
        Map<String, Object> lastMapping = null;
        for (String index : indices) {
            try {
                Map<String, Object> mapping = metaData.index(index).mapping(Constants.DEFAULT_MAPPING_TYPE).getSourceAsMap();
                if (lastMapping != null && !lastMapping.equals(mapping)) {
                    return false;
                }
                lastMapping = mapping;
            } catch (ElasticsearchParseException e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    private class ResultSetReceiver implements ResultReceiver {

        private final AddColumnAnalyzedStatement analysis;
        private final CompletableFuture<?> result;

        private long count;

        ResultSetReceiver(AddColumnAnalyzedStatement analysis, CompletableFuture<?> result) {
            this.analysis = analysis;
            this.result = result;
        }

        @Override
        public void setNextRow(Row row) {
            count = (long) row.get(0);
        }

        @Override
        public void batchFinished() {
        }

        @Override
        public void allFinished(boolean interrupted) {
            if (count == 0L) {
                addColumnToTable(analysis, result);
            } else {
                String columnFailure = analysis.newPrimaryKeys() ? "primary key" : "generated";
                fail(new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "Cannot add a %s column to a table that isn't empty", columnFailure)));
            }
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            result.completeExceptionally(t);
        }

        @Override
        public CompletableFuture<?> completionFuture() {
            return result;
        }

    }
}
