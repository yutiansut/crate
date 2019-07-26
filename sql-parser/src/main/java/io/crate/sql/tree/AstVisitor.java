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

package io.crate.sql.tree;


import javax.annotation.Nullable;

public abstract class AstVisitor<T, R, C> {

    public R process(Node<T> node, @Nullable C context) {
        return node.accept(this, context);
    }

    protected R visitNode(Node<T> node, C context) {
        return null;
    }

    protected R visitExpression(Expression<T> node, C context) {
        return visitNode(node, context);
    }

    protected R visitCurrentTime(CurrentTime<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitExtract(Extract<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitArithmeticExpression(ArithmeticExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitBetweenPredicate(BetweenPredicate<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitComparisonExpression(ComparisonExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitLiteral(Literal<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitDoubleLiteral(DoubleLiteral<T> node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitStatement(Statement<T> node, C context) {
        return visitNode(node, context);
    }

    protected R visitQuery(Query<T> node, C context) {
        return visitStatement(node, context);
    }

    protected R visitExplain(Explain<T> node, C context) {
        return visitStatement(node, context);
    }

    protected R visitShowTables(ShowTables<T> node, C context) {
        return visitStatement(node, context);
    }

    protected R visitShowSchemas(ShowSchemas<T> node, C context) {
        return visitStatement(node, context);
    }

    protected R visitShowColumns(ShowColumns<T> node, C context) {
        return visitStatement(node, context);
    }

    protected R visitSelect(Select<T> node, C context) {
        return visitNode(node, context);
    }

    protected R visitRelation(Relation<T> node, C context) {
        return visitNode(node, context);
    }

    protected R visitQueryBody(QueryBody<T> node, C context) {
        return visitRelation(node, context);
    }

    protected R visitQuerySpecification(QuerySpecification<T> node, C context) {
        return visitQueryBody(node, context);
    }

    protected R visitSetOperation(SetOperation<T> node, C context) {
        return visitQueryBody(node, context);
    }

    protected R visitUnion(Union<T> node, C context) {
        return visitSetOperation(node, context);
    }

    protected R visitIntersect(Intersect<T> node, C context) {
        return visitSetOperation(node, context);
    }

    protected R visitExcept(Except<T> node, C context) {
        return visitSetOperation(node, context);
    }

    protected R visitWhenClause(WhenClause<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitInPredicate(InPredicate<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitFunctionCall(FunctionCall<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitSimpleCaseExpression(SimpleCaseExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitStringLiteral(StringLiteral<T> node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitEscapedCharStringLiteral(EscapedCharStringLiteral<T> node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitBooleanLiteral(BooleanLiteral<T> node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitInListExpression(InListExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitQualifiedNameReference(QualifiedNameReference<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitIfExpression(IfExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitNullLiteral(NullLiteral<T> node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitNegativeExpression(NegativeExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitNotExpression(NotExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitSelectItem(SelectItem<T> node, C context) {
        return visitNode(node, context);
    }

    protected R visitSingleColumn(SingleColumn<T> node, C context) {
        return visitSelectItem(node, context);
    }

    protected R visitAllColumns(AllColumns<T> node, C context) {
        return visitSelectItem(node, context);
    }

    protected R visitSearchedCaseExpression(SearchedCaseExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitLikePredicate(LikePredicate<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitIsNotNullPredicate(IsNotNullPredicate<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitIsNullPredicate(IsNullPredicate<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitLongLiteral(LongLiteral<T> node, C context) {
        return visitLiteral(node, context);
    }

    protected R visitLogicalBinaryExpression(LogicalBinaryExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitSubqueryExpression(SubqueryExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitSortItem(SortItem<T> node, C context) {
        return visitNode(node, context);
    }

    protected R visitTable(Table<T> node, C context) {
        return visitQueryBody(node, context);
    }

    protected R visitTableSubquery(TableSubquery<T> node, C context) {
        return visitQueryBody(node, context);
    }

    protected R visitAliasedRelation(AliasedRelation<T> node, C context) {
        return visitRelation(node, context);
    }

    protected R visitJoin(Join<T> node, C context) {
        return visitRelation(node, context);
    }

    protected R visitExists(ExistsPredicate<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitCast(Cast<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitTryCast(TryCast<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitSubscriptExpression(SubscriptExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    public R visitParameterExpression(ParameterExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    public R visitInsert(Insert<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitValuesList(ValuesList<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitDelete(Delete<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitUpdate(Update<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitAssignment(Assignment<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitCopyFrom(CopyFrom<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitCreateTable(CreateTable<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateFunction(CreateFunction<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitFunctionArgument(FunctionArgument<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitDropFunction(DropFunction<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitDropUser(DropUser<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitGrantPrivilege(GrantPrivilege<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitDenyPrivilege(DenyPrivilege<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitRevokePrivilege(RevokePrivilege<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitShowCreateTable(ShowCreateTable<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitTableElement(TableElement<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitClusteredBy(ClusteredBy<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitColumnDefinition(ColumnDefinition<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitColumnType(ColumnType<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitObjectColumnType(ObjectColumnType<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitColumnConstraint(ColumnConstraint<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitPrimaryKeyColumnConstraint(PrimaryKeyColumnConstraint<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitNotNullColumnConstraint(NotNullColumnConstraint<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitIndexColumnConstraint(IndexColumnConstraint<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitColumnStorageDefinition(ColumnStorageDefinition<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitGenericProperties(GenericProperties<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitGenericProperty(GenericProperty<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitPrimaryKeyConstraint(PrimaryKeyConstraint<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitIndexDefinition(IndexDefinition<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitCollectionColumnType(CollectionColumnType<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitDropTable(DropTable<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateAnalyzer(CreateAnalyzer<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitDropAnalyzer(DropAnalyzer<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitTokenizer(Tokenizer<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitCharFilters(CharFilters<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitTokenFilters(TokenFilters<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitCreateBlobTable(CreateBlobTable<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitDropBlobTable(DropBlobTable<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitRefreshStatement(RefreshStatement<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitOptimizeStatement(OptimizeStatement<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterTable(AlterTable<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterTableOpenClose(AlterTableOpenClose<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterTableRename(AlterTableRename<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterTableReroute(AlterTableReroute<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterBlobTable(AlterBlobTable<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterClusterRerouteRetryFailed(AlterClusterRerouteRetryFailed<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterUser(AlterUser<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitCopyTo(CopyTo<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitPartitionedBy(PartitionedBy<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitArrayComparisonExpression(ArrayComparisonExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    protected R visitArraySubQueryExpression(ArraySubQueryExpression<T> node, C context) {
        return visitExpression(node, context);
    }

    public R visitArrayLiteral(ArrayLiteral<T> node, C context) {
        return visitLiteral(node, context);
    }

    public R visitObjectLiteral(ObjectLiteral<T> node, C context) {
        return visitLiteral(node, context);
    }

    public R visitArrayLikePredicate(ArrayLikePredicate<T> node, C context) {
        return visitExpression(node, context);
    }

    public R visitSetStatement(SetStatement<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitResetStatement(ResetStatement<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitAlterTableAddColumnStatement(AlterTableAddColumn<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitRerouteMoveShard(RerouteMoveShard<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitRerouteAllocateReplicaShard(RerouteAllocateReplicaShard<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitRerouteCancelShard(RerouteCancelShard<T> node, C context) {
        return visitNode(node, context);
    }

    public R visitAddColumnDefinition(AddColumnDefinition<T> node, C context) {
        return visitTableElement(node, context);
    }

    public R visitInsertFromValues(InsertFromValues<T> node, C context) {
        return visitInsert(node, context);
    }

    public R visitInsertFromSubquery(InsertFromSubquery<T> node, C context) {
        return visitInsert(node, context);
    }

    public R visitMatchPredicate(MatchPredicate<T> node, C context) {
        return visitExpression(node, context);
    }

    public R visitMatchPredicateColumnIdent(MatchPredicateColumnIdent<T> node, C context) {
        return visitExpression(node, context);
    }

    public R visitKillStatement(KillStatement<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitDeallocateStatement(DeallocateStatement<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitDropRepository(DropRepository<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateRepository(CreateRepository<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitDropSnapshot(DropSnapshot<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateSnapshot(CreateSnapshot<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitRestoreSnapshot(RestoreSnapshot<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitTableFunction(TableFunction<T> node, C context) {
        return visitQueryBody(node, context);
    }

    public R visitBegin(BeginStatement<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitCommit(CommitStatement<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitShowTransaction(ShowTransaction<T> showTransaction, C context) {
        return visitStatement(showTransaction, context);
    }

    public R visitShowSessionParameter(ShowSessionParameter<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateUser(CreateUser<T> node, C context) {
        return visitStatement(node, context);
    }

    public R visitCreateView(CreateView<T> createView, C context) {
        return visitStatement(createView, context);
    }

    public R visitDropView(DropView<T> dropView, C context) {
        return visitStatement(dropView, context);
    }

    public R visitSwapTable(SwapTable<T> swapTable, C context) {
        return visitStatement(swapTable, context);
    }

    public R visitFrameBound(FrameBound<T> frameBound, C context) {
        return visitNode(frameBound, context);
    }

    public R visitWindow(Window<T> window, C context) {
        return visitNode(window, context);
    }

    public R visitWindowFrame(WindowFrame<T> windowFrame, C context) {
        return visitNode(windowFrame, context);
    }

    public R visitGCDanglingArtifacts(GCDanglingArtifacts<T> gcDanglingArtifacts, C context) {
        return visitStatement(gcDanglingArtifacts, context);
    }

    public R visitAlterClusterDecommissionNode(DecommissionNodeStatement<T> decommissionNodeStatement, C context) {
        return visitStatement(decommissionNodeStatement, context);
    }

    public R visitReroutePromoteReplica(PromoteReplica<T> promoteReplica, C context) {
        return visitNode(promoteReplica, context);
    }

    public R visitValues(Values<T> values, C context) {
        return visitRelation(values, context);
    }
}
