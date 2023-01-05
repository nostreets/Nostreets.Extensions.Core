// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Nostreets.Extensions.Helpers.Data.QueryProvider
{
    /// <summary>
    /// Rewrite aggregate expressions, moving them into same select expression that has the group-by clause
    /// </summary>
    public class AggregateRewriter : DbExpressionVisitor
    {
        QueryLanguage language;
        ILookup<TableAlias, AggregateSubqueryExpression> lookup;
        Dictionary<AggregateSubqueryExpression, Expression> map;

        private AggregateRewriter(QueryLanguage language, Expression expr)
        {
            this.language = language;
            this.map = new Dictionary<AggregateSubqueryExpression, Expression>();
            this.lookup = AggregateGatherer.Gather(expr).ToLookup(a => a.GroupByAlias);
        }

        public static Expression Rewrite(QueryLanguage language, Expression expr)
        {
            return new AggregateRewriter(language, expr).Visit(expr);
        }

        protected override Expression VisitSelect(SelectExpression select)
        {
            select = (SelectExpression)base.VisitSelect(select);
            if (lookup.Contains(select.Alias))
            {
                List<ColumnDeclaration> aggColumns = new List<ColumnDeclaration>(select.Columns);
                foreach (AggregateSubqueryExpression ae in lookup[select.Alias])
                {
                    string name = "agg" + aggColumns.Count;
                    var colType = this.language.TypeSystem.GetColumnType(ae.Type);
                    ColumnDeclaration cd = new ColumnDeclaration(name, ae.AggregateInGroupSelect, colType);
                    this.map.Add(ae, new ColumnExpression(ae.Type, colType, ae.GroupByAlias, name));
                    aggColumns.Add(cd);
                }
                return new SelectExpression(select.Alias, aggColumns, select.From, select.Where, select.OrderBy, select.GroupBy, select.IsDistinct, select.Skip, select.Take, select.IsReverse);
            }
            return select;
        }

        protected override Expression VisitAggregateSubquery(AggregateSubqueryExpression aggregate)
        {
            Expression mapped;
            if (this.map.TryGetValue(aggregate, out mapped))
            {
                return mapped;
            }
            return this.Visit(aggregate.AggregateAsSubquery);
        }

        class AggregateGatherer : DbExpressionVisitor
        {
            List<AggregateSubqueryExpression> aggregates = new List<AggregateSubqueryExpression>();
            private AggregateGatherer()
            {
            }

            internal static List<AggregateSubqueryExpression> Gather(Expression expression)
            {
                AggregateGatherer gatherer = new AggregateGatherer();
                gatherer.Visit(expression);
                return gatherer.aggregates;
            }

            protected override Expression VisitAggregateSubquery(AggregateSubqueryExpression aggregate)
            {
                this.aggregates.Add(aggregate);
                return base.VisitAggregateSubquery(aggregate);
            }
        }
    }


    /// <summary>
    /// Moves order-bys to the outermost select if possible
    /// </summary>
    public class OrderByRewriter : DbExpressionVisitor
    {
        QueryLanguage language;
        IList<OrderExpression> gatheredOrderings;
        bool isOuterMostSelect;

        private OrderByRewriter(QueryLanguage language)
        {
            this.language = language;
            this.isOuterMostSelect = true;
        }

        public static Expression Rewrite(QueryLanguage language, Expression expression)
        {
            return new OrderByRewriter(language).Visit(expression);
        }

        protected override Expression VisitSelect(SelectExpression select)
        {
            bool saveIsOuterMostSelect = this.isOuterMostSelect;
            try
            {
                this.isOuterMostSelect = false;
                select = (SelectExpression)base.VisitSelect(select);

                bool hasOrderBy = select.OrderBy != null && select.OrderBy.Count > 0;
                bool hasGroupBy = select.GroupBy != null && select.GroupBy.Count > 0;
                bool canHaveOrderBy = saveIsOuterMostSelect || select.Take != null || select.Skip != null;
                bool canReceiveOrderings = canHaveOrderBy && !hasGroupBy && !select.IsDistinct && !AggregateChecker.HasAggregates(select);

                if (hasOrderBy)
                {
                    this.PrependOrderings(select.OrderBy);
                }

                if (select.IsReverse)
                {
                    this.ReverseOrderings();
                }

                IEnumerable<OrderExpression> orderings = null;
                if (canReceiveOrderings)
                {
                    orderings = this.gatheredOrderings;
                }
                else if (canHaveOrderBy)
                {
                    orderings = select.OrderBy;
                }
                bool canPassOnOrderings = !saveIsOuterMostSelect && !hasGroupBy && !select.IsDistinct;
                ReadOnlyCollection<ColumnDeclaration> columns = select.Columns;
                if (this.gatheredOrderings != null)
                {
                    if (canPassOnOrderings)
                    {
                        var producedAliases = DeclaredAliasGatherer.Gather(select.From);
                        // reproject order expressions using this select's alias so the outer select will have properly formed expressions
                        BindResult project = this.RebindOrderings(this.gatheredOrderings, select.Alias, producedAliases, select.Columns);
                        this.gatheredOrderings = null;
                        this.PrependOrderings(project.Orderings);
                        columns = project.Columns;
                    }
                    else
                    {
                        this.gatheredOrderings = null;
                    }
                }
                if (orderings != select.OrderBy || columns != select.Columns || select.IsReverse)
                {
                    select = new SelectExpression(select.Alias, columns, select.From, select.Where, orderings, select.GroupBy, select.IsDistinct, select.Skip, select.Take, false);
                }
                return select;
            }
            finally
            {
                this.isOuterMostSelect = saveIsOuterMostSelect;
            }
        }

        protected override Expression VisitSubquery(SubqueryExpression subquery)
        {
            var saveOrderings = this.gatheredOrderings;
            this.gatheredOrderings = null;
            var result = base.VisitSubquery(subquery);
            this.gatheredOrderings = saveOrderings;
            return result;
        }

        protected override Expression VisitJoin(JoinExpression join)
        {
            // make sure order by expressions lifted up from the left side are not lost
            // when visiting the right side
            Expression left = this.VisitSource(join.Left);
            IList<OrderExpression> leftOrders = this.gatheredOrderings;
            this.gatheredOrderings = null; // start on the right with a clean slate
            Expression right = this.VisitSource(join.Right);
            this.PrependOrderings(leftOrders);
            Expression condition = this.Visit(join.Condition);
            if (left != join.Left || right != join.Right || condition != join.Condition)
            {
                return new JoinExpression(join.Join, left, right, condition);
            }
            return join;
        }

        /// <summary>
        /// Add a sequence of order expressions to an accumulated list, prepending so as
        /// to give precedence to the new expressions over any previous expressions
        /// </summary>
        /// <param name="newOrderings"></param>
        protected void PrependOrderings(IList<OrderExpression> newOrderings)
        {
            if (newOrderings != null)
            {
                if (this.gatheredOrderings == null)
                {
                    this.gatheredOrderings = new List<OrderExpression>();
                }
                for (int i = newOrderings.Count - 1; i >= 0; i--)
                {
                    this.gatheredOrderings.Insert(0, newOrderings[i]);
                }
                // trim off obvious duplicates
                HashSet<string> unique = new HashSet<string>();
                for (int i = 0; i < this.gatheredOrderings.Count;) 
                {
                    ColumnExpression column = this.gatheredOrderings[i].Expression as ColumnExpression;
                    if (column != null)
                    {
                        string hash = column.Alias + ":" + column.Name;
                        if (unique.Contains(hash))
                        {
                            this.gatheredOrderings.RemoveAt(i);
                            // don't increment 'i', just continue
                            continue;
                        }
                        else
                        {
                            unique.Add(hash);
                        }
                    }
                    i++;
                }
            }
        }

        protected void ReverseOrderings()
        {
            if (this.gatheredOrderings != null)
            {
                for (int i = 0, n = this.gatheredOrderings.Count; i < n; i++)
                {
                    var ord = this.gatheredOrderings[i];
                    this.gatheredOrderings[i] =
                        new OrderExpression(
                            ord.OrderType == OrderType.Ascending ? OrderType.Descending : OrderType.Ascending,
                            ord.Expression
                            );
                }
            }
        }

        protected class BindResult
        {
            ReadOnlyCollection<ColumnDeclaration> columns;
            ReadOnlyCollection<OrderExpression> orderings;
            public BindResult(IEnumerable<ColumnDeclaration> columns, IEnumerable<OrderExpression> orderings)
            {
                this.columns = columns as ReadOnlyCollection<ColumnDeclaration>;
                if (this.columns == null)
                {
                    this.columns = new List<ColumnDeclaration>(columns).AsReadOnly();
                }
                this.orderings = orderings as ReadOnlyCollection<OrderExpression>;
                if (this.orderings == null)
                {
                    this.orderings = new List<OrderExpression>(orderings).AsReadOnly();
                }
            }
            public ReadOnlyCollection<ColumnDeclaration> Columns
            {
                get { return this.columns; }
            }
            public ReadOnlyCollection<OrderExpression> Orderings
            {
                get { return this.orderings; }
            }
        }

        /// <summary>
        /// Rebind order expressions to reference a new alias and add to column declarations if necessary
        /// </summary>
        protected virtual BindResult RebindOrderings(IEnumerable<OrderExpression> orderings, TableAlias alias, HashSet<TableAlias> existingAliases, IEnumerable<ColumnDeclaration> existingColumns)
        {
            List<ColumnDeclaration> newColumns = null;
            List<OrderExpression> newOrderings = new List<OrderExpression>();
            foreach (OrderExpression ordering in orderings)
            {
                Expression expr = ordering.Expression;
                ColumnExpression column = expr as ColumnExpression;
                if (column == null || (existingAliases != null && existingAliases.Contains(column.Alias)))
                {
                    // check to see if a declared column already contains a similar expression
                    int iOrdinal = 0;
                    foreach (ColumnDeclaration decl in existingColumns)
                    {
                        ColumnExpression declColumn = decl.Expression as ColumnExpression;
                        if (decl.Expression == ordering.Expression ||
                            (column != null && declColumn != null && column.Alias == declColumn.Alias && column.Name == declColumn.Name))
                        {
                            // found it, so make a reference to this column
                            expr = new ColumnExpression(column.Type, column.QueryType, alias, decl.Name);
                            break;
                        }
                        iOrdinal++;
                    }
                    // if not already projected, add a new column declaration for it
                    if (expr == ordering.Expression)
                    {
                        if (newColumns == null)
                        {
                            newColumns = new List<ColumnDeclaration>(existingColumns);
                            existingColumns = newColumns;
                        }
                        string colName = column != null ? column.Name : "c" + iOrdinal;
                        colName = newColumns.GetAvailableColumnName(colName);
                        var colType = this.language.TypeSystem.GetColumnType(expr.Type);
                        newColumns.Add(new ColumnDeclaration(colName, ordering.Expression, colType));
                        expr = new ColumnExpression(expr.Type, colType, alias, colName);
                    }
                    newOrderings.Add(new OrderExpression(ordering.OrderType, expr));
                }
            }
            return new BindResult(existingColumns, newOrderings);
        }
    }


    /// <summary>
    /// Attempt to rewrite cross joins as inner joins
    /// </summary>
    public class CrossJoinRewriter : DbExpressionVisitor
    {
        private Expression currentWhere;

        public static Expression Rewrite(Expression expression)
        {
            return new CrossJoinRewriter().Visit(expression);
        }

        protected override Expression VisitSelect(SelectExpression select)
        {
            var saveWhere = this.currentWhere;
            try
            {
                this.currentWhere = select.Where;
                var result = (SelectExpression)base.VisitSelect(select);
                if (this.currentWhere != result.Where)
                {
                    return result.SetWhere(this.currentWhere);
                }
                return result;
            }
            finally
            {
                this.currentWhere = saveWhere;
            }
        }

        protected override Expression VisitJoin(JoinExpression join)
        {
            join = (JoinExpression)base.VisitJoin(join);
            if (join.Join == JoinType.CrossJoin && this.currentWhere != null)
            {
                // try to figure out which parts of the current where expression can be used for a join condition
                var declaredLeft = DeclaredAliasGatherer.Gather(join.Left);
                var declaredRight = DeclaredAliasGatherer.Gather(join.Right);
                var declared = new HashSet<TableAlias>(declaredLeft.Union(declaredRight));
                var exprs = this.currentWhere.Split(ExpressionType.And, ExpressionType.AndAlso);
                var good = exprs.Where(e => CanBeJoinCondition(e, declaredLeft, declaredRight, declared)).ToList();
                if (good.Count > 0)
                {
                    var condition = good.Join(ExpressionType.And);
                    join = this.UpdateJoin(join, JoinType.InnerJoin, join.Left, join.Right, condition);
                    var newWhere = exprs.Where(e => !good.Contains(e)).Join(ExpressionType.And);
                    this.currentWhere = newWhere;
                }
            }
            return join;
        }

        private bool CanBeJoinCondition(Expression expression, HashSet<TableAlias> left, HashSet<TableAlias> right, HashSet<TableAlias> all)
        {
            // an expression is good if it has at least one reference to an alias from both left & right sets and does
            // not have any additional references that are not in both left & right sets
            var referenced = ReferencedAliasGatherer.Gather(expression);
            var leftOkay = referenced.Intersect(left).Any();
            var rightOkay = referenced.Intersect(right).Any();
            var subset = referenced.IsSubsetOf(all);
            return leftOkay && rightOkay && subset;
        }
    }

    /// <summary>
    /// rewrites nested projections into client-side joins
    /// </summary>
    public class ClientJoinedProjectionRewriter : DbExpressionVisitor
    {
        QueryPolicy policy;
        QueryLanguage language;
        bool isTopLevel = true;
        SelectExpression currentSelect;
        MemberInfo currentMember;
        bool canJoinOnClient = true;

        private ClientJoinedProjectionRewriter(QueryPolicy policy, QueryLanguage language)
        {
            this.policy = policy;
            this.language = language;
        }

        public static Expression Rewrite(QueryPolicy policy, QueryLanguage language, Expression expression)
        {
            return new ClientJoinedProjectionRewriter(policy, language).Visit(expression);
        }

        protected override MemberAssignment VisitMemberAssignment(MemberAssignment assignment)
        {
            var saveMember = this.currentMember;
            this.currentMember = assignment.Member;
            Expression e = this.Visit(assignment.Expression);
            this.currentMember = saveMember;
            return this.UpdateMemberAssignment(assignment, assignment.Member, e);
        }

        protected override Expression VisitMemberAndExpression(MemberInfo member, Expression expression)
        {
            var saveMember = this.currentMember;
            this.currentMember = member;
            Expression e = this.Visit(expression);
            this.currentMember = saveMember;
            return e;
        }

        protected override Expression VisitProjection(ProjectionExpression proj)
        {
            SelectExpression save = this.currentSelect;
            this.currentSelect = proj.Select;
            try
            {
                if (!this.isTopLevel)
                {
                    if (this.CanJoinOnClient(this.currentSelect))
                    {
                        // make a query that combines all the constraints from the outer queries into a single select
                        SelectExpression newOuterSelect = (SelectExpression)QueryDuplicator.Duplicate(save);

                        // remap any references to the outer select to the new alias;
                        SelectExpression newInnerSelect = (SelectExpression)ColumnMapper.Map(proj.Select, newOuterSelect.Alias, save.Alias);
                        // add outer-join test
                        ProjectionExpression newInnerProjection = this.language.AddOuterJoinTest(new ProjectionExpression(newInnerSelect, proj.Projector));
                        newInnerSelect = newInnerProjection.Select;
                        Expression newProjector = newInnerProjection.Projector;

                        TableAlias newAlias = new TableAlias();
                        var pc = ColumnProjector.ProjectColumns(this.language, newProjector, null, newAlias, newOuterSelect.Alias, newInnerSelect.Alias);

                        JoinExpression join = new JoinExpression(JoinType.OuterApply, newOuterSelect, newInnerSelect, null);
                        SelectExpression joinedSelect = new SelectExpression(newAlias, pc.Columns, join, null, null, null, proj.IsSingleton, null, null, false);

                        // apply client-join treatment recursively
                        this.currentSelect = joinedSelect;
                        newProjector = this.Visit(pc.Projector);

                        // compute keys (this only works if join condition was a single column comparison)
                        List<Expression> outerKeys = new List<Expression>();
                        List<Expression> innerKeys = new List<Expression>();
                        if (this.GetEquiJoinKeyExpressions(newInnerSelect.Where, newOuterSelect.Alias, outerKeys, innerKeys))
                        {
                            // outerKey needs to refer to the outer-scope's alias
                            var outerKey = outerKeys.Select(k => ColumnMapper.Map(k, save.Alias, newOuterSelect.Alias));
                            // innerKey needs to refer to the new alias for the select with the new join
                            var innerKey = innerKeys.Select(k => ColumnMapper.Map(k, joinedSelect.Alias, ((ColumnExpression)k).Alias));
                            ProjectionExpression newProjection = new ProjectionExpression(joinedSelect, newProjector, proj.Aggregator);
                            return new ClientJoinExpression(newProjection, outerKey, innerKey);
                        }
                    }
                    else
                    {
                        bool saveJoin = this.canJoinOnClient;
                        this.canJoinOnClient = false;
                        var result = base.VisitProjection(proj);
                        this.canJoinOnClient = saveJoin;
                        return result;
                    }
                }
                else
                {
                    this.isTopLevel = false;
                }
                return base.VisitProjection(proj);
            }
            finally 
            {
                this.currentSelect = save;
            }
        }

        private bool CanJoinOnClient(SelectExpression select)
        {
            // can add singleton (1:0,1) join if no grouping/aggregates or distinct
            return 
                this.canJoinOnClient 
                && this.currentMember != null 
                && !this.policy.IsDeferLoaded(this.currentMember)
                && !select.IsDistinct
                && (select.GroupBy == null || select.GroupBy.Count == 0)
                && !AggregateChecker.HasAggregates(select);
        }

        private bool GetEquiJoinKeyExpressions(Expression predicate, TableAlias outerAlias, List<Expression> outerExpressions, List<Expression> innerExpressions)
        {
            if (predicate.NodeType == ExpressionType.Equal)
            {
                var b = (BinaryExpression)predicate;
                ColumnExpression leftCol = this.GetColumnExpression(b.Left);
                ColumnExpression rightCol = this.GetColumnExpression(b.Right);
                if (leftCol != null && rightCol != null)
                {
                    if (leftCol.Alias == outerAlias)
                    {
                        outerExpressions.Add(b.Left);
                        innerExpressions.Add(b.Right);
                        return true;
                    }
                    else if (rightCol.Alias == outerAlias)
                    {
                        innerExpressions.Add(b.Left);
                        outerExpressions.Add(b.Right);
                        return true;
                    }
                }
            }

            bool hadKey = false;
            var parts = predicate.Split(ExpressionType.And, ExpressionType.AndAlso);
            if (parts.Length > 1)
            {
                foreach (var part in parts)
                {
                    bool hasOuterAliasReference = ReferencedAliasGatherer.Gather(part).Contains(outerAlias);
                    if (hasOuterAliasReference)
                    {
                        if (!GetEquiJoinKeyExpressions(part, outerAlias, outerExpressions, innerExpressions))
                            return false;
                        hadKey = true;
                    }
                }
            }

            return hadKey;
        }

        private ColumnExpression GetColumnExpression(Expression expression)
        {
            // ignore converions 
            while (expression.NodeType == ExpressionType.Convert || expression.NodeType == ExpressionType.ConvertChecked)
            {
                expression = ((UnaryExpression)expression).Operand;
            }
            return expression as ColumnExpression;
        }

        protected override Expression VisitSubquery(SubqueryExpression subquery)
        {
            return subquery;
        }

        protected override Expression VisitCommand(CommandExpression command)
        {
            this.isTopLevel = true;
            return base.VisitCommand(command);
        }
    }

    /// <summary>
    /// Rewrites queries with skip and take to use the nested queries with inverted ordering technique
    /// </summary>
    public class SkipToNestedOrderByRewriter : DbExpressionVisitor
    {
        QueryLanguage language;

        private SkipToNestedOrderByRewriter(QueryLanguage language)
        {
            this.language = language;
        }

        public static Expression Rewrite(QueryLanguage language, Expression expression)
        {
            return new SkipToNestedOrderByRewriter(language).Visit(expression);
        }

        protected override Expression VisitSelect(SelectExpression select)
        {
            // select * from table order by x skip s take t 
            // =>
            // select * from (select top s * from (select top s + t from table order by x) order by -x) order by x

            select = (SelectExpression)base.VisitSelect(select);

            if (select.Skip != null && select.Take != null && select.OrderBy.Count > 0)
            {
                var skip = select.Skip;
                var take = select.Take;
                var skipPlusTake = PartialEvaluator.Eval(Expression.Add(skip, take));

                select = select.SetTake(skipPlusTake).SetSkip(null);
                select = select.AddRedundantSelect(this.language, new TableAlias());
                select = select.SetTake(take);

                // propogate order-bys to new layer
                select = (SelectExpression)OrderByRewriter.Rewrite(this.language, select);
                var inverted = select.OrderBy.Select(ob => new OrderExpression(
                    ob.OrderType == OrderType.Ascending ? OrderType.Descending : OrderType.Ascending,
                    ob.Expression
                    ));
                select = select.SetOrderBy(inverted);

                select = select.AddRedundantSelect(this.language, new TableAlias());
                select = select.SetTake(Expression.Constant(0)); // temporary
                select = (SelectExpression)OrderByRewriter.Rewrite(this.language, select);
                var reverted = select.OrderBy.Select(ob => new OrderExpression(
                    ob.OrderType == OrderType.Ascending ? OrderType.Descending : OrderType.Ascending,
                    ob.Expression
                    ));
                select = select.SetOrderBy(reverted);
                select = select.SetTake(null);
            }

            return select;
        }
    }


    /// <summary>
    /// Rewrites take and skip expressions into uses of TSQL row_number function
    /// </summary>
    public class SkipToRowNumberRewriter : DbExpressionVisitor
    {
        QueryLanguage language;

        private SkipToRowNumberRewriter(QueryLanguage language)
        {
            this.language = language;
        }

        public static Expression Rewrite(QueryLanguage language, Expression expression)
        {
            return new SkipToRowNumberRewriter(language).Visit(expression);
        }

        protected override Expression VisitSelect(SelectExpression select)
        {
            select = (SelectExpression)base.VisitSelect(select);
            if (select.Skip != null)
            {
                SelectExpression newSelect = select.SetSkip(null).SetTake(null);
                bool canAddColumn = !select.IsDistinct && (select.GroupBy == null || select.GroupBy.Count == 0);
                if (!canAddColumn)
                {
                    newSelect = newSelect.AddRedundantSelect(this.language, new TableAlias());
                }
                var colType = this.language.TypeSystem.GetColumnType(typeof(int));
                newSelect = newSelect.AddColumn(new ColumnDeclaration("_rownum", new RowNumberExpression(select.OrderBy), colType));

                // add layer for WHERE clause that references new rownum column
                newSelect = newSelect.AddRedundantSelect(this.language, new TableAlias());
                newSelect = newSelect.RemoveColumn(newSelect.Columns.Single(c => c.Name == "_rownum"));

                var newAlias = ((SelectExpression)newSelect.From).Alias;
                ColumnExpression rnCol = new ColumnExpression(typeof(int), colType, newAlias, "_rownum");
                Expression where;
                if (select.Take != null)
                {
                    where = new BetweenExpression(rnCol, Expression.Add(select.Skip, Expression.Constant(1)), Expression.Add(select.Skip, select.Take));
                }
                else
                {
                    where = rnCol.GreaterThan(select.Skip);
                }
                if (newSelect.Where != null)
                {
                    where = newSelect.Where.And(where);
                }
                newSelect = newSelect.SetWhere(where);

                select = newSelect;
            }
            return select;
        }
    }


    /// <summary>
    /// Rewrites nested singleton projection into server-side joins
    /// </summary>
    public class SingletonProjectionRewriter : DbExpressionVisitor
    {
        QueryLanguage language;
        bool isTopLevel = true;
        SelectExpression currentSelect;

        private SingletonProjectionRewriter(QueryLanguage language)
        {
            this.language = language;
        }

        public static Expression Rewrite(QueryLanguage language, Expression expression)
        {
            return new SingletonProjectionRewriter(language).Visit(expression);
        }

        protected override Expression VisitClientJoin(ClientJoinExpression join)
        {
            // treat client joins as new top level
            var saveTop = this.isTopLevel;
            var saveSelect = this.currentSelect;
            this.isTopLevel = true;
            this.currentSelect = null;
            Expression result = base.VisitClientJoin(join);
            this.isTopLevel = saveTop;
            this.currentSelect = saveSelect;
            return result;
        }

        protected override Expression VisitProjection(ProjectionExpression proj)
        {
            if (isTopLevel)
            {
                isTopLevel = false;
                this.currentSelect = proj.Select;
                Expression projector = this.Visit(proj.Projector);
                if (projector != proj.Projector || this.currentSelect != proj.Select)
                {
                    return new ProjectionExpression(this.currentSelect, projector, proj.Aggregator);
                }
                return proj;
            }

            if (proj.IsSingleton && this.CanJoinOnServer(this.currentSelect))
            {
                TableAlias newAlias = new TableAlias();
                this.currentSelect = this.currentSelect.AddRedundantSelect(this.language, newAlias);

                // remap any references to the outer select to the new alias;
                SelectExpression source = (SelectExpression)ColumnMapper.Map(proj.Select, newAlias, this.currentSelect.Alias);

                // add outer-join test
                ProjectionExpression pex = this.language.AddOuterJoinTest(new ProjectionExpression(source, proj.Projector));

                var pc = ColumnProjector.ProjectColumns(this.language, pex.Projector, this.currentSelect.Columns, this.currentSelect.Alias, newAlias, proj.Select.Alias);

                JoinExpression join = new JoinExpression(JoinType.OuterApply, this.currentSelect.From, pex.Select, null);

                this.currentSelect = new SelectExpression(this.currentSelect.Alias, pc.Columns, join, null);
                return this.Visit(pc.Projector);
            }

            var saveTop = this.isTopLevel;
            var saveSelect = this.currentSelect;
            this.isTopLevel = true;
            this.currentSelect = null;
            Expression result = base.VisitProjection(proj);
            this.isTopLevel = saveTop;
            this.currentSelect = saveSelect;
            return result;
        }

        private bool CanJoinOnServer(SelectExpression select)
        {
            // can add singleton (1:0,1) join if no grouping/aggregates or distinct
            return !select.IsDistinct
                && (select.GroupBy == null || select.GroupBy.Count == 0)
                && !AggregateChecker.HasAggregates(select);
        }

        protected override Expression VisitSubquery(SubqueryExpression subquery)
        {
            return subquery;
        }

        protected override Expression VisitCommand(CommandExpression command)
        {
            this.isTopLevel = true;
            return base.VisitCommand(command);
        }
    }

    public class ComparisonRewriter : DbExpressionVisitor
    {
        QueryMapping mapping;

        private ComparisonRewriter(QueryMapping mapping)
        {
            this.mapping = mapping;
        }

        public static Expression Rewrite(QueryMapping mapping, Expression expression)
        {
            return new ComparisonRewriter(mapping).Visit(expression);
        }

        protected override Expression VisitBinary(BinaryExpression b)
        {
            switch (b.NodeType)
            {
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                    Expression result = this.Compare(b);
                    if (result == b)
                        goto default;
                    return this.Visit(result);
                default:
                    return base.VisitBinary(b);
            }
        }

        protected Expression SkipConvert(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Convert)
            {
                expression = ((UnaryExpression)expression).Operand;
            }
            return expression;
        }

        protected Expression Compare(BinaryExpression bop)
        {
            var e1 = this.SkipConvert(bop.Left);
            var e2 = this.SkipConvert(bop.Right);

            OuterJoinedExpression oj1 = e1 as OuterJoinedExpression;
            OuterJoinedExpression oj2 = e2 as OuterJoinedExpression;

            EntityExpression entity1 = oj1 != null ? oj1.Expression as EntityExpression : e1 as EntityExpression;
            EntityExpression entity2 = oj2 != null ? oj2.Expression as EntityExpression : e2 as EntityExpression;

            bool negate = bop.NodeType == ExpressionType.NotEqual;

            // check for outer-joined entity comparing against null. These are special because outer joins have 
            // a test expression specifically desgined to be tested against null to determine if the joined side exists.
            if (oj1 != null && e2.NodeType == ExpressionType.Constant && ((ConstantExpression)e2).Value == null)
            {
                return MakeIsNull(oj1.Test, negate);
            }
            else if (oj2 != null && e1.NodeType == ExpressionType.Constant && ((ConstantExpression)e1).Value == null)
            {
                return MakeIsNull(oj2.Test, negate);
            }

            // if either side is an entity construction expression then compare using its primary key members
            if (entity1 != null)
            {
                return this.MakePredicate(e1, e2, this.mapping.GetPrimaryKeyMembers(entity1.Entity), negate);
            }
            else if (entity2 != null)
            {
                return this.MakePredicate(e1, e2, this.mapping.GetPrimaryKeyMembers(entity2.Entity), negate);
            }

            // check for comparison of user constructed type projections
            var dm1 = this.GetDefinedMembers(e1);
            var dm2 = this.GetDefinedMembers(e2);

            if (dm1 == null && dm2 == null)
            {
                // neither are constructed types
                return bop;
            }

            if (dm1 != null && dm2 != null)
            {
                // both are constructed types, so they'd better have the same members declared
                HashSet<string> names1 = new HashSet<string>(dm1.Select(m => m.Name));
                HashSet<string> names2 = new HashSet<string>(dm2.Select(m => m.Name));
                if (names1.IsSubsetOf(names2) && names2.IsSubsetOf(names1))
                {
                    return MakePredicate(e1, e2, dm1, negate);
                }
            }
            else if (dm1 != null)
            {
                return MakePredicate(e1, e2, dm1, negate);
            }
            else if (dm2 != null)
            {
                return MakePredicate(e1, e2, dm2, negate);
            }

            throw new InvalidOperationException("Cannot compare two constructed types with different sets of members assigned.");
        }

        protected Expression MakeIsNull(Expression expression, bool negate)
        {
            Expression isnull = new IsNullExpression(expression);
            return negate ? Expression.Not(isnull) : isnull;
        }

        protected Expression MakePredicate(Expression e1, Expression e2, IEnumerable<MemberInfo> members, bool negate)
        {
            var pred = members.Select(m =>
                QueryBinder.BindMember(e1, m).Equal(QueryBinder.BindMember(e2, m))
                ).Join(ExpressionType.And);
            if (negate)
                pred = Expression.Not(pred);
            return pred;
        }

        private IEnumerable<MemberInfo> GetDefinedMembers(Expression expr)
        {
            MemberInitExpression mini = expr as MemberInitExpression;
            if (mini != null)
            {
                var members = mini.Bindings.Select(b => FixMember(b.Member));
                if (mini.NewExpression.Members != null)
                {
                    members.Concat(mini.NewExpression.Members.Select(m => FixMember(m)));
                }
                return members;
            }
            else
            {
                NewExpression nex = expr as NewExpression;
                if (nex != null && nex.Members != null)
                {
                    return nex.Members.Select(m => FixMember(m));
                }
            }
            return null;
        }

        private static MemberInfo FixMember(MemberInfo member)
        {
            if (member.MemberType == MemberTypes.Method && member.Name.StartsWith("get_"))
            {
                return member.DeclaringType.GetProperty(member.Name.Substring(4));
            }
            return member;
        }
    }

    /// <summary>
    /// Attempts to rewrite cross-apply and outer-apply joins as inner and left-outer joins
    /// </summary>
    public class CrossApplyRewriter : DbExpressionVisitor
    {
        QueryLanguage language;

        private CrossApplyRewriter(QueryLanguage language)
        {
            this.language = language;
        }

        public static Expression Rewrite(QueryLanguage language, Expression expression)
        {
            return new CrossApplyRewriter(language).Visit(expression);
        }

        protected override Expression VisitJoin(JoinExpression join)
        {
            join = (JoinExpression)base.VisitJoin(join);

            if (join.Join == JoinType.CrossApply || join.Join == JoinType.OuterApply)
            {
                if (join.Right is TableExpression)
                {
                    return new JoinExpression(JoinType.CrossJoin, join.Left, join.Right, null);
                }
                else
                {
                    SelectExpression select = join.Right as SelectExpression;
                    // Only consider rewriting cross apply if 
                    //   1) right side is a select
                    //   2) other than in the where clause in the right-side select, no left-side declared aliases are referenced
                    //   3) and has no behavior that would change semantics if the where clause is removed (like groups, aggregates, take, skip, etc).
                    // Note: it is best to attempt this after redundant subqueries have been removed.
                    if (select != null
                        && select.Take == null
                        && select.Skip == null
                        && !AggregateChecker.HasAggregates(select)
                        && (select.GroupBy == null || select.GroupBy.Count == 0))
                    {
                        SelectExpression selectWithoutWhere = select.SetWhere(null);
                        HashSet<TableAlias> referencedAliases = ReferencedAliasGatherer.Gather(selectWithoutWhere);
                        HashSet<TableAlias> declaredAliases = DeclaredAliasGatherer.Gather(join.Left);
                        referencedAliases.IntersectWith(declaredAliases);
                        if (referencedAliases.Count == 0)
                        {
                            Expression where = select.Where;
                            select = selectWithoutWhere;
                            var pc = ColumnProjector.ProjectColumns(this.language, where, select.Columns, select.Alias, DeclaredAliasGatherer.Gather(select.From));
                            select = select.SetColumns(pc.Columns);
                            where = pc.Projector;
                            JoinType jt = (where == null) ? JoinType.CrossJoin : (join.Join == JoinType.CrossApply ? JoinType.InnerJoin : JoinType.LeftOuter);
                            return new JoinExpression(jt, join.Left, select, where);
                        }
                    }
                }
            }

            return join;
        }

        private bool CanBeColumn(Expression expr)
        {
            return expr != null && expr.NodeType == (ExpressionType)DbExpressionType.Column;
        }
    }
}