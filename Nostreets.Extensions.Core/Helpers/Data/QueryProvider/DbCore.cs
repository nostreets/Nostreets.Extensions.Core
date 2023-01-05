// Copyright (c) Microsoft Corporation.  All rights reserved.
// This source code is made available under the terms of the Microsoft Public License (MS-PL)

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Nostreets.Extensions.Helpers.Data.QueryProvider
{
    public class DbTypeSystem : QueryTypeSystem
    {
        public override QueryType Parse(string typeDeclaration)
        {
            string[] args = null;
            string typeName = null;
            string remainder = null;
            int openParen = typeDeclaration.IndexOf('(');
            if (openParen >= 0)
            {
                typeName = typeDeclaration.Substring(0, openParen).Trim();

                int closeParen = typeDeclaration.IndexOf(')', openParen);
                if (closeParen < openParen) closeParen = typeDeclaration.Length;

                string argstr = typeDeclaration.Substring(openParen + 1, closeParen - (openParen + 1));
                args = argstr.Split(',');
                remainder = typeDeclaration.Substring(closeParen + 1);
            }
            else
            {
                int space = typeDeclaration.IndexOf(' ');
                if (space >= 0)
                {
                    typeName = typeDeclaration.Substring(0, space);
                    remainder = typeDeclaration.Substring(space + 1).Trim();
                }
                else
                {
                    typeName = typeDeclaration;
                }
            }

            bool isNotNull = (remainder != null) ? remainder.ToUpper().Contains("NOT NULL") : false;

            return this.GetQueryType(typeName, args, isNotNull);
        }

        public virtual QueryType GetQueryType(string typeName, string[] args, bool isNotNull)
        {
            if (String.Compare(typeName, "rowversion", StringComparison.OrdinalIgnoreCase) == 0)
            {
                typeName = "Timestamp";
            }

            if (String.Compare(typeName, "numeric", StringComparison.OrdinalIgnoreCase) == 0)
            {
                typeName = "Decimal";
            }

            if (String.Compare(typeName, "sql_variant", StringComparison.OrdinalIgnoreCase) == 0)
            {
                typeName = "Variant";
            }

            SqlDbType dbType = this.GetSqlType(typeName);

            int length = 0;
            short precision = 0;
            short scale = 0;

            switch (dbType)
            {
                case SqlDbType.Binary:
                case SqlDbType.Char:
                case SqlDbType.Image:
                case SqlDbType.NChar:
                case SqlDbType.NVarChar:
                case SqlDbType.VarBinary:
                case SqlDbType.VarChar:
                    if (args == null || args.Length < 1)
                    {
                        length = 80;
                    }
                    else if (string.Compare(args[0], "max", true) == 0)
                    {
                        length = Int32.MaxValue;
                    }
                    else
                    {
                        length = Int32.Parse(args[0]);
                    }
                    break;
                case SqlDbType.Money:
                    if (args == null || args.Length < 1)
                    {
                        precision = 29;
                    }
                    else
                    {
                        precision = Int16.Parse(args[0]);
                    }
                    if (args == null || args.Length < 2)
                    {
                        scale = 4;
                    }
                    else
                    {
                        scale = Int16.Parse(args[1]);
                    }
                    break;
                case SqlDbType.Decimal:
                    if (args == null || args.Length < 1)
                    {
                        precision = 29;
                    }
                    else
                    {
                        precision = Int16.Parse(args[0]);
                    }
                    if (args == null || args.Length < 2)
                    {
                        scale = 0;
                    }
                    else
                    {
                        scale = Int16.Parse(args[1]);
                    }
                    break;
                case SqlDbType.Float:
                case SqlDbType.Real:
                    if (args == null || args.Length < 1)
                    {
                        precision = 29;
                    }
                    else
                    {
                        precision = Int16.Parse(args[0]);
                    }
                    break;
            }

            return NewType(dbType, isNotNull, length, precision, scale);
        }

        public virtual QueryType NewType(SqlDbType type, bool isNotNull, int length, short precision, short scale)
        {
            return new DbQueryType(type, isNotNull, length, precision, scale);
        }

        public virtual SqlDbType GetSqlType(string typeName)
        {
            return (SqlDbType)Enum.Parse(typeof(SqlDbType), typeName, true);
        }

        public virtual int StringDefaultSize
        {
            get { return Int32.MaxValue; }
        }

        public virtual int BinaryDefaultSize
        {
            get { return Int32.MaxValue; }
        }

        public override QueryType GetColumnType(Type type)
        {
            bool isNotNull = type.IsValueType && !TypeHelper.IsNullableType(type);
            type = TypeHelper.GetNonNullableType(type);
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Boolean:
                    return NewType(SqlDbType.Bit, isNotNull, 0, 0, 0);
                case TypeCode.SByte:
                case TypeCode.Byte:
                    return NewType(SqlDbType.TinyInt, isNotNull, 0, 0, 0);
                case TypeCode.Int16:
                case TypeCode.UInt16:
                    return NewType(SqlDbType.SmallInt, isNotNull, 0, 0, 0);
                case TypeCode.Int32:
                case TypeCode.UInt32:
                    return NewType(SqlDbType.Int, isNotNull, 0, 0, 0);
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    return NewType(SqlDbType.BigInt, isNotNull, 0, 0, 0);
                case TypeCode.Single:
                case TypeCode.Double:
                    return NewType(SqlDbType.Float, isNotNull, 0, 0, 0);
                case TypeCode.String:
                    return NewType(SqlDbType.NVarChar, isNotNull, this.StringDefaultSize, 0, 0);
                case TypeCode.Char:
                    return NewType(SqlDbType.NChar, isNotNull, 1, 0, 0);
                case TypeCode.DateTime:
                    return NewType(SqlDbType.DateTime, isNotNull, 0, 0, 0);
                case TypeCode.Decimal:
                    return NewType(SqlDbType.Decimal, isNotNull, 0, 29, 4);
                default:
                    if (type == typeof(byte[]))
                        return NewType(SqlDbType.VarBinary, isNotNull, this.BinaryDefaultSize, 0, 0);
                    else if (type == typeof(Guid))
                        return NewType(SqlDbType.UniqueIdentifier, isNotNull, 0, 0, 0);
                    else if (type == typeof(DateTimeOffset))
                        return NewType(SqlDbType.DateTimeOffset, isNotNull, 0, 0, 0);
                    else if (type == typeof(TimeSpan))
                        return NewType(SqlDbType.Time, isNotNull, 0, 0, 0);
                    return null;
            }
        }

        public static DbType GetDbType(SqlDbType dbType)
        {
            switch (dbType)
            {
                case SqlDbType.BigInt:
                    return DbType.Int64;
                case SqlDbType.Binary:
                    return DbType.Binary;
                case SqlDbType.Bit:
                    return DbType.Boolean;
                case SqlDbType.Char:
                    return DbType.AnsiStringFixedLength;
                case SqlDbType.Date:
                    return DbType.Date;
                case SqlDbType.DateTime:
                case SqlDbType.SmallDateTime:
                    return DbType.DateTime;
                case SqlDbType.DateTime2:
                    return DbType.DateTime2;
                case SqlDbType.DateTimeOffset:
                    return DbType.DateTimeOffset;
                case SqlDbType.Decimal:
                    return DbType.Decimal;
                case SqlDbType.Float:
                case SqlDbType.Real:
                    return DbType.Double;
                case SqlDbType.Image:
                    return DbType.Binary;
                case SqlDbType.Int:
                    return DbType.Int32;
                case SqlDbType.Money:
                case SqlDbType.SmallMoney:
                    return DbType.Currency;
                case SqlDbType.NChar:
                    return DbType.StringFixedLength;
                case SqlDbType.NText:
                case SqlDbType.NVarChar:
                    return DbType.String;
                case SqlDbType.SmallInt:
                    return DbType.Int16;
                case SqlDbType.Text:
                    return DbType.AnsiString;
                case SqlDbType.Time:
                    return DbType.Time;
                case SqlDbType.Timestamp:
                    return DbType.Binary;
                case SqlDbType.TinyInt:
                    return DbType.SByte;
                case SqlDbType.Udt:
                    return DbType.Object;
                case SqlDbType.UniqueIdentifier:
                    return DbType.Guid;
                case SqlDbType.VarBinary:
                    return DbType.Binary;
                case SqlDbType.VarChar:
                    return DbType.AnsiString;
                case SqlDbType.Variant:
                    return DbType.Object;
                case SqlDbType.Xml:
                    return DbType.String;
                default:
                    throw new InvalidOperationException(string.Format("Unhandled sql type: {0}", dbType));
            }
        }

        public static bool IsVariableLength(SqlDbType dbType)
        {
            switch (dbType)
            {
                case SqlDbType.Image:
                case SqlDbType.NText:
                case SqlDbType.NVarChar:
                case SqlDbType.Text:
                case SqlDbType.VarBinary:
                case SqlDbType.VarChar:
                case SqlDbType.Xml:
                    return true;
                default:
                    return false;
            }
        }

        public override string GetVariableDeclaration(QueryType type, bool suppressSize)
        {
            var sqlType = (DbQueryType)type;
            StringBuilder sb = new StringBuilder();
            sb.Append(sqlType.SqlDbType.ToString().ToUpper());
            if (sqlType.Length > 0 && !suppressSize)
            {
                if (sqlType.Length == Int32.MaxValue)
                {
                    sb.Append("(max)");
                }
                else
                {
                    sb.AppendFormat("({0})", sqlType.Length);
                }
            }
            else if (sqlType.Precision != 0)
            {
                if (sqlType.Scale != 0)
                {
                    sb.AppendFormat("({0},{1})", sqlType.Precision, sqlType.Scale);
                }
                else
                {
                    sb.AppendFormat("({0})", sqlType.Precision);
                }
            }
            return sb.ToString();
        }
    }

    public class DbQueryType : QueryType
    {
        SqlDbType dbType;
        bool notNull;
        int length;
        short precision;
        short scale;

        public DbQueryType(SqlDbType dbType, bool notNull, int length, short precision, short scale)
        {
            this.dbType = dbType;
            this.notNull = notNull;
            this.length = length;
            this.precision = precision;
            this.scale = scale;
        }

        public DbType DbType
        {
            get { return DbTypeSystem.GetDbType(this.dbType); }
        }

        public SqlDbType SqlDbType
        {
            get { return this.dbType; }
        }

        public override int Length
        {
            get { return this.length; }
        }

        public override bool NotNull
        {
            get { return this.notNull; }
        }

        public override short Precision
        {
            get { return this.precision; }
        }

        public override short Scale
        {
            get { return this.scale; }
        }
    }


    /// <summary>
    /// Determines if two expressions are equivalent. Supports DbExpression nodes.
    /// </summary>
    public class DbExpressionComparer : ExpressionComparer
    {
        ScopedDictionary<TableAlias, TableAlias> aliasScope;

        protected DbExpressionComparer(
            ScopedDictionary<ParameterExpression, ParameterExpression> parameterScope,
            Func<object, object, bool> fnCompare,
            ScopedDictionary<TableAlias, TableAlias> aliasScope)
            : base(parameterScope, fnCompare)
        {
            this.aliasScope = aliasScope;
        }

        public new static bool AreEqual(Expression a, Expression b)
        {
            return AreEqual(null, null, a, b, null);
        }

        public new static bool AreEqual(Expression a, Expression b, Func<object, object, bool> fnCompare)
        {
            return AreEqual(null, null, a, b, fnCompare);
        }

        public static bool AreEqual(ScopedDictionary<ParameterExpression, ParameterExpression> parameterScope, ScopedDictionary<TableAlias, TableAlias> aliasScope, Expression a, Expression b)
        {
            return new DbExpressionComparer(parameterScope, null, aliasScope).Compare(a, b);
        }

        public static bool AreEqual(ScopedDictionary<ParameterExpression, ParameterExpression> parameterScope, ScopedDictionary<TableAlias, TableAlias> aliasScope, Expression a, Expression b, Func<object, object, bool> fnCompare)
        {
            return new DbExpressionComparer(parameterScope, fnCompare, aliasScope).Compare(a, b);
        }

        protected override bool Compare(Expression a, Expression b)
        {
            if (a == b)
                return true;
            if (a == null || b == null)
                return false;
            if (a.NodeType != b.NodeType)
                return false;
            if (a.Type != b.Type)
                return false;
            switch ((DbExpressionType)a.NodeType)
            {
                case DbExpressionType.Table:
                    return this.CompareTable((TableExpression)a, (TableExpression)b);
                case DbExpressionType.Column:
                    return this.CompareColumn((ColumnExpression)a, (ColumnExpression)b);
                case DbExpressionType.Select:
                    return this.CompareSelect((SelectExpression)a, (SelectExpression)b);
                case DbExpressionType.Join:
                    return this.CompareJoin((JoinExpression)a, (JoinExpression)b);
                case DbExpressionType.Aggregate:
                    return this.CompareAggregate((AggregateExpression)a, (AggregateExpression)b);
                case DbExpressionType.Scalar:
                case DbExpressionType.Exists:
                case DbExpressionType.In:
                    return this.CompareSubquery((SubqueryExpression)a, (SubqueryExpression)b);
                case DbExpressionType.AggregateSubquery:
                    return this.CompareAggregateSubquery((AggregateSubqueryExpression)a, (AggregateSubqueryExpression)b);
                case DbExpressionType.IsNull:
                    return this.CompareIsNull((IsNullExpression)a, (IsNullExpression)b);
                case DbExpressionType.Between:
                    return this.CompareBetween((BetweenExpression)a, (BetweenExpression)b);
                case DbExpressionType.RowCount:
                    return this.CompareRowNumber((RowNumberExpression)a, (RowNumberExpression)b);
                case DbExpressionType.Projection:
                    return this.CompareProjection((ProjectionExpression)a, (ProjectionExpression)b);
                case DbExpressionType.NamedValue:
                    return this.CompareNamedValue((NamedValueExpression)a, (NamedValueExpression)b);
                case DbExpressionType.Insert:
                    return this.CompareInsert((InsertCommand)a, (InsertCommand)b);
                case DbExpressionType.Update:
                    return this.CompareUpdate((UpdateCommand)a, (UpdateCommand)b);
                case DbExpressionType.Delete:
                    return this.CompareDelete((DeleteCommand)a, (DeleteCommand)b);
                case DbExpressionType.Batch:
                    return this.CompareBatch((BatchExpression)a, (BatchExpression)b);
                case DbExpressionType.Function:
                    return this.CompareFunction((FunctionExpression)a, (FunctionExpression)b);
                case DbExpressionType.Entity:
                    return this.CompareEntity((EntityExpression)a, (EntityExpression)b);
                case DbExpressionType.If:
                    return this.CompareIf((IFCommand)a, (IFCommand)b);
                case DbExpressionType.Block:
                    return this.CompareBlock((BlockCommand)a, (BlockCommand)b);
                default:
                    return base.Compare(a, b);
            }
        }

        protected virtual bool CompareTable(TableExpression a, TableExpression b)
        {
            return a.Name == b.Name;
        }

        protected virtual bool CompareColumn(ColumnExpression a, ColumnExpression b)
        {
            return this.CompareAlias(a.Alias, b.Alias) && a.Name == b.Name;
        }

        protected virtual bool CompareAlias(TableAlias a, TableAlias b)
        {
            if (this.aliasScope != null)
            {
                TableAlias mapped;
                if (this.aliasScope.TryGetValue(a, out mapped))
                    return mapped == b;
            }
            return a == b;
        }

        protected virtual bool CompareSelect(SelectExpression a, SelectExpression b)
        {
            var save = this.aliasScope;
            try
            {
                if (!this.Compare(a.From, b.From))
                    return false;

                this.aliasScope = new ScopedDictionary<TableAlias, TableAlias>(save);
                this.MapAliases(a.From, b.From);

                return this.Compare(a.Where, b.Where)
                    && this.CompareOrderList(a.OrderBy, b.OrderBy)
                    && this.CompareExpressionList(a.GroupBy, b.GroupBy)
                    && this.Compare(a.Skip, b.Skip)
                    && this.Compare(a.Take, b.Take)
                    && a.IsDistinct == b.IsDistinct
                    && a.IsReverse == b.IsReverse
                    && this.CompareColumnDeclarations(a.Columns, b.Columns);
            }
            finally
            {
                this.aliasScope = save;
            }
        }

        private void MapAliases(Expression a, Expression b)
        {
            TableAlias[] prodA = DeclaredAliasGatherer.Gather(a).ToArray();
            TableAlias[] prodB = DeclaredAliasGatherer.Gather(b).ToArray();
            for (int i = 0, n = prodA.Length; i < n; i++)
            {
                this.aliasScope.Add(prodA[i], prodB[i]);
            }
        }

        protected virtual bool CompareOrderList(ReadOnlyCollection<OrderExpression> a, ReadOnlyCollection<OrderExpression> b)
        {
            if (a == b)
                return true;
            if (a == null || b == null)
                return false;
            if (a.Count != b.Count)
                return false;
            for (int i = 0, n = a.Count; i < n; i++)
            {
                if (a[i].OrderType != b[i].OrderType ||
                    !this.Compare(a[i].Expression, b[i].Expression))
                    return false;
            }
            return true;
        }

        protected virtual bool CompareColumnDeclarations(ReadOnlyCollection<ColumnDeclaration> a, ReadOnlyCollection<ColumnDeclaration> b)
        {
            if (a == b)
                return true;
            if (a == null || b == null)
                return false;
            if (a.Count != b.Count)
                return false;
            for (int i = 0, n = a.Count; i < n; i++)
            {
                if (!this.CompareColumnDeclaration(a[i], b[i]))
                    return false;
            }
            return true;
        }

        protected virtual bool CompareColumnDeclaration(ColumnDeclaration a, ColumnDeclaration b)
        {
            return a.Name == b.Name && this.Compare(a.Expression, b.Expression);
        }

        protected virtual bool CompareJoin(JoinExpression a, JoinExpression b)
        {
            if (a.Join != b.Join || !this.Compare(a.Left, b.Left))
                return false;

            if (a.Join == JoinType.CrossApply || a.Join == JoinType.OuterApply)
            {
                var save = this.aliasScope;
                try
                {
                    this.aliasScope = new ScopedDictionary<TableAlias, TableAlias>(this.aliasScope);
                    this.MapAliases(a.Left, b.Left);

                    return this.Compare(a.Right, b.Right)
                        && this.Compare(a.Condition, b.Condition);
                }
                finally
                {
                    this.aliasScope = save;
                }
            }
            else
            {
                return this.Compare(a.Right, b.Right)
                    && this.Compare(a.Condition, b.Condition);
            }
        }

        protected virtual bool CompareAggregate(AggregateExpression a, AggregateExpression b)
        {
            return a.AggregateName == b.AggregateName && this.Compare(a.Argument, b.Argument);
        }

        protected virtual bool CompareIsNull(IsNullExpression a, IsNullExpression b)
        {
            return this.Compare(a.Expression, b.Expression);
        }

        protected virtual bool CompareBetween(BetweenExpression a, BetweenExpression b)
        {
            return this.Compare(a.Expression, b.Expression)
                && this.Compare(a.Lower, b.Lower)
                && this.Compare(a.Upper, b.Upper);
        }

        protected virtual bool CompareRowNumber(RowNumberExpression a, RowNumberExpression b)
        {
            return this.CompareOrderList(a.OrderBy, b.OrderBy);
        }

        protected virtual bool CompareNamedValue(NamedValueExpression a, NamedValueExpression b)
        {
            return a.Name == b.Name && this.Compare(a.Value, b.Value);
        }

        protected virtual bool CompareSubquery(SubqueryExpression a, SubqueryExpression b)
        {
            if (a.NodeType != b.NodeType)
                return false;
            switch ((DbExpressionType)a.NodeType)
            {
                case DbExpressionType.Scalar:
                    return this.CompareScalar((ScalarExpression)a, (ScalarExpression)b);
                case DbExpressionType.Exists:
                    return this.CompareExists((ExistsExpression)a, (ExistsExpression)b);
                case DbExpressionType.In:
                    return this.CompareIn((InExpression)a, (InExpression)b);
            }
            return false;
        }

        protected virtual bool CompareScalar(ScalarExpression a, ScalarExpression b)
        {
            return this.Compare(a.Select, b.Select);
        }

        protected virtual bool CompareExists(ExistsExpression a, ExistsExpression b)
        {
            return this.Compare(a.Select, b.Select);
        }

        protected virtual bool CompareIn(InExpression a, InExpression b)
        {
            return this.Compare(a.Expression, b.Expression)
                && this.Compare(a.Select, b.Select)
                && this.CompareExpressionList(a.Values, b.Values);
        }

        protected virtual bool CompareAggregateSubquery(AggregateSubqueryExpression a, AggregateSubqueryExpression b)
        {
            return this.Compare(a.AggregateAsSubquery, b.AggregateAsSubquery)
                && this.Compare(a.AggregateInGroupSelect, b.AggregateInGroupSelect)
                && a.GroupByAlias == b.GroupByAlias;
        }

        protected virtual bool CompareProjection(ProjectionExpression a, ProjectionExpression b)
        {
            if (!this.Compare(a.Select, b.Select))
                return false;

            var save = this.aliasScope;
            try
            {
                this.aliasScope = new ScopedDictionary<TableAlias, TableAlias>(this.aliasScope);
                this.aliasScope.Add(a.Select.Alias, b.Select.Alias);

                return this.Compare(a.Projector, b.Projector)
                    && this.Compare(a.Aggregator, b.Aggregator)
                    && a.IsSingleton == b.IsSingleton;
            }
            finally
            {
                this.aliasScope = save;
            }
        }

        protected virtual bool CompareInsert(InsertCommand x, InsertCommand y)
        {
            return this.Compare(x.Table, y.Table)
                && this.CompareColumnAssignments(x.Assignments, y.Assignments);
        }

        protected virtual bool CompareColumnAssignments(ReadOnlyCollection<ColumnAssignment> x, ReadOnlyCollection<ColumnAssignment> y)
        {
            if (x == y)
                return true;
            if (x.Count != y.Count)
                return false;
            for (int i = 0, n = x.Count; i < n; i++)
            {
                if (!this.Compare(x[i].Column, y[i].Column) || !this.Compare(x[i].Expression, y[i].Expression))
                    return false;
            }
            return true;
        }

        protected virtual bool CompareUpdate(UpdateCommand x, UpdateCommand y)
        {
            return this.Compare(x.Table, y.Table) && this.Compare(x.Where, y.Where) && this.CompareColumnAssignments(x.Assignments, y.Assignments);
        }

        protected virtual bool CompareDelete(DeleteCommand x, DeleteCommand y)
        {
            return this.Compare(x.Table, y.Table) && this.Compare(x.Where, y.Where);
        }

        protected virtual bool CompareBatch(BatchExpression x, BatchExpression y)
        {
            return this.Compare(x.Input, y.Input) && this.Compare(x.Operation, y.Operation)
                && this.Compare(x.BatchSize, y.BatchSize) && this.Compare(x.Stream, y.Stream);
        }

        protected virtual bool CompareIf(IFCommand x, IFCommand y)
        {
            return this.Compare(x.Check, y.Check) && this.Compare(x.IfTrue, y.IfTrue) && this.Compare(x.IfFalse, y.IfFalse);
        }

        protected virtual bool CompareBlock(BlockCommand x, BlockCommand y)
        {
            if (x.Commands.Count != y.Commands.Count)
                return false;
            for (int i = 0, n = x.Commands.Count; i < n; i++)
            {
                if (!this.Compare(x.Commands[i], y.Commands[i]))
                    return false;
            }
            return true;
        }

        protected virtual bool CompareFunction(FunctionExpression x, FunctionExpression y)
        {
            return x.Name == y.Name && this.CompareExpressionList(x.Arguments, y.Arguments);
        }

        protected virtual bool CompareEntity(EntityExpression x, EntityExpression y)
        {
            return x.Entity == y.Entity && this.Compare(x.Expression, y.Expression);
        }
    }


    /// <summary>
    /// Writes out an expression tree (including DbExpression nodes) in a C#-ish syntax
    /// </summary>
    public class DbExpressionWriter : ExpressionWriter
    {
        QueryLanguage language;
        Dictionary<TableAlias, int> aliasMap = new Dictionary<TableAlias, int>();

        protected DbExpressionWriter(TextWriter writer, QueryLanguage language)
            : base(writer)
        {
            this.language = language;
        }

        public new static void Write(TextWriter writer, Expression expression)
        {
            Write(writer, null, expression);
        }

        public static void Write(TextWriter writer, QueryLanguage language, Expression expression)
        {
            new DbExpressionWriter(writer, language).Visit(expression);
        }

        public new static string WriteToString(Expression expression)
        {
            return WriteToString(null, expression);
        }

        public static string WriteToString(QueryLanguage language, Expression expression)
        {
            StringWriter sw = new StringWriter();
            Write(sw, language, expression);
            return sw.ToString();
        }

        protected override Expression Visit(Expression exp)
        {
            if (exp == null)
                return null;

            switch ((DbExpressionType)exp.NodeType)
            {
                case DbExpressionType.Projection:
                    return this.VisitProjection((ProjectionExpression)exp);
                case DbExpressionType.ClientJoin:
                    return this.VisitClientJoin((ClientJoinExpression)exp);
                case DbExpressionType.Select:
                    return this.VisitSelect((SelectExpression)exp);
                case DbExpressionType.OuterJoined:
                    return this.VisitOuterJoined((OuterJoinedExpression)exp);
                case DbExpressionType.Column:
                    return this.VisitColumn((ColumnExpression)exp);
                case DbExpressionType.Insert:
                case DbExpressionType.Update:
                case DbExpressionType.Delete:
                case DbExpressionType.If:
                case DbExpressionType.Block:
                case DbExpressionType.Declaration:
                    return this.VisitCommand((CommandExpression)exp);
                case DbExpressionType.Batch:
                    return this.VisitBatch((BatchExpression)exp);
                case DbExpressionType.Function:
                    return this.VisitFunction((FunctionExpression)exp);
                case DbExpressionType.Entity:
                    return this.VisitEntity((EntityExpression)exp);
                default:
                    if (exp is DbExpression)
                    {
                        this.Write(this.FormatQuery(exp));
                        return exp;
                    }
                    else
                    {
                        return base.Visit(exp);
                    }
            }
        }

        protected void AddAlias(TableAlias alias)
        {
            if (!this.aliasMap.ContainsKey(alias))
            {
                this.aliasMap.Add(alias, this.aliasMap.Count);
            }
        }

        protected virtual Expression VisitProjection(ProjectionExpression projection)
        {
            this.AddAlias(projection.Select.Alias);
            this.Write("Project(");
            this.WriteLine(Indentation.Inner);
            this.Write("@\"");
            this.Visit(projection.Select);
            this.Write("\",");
            this.WriteLine(Indentation.Same);
            this.Visit(projection.Projector);
            this.Write(",");
            this.WriteLine(Indentation.Same);
            this.Visit(projection.Aggregator);
            this.WriteLine(Indentation.Outer);
            this.Write(")");
            return projection;
        }

        protected virtual Expression VisitClientJoin(ClientJoinExpression join)
        {
            this.AddAlias(join.Projection.Select.Alias);
            this.Write("ClientJoin(");
            this.WriteLine(Indentation.Inner);
            this.Write("OuterKey(");
            this.VisitExpressionList(join.OuterKey);
            this.Write("),");
            this.WriteLine(Indentation.Same);
            this.Write("InnerKey(");
            this.VisitExpressionList(join.InnerKey);
            this.Write("),");
            this.WriteLine(Indentation.Same);
            this.Visit(join.Projection);
            this.WriteLine(Indentation.Outer);
            this.Write(")");
            return join;
        }

        protected virtual Expression VisitOuterJoined(OuterJoinedExpression outer)
        {
            this.Write("Outer(");
            this.WriteLine(Indentation.Inner);
            this.Visit(outer.Test);
            this.Write(", ");
            this.WriteLine(Indentation.Same);
            this.Visit(outer.Expression);
            this.WriteLine(Indentation.Outer);
            this.Write(")");
            return outer;
        }

        protected virtual Expression VisitSelect(SelectExpression select)
        {
            this.Write(select.QueryText);
            return select;
        }

        protected virtual Expression VisitCommand(CommandExpression command)
        {
            this.Write(this.FormatQuery(command));
            return command;
        }

        protected virtual string FormatQuery(Expression query)
        {
            if (this.language != null)
            {
                //return this.language.Format(query);
            }
            return SqlFormatter.Format(query, true);
        }

        protected virtual Expression VisitBatch(BatchExpression batch)
        {
            this.Write("Batch(");
            this.WriteLine(Indentation.Inner);
            this.Visit(batch.Input);
            this.Write(",");
            this.WriteLine(Indentation.Same);
            this.Visit(batch.Operation);
            this.Write(",");
            this.WriteLine(Indentation.Same);
            this.Visit(batch.BatchSize);
            this.Write(", ");
            this.Visit(batch.Stream);
            this.WriteLine(Indentation.Outer);
            this.Write(")");
            return batch;
        }

        protected virtual Expression VisitVariable(VariableExpression vex)
        {
            this.Write(this.FormatQuery(vex));
            return vex;
        }

        protected virtual Expression VisitFunction(FunctionExpression function)
        {
            this.Write("FUNCTION ");
            this.Write(function.Name);
            if (function.Arguments.Count > 0)
            {
                this.Write("(");
                this.VisitExpressionList(function.Arguments);
                this.Write(")");
            }
            return function;
        }

        protected virtual Expression VisitEntity(EntityExpression entity)
        {
            this.Visit(entity.Expression);
            return entity;
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            if (c.Type == typeof(QueryCommand))
            {
                QueryCommand qc = (QueryCommand)c.Value;
                this.Write("new QueryCommand {");
                this.WriteLine(Indentation.Inner);
                this.Write("\"" + qc.CommandText + "\"");
                this.Write(",");
                this.WriteLine(Indentation.Same);
                this.Visit(Expression.Constant(qc.Parameters));
                this.Write(")");
                this.WriteLine(Indentation.Outer);
                return c;
            }
            return base.VisitConstant(c);
        }

        protected virtual Expression VisitColumn(ColumnExpression column)
        {
            int iAlias;
            string aliasName =
                this.aliasMap.TryGetValue(column.Alias, out iAlias)
                ? "A" + iAlias
                : "A" + (column.Alias != null ? column.Alias.GetHashCode().ToString() : "") + "?";

            this.Write(aliasName);
            this.Write(".");
            this.Write("Column(\"");
            this.Write(column.Name);
            this.Write("\")");
            return column;
        }
    }

    /// <summary>
    /// Replaces references to one specific instance of an expression node with another node.
    /// Supports DbExpression nodes
    /// </summary>
    public class DbExpressionReplacer : DbExpressionVisitor
    {
        Expression searchFor;
        Expression replaceWith;

        private DbExpressionReplacer(Expression searchFor, Expression replaceWith)
        {
            this.searchFor = searchFor;
            this.replaceWith = replaceWith;
        }

        public static Expression Replace(Expression expression, Expression searchFor, Expression replaceWith)
        {
            return new DbExpressionReplacer(searchFor, replaceWith).Visit(expression);
        }

        public static Expression ReplaceAll(Expression expression, Expression[] searchFor, Expression[] replaceWith)
        {
            for (int i = 0, n = searchFor.Length; i < n; i++)
            {
                expression = Replace(expression, searchFor[i], replaceWith[i]);
            }
            return expression;
        }

        protected override Expression Visit(Expression exp)
        {
            if (exp == this.searchFor)
            {
                return this.replaceWith;
            }
            return base.Visit(exp);
        }
    }

    /// <summary>
    /// An extended expression visitor including custom DbExpression nodes
    /// </summary>
    public abstract class DbExpressionVisitor : ExpressionVisitor
    {
        #region Updates
        protected SelectExpression UpdateSelect(
          SelectExpression select,
          Expression from, Expression where,
          IEnumerable<OrderExpression> orderBy, IEnumerable<Expression> groupBy,
          Expression skip, Expression take,
          bool isDistinct, bool isReverse,
          IEnumerable<ColumnDeclaration> columns
          )
        {
            if (from != select.From
                || where != select.Where
                || orderBy != select.OrderBy
                || groupBy != select.GroupBy
                || take != select.Take
                || skip != select.Skip
                || isDistinct != select.IsDistinct
                || columns != select.Columns
                || isReverse != select.IsReverse
                )
            {
                return new SelectExpression(select.Alias, columns, from, where, orderBy, groupBy, isDistinct, skip, take, isReverse);
            }
            return select;
        }

        protected EntityExpression UpdateEntity(EntityExpression entity, Expression expression)
        {
            if (expression != entity.Expression)
            {
                return new EntityExpression(entity.Entity, expression);
            }
            return entity;
        }

        protected JoinExpression UpdateJoin(JoinExpression join, JoinType joinType, Expression left, Expression right, Expression condition)
        {
            if (joinType != join.Join || left != join.Left || right != join.Right || condition != join.Condition)
            {
                return new JoinExpression(joinType, left, right, condition);
            }
            return join;
        }

        protected OuterJoinedExpression UpdateOuterJoined(OuterJoinedExpression outer, Expression test, Expression expression)
        {
            if (test != outer.Test || expression != outer.Expression)
            {
                return new OuterJoinedExpression(test, expression);
            }
            return outer;
        }

        protected AggregateExpression UpdateAggregate(AggregateExpression aggregate, Type type, string aggType, Expression arg, bool isDistinct)
        {
            if (type != aggregate.Type || aggType != aggregate.AggregateName || arg != aggregate.Argument || isDistinct != aggregate.IsDistinct)
            {
                return new AggregateExpression(type, aggType, arg, isDistinct);
            }
            return aggregate;
        }

        protected IsNullExpression UpdateIsNull(IsNullExpression isnull, Expression expression)
        {
            if (expression != isnull.Expression)
            {
                return new IsNullExpression(expression);
            }
            return isnull;
        }

        protected BetweenExpression UpdateBetween(BetweenExpression between, Expression expression, Expression lower, Expression upper)
        {
            if (expression != between.Expression || lower != between.Lower || upper != between.Upper)
            {
                return new BetweenExpression(expression, lower, upper);
            }
            return between;
        }

        protected RowNumberExpression UpdateRowNumber(RowNumberExpression rowNumber, IEnumerable<OrderExpression> orderBy)
        {
            if (orderBy != rowNumber.OrderBy)
            {
                return new RowNumberExpression(orderBy);
            }
            return rowNumber;
        }

        protected ScalarExpression UpdateScalar(ScalarExpression scalar, SelectExpression select)
        {
            if (select != scalar.Select)
            {
                return new ScalarExpression(scalar.Type, select);
            }
            return scalar;
        }

        protected ExistsExpression UpdateExists(ExistsExpression exists, SelectExpression select)
        {
            if (select != exists.Select)
            {
                return new ExistsExpression(select);
            }
            return exists;
        }

        protected InExpression UpdateIn(InExpression @in, Expression expression, SelectExpression select, IEnumerable<Expression> values)
        {
            if (expression != @in.Expression || select != @in.Select || values != @in.Values)
            {
                if (select != null)
                {
                    return new InExpression(expression, select);
                }
                else
                {
                    return new InExpression(expression, values);
                }
            }
            return @in;
        }

        protected AggregateSubqueryExpression UpdateAggregateSubquery(AggregateSubqueryExpression aggregate, ScalarExpression subquery)
        {
            if (subquery != aggregate.AggregateAsSubquery)
            {
                return new AggregateSubqueryExpression(aggregate.GroupByAlias, aggregate.AggregateInGroupSelect, subquery);
            }
            return aggregate;
        }

        protected ProjectionExpression UpdateProjection(ProjectionExpression proj, SelectExpression select, Expression projector, LambdaExpression aggregator)
        {
            if (select != proj.Select || projector != proj.Projector || aggregator != proj.Aggregator)
            {
                return new ProjectionExpression(select, projector, aggregator);
            }
            return proj;
        }

        protected ClientJoinExpression UpdateClientJoin(ClientJoinExpression join, ProjectionExpression projection, IEnumerable<Expression> outerKey, IEnumerable<Expression> innerKey)
        {
            if (projection != join.Projection || outerKey != join.OuterKey || innerKey != join.InnerKey)
            {
                return new ClientJoinExpression(projection, outerKey, innerKey);
            }
            return join;
        }

        protected InsertCommand UpdateInsert(InsertCommand insert, TableExpression table, IEnumerable<ColumnAssignment> assignments)
        {
            if (table != insert.Table || assignments != insert.Assignments)
            {
                return new InsertCommand(table, assignments);
            }
            return insert;
        }

        protected UpdateCommand UpdateUpdate(UpdateCommand update, TableExpression table, Expression where, IEnumerable<ColumnAssignment> assignments)
        {
            if (table != update.Table || where != update.Where || assignments != update.Assignments)
            {
                return new UpdateCommand(table, where, assignments);
            }
            return update;
        }

        protected DeleteCommand UpdateDelete(DeleteCommand delete, TableExpression table, Expression where)
        {
            if (table != delete.Table || where != delete.Where)
            {
                return new DeleteCommand(table, where);
            }
            return delete;
        }

        protected BatchExpression UpdateBatch(BatchExpression batch, Expression input, LambdaExpression operation, Expression batchSize, Expression stream)
        {
            if (input != batch.Input || operation != batch.Operation || batchSize != batch.BatchSize || stream != batch.Stream)
            {
                return new BatchExpression(input, operation, batchSize, stream);
            }
            return batch;
        }

        protected IFCommand UpdateIf(IFCommand ifx, Expression check, Expression ifTrue, Expression ifFalse)
        {
            if (check != ifx.Check || ifTrue != ifx.IfTrue || ifFalse != ifx.IfFalse)
            {
                return new IFCommand(check, ifTrue, ifFalse);
            }
            return ifx;
        }

        protected BlockCommand UpdateBlock(BlockCommand block, IList<Expression> commands)
        {
            if (block.Commands != commands)
            {
                return new BlockCommand(commands);
            }
            return block;
        }

        protected DeclarationCommand UpdateDeclaration(DeclarationCommand decl, IEnumerable<VariableDeclaration> variables, SelectExpression source)
        {
            if (variables != decl.Variables || source != decl.Source)
            {
                return new DeclarationCommand(variables, source);
            }
            return decl;
        }

        protected FunctionExpression UpdateFunction(FunctionExpression func, string name, IEnumerable<Expression> arguments)
        {
            if (name != func.Name || arguments != func.Arguments)
            {
                return new FunctionExpression(func.Type, name, arguments);
            }
            return func;
        }

        protected ColumnAssignment UpdateColumnAssignment(ColumnAssignment ca, ColumnExpression c, Expression e)
        {
            if (c != ca.Column || e != ca.Expression)
            {
                return new ColumnAssignment(c, e);
            }
            return ca;
        } 
        #endregion

        #region Visits

        protected override Expression Visit(Expression exp)
        {
            if (exp == null)
            {
                return null;
            }
            switch ((DbExpressionType)exp.NodeType)
            {
                case DbExpressionType.Table:
                    return this.VisitTable((TableExpression)exp);
                case DbExpressionType.Column:
                    return this.VisitColumn((ColumnExpression)exp);
                case DbExpressionType.Select:
                    return this.VisitSelect((SelectExpression)exp);
                case DbExpressionType.Join:
                    return this.VisitJoin((JoinExpression)exp);
                case DbExpressionType.OuterJoined:
                    return this.VisitOuterJoined((OuterJoinedExpression)exp);
                case DbExpressionType.Aggregate:
                    return this.VisitAggregate((AggregateExpression)exp);
                case DbExpressionType.Scalar:
                case DbExpressionType.Exists:
                case DbExpressionType.In:
                    return this.VisitSubquery((SubqueryExpression)exp);
                case DbExpressionType.AggregateSubquery:
                    return this.VisitAggregateSubquery((AggregateSubqueryExpression)exp);
                case DbExpressionType.IsNull:
                    return this.VisitIsNull((IsNullExpression)exp);
                case DbExpressionType.Between:
                    return this.VisitBetween((BetweenExpression)exp);
                case DbExpressionType.RowCount:
                    return this.VisitRowNumber((RowNumberExpression)exp);
                case DbExpressionType.Projection:
                    return this.VisitProjection((ProjectionExpression)exp);
                case DbExpressionType.NamedValue:
                    return this.VisitNamedValue((NamedValueExpression)exp);
                case DbExpressionType.ClientJoin:
                    return this.VisitClientJoin((ClientJoinExpression)exp);
                case DbExpressionType.Insert:
                case DbExpressionType.Update:
                case DbExpressionType.Delete:
                case DbExpressionType.If:
                case DbExpressionType.Block:
                case DbExpressionType.Declaration:
                    return this.VisitCommand((CommandExpression)exp);
                case DbExpressionType.Batch:
                    return this.VisitBatch((BatchExpression)exp);
                case DbExpressionType.Variable:
                    return this.VisitVariable((VariableExpression)exp);
                case DbExpressionType.Function:
                    return this.VisitFunction((FunctionExpression)exp);
                case DbExpressionType.Entity:
                    return this.VisitEntity((EntityExpression)exp);
                default:
                    return base.Visit(exp);
            }
        }

        protected virtual Expression VisitEntity(EntityExpression entity)
        {
            var exp = this.Visit(entity.Expression);
            return this.UpdateEntity(entity, exp);
        }

        protected virtual Expression VisitTable(TableExpression table)
        {
            return table;
        }

        protected virtual Expression VisitColumn(ColumnExpression column)
        {
            return column;
        }

        protected virtual Expression VisitSelect(SelectExpression select)
        {
            var from = this.VisitSource(select.From);
            var where = this.Visit(select.Where);
            var orderBy = this.VisitOrderBy(select.OrderBy);
            var groupBy = this.VisitExpressionList(select.GroupBy);
            var skip = this.Visit(select.Skip);
            var take = this.Visit(select.Take);
            var columns = this.VisitColumnDeclarations(select.Columns);
            return this.UpdateSelect(select, from, where, orderBy, groupBy, skip, take, select.IsDistinct, select.IsReverse, columns);
        }

        protected virtual Expression VisitJoin(JoinExpression join)
        {
            var left = this.VisitSource(join.Left);
            var right = this.VisitSource(join.Right);
            var condition = this.Visit(join.Condition);
            return this.UpdateJoin(join, join.Join, left, right, condition);
        }

        protected virtual Expression VisitOuterJoined(OuterJoinedExpression outer)
        {
            var test = this.Visit(outer.Test);
            var expression = this.Visit(outer.Expression);
            return this.UpdateOuterJoined(outer, test, expression);
        }

        protected virtual Expression VisitAggregate(AggregateExpression aggregate)
        {
            var arg = this.Visit(aggregate.Argument);
            return this.UpdateAggregate(aggregate, aggregate.Type, aggregate.AggregateName, arg, aggregate.IsDistinct);
        }

        protected virtual Expression VisitIsNull(IsNullExpression isnull)
        {
            var expr = this.Visit(isnull.Expression);
            return this.UpdateIsNull(isnull, expr);
        }

        protected virtual Expression VisitBetween(BetweenExpression between)
        {
            var expr = this.Visit(between.Expression);
            var lower = this.Visit(between.Lower);
            var upper = this.Visit(between.Upper);
            return this.UpdateBetween(between, expr, lower, upper);
        }

        protected virtual Expression VisitRowNumber(RowNumberExpression rowNumber)
        {
            var orderby = this.VisitOrderBy(rowNumber.OrderBy);
            return this.UpdateRowNumber(rowNumber, orderby);
        }

        protected virtual Expression VisitNamedValue(NamedValueExpression value)
        {
            return value;
        }

        protected virtual Expression VisitSubquery(SubqueryExpression subquery)
        {
            switch ((DbExpressionType)subquery.NodeType)
            {
                case DbExpressionType.Scalar:
                    return this.VisitScalar((ScalarExpression)subquery);
                case DbExpressionType.Exists:
                    return this.VisitExists((ExistsExpression)subquery);
                case DbExpressionType.In:
                    return this.VisitIn((InExpression)subquery);
            }
            return subquery;
        }

        protected virtual Expression VisitScalar(ScalarExpression scalar)
        {
            var select = (SelectExpression)this.Visit(scalar.Select);
            return this.UpdateScalar(scalar, select);
        }

        protected virtual Expression VisitExists(ExistsExpression exists)
        {
            var select = (SelectExpression)this.Visit(exists.Select);
            return this.UpdateExists(exists, select);
        }

        protected virtual Expression VisitIn(InExpression @in)
        {
            var expr = this.Visit(@in.Expression);
            var select = (SelectExpression)this.Visit(@in.Select);
            var values = this.VisitExpressionList(@in.Values);
            return this.UpdateIn(@in, expr, select, values);
        }

        protected virtual Expression VisitAggregateSubquery(AggregateSubqueryExpression aggregate)
        {
            var subquery = (ScalarExpression)this.Visit(aggregate.AggregateAsSubquery);
            return this.UpdateAggregateSubquery(aggregate, subquery);
        }

        protected virtual Expression VisitSource(Expression source)
        {
            return this.Visit(source);
        }

        protected virtual Expression VisitProjection(ProjectionExpression proj)
        {
            var select = (SelectExpression)this.Visit(proj.Select);
            var projector = this.Visit(proj.Projector);
            return this.UpdateProjection(proj, select, projector, proj.Aggregator);
        }

        protected virtual Expression VisitClientJoin(ClientJoinExpression join)
        {
            var projection = (ProjectionExpression)this.Visit(join.Projection);
            var outerKey = this.VisitExpressionList(join.OuterKey);
            var innerKey = this.VisitExpressionList(join.InnerKey);
            return this.UpdateClientJoin(join, projection, outerKey, innerKey);
        }

        protected virtual Expression VisitCommand(CommandExpression command)
        {
            switch ((DbExpressionType)command.NodeType)
            {
                case DbExpressionType.Insert:
                    return this.VisitInsert((InsertCommand)command);
                case DbExpressionType.Update:
                    return this.VisitUpdate((UpdateCommand)command);
                case DbExpressionType.Delete:
                    return this.VisitDelete((DeleteCommand)command);
                case DbExpressionType.If:
                    return this.VisitIf((IFCommand)command);
                case DbExpressionType.Block:
                    return this.VisitBlock((BlockCommand)command);
                case DbExpressionType.Declaration:
                    return this.VisitDeclaration((DeclarationCommand)command);
                default:
                    return this.VisitUnknown(command);
            }
        }

        protected virtual Expression VisitInsert(InsertCommand insert)
        {
            var table = (TableExpression)this.Visit(insert.Table);
            var assignments = this.VisitColumnAssignments(insert.Assignments);
            return this.UpdateInsert(insert, table, assignments);
        }

        protected virtual Expression VisitUpdate(UpdateCommand update)
        {
            var table = (TableExpression)this.Visit(update.Table);
            var where = this.Visit(update.Where);
            var assignments = this.VisitColumnAssignments(update.Assignments);
            return this.UpdateUpdate(update, table, where, assignments);
        }

        protected virtual Expression VisitDelete(DeleteCommand delete)
        {
            var table = (TableExpression)this.Visit(delete.Table);
            var where = this.Visit(delete.Where);
            return this.UpdateDelete(delete, table, where);
        }

        protected virtual Expression VisitBatch(BatchExpression batch)
        {
            var operation = (LambdaExpression)this.Visit(batch.Operation);
            var batchSize = this.Visit(batch.BatchSize);
            var stream = this.Visit(batch.Stream);
            return this.UpdateBatch(batch, batch.Input, operation, batchSize, stream);
        }

        protected virtual Expression VisitIf(IFCommand ifx)
        {
            var check = this.Visit(ifx.Check);
            var ifTrue = this.Visit(ifx.IfTrue);
            var ifFalse = this.Visit(ifx.IfFalse);
            return this.UpdateIf(ifx, check, ifTrue, ifFalse);
        }

        protected virtual Expression VisitBlock(BlockCommand block)
        {
            var commands = this.VisitExpressionList(block.Commands);
            return this.UpdateBlock(block, commands);
        }

        protected virtual Expression VisitDeclaration(DeclarationCommand decl)
        {
            var variables = this.VisitVariableDeclarations(decl.Variables);
            var source = (SelectExpression)this.Visit(decl.Source);
            return this.UpdateDeclaration(decl, variables, source);

        }

        protected virtual Expression VisitVariable(VariableExpression vex)
        {
            return vex;
        }

        protected virtual Expression VisitFunction(FunctionExpression func)
        {
            var arguments = this.VisitExpressionList(func.Arguments);
            return this.UpdateFunction(func, func.Name, arguments);
        }

        protected virtual ColumnAssignment VisitColumnAssignment(ColumnAssignment ca)
        {
            ColumnExpression c = (ColumnExpression)this.Visit(ca.Column);
            Expression e = this.Visit(ca.Expression);
            return this.UpdateColumnAssignment(ca, c, e);
        }

        protected virtual ReadOnlyCollection<ColumnAssignment> VisitColumnAssignments(ReadOnlyCollection<ColumnAssignment> assignments)
        {
            List<ColumnAssignment> alternate = null;
            for (int i = 0, n = assignments.Count; i < n; i++)
            {
                ColumnAssignment assignment = this.VisitColumnAssignment(assignments[i]);
                if (alternate == null && assignment != assignments[i])
                {
                    alternate = assignments.Take(i).ToList();
                }
                if (alternate != null)
                {
                    alternate.Add(assignment);
                }
            }
            if (alternate != null)
            {
                return alternate.AsReadOnly();
            }
            return assignments;
        }

        protected virtual ReadOnlyCollection<ColumnDeclaration> VisitColumnDeclarations(ReadOnlyCollection<ColumnDeclaration> columns)
        {
            List<ColumnDeclaration> alternate = null;
            for (int i = 0, n = columns.Count; i < n; i++)
            {
                ColumnDeclaration column = columns[i];
                Expression e = this.Visit(column.Expression);
                if (alternate == null && e != column.Expression)
                {
                    alternate = columns.Take(i).ToList();
                }
                if (alternate != null)
                {
                    alternate.Add(new ColumnDeclaration(column.Name, e, column.QueryType));
                }
            }
            if (alternate != null)
            {
                return alternate.AsReadOnly();
            }
            return columns;
        }

        protected virtual ReadOnlyCollection<VariableDeclaration> VisitVariableDeclarations(ReadOnlyCollection<VariableDeclaration> decls)
        {
            List<VariableDeclaration> alternate = null;
            for (int i = 0, n = decls.Count; i < n; i++)
            {
                VariableDeclaration decl = decls[i];
                Expression e = this.Visit(decl.Expression);
                if (alternate == null && e != decl.Expression)
                {
                    alternate = decls.Take(i).ToList();
                }
                if (alternate != null)
                {
                    alternate.Add(new VariableDeclaration(decl.Name, decl.QueryType, e));
                }
            }
            if (alternate != null)
            {
                return alternate.AsReadOnly();
            }
            return decls;
        }

        protected virtual ReadOnlyCollection<OrderExpression> VisitOrderBy(ReadOnlyCollection<OrderExpression> expressions)
        {
            if (expressions != null)
            {
                List<OrderExpression> alternate = null;
                for (int i = 0, n = expressions.Count; i < n; i++)
                {
                    OrderExpression expr = expressions[i];
                    Expression e = this.Visit(expr.Expression);
                    if (alternate == null && e != expr.Expression)
                    {
                        alternate = expressions.Take(i).ToList();
                    }
                    if (alternate != null)
                    {
                        alternate.Add(new OrderExpression(expr.OrderType, e));
                    }
                }
                if (alternate != null)
                {
                    return alternate.AsReadOnly();
                }
            }
            return expressions;
        } 
        #endregion
    }
}
