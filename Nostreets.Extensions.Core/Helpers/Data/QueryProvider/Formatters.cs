using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Nostreets.Extensions.Helpers.Data.QueryProvider
{
    /// <summary>
    /// Formats a query expression into TSQL language syntax
    /// </summary>
    public class TSqlFormatter : SqlFormatter
    {
        protected TSqlFormatter(QueryLanguage language, bool forDebug = false)
            : base(language, forDebug)
        {
        }

        public static new string Format(Expression expression)
        {
            return Format(expression, new TSqlLanguage());
        }

        public static string Format(Expression expression, QueryMapper map)
        {
            return Format(expression, new TSqlLanguage());

        }


        public static string Format(Expression expression, QueryLanguage language, bool forDebug = false)
        {
            TSqlFormatter formatter = new TSqlFormatter(language, forDebug);
            formatter.Visit(expression);
            return formatter.ToString();
        }

        protected override void WriteAggregateName(string aggregateName)
        {
            if (aggregateName == "LongCount")
            {
                this.Write("COUNT_BIG");
            }
            else
            {
                base.WriteAggregateName(aggregateName);
            }
        }

        protected override Expression VisitMemberAccess(MemberExpression m)
        {
            if (m.Member.DeclaringType == typeof(string))
            {
                switch (m.Member.Name)
                {
                    case "Length":
                        this.Write("LEN(");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                }
            }
            else if (m.Member.DeclaringType == typeof(DateTime) || m.Member.DeclaringType == typeof(DateTimeOffset))
            {
                switch (m.Member.Name)
                {
                    case "Day":
                        this.Write("DAY(");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "Month":
                        this.Write("MONTH(");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "Year":
                        this.Write("YEAR(");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "Hour":
                        this.Write("DATEPART(hour, ");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "Minute":
                        this.Write("DATEPART(minute, ");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "Second":
                        this.Write("DATEPART(second, ");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "Millisecond":
                        this.Write("DATEPART(millisecond, ");
                        this.Visit(m.Expression);
                        this.Write(")");
                        return m;
                    case "DayOfWeek":
                        this.Write("(DATEPART(weekday, ");
                        this.Visit(m.Expression);
                        this.Write(") - 1)");
                        return m;
                    case "DayOfYear":
                        this.Write("(DATEPART(dayofyear, ");
                        this.Visit(m.Expression);
                        this.Write(") - 1)");
                        return m;
                }
            }
            return base.VisitMemberAccess(m);
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            if (m.Method.DeclaringType == typeof(string))
            {
                switch (m.Method.Name)
                {
                    case "StartsWith":
                        this.Write("(");
                        this.Visit(m.Object);
                        this.Write(" LIKE ");
                        this.Visit(m.Arguments[0]);
                        this.Write(" + '%')");
                        return m;
                    case "EndsWith":
                        this.Write("(");
                        this.Visit(m.Object);
                        this.Write(" LIKE '%' + ");
                        this.Visit(m.Arguments[0]);
                        this.Write(")");
                        return m;
                    case "Contains":
                        this.Write("(");
                        this.Visit(m.Object);
                        this.Write(" LIKE '%' + ");
                        this.Visit(m.Arguments[0]);
                        this.Write(" + '%')");
                        return m;
                    case "Concat":
                        IList<Expression> args = m.Arguments;
                        if (args.Count == 1 && args[0].NodeType == ExpressionType.NewArrayInit)
                        {
                            args = ((NewArrayExpression)args[0]).Expressions;
                        }
                        for (int i = 0, n = args.Count; i < n; i++)
                        {
                            if (i > 0) this.Write(" + ");
                            this.Visit(args[i]);
                        }
                        return m;
                    case "IsNullOrEmpty":
                        this.Write("(");
                        this.Visit(m.Arguments[0]);
                        this.Write(" IS NULL OR ");
                        this.Visit(m.Arguments[0]);
                        this.Write(" = '')");
                        return m;
                    case "ToUpper":
                        this.Write("UPPER(");
                        this.Visit(m.Object);
                        this.Write(")");
                        return m;
                    case "ToLower":
                        this.Write("LOWER(");
                        this.Visit(m.Object);
                        this.Write(")");
                        return m;
                    case "Replace":
                        this.Write("REPLACE(");
                        this.Visit(m.Object);
                        this.Write(", ");
                        this.Visit(m.Arguments[0]);
                        this.Write(", ");
                        this.Visit(m.Arguments[1]);
                        this.Write(")");
                        return m;
                    case "Substring":
                        this.Write("SUBSTRING(");
                        this.Visit(m.Object);
                        this.Write(", ");
                        this.Visit(m.Arguments[0]);
                        this.Write(" + 1, ");
                        if (m.Arguments.Count == 2)
                        {
                            this.Visit(m.Arguments[1]);
                        }
                        else
                        {
                            this.Write("8000");
                        }
                        this.Write(")");
                        return m;
                    case "Remove":
                        this.Write("STUFF(");
                        this.Visit(m.Object);
                        this.Write(", ");
                        this.Visit(m.Arguments[0]);
                        this.Write(" + 1, ");
                        if (m.Arguments.Count == 2)
                        {
                            this.Visit(m.Arguments[1]);
                        }
                        else
                        {
                            this.Write("8000");
                        }
                        this.Write(", '')");
                        return m;
                    case "IndexOf":
                        this.Write("(CHARINDEX(");
                        this.Visit(m.Arguments[0]);
                        this.Write(", ");
                        this.Visit(m.Object);
                        if (m.Arguments.Count == 2 && m.Arguments[1].Type == typeof(int))
                        {
                            this.Write(", ");
                            this.Visit(m.Arguments[1]);
                            this.Write(" + 1");
                        }
                        this.Write(") - 1)");
                        return m;
                    case "Trim":
                        this.Write("RTRIM(LTRIM(");
                        this.Visit(m.Object);
                        this.Write("))");
                        return m;
                }
            }
            else if (m.Method.DeclaringType == typeof(DateTime))
            {
                switch (m.Method.Name)
                {
                    case "op_Subtract":
                        if (m.Arguments[1].Type == typeof(DateTime))
                        {
                            this.Write("DATEDIFF(");
                            this.Visit(m.Arguments[0]);
                            this.Write(", ");
                            this.Visit(m.Arguments[1]);
                            this.Write(")");
                            return m;
                        }
                        break;
                    case "AddYears":
                        this.Write("DATEADD(YYYY,");
                        this.Visit(m.Arguments[0]);
                        this.Write(",");
                        this.Visit(m.Object);
                        this.Write(")");
                        return m;
                    case "AddMonths":
                        this.Write("DATEADD(MM,");
                        this.Visit(m.Arguments[0]);
                        this.Write(",");
                        this.Visit(m.Object);
                        this.Write(")");
                        return m;
                    case "AddDays":
                        this.Write("DATEADD(DAY,");
                        this.Visit(m.Arguments[0]);
                        this.Write(",");
                        this.Visit(m.Object);
                        this.Write(")");
                        return m;
                    case "AddHours":
                        this.Write("DATEADD(HH,");
                        this.Visit(m.Arguments[0]);
                        this.Write(",");
                        this.Visit(m.Object);
                        this.Write(")");
                        return m;
                    case "AddMinutes":
                        this.Write("DATEADD(MI,");
                        this.Visit(m.Arguments[0]);
                        this.Write(",");
                        this.Visit(m.Object);
                        this.Write(")");
                        return m;
                    case "AddSeconds":
                        this.Write("DATEADD(SS,");
                        this.Visit(m.Arguments[0]);
                        this.Write(",");
                        this.Visit(m.Object);
                        this.Write(")");
                        return m;
                    case "AddMilliseconds":
                        this.Write("DATEADD(MS,");
                        this.Visit(m.Arguments[0]);
                        this.Write(",");
                        this.Visit(m.Object);
                        this.Write(")");
                        return m;
                }
            }
            else if (m.Method.DeclaringType == typeof(Decimal))
            {
                switch (m.Method.Name)
                {
                    case "Add":
                    case "Subtract":
                    case "Multiply":
                    case "Divide":
                    case "Remainder":
                        this.Write("(");
                        this.VisitValue(m.Arguments[0]);
                        this.Write(" ");
                        this.Write(GetOperator(m.Method.Name));
                        this.Write(" ");
                        this.VisitValue(m.Arguments[1]);
                        this.Write(")");
                        return m;
                    case "Negate":
                        this.Write("-");
                        this.Visit(m.Arguments[0]);
                        this.Write("");
                        return m;
                    case "Ceiling":
                    case "Floor":
                        this.Write(m.Method.Name.ToUpper());
                        this.Write("(");
                        this.Visit(m.Arguments[0]);
                        this.Write(")");
                        return m;
                    case "Round":
                        if (m.Arguments.Count == 1)
                        {
                            this.Write("ROUND(");
                            this.Visit(m.Arguments[0]);
                            this.Write(", 0)");
                            return m;
                        }
                        else if (m.Arguments.Count == 2 && m.Arguments[1].Type == typeof(int))
                        {
                            this.Write("ROUND(");
                            this.Visit(m.Arguments[0]);
                            this.Write(", ");
                            this.Visit(m.Arguments[1]);
                            this.Write(")");
                            return m;
                        }
                        break;
                    case "Truncate":
                        this.Write("ROUND(");
                        this.Visit(m.Arguments[0]);
                        this.Write(", 0, 1)");
                        return m;
                }
            }
            else if (m.Method.DeclaringType == typeof(Math))
            {
                switch (m.Method.Name)
                {
                    case "Abs":
                    case "Acos":
                    case "Asin":
                    case "Atan":
                    case "Cos":
                    case "Exp":
                    case "Log10":
                    case "Sin":
                    case "Tan":
                    case "Sqrt":
                    case "Sign":
                    case "Ceiling":
                    case "Floor":
                        this.Write(m.Method.Name.ToUpper());
                        this.Write("(");
                        this.Visit(m.Arguments[0]);
                        this.Write(")");
                        return m;
                    case "Atan2":
                        this.Write("ATN2(");
                        this.Visit(m.Arguments[0]);
                        this.Write(", ");
                        this.Visit(m.Arguments[1]);
                        this.Write(")");
                        return m;
                    case "Log":
                        if (m.Arguments.Count == 1)
                        {
                            goto case "Log10";
                        }
                        break;
                    case "Pow":
                        this.Write("POWER(");
                        this.Visit(m.Arguments[0]);
                        this.Write(", ");
                        this.Visit(m.Arguments[1]);
                        this.Write(")");
                        return m;
                    case "Round":
                        if (m.Arguments.Count == 1)
                        {
                            this.Write("ROUND(");
                            this.Visit(m.Arguments[0]);
                            this.Write(", 0)");
                            return m;
                        }
                        else if (m.Arguments.Count == 2 && m.Arguments[1].Type == typeof(int))
                        {
                            this.Write("ROUND(");
                            this.Visit(m.Arguments[0]);
                            this.Write(", ");
                            this.Visit(m.Arguments[1]);
                            this.Write(")");
                            return m;
                        }
                        break;
                    case "Truncate":
                        this.Write("ROUND(");
                        this.Visit(m.Arguments[0]);
                        this.Write(", 0, 1)");
                        return m;
                }
            }
            if (m.Method.Name == "ToString")
            {
                if (m.Object.Type != typeof(string))
                {
                    this.Write("CONVERT(NVARCHAR, ");
                    this.Visit(m.Object);
                    this.Write(")");
                }
                else
                {
                    this.Visit(m.Object);
                }
                return m;
            }
            else if (!m.Method.IsStatic && m.Method.Name == "CompareTo" && m.Method.ReturnType == typeof(int) && m.Arguments.Count == 1)
            {
                this.Write("(CASE WHEN ");
                this.Visit(m.Object);
                this.Write(" = ");
                this.Visit(m.Arguments[0]);
                this.Write(" THEN 0 WHEN ");
                this.Visit(m.Object);
                this.Write(" < ");
                this.Visit(m.Arguments[0]);
                this.Write(" THEN -1 ELSE 1 END)");
                return m;
            }
            else if (m.Method.IsStatic && m.Method.Name == "Compare" && m.Method.ReturnType == typeof(int) && m.Arguments.Count == 2)
            {
                this.Write("(CASE WHEN ");
                this.Visit(m.Arguments[0]);
                this.Write(" = ");
                this.Visit(m.Arguments[1]);
                this.Write(" THEN 0 WHEN ");
                this.Visit(m.Arguments[0]);
                this.Write(" < ");
                this.Visit(m.Arguments[1]);
                this.Write(" THEN -1 ELSE 1 END)");
                return m;
            }
            return base.VisitMethodCall(m);
        }

        protected override NewExpression VisitNew(NewExpression nex)
        {
            if (nex.Constructor.DeclaringType == typeof(DateTime))
            {
                if (nex.Arguments.Count == 3)
                {
                    this.Write("Convert(DateTime, ");
                    this.Write("Convert(nvarchar, ");
                    this.Visit(nex.Arguments[0]);
                    this.Write(") + '/' + ");
                    this.Write("Convert(nvarchar, ");
                    this.Visit(nex.Arguments[1]);
                    this.Write(") + '/' + ");
                    this.Write("Convert(nvarchar, ");
                    this.Visit(nex.Arguments[2]);
                    this.Write("))");
                    return nex;
                }
                else if (nex.Arguments.Count == 6)
                {
                    this.Write("Convert(DateTime, ");
                    this.Write("Convert(nvarchar, ");
                    this.Visit(nex.Arguments[0]);
                    this.Write(") + '/' + ");
                    this.Write("Convert(nvarchar, ");
                    this.Visit(nex.Arguments[1]);
                    this.Write(") + '/' + ");
                    this.Write("Convert(nvarchar, ");
                    this.Visit(nex.Arguments[2]);
                    this.Write(") + ' ' + ");
                    this.Write("Convert(nvarchar, ");
                    this.Visit(nex.Arguments[3]);
                    this.Write(") + ':' + ");
                    this.Write("Convert(nvarchar, ");
                    this.Visit(nex.Arguments[4]);
                    this.Write(") + ':' + ");
                    this.Write("Convert(nvarchar, ");
                    this.Visit(nex.Arguments[5]);
                    this.Write("))");
                    return nex;
                }
            }
            return base.VisitNew(nex);
        }

        protected override Expression VisitBinary(BinaryExpression b)
        {
            if (b.NodeType == ExpressionType.Power)
            {
                this.Write("POWER(");
                this.VisitValue(b.Left);
                this.Write(", ");
                this.VisitValue(b.Right);
                this.Write(")");
                return b;
            }
            else if (b.NodeType == ExpressionType.Coalesce)
            {
                this.Write("COALESCE(");
                this.VisitValue(b.Left);
                this.Write(", ");
                Expression right = b.Right;
                while (right.NodeType == ExpressionType.Coalesce)
                {
                    BinaryExpression rb = (BinaryExpression)right;
                    this.VisitValue(rb.Left);
                    this.Write(", ");
                    right = rb.Right;
                }
                this.VisitValue(right);
                this.Write(")");
                return b;
            }
            else if (b.NodeType == ExpressionType.LeftShift)
            {
                this.Write("(");
                this.VisitValue(b.Left);
                this.Write(" * POWER(2, ");
                this.VisitValue(b.Right);
                this.Write("))");
                return b;
            }
            else if (b.NodeType == ExpressionType.RightShift)
            {
                this.Write("(");
                this.VisitValue(b.Left);
                this.Write(" / POWER(2, ");
                this.VisitValue(b.Right);
                this.Write("))");
                return b;
            }
            return base.VisitBinary(b);
        }

        protected override Expression VisitValue(Expression expr)
        {
            if (IsPredicate(expr))
            {
                this.Write("CASE WHEN (");
                this.Visit(expr);
                this.Write(") THEN 1 ELSE 0 END");
                return expr;
            }
            return base.VisitValue(expr);
        }

        protected override Expression VisitConditional(ConditionalExpression c)
        {
            if (this.IsPredicate(c.Test))
            {
                this.Write("(CASE WHEN ");
                this.VisitPredicate(c.Test);
                this.Write(" THEN ");
                this.VisitValue(c.IfTrue);
                Expression ifFalse = c.IfFalse;
                while (ifFalse != null && ifFalse.NodeType == ExpressionType.Conditional)
                {
                    ConditionalExpression fc = (ConditionalExpression)ifFalse;
                    this.Write(" WHEN ");
                    this.VisitPredicate(fc.Test);
                    this.Write(" THEN ");
                    this.VisitValue(fc.IfTrue);
                    ifFalse = fc.IfFalse;
                }
                if (ifFalse != null)
                {
                    this.Write(" ELSE ");
                    this.VisitValue(ifFalse);
                }
                this.Write(" END)");
            }
            else
            {
                this.Write("(CASE ");
                this.VisitValue(c.Test);
                this.Write(" WHEN 0 THEN ");
                this.VisitValue(c.IfFalse);
                this.Write(" ELSE ");
                this.VisitValue(c.IfTrue);
                this.Write(" END)");
            }
            return c;
        }

        protected override Expression VisitRowNumber(RowNumberExpression rowNumber)
        {
            this.Write("ROW_NUMBER() OVER(");
            if (rowNumber.OrderBy != null && rowNumber.OrderBy.Count > 0)
            {
                this.Write("ORDER BY ");
                for (int i = 0, n = rowNumber.OrderBy.Count; i < n; i++)
                {
                    OrderExpression exp = rowNumber.OrderBy[i];
                    if (i > 0)
                    {
                        this.Write(", ");
                    }
                    this.VisitValue(exp.Expression);
                    if (exp.OrderType != OrderType.Ascending)
                    {
                        this.Write(" DESC");
                    }
                }
            }
            this.Write(")");
            return rowNumber;
        }

        protected override Expression VisitIf(IFCommand ifx)
        {
            if (!this.Language.AllowsMultipleCommands)
            {
                return base.VisitIf(ifx);
            }
            this.Write("IF ");
            this.Visit(ifx.Check);
            this.WriteLine(Indentation.Same);
            this.Write("BEGIN");
            this.WriteLine(Indentation.Inner);
            this.VisitStatement(ifx.IfTrue);
            this.WriteLine(Indentation.Outer);
            if (ifx.IfFalse != null)
            {
                this.Write("END ELSE BEGIN");
                this.WriteLine(Indentation.Inner);
                this.VisitStatement(ifx.IfFalse);
                this.WriteLine(Indentation.Outer);
            }
            this.Write("END");
            return ifx;
        }

        protected override Expression VisitBlock(BlockCommand block)
        {
            if (!this.Language.AllowsMultipleCommands)
            {
                return base.VisitBlock(block);
            }

            for (int i = 0, n = block.Commands.Count; i < n; i++)
            {
                if (i > 0)
                {
                    this.WriteLine(Indentation.Same);
                    this.WriteLine(Indentation.Same);
                }
                this.VisitStatement(block.Commands[i]);
            }
            return block;
        }

        protected override Expression VisitDeclaration(DeclarationCommand decl)
        {
            if (!this.Language.AllowsMultipleCommands)
            {
                return base.VisitDeclaration(decl);
            }

            for (int i = 0, n = decl.Variables.Count; i < n; i++)
            {
                var v = decl.Variables[i];
                if (i > 0)
                    this.WriteLine(Indentation.Same);
                this.Write("DECLARE @");
                this.Write(v.Name);
                this.Write(" ");
                this.Write(this.Language.TypeSystem.GetVariableDeclaration(v.QueryType, false));
            }
            if (decl.Source != null)
            {
                this.WriteLine(Indentation.Same);
                this.Write("SELECT ");
                for (int i = 0, n = decl.Variables.Count; i < n; i++)
                {
                    if (i > 0)
                        this.Write(", ");
                    this.Write("@");
                    this.Write(decl.Variables[i].Name);
                    this.Write(" = ");
                    this.Visit(decl.Source.Columns[i].Expression);
                }
                if (decl.Source.From != null)
                {
                    this.WriteLine(Indentation.Same);
                    this.Write("FROM ");
                    this.VisitSource(decl.Source.From);
                }
                if (decl.Source.Where != null)
                {
                    this.WriteLine(Indentation.Same);
                    this.Write("WHERE ");
                    this.Visit(decl.Source.Where);
                }
            }
            else
            {
                for (int i = 0, n = decl.Variables.Count; i < n; i++)
                {
                    var v = decl.Variables[i];
                    if (v.Expression != null)
                    {
                        this.WriteLine(Indentation.Same);
                        this.Write("SET @");
                        this.Write(v.Name);
                        this.Write(" = ");
                        this.Visit(v.Expression);
                    }
                }
            }
            return decl;
        }
    }

    /// <summary>
    /// Formats a query expression into common SQL language syntax
    /// </summary>
    public class SqlFormatter : DbExpressionVisitor
    {
        QueryLanguage language;
        StringBuilder sb;
        Dictionary<TableAlias, string> aliases;

        bool hideColumnAliases;
        bool hideTableAliases;
        bool isNested;
        bool forDebug;
        int indent = 2;
        int depth;

        protected SqlFormatter(QueryLanguage language, bool forDebug)
        {
            this.language = language;
            this.sb = new StringBuilder();
            this.aliases = new Dictionary<TableAlias, string>();
            this.forDebug = forDebug;
        }

        protected SqlFormatter(QueryLanguage language)
            : this(language, false)
        {
        }

        public static string Format(Expression expression, bool forDebug)
        {
            var formatter = new SqlFormatter(null, forDebug);
            formatter.Visit(expression);
            return formatter.ToString();
        }

        public static string Format(Expression expression)
        {
            var formatter = new SqlFormatter(null, false);
            formatter.Visit(expression);
            return formatter.ToString();
        }
       
        public override string ToString()
        {
            return this.sb.ToString();
        }

        protected virtual QueryLanguage Language
        {
            get { return this.language; }
        }

        protected bool HideColumnAliases
        {
            get { return this.hideColumnAliases; }
            set { this.hideColumnAliases = value; }
        }

        protected bool HideTableAliases
        {
            get { return this.hideTableAliases; }
            set { this.hideTableAliases = value; }
        }

        protected bool IsNested
        {
            get { return this.isNested; }
            set { this.isNested = value; }
        }

        protected bool ForDebug
        {
            get { return this.forDebug; }
        }

        public int IndentationWidth
        {
            get { return this.indent; }
            set { this.indent = value; }
        }

        #region Writes
        protected virtual void WriteValue(object value)
        {
            if (value == null)
            {
                this.Write("NULL");
            }
            else if (value.GetType().IsEnum)
            {
                this.Write(Convert.ChangeType(value, Enum.GetUnderlyingType(value.GetType())));
            }
            else
            {
                switch (Type.GetTypeCode(value.GetType()))
                {
                    case TypeCode.Boolean:
                        this.Write(((bool)value) ? 1 : 0);
                        break;
                    case TypeCode.String:
                        this.Write("'");
                        this.Write(value);
                        this.Write("'");
                        break;
                    case TypeCode.Object:
                        throw new NotSupportedException(string.Format("The constant for '{0}' is not supported", value));
                    case TypeCode.Single:
                    case TypeCode.Double:
                        string str = value.ToString();
                        if (!str.Contains('.'))
                        {
                            str += ".0";
                        }
                        this.Write(str);
                        break;
                    default:
                        this.Write(value);
                        break;
                }
            }
        }

        protected void Write(object value)
        {
            this.sb.Append(value);
        }

        protected virtual void WriteParameterName(string name)
        {
            this.Write("@" + name);
        }

        protected virtual void WriteVariableName(string name)
        {
            this.WriteParameterName(name);
        }

        protected virtual void WriteAsAliasName(string aliasName)
        {
            this.Write("AS ");
            this.WriteAliasName(aliasName);
        }

        protected virtual void WriteAliasName(string aliasName)
        {
            this.Write(aliasName);
        }

        protected virtual void WriteAsColumnName(string columnName)
        {
            this.Write("AS ");
            this.WriteColumnName(columnName);
        }

        protected virtual void WriteColumnName(string columnName)
        {
            string name = (this.Language != null) ? this.Language.Quote(columnName) : columnName;
            this.Write(name);
        }

        protected virtual void WriteTableName(string tableName)
        {
            string name = (this.Language != null) ? this.Language.Quote(tableName) : tableName;
            this.Write(name);
        }

        protected void WriteLine(Indentation style)
        {
            sb.AppendLine();
            this.Indent(style);
            for (int i = 0, n = this.depth * this.indent; i < n; i++)
            {
                this.Write(" ");
            }
        }

        protected virtual void WriteTopClause(Expression expression)
        {
            this.Write("TOP (");
            this.Visit(expression);
            this.Write(") ");
        }

        protected virtual void WriteColumns(ReadOnlyCollection<ColumnDeclaration> columns)
        {
            if (columns.Count > 0)
            {
                for (int i = 0, n = columns.Count; i < n; i++)
                {
                    ColumnDeclaration column = columns[i];
                    if (i > 0)
                    {
                        this.Write(", ");
                    }
                    ColumnExpression c = this.VisitValue(column.Expression) as ColumnExpression;
                    if (!string.IsNullOrEmpty(column.Name) && (c == null || c.Name != column.Name))
                    {
                        this.Write(" ");
                        this.WriteAsColumnName(column.Name);
                    }
                }
            }
            else
            {
                this.Write("NULL ");
                if (this.isNested)
                {
                    this.WriteAsColumnName("tmp");
                    this.Write(" ");
                }
            }
        }

        protected virtual void WriteAggregateName(string aggregateName)
        {
            switch (aggregateName)
            {
                case "Average":
                    this.Write("AVG");
                    break;
                case "LongCount":
                    this.Write("COUNT");
                    break;
                default:
                    this.Write(aggregateName.ToUpper());
                    break;
            }
        }

        #endregion

        #region Vists
        protected virtual Expression VisitValue(Expression expr)
        {
            return this.Visit(expr);
        }

        protected virtual Expression VisitPredicate(Expression expr)
        {
            this.Visit(expr);
            if (!IsPredicate(expr))
            {
                this.Write(" <> 0");
            }
            return expr;
        }

        protected virtual Expression VisitJoinLeft(Expression source)
        {
            return this.VisitSource(source);
        }

        protected virtual Expression VisitJoinRight(Expression source)
        {
            return this.VisitSource(source);
        }

        protected virtual void VisitStatement(Expression expression)
        {
            var p = expression as ProjectionExpression;
            if (p != null)
            {
                this.Visit(p.Select);
            }
            else
            {
                this.Visit(expression);
            }
        }


        protected override Expression Visit(Expression exp)
        {
            if (exp == null) return null;

            // check for supported node types first 
            // non-supported ones should not be visited (as they would produce bad SQL)
            switch (exp.NodeType)
            {
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                case ExpressionType.Not:
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                case ExpressionType.UnaryPlus:
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.Coalesce:
                case ExpressionType.RightShift:
                case ExpressionType.LeftShift:
                case ExpressionType.ExclusiveOr:
                case ExpressionType.Power:
                case ExpressionType.Conditional:
                case ExpressionType.Constant:
                case ExpressionType.MemberAccess:
                case ExpressionType.Call:
                case ExpressionType.New:
                case (ExpressionType)DbExpressionType.Table:
                case (ExpressionType)DbExpressionType.Column:
                case (ExpressionType)DbExpressionType.Select:
                case (ExpressionType)DbExpressionType.Join:
                case (ExpressionType)DbExpressionType.Aggregate:
                case (ExpressionType)DbExpressionType.Scalar:
                case (ExpressionType)DbExpressionType.Exists:
                case (ExpressionType)DbExpressionType.In:
                case (ExpressionType)DbExpressionType.AggregateSubquery:
                case (ExpressionType)DbExpressionType.IsNull:
                case (ExpressionType)DbExpressionType.Between:
                case (ExpressionType)DbExpressionType.RowCount:
                case (ExpressionType)DbExpressionType.Projection:
                case (ExpressionType)DbExpressionType.NamedValue:
                case (ExpressionType)DbExpressionType.Insert:
                case (ExpressionType)DbExpressionType.Update:
                case (ExpressionType)DbExpressionType.Delete:
                case (ExpressionType)DbExpressionType.Block:
                case (ExpressionType)DbExpressionType.If:
                case (ExpressionType)DbExpressionType.Declaration:
                case (ExpressionType)DbExpressionType.Variable:
                case (ExpressionType)DbExpressionType.Function:
                    return base.Visit(exp);

                case ExpressionType.ArrayLength:
                case ExpressionType.Quote:
                case ExpressionType.TypeAs:
                case ExpressionType.ArrayIndex:
                case ExpressionType.TypeIs:
                case ExpressionType.Parameter:
                case ExpressionType.Lambda:
                case ExpressionType.NewArrayInit:
                case ExpressionType.NewArrayBounds:
                case ExpressionType.Invoke:
                case ExpressionType.MemberInit:
                case ExpressionType.ListInit:
                default:
                    if (!forDebug)
                    {
                        throw new NotSupportedException(string.Format("The LINQ expression node of type {0} is not supported", exp.NodeType));
                    }
                    else
                    {
                        this.Write(string.Format("?{0}?(", exp.NodeType));
                        base.Visit(exp);
                        this.Write(")");
                        return exp;
                    }
            }
        }

        protected override Expression VisitMemberAccess(MemberExpression m)
        {
            if (this.forDebug)
            {
                this.Visit(m.Expression);
                this.Write(".");
                this.Write(m.Member.Name);
                return m;
            }
            else
            {
                throw new NotSupportedException(string.Format("The member access '{0}' is not supported", m.Member));
            }
        }

        protected override Expression VisitMethodCall(MethodCallExpression m)
        {
            if (m.Method.DeclaringType == typeof(Decimal))
            {
                switch (m.Method.Name)
                {
                    case "Add":
                    case "Subtract":
                    case "Multiply":
                    case "Divide":
                    case "Remainder":
                        this.Write("(");
                        this.VisitValue(m.Arguments[0]);
                        this.Write(" ");
                        this.Write(GetOperator(m.Method.Name));
                        this.Write(" ");
                        this.VisitValue(m.Arguments[1]);
                        this.Write(")");
                        return m;
                    case "Negate":
                        this.Write("-");
                        this.Visit(m.Arguments[0]);
                        this.Write("");
                        return m;
                    case "Compare":
                        this.Visit(Expression.Condition(
                            Expression.Equal(m.Arguments[0], m.Arguments[1]),
                            Expression.Constant(0),
                            Expression.Condition(
                                Expression.LessThan(m.Arguments[0], m.Arguments[1]),
                                Expression.Constant(-1),
                                Expression.Constant(1)
                                )));
                        return m;
                }
            }
            else if (m.Method.Name == "ToString" && m.Object.Type == typeof(string))
            {
                return this.Visit(m.Object);  // no op
            }
            else if (m.Method.Name == "Equals")
            {
                if (m.Method.IsStatic && m.Method.DeclaringType == typeof(object))
                {
                    this.Write("(");
                    this.Visit(m.Arguments[0]);
                    this.Write(" = ");
                    this.Visit(m.Arguments[1]);
                    this.Write(")");
                    return m;
                }
                else if (!m.Method.IsStatic && m.Arguments.Count == 1 && m.Arguments[0].Type == m.Object.Type)
                {
                    this.Write("(");
                    this.Visit(m.Object);
                    this.Write(" = ");
                    this.Visit(m.Arguments[0]);
                    this.Write(")");
                    return m;
                }
            }
            if (this.forDebug)
            {
                if (m.Object != null)
                {
                    this.Visit(m.Object);
                    this.Write(".");
                }
                this.Write(string.Format("?{0}?", m.Method.Name));
                this.Write("(");
                for (int i = 0; i < m.Arguments.Count; i++)
                {
                    if (i > 0)
                        this.Write(", ");
                    this.Visit(m.Arguments[i]);
                }
                this.Write(")");
                return m;
            }
            else
            {
                throw new NotSupportedException(string.Format("The method '{0}' is not supported", m.Method.Name));
            }
        }

        protected override NewExpression VisitNew(NewExpression nex)
        {
            if (this.forDebug)
            {
                this.Write("?new?");
                this.Write(nex.Type.Name);
                this.Write("(");
                for (int i = 0; i < nex.Arguments.Count; i++)
                {
                    if (i > 0)
                        this.Write(", ");
                    this.Visit(nex.Arguments[i]);
                }
                this.Write(")");
                return nex;
            }
            else
            {
                throw new NotSupportedException(string.Format("The constructor for '{0}' is not supported", nex.Constructor.DeclaringType));
            }
        }

        protected override Expression VisitUnary(UnaryExpression u)
        {
            string op = this.GetOperator(u);
            switch (u.NodeType)
            {
                case ExpressionType.Not:
                    if (u.Operand is IsNullExpression)
                    {
                        this.Visit(((IsNullExpression)u.Operand).Expression);
                        this.Write(" IS NOT NULL");
                    }
                    else if (IsBoolean(u.Operand.Type) || op.Length > 1)
                    {
                        this.Write(op);
                        this.Write(" ");
                        this.VisitPredicate(u.Operand);
                    }
                    else
                    {
                        this.Write(op);
                        this.VisitValue(u.Operand);
                    }
                    break;
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    this.Write(op);
                    this.VisitValue(u.Operand);
                    break;
                case ExpressionType.UnaryPlus:
                    this.VisitValue(u.Operand);
                    break;
                case ExpressionType.Convert:
                    // ignore conversions for now
                    this.Visit(u.Operand);
                    break;
                default:
                    if (this.forDebug)
                    {
                        this.Write(string.Format("?{0}?", u.NodeType));
                        this.Write("(");
                        this.Visit(u.Operand);
                        this.Write(")");
                        return u;
                    }
                    else
                    {
                        throw new NotSupportedException(string.Format("The unary operator '{0}' is not supported", u.NodeType));
                    }
            }
            return u;
        }

        protected override Expression VisitBinary(BinaryExpression b)
        {
            string op = this.GetOperator(b);
            Expression left = b.Left;
            Expression right = b.Right;

            this.Write("(");
            switch (b.NodeType)
            {
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    if (this.IsBoolean(left.Type))
                    {
                        this.VisitPredicate(left);
                        this.Write(" ");
                        this.Write(op);
                        this.Write(" ");
                        this.VisitPredicate(right);
                    }
                    else
                    {
                        this.VisitValue(left);
                        this.Write(" ");
                        this.Write(op);
                        this.Write(" ");
                        this.VisitValue(right);
                    }
                    break;
                case ExpressionType.Equal:
                    if (right.NodeType == ExpressionType.Constant)
                    {
                        ConstantExpression ce = (ConstantExpression)right;
                        if (ce.Value == null)
                        {
                            this.Visit(left);
                            this.Write(" IS NULL");
                            break;
                        }
                    }
                    else if (left.NodeType == ExpressionType.Constant)
                    {
                        ConstantExpression ce = (ConstantExpression)left;
                        if (ce.Value == null)
                        {
                            this.Visit(right);
                            this.Write(" IS NULL");
                            break;
                        }
                    }
                    goto case ExpressionType.LessThan;
                case ExpressionType.NotEqual:
                    if (right.NodeType == ExpressionType.Constant)
                    {
                        ConstantExpression ce = (ConstantExpression)right;
                        if (ce.Value == null)
                        {
                            this.Visit(left);
                            this.Write(" IS NOT NULL");
                            break;
                        }
                    }
                    else if (left.NodeType == ExpressionType.Constant)
                    {
                        ConstantExpression ce = (ConstantExpression)left;
                        if (ce.Value == null)
                        {
                            this.Visit(right);
                            this.Write(" IS NOT NULL");
                            break;
                        }
                    }
                    goto case ExpressionType.LessThan;
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                    // check for special x.CompareTo(y) && type.Compare(x,y)
                    if (left.NodeType == ExpressionType.Call && right.NodeType == ExpressionType.Constant)
                    {
                        MethodCallExpression mc = (MethodCallExpression)left;
                        ConstantExpression ce = (ConstantExpression)right;
                        if (ce.Value != null && ce.Value.GetType() == typeof(int) && ((int)ce.Value) == 0)
                        {
                            if (mc.Method.Name == "CompareTo" && !mc.Method.IsStatic && mc.Arguments.Count == 1)
                            {
                                left = mc.Object;
                                right = mc.Arguments[0];
                            }
                            else if (
                                (mc.Method.DeclaringType == typeof(string) || mc.Method.DeclaringType == typeof(decimal))
                                  && mc.Method.Name == "Compare" && mc.Method.IsStatic && mc.Arguments.Count == 2)
                            {
                                left = mc.Arguments[0];
                                right = mc.Arguments[1];
                            }
                        }
                    }
                    goto case ExpressionType.Add;
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                case ExpressionType.Divide:
                case ExpressionType.Modulo:
                case ExpressionType.ExclusiveOr:
                case ExpressionType.LeftShift:
                case ExpressionType.RightShift:
                    this.VisitValue(left);
                    this.Write(" ");
                    this.Write(op);
                    this.Write(" ");
                    this.VisitValue(right);
                    break;
                default:
                    if (this.forDebug)
                    {
                        this.Write(string.Format("?{0}?", b.NodeType));
                        this.Write("(");
                        this.Visit(b.Left);
                        this.Write(", ");
                        this.Visit(b.Right);
                        this.Write(")");
                        return b;
                    }
                    else
                    {
                        throw new NotSupportedException(string.Format("The binary operator '{0}' is not supported", b.NodeType));
                    }
            }
            this.Write(")");
            return b;
        }

        protected override Expression VisitConditional(ConditionalExpression c)
        {
            if (this.forDebug)
            {
                this.Write("?iff?(");
                this.Visit(c.Test);
                this.Write(", ");
                this.Visit(c.IfTrue);
                this.Write(", ");
                this.Visit(c.IfFalse);
                this.Write(")");
                return c;
            }
            else
            {
                throw new NotSupportedException(string.Format("Conditional expressions not supported"));
            }
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            this.WriteValue(c.Value);
            return c;
        }

        protected override Expression VisitColumn(ColumnExpression column)
        {
            if (column.Alias != null && !this.HideColumnAliases)
            {
                this.WriteAliasName(GetAliasName(column.Alias));
                this.Write(".");
            }
            this.WriteColumnName(column.Name);
            return column;
        }

        protected override Expression VisitProjection(ProjectionExpression proj)
        {
            // treat these like scalar subqueries
            if ((proj.Projector is ColumnExpression) || this.forDebug)
            {
                this.Write("(");
                this.WriteLine(Indentation.Inner);
                this.Visit(proj.Select);
                this.Write(")");
                this.Indent(Indentation.Outer);
            }
            else
            {
                throw new NotSupportedException("Non-scalar projections cannot be translated to SQL.");
            }
            return proj;
        }

        protected override Expression VisitSelect(SelectExpression select)
        {
            this.AddAliases(select.From);
            this.Write("SELECT ");
            if (select.IsDistinct)
            {
                this.Write("DISTINCT ");
            }
            if (select.Take != null)
            {
                this.WriteTopClause(select.Take);
            }
            this.WriteColumns(select.Columns);
            if (select.From != null)
            {
                this.WriteLine(Indentation.Same);
                this.Write("FROM ");
                this.VisitSource(select.From);
            }
            if (select.Where != null)
            {
                this.WriteLine(Indentation.Same);
                this.Write("WHERE ");
                this.VisitPredicate(select.Where);
            }
            if (select.GroupBy != null && select.GroupBy.Count > 0)
            {
                this.WriteLine(Indentation.Same);
                this.Write("GROUP BY ");
                for (int i = 0, n = select.GroupBy.Count; i < n; i++)
                {
                    if (i > 0)
                    {
                        this.Write(", ");
                    }
                    this.VisitValue(select.GroupBy[i]);
                }
            }
            if (select.OrderBy != null && select.OrderBy.Count > 0)
            {
                this.WriteLine(Indentation.Same);
                this.Write("ORDER BY ");
                for (int i = 0, n = select.OrderBy.Count; i < n; i++)
                {
                    OrderExpression exp = select.OrderBy[i];
                    if (i > 0)
                    {
                        this.Write(", ");
                    }
                    this.VisitValue(exp.Expression);
                    if (exp.OrderType != OrderType.Ascending)
                    {
                        this.Write(" DESC");
                    }
                }
            }
            return select;
        }

        protected override Expression VisitSource(Expression source)
        {
            bool saveIsNested = this.isNested;
            this.isNested = true;
            switch ((DbExpressionType)source.NodeType)
            {
                case DbExpressionType.Table:
                    TableExpression table = (TableExpression)source;
                    this.WriteTableName(table.Name);
                    if (!this.HideTableAliases)
                    {
                        this.Write(" ");
                        this.WriteAsAliasName(GetAliasName(table.Alias));
                    }
                    break;
                case DbExpressionType.Select:
                    SelectExpression select = (SelectExpression)source;
                    this.Write("(");
                    this.WriteLine(Indentation.Inner);
                    this.Visit(select);
                    this.WriteLine(Indentation.Same);
                    this.Write(") ");
                    this.WriteAsAliasName(GetAliasName(select.Alias));
                    this.Indent(Indentation.Outer);
                    break;
                case DbExpressionType.Join:
                    this.VisitJoin((JoinExpression)source);
                    break;
                default:
                    throw new InvalidOperationException("Select source is not valid type");
            }
            this.isNested = saveIsNested;
            return source;
        }

        protected override Expression VisitJoin(JoinExpression join)
        {
            this.VisitJoinLeft(join.Left);
            this.WriteLine(Indentation.Same);
            switch (join.Join)
            {
                case JoinType.CrossJoin:
                    this.Write("CROSS JOIN ");
                    break;
                case JoinType.InnerJoin:
                    this.Write("INNER JOIN ");
                    break;
                case JoinType.CrossApply:
                    this.Write("CROSS APPLY ");
                    break;
                case JoinType.OuterApply:
                    this.Write("OUTER APPLY ");
                    break;
                case JoinType.LeftOuter:
                case JoinType.SingletonLeftOuter:
                    this.Write("LEFT OUTER JOIN ");
                    break;
            }
            this.VisitJoinRight(join.Right);
            if (join.Condition != null)
            {
                this.WriteLine(Indentation.Inner);
                this.Write("ON ");
                this.VisitPredicate(join.Condition);
                this.Indent(Indentation.Outer);
            }
            return join;
        }

        protected override Expression VisitAggregate(AggregateExpression aggregate)
        {
            this.WriteAggregateName(aggregate.AggregateName);
            this.Write("(");
            if (aggregate.IsDistinct)
            {
                this.Write("DISTINCT ");
            }
            if (aggregate.Argument != null)
            {
                this.VisitValue(aggregate.Argument);
            }
            else if (RequiresAsteriskWhenNoArgument(aggregate.AggregateName))
            {
                this.Write("*");
            }
            this.Write(")");
            return aggregate;
        }

        protected override Expression VisitIsNull(IsNullExpression isnull)
        {
            this.VisitValue(isnull.Expression);
            this.Write(" IS NULL");
            return isnull;
        }

        protected override Expression VisitBetween(BetweenExpression between)
        {
            this.VisitValue(between.Expression);
            this.Write(" BETWEEN ");
            this.VisitValue(between.Lower);
            this.Write(" AND ");
            this.VisitValue(between.Upper);
            return between;
        }

        protected override Expression VisitRowNumber(RowNumberExpression rowNumber)
        {
            throw new NotSupportedException();
        }

        protected override Expression VisitScalar(ScalarExpression subquery)
        {
            this.Write("(");
            this.WriteLine(Indentation.Inner);
            this.Visit(subquery.Select);
            this.WriteLine(Indentation.Same);
            this.Write(")");
            this.Indent(Indentation.Outer);
            return subquery;
        }

        protected override Expression VisitExists(ExistsExpression exists)
        {
            this.Write("EXISTS(");
            this.WriteLine(Indentation.Inner);
            this.Visit(exists.Select);
            this.WriteLine(Indentation.Same);
            this.Write(")");
            this.Indent(Indentation.Outer);
            return exists;
        }

        protected override Expression VisitIn(InExpression @in)
        {
            if (@in.Values != null)
            {
                if (@in.Values.Count == 0)
                {
                    this.Write("0 <> 0");
                }
                else
                {
                    this.VisitValue(@in.Expression);
                    this.Write(" IN (");
                    for (int i = 0, n = @in.Values.Count; i < n; i++)
                    {
                        if (i > 0) this.Write(", ");
                        this.VisitValue(@in.Values[i]);
                    }
                    this.Write(")");
                }
            }
            else
            {
                this.VisitValue(@in.Expression);
                this.Write(" IN (");
                this.WriteLine(Indentation.Inner);
                this.Visit(@in.Select);
                this.WriteLine(Indentation.Same);
                this.Write(")");
                this.Indent(Indentation.Outer);
            }
            return @in;
        }

        protected override Expression VisitNamedValue(NamedValueExpression value)
        {
            this.WriteParameterName(value.Name);
            return value;
        }

        protected override Expression VisitInsert(InsertCommand insert)
        {
            this.Write("INSERT INTO ");
            this.WriteTableName(insert.Table.Name);
            this.Write("(");
            for (int i = 0, n = insert.Assignments.Count; i < n; i++)
            {
                ColumnAssignment ca = insert.Assignments[i];
                if (i > 0) this.Write(", ");
                this.WriteColumnName(ca.Column.Name);
            }
            this.Write(")");
            this.WriteLine(Indentation.Same);
            this.Write("VALUES (");
            for (int i = 0, n = insert.Assignments.Count; i < n; i++)
            {
                ColumnAssignment ca = insert.Assignments[i];
                if (i > 0) this.Write(", ");
                this.Visit(ca.Expression);
            }
            this.Write(")");
            return insert;
        }

        protected override Expression VisitUpdate(UpdateCommand update)
        {
            this.Write("UPDATE ");
            this.WriteTableName(update.Table.Name);
            this.WriteLine(Indentation.Same);
            bool saveHide = this.HideColumnAliases;
            this.HideColumnAliases = true;
            this.Write("SET ");
            for (int i = 0, n = update.Assignments.Count; i < n; i++)
            {
                ColumnAssignment ca = update.Assignments[i];
                if (i > 0) this.Write(", ");
                this.Visit(ca.Column);
                this.Write(" = ");
                this.Visit(ca.Expression);
            }
            if (update.Where != null)
            {
                this.WriteLine(Indentation.Same);
                this.Write("WHERE ");
                this.VisitPredicate(update.Where);
            }
            this.HideColumnAliases = saveHide;
            return update;
        }

        protected override Expression VisitDelete(DeleteCommand delete)
        {
            this.Write("DELETE FROM ");
            bool saveHideTable = this.HideTableAliases;
            bool saveHideColumn = this.HideColumnAliases;
            this.HideTableAliases = true;
            this.HideColumnAliases = true;
            this.VisitSource(delete.Table);
            if (delete.Where != null)
            {
                this.WriteLine(Indentation.Same);
                this.Write("WHERE ");
                this.VisitPredicate(delete.Where);
            }
            this.HideTableAliases = saveHideTable;
            this.HideColumnAliases = saveHideColumn;
            return delete;
        }

        protected override Expression VisitIf(IFCommand ifx)
        {
            throw new NotSupportedException();
        }

        protected override Expression VisitBlock(BlockCommand block)
        {
            throw new NotSupportedException();
        }

        protected override Expression VisitDeclaration(DeclarationCommand decl)
        {
            throw new NotSupportedException();
        }

        protected override Expression VisitVariable(VariableExpression vex)
        {
            this.WriteVariableName(vex.Name);
            return vex;
        }

        protected override Expression VisitFunction(FunctionExpression func)
        {
            this.Write(func.Name);
            if (func.Arguments.Count > 0)
            {
                this.Write("(");
                for (int i = 0, n = func.Arguments.Count; i < n; i++)
                {
                    if (i > 0) this.Write(", ");
                    this.Visit(func.Arguments[i]);
                }
                this.Write(")");
            }
            return func;
        }
        #endregion

        #region Other Logic
        protected void Indent(Indentation style)
        {
            if (style == Indentation.Inner)
            {
                this.depth++;
            }
            else if (style == Indentation.Outer)
            {
                this.depth--;
                System.Diagnostics.Debug.Assert(this.depth >= 0);
            }
        }

        protected virtual string GetAliasName(TableAlias alias)
        {
            string name;
            if (!this.aliases.TryGetValue(alias, out name))
            {
                name = "A" + alias.GetHashCode() + "?";
                this.aliases.Add(alias, name);
            }
            return name;
        }

        protected void AddAlias(TableAlias alias)
        {
            string name;
            if (!this.aliases.TryGetValue(alias, out name))
            {
                name = "t" + this.aliases.Count;
                this.aliases.Add(alias, name);
            }
        }

        protected virtual void AddAliases(Expression expr)
        {
            AliasedExpression ax = expr as AliasedExpression;
            if (ax != null)
            {
                this.AddAlias(ax.Alias);
            }
            else
            {
                JoinExpression jx = expr as JoinExpression;
                if (jx != null)
                {
                    this.AddAliases(jx.Left);
                    this.AddAliases(jx.Right);
                }
            }
        }

        protected virtual string GetOperator(string methodName)
        {
            switch (methodName)
            {
                case "Add": return "+";
                case "Subtract": return "-";
                case "Multiply": return "*";
                case "Divide": return "/";
                case "Negate": return "-";
                case "Remainder": return "%";
                default: return null;
            }
        }

        protected virtual string GetOperator(UnaryExpression u)
        {
            switch (u.NodeType)
            {
                case ExpressionType.Negate:
                case ExpressionType.NegateChecked:
                    return "-";
                case ExpressionType.UnaryPlus:
                    return "+";
                case ExpressionType.Not:
                    return IsBoolean(u.Operand.Type) ? "NOT" : "~";
                default:
                    return "";
            }
        }

        protected virtual string GetOperator(BinaryExpression b)
        {
            switch (b.NodeType)
            {
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                    return (IsBoolean(b.Left.Type)) ? "AND" : "&";
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    return (IsBoolean(b.Left.Type) ? "OR" : "|");
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "<>";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.Add:
                case ExpressionType.AddChecked:
                    return "+";
                case ExpressionType.Subtract:
                case ExpressionType.SubtractChecked:
                    return "-";
                case ExpressionType.Multiply:
                case ExpressionType.MultiplyChecked:
                    return "*";
                case ExpressionType.Divide:
                    return "/";
                case ExpressionType.Modulo:
                    return "%";
                case ExpressionType.ExclusiveOr:
                    return "^";
                case ExpressionType.LeftShift:
                    return "<<";
                case ExpressionType.RightShift:
                    return ">>";
                default:
                    return "";
            }
        }

        protected virtual bool IsBoolean(Type type)
        {
            return type == typeof(bool) || type == typeof(bool?);
        }

        protected virtual bool IsPredicate(Expression expr)
        {
            switch (expr.NodeType)
            {
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    return IsBoolean(((BinaryExpression)expr).Type);
                case ExpressionType.Not:
                    return IsBoolean(((UnaryExpression)expr).Type);
                case ExpressionType.Equal:
                case ExpressionType.NotEqual:
                case ExpressionType.LessThan:
                case ExpressionType.LessThanOrEqual:
                case ExpressionType.GreaterThan:
                case ExpressionType.GreaterThanOrEqual:
                case (ExpressionType)DbExpressionType.IsNull:
                case (ExpressionType)DbExpressionType.Between:
                case (ExpressionType)DbExpressionType.Exists:
                case (ExpressionType)DbExpressionType.In:
                    return true;
                case ExpressionType.Call:
                    return IsBoolean(((MethodCallExpression)expr).Type);
                default:
                    return false;
            }
        }

        protected virtual bool IsInteger(Type type)
        {
            return TypeHelper.IsInteger(type);
        }

        protected virtual bool RequiresAsteriskWhenNoArgument(string aggregateName)
        {
            return aggregateName == "Count" || aggregateName == "LongCount";
        }
        #endregion

        protected enum Indentation
        {
            Same,
            Inner,
            Outer
        }
    }
}
