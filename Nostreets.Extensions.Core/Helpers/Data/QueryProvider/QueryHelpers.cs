using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Nostreets.Extensions.Helpers.Data.QueryProvider
{

    /// <summary>
    /// Creates a reusable, parameterized representation of a query that caches the execution plan
    /// </summary>
    public static class QueryCompiler
    {
        public static Delegate Compile(LambdaExpression query)
        {
            CompiledQuery cq = new CompiledQuery(query);
            return StrongDelegate.CreateDelegate(query.Type, (Func<object[], object>)cq.Invoke);
        }

        public static D Compile<D>(Expression<D> query)
        {
            return (D)(object)Compile((LambdaExpression)query);
        }

        public static Func<TResult> Compile<TResult>(Expression<Func<TResult>> query)
        {
            return new CompiledQuery(query).Invoke<TResult>;
        }

        public static Func<T1, TResult> Compile<T1, TResult>(Expression<Func<T1, TResult>> query)
        {
            return new CompiledQuery(query).Invoke<T1, TResult>;
        }

        public static Func<T1, T2, TResult> Compile<T1, T2, TResult>(Expression<Func<T1, T2, TResult>> query)
        {
            return new CompiledQuery(query).Invoke<T1, T2, TResult>;
        }

        public static Func<T1, T2, T3, TResult> Compile<T1, T2, T3, TResult>(Expression<Func<T1, T2, T3, TResult>> query)
        {
            return new CompiledQuery(query).Invoke<T1, T2, T3, TResult>;
        }

        public static Func<T1, T2, T3, T4, TResult> Compile<T1, T2, T3, T4, TResult>(Expression<Func<T1, T2, T3, T4, TResult>> query)
        {
            return new CompiledQuery(query).Invoke<T1, T2, T3, T4, TResult>;
        }

        public static Func<IEnumerable<T>> Compile<T>(this IQueryable<T> source)
        {
            return Compile<IEnumerable<T>>(
                Expression.Lambda<Func<IEnumerable<T>>>(((IQueryable)source).Expression)
                );
        }

        public class CompiledQuery
        {
            LambdaExpression query;
            Delegate fnQuery;

            internal CompiledQuery(LambdaExpression query)
            {
                this.query = query;
            }

            public LambdaExpression Query
            {
                get { return this.query; }
            }

            internal void Compile(params object[] args)
            {
                if (this.fnQuery == null)
                {
                    // first identify the query provider being used
                    Expression body = this.query.Body;

                    // ask the query provider to compile the query by 'executing' the lambda expression
                    IQueryProvider provider = this.FindProvider(body, args);
                    if (provider == null)
                    {
                        throw new InvalidOperationException("Could not find query provider");
                    }

                    Delegate result = (Delegate)provider.Execute(this.query);
                    System.Threading.Interlocked.CompareExchange(ref this.fnQuery, result, null);
                }
            }

            internal IQueryProvider FindProvider(Expression expression, object[] args)
            {
                Expression root = this.FindProviderInExpression(expression) as ConstantExpression;
                if (root == null && args != null && args.Length > 0)
                {
                    Expression replaced = ExpressionReplacer.ReplaceAll(
                        expression,
                        this.query.Parameters.ToArray(),
                        args.Select((a, i) => Expression.Constant(a, this.query.Parameters[i].Type)).ToArray()
                        );
                    root = this.FindProviderInExpression(replaced);
                }
                if (root != null) 
                {
                    ConstantExpression cex = root as ConstantExpression;
                    if (cex == null)
                    {
                        cex = PartialEvaluator.Eval(root) as ConstantExpression;
                    }
                    if (cex != null)
                    {
                        IQueryProvider provider = cex.Value as IQueryProvider;
                        if (provider == null)
                        {
                            IQueryable query = cex.Value as IQueryable;
                            if (query != null)
                            {
                                provider = query.Provider;
                            }
                        }
                        return provider;
                    }
                }
                return null;
            }

            private Expression FindProviderInExpression(Expression expression)
            {
                Expression root = TypedSubtreeFinder.Find(expression, typeof(IQueryProvider));
                if (root == null)
                {
                    root = TypedSubtreeFinder.Find(expression, typeof(IQueryable));
                }
                return root;
            }

            public object Invoke(object[] args)
            {
                this.Compile(args);
                if (invoker == null)
                {
                    invoker = GetInvoker();
                }
                if (invoker != null)
                {
                    return invoker(args);
                }
                else
                {
                    try
                    {
                        return this.fnQuery.DynamicInvoke(args);
                    }
                    catch (TargetInvocationException tie)
                    {
                        throw tie.InnerException;
                    }
                }
            }

            Func<object[], object> invoker;
            bool checkedForInvoker;

            private Func<object[], object> GetInvoker()
            {
                if (this.fnQuery != null && this.invoker == null && !checkedForInvoker)
                {
                    this.checkedForInvoker = true;
                    Type fnType = this.fnQuery.GetType();
                    if (fnType.FullName.StartsWith("System.Func`"))
                    {
                        var typeArgs = fnType.GetGenericArguments();
                        MethodInfo method = this.GetType().GetMethod("FastInvoke"+typeArgs.Length, BindingFlags.Public|BindingFlags.Instance);
                        if (method != null)
                        {
                            this.invoker = (Func<object[], object>)Delegate.CreateDelegate(typeof(Func<object[], object>), this, method.MakeGenericMethod(typeArgs));
                        }
                    }
                }
                return this.invoker;
            }

            public object FastInvoke1<R>(object[] args)
            {
                return ((Func<R>)this.fnQuery)();
            }

            public object FastInvoke2<A1, R>(object[] args)
            {
                return ((Func<A1, R>)this.fnQuery)((A1)args[0]);
            }

            public object FastInvoke3<A1, A2, R>(object[] args)
            {
                return ((Func<A1, A2, R>)this.fnQuery)((A1)args[0], (A2)args[1]);
            }

            public object FastInvoke4<A1, A2, A3, R>(object[] args)
            {
                return ((Func<A1, A2, A3, R>)this.fnQuery)((A1)args[0], (A2)args[1], (A3)args[2]);
            }

            public object FastInvoke5<A1, A2, A3, A4, R>(object[] args)
            {
                return ((Func<A1, A2, A3, A4, R>)this.fnQuery)((A1)args[0], (A2)args[1], (A3)args[2], (A4)args[3]);
            }

            internal TResult Invoke<TResult>()
            {
                this.Compile(null);
                return ((Func<TResult>)this.fnQuery)();
            }

            internal TResult Invoke<T1, TResult>(T1 arg)
            {
                this.Compile(arg);
                return ((Func<T1, TResult>)this.fnQuery)(arg);
            }

            internal TResult Invoke<T1, T2, TResult>(T1 arg1, T2 arg2)
            {
                this.Compile(arg1, arg2);
                return ((Func<T1, T2, TResult>)this.fnQuery)(arg1, arg2);
            }

            internal TResult Invoke<T1, T2, T3, TResult>(T1 arg1, T2 arg2, T3 arg3)
            {
                this.Compile(arg1, arg2, arg3);
                return ((Func<T1, T2, T3, TResult>)this.fnQuery)(arg1, arg2, arg3);
            }

            internal TResult Invoke<T1, T2, T3, T4, TResult>(T1 arg1, T2 arg2, T3 arg3, T4 arg4)
            {
                this.Compile(arg1, arg2, arg3, arg4);
                return ((Func<T1, T2, T3, T4, TResult>)this.fnQuery)(arg1, arg2, arg3, arg4);
            }
        }
    }
    public class QueryCommand
    {
        string commandText;
        ReadOnlyCollection<QueryParameter> parameters;

        public QueryCommand(string commandText, IEnumerable<QueryParameter> parameters)
        {
            this.commandText = commandText;
            this.parameters = parameters.ToReadOnly();
        }

        public string CommandText
        {
            get { return this.commandText; }
        }

        public ReadOnlyCollection<QueryParameter> Parameters
        {
            get { return this.parameters; }
        }
    }

    public class QueryParameter
    {
        string name;
        Type type;
        QueryType queryType;

        public QueryParameter(string name, Type type, QueryType queryType)
        {
            this.name = name;
            this.type = type;
            this.queryType = queryType;
        }

        public string Name
        {
            get { return this.name; }
        }

        public Type Type
        {
            get { return this.type; }
        }

        public QueryType QueryType
        {
            get { return this.queryType; }
        }
    }


    /// <summary>
    /// Defines query execution and materialization policies. 
    /// </summary>
    public class QueryTranslator
    {
        QueryLinguist linguist;
        QueryMapper mapper;
        QueryPolice police;

        public QueryTranslator(QueryLanguage language, QueryMapping mapping, QueryPolicy policy)
        {
            this.linguist = language.CreateLinguist(this);
            this.mapper = mapping.CreateMapper(this);
            this.police = policy.CreatePolice(this);
        }

        public QueryLinguist Linguist
        {
            get { return this.linguist; }
        }

        public QueryMapper Mapper
        {
            get { return this.mapper; }
        }

        public QueryPolice Police
        {
            get { return this.police; }
        }

        public virtual Expression Translate(Expression expression)
        {
            // pre-evaluate local sub-trees
            expression = PartialEvaluator.Eval(expression, this.mapper.Mapping.CanBeEvaluatedLocally);

            // apply mapping (binds LINQ operators too)
            expression = this.mapper.Translate(expression);

            // any policy specific translations or validations
            expression = this.police.Translate(expression);

            // any language specific translations or validations
            expression = this.linguist.Translate(expression);

            return expression;
        }
    }

     /// <summary>
    /// Defines the language rules for the query provider
    /// </summary>
    public abstract class QueryLanguage
    {
        public abstract QueryTypeSystem TypeSystem { get; }
        public abstract Expression GetGeneratedIdExpression(MemberInfo member);

        public virtual string Quote(string name)
        {
            return name;
        }

        public virtual bool AllowsMultipleCommands
        {
            get { return false; }
        }

        public virtual bool AllowSubqueryInSelectWithoutFrom
        {
            get { return false; }
        }

        public virtual bool AllowDistinctInAggregates
        {
            get { return false; }
        }

        public virtual Expression GetRowsAffectedExpression(Expression command)
        {
            return new FunctionExpression(typeof(int), "@@ROWCOUNT", null);
        }

        public virtual bool IsRowsAffectedExpressions(Expression expression)
        {
            FunctionExpression fex = expression as FunctionExpression;
            return fex != null && fex.Name == "@@ROWCOUNT";
        }

        public virtual Expression GetOuterJoinTest(SelectExpression select)
        {
            // if the column is used in the join condition (equality test)
            // if it is null in the database then the join test won't match (null != null) so the row won't appear
            // we can safely use this existing column as our test to determine if the outer join produced a row

            // find a column that is used in equality test
            var aliases = DeclaredAliasGatherer.Gather(select.From);
            var joinColumns = JoinColumnGatherer.Gather(aliases, select).ToList();
            if (joinColumns.Count > 0)
            {
                // prefer one that is already in the projection list.
                foreach (var jc in joinColumns)
                {
                    foreach (var col in select.Columns)
                    {
                        if (jc.Equals(col.Expression))
                        {
                            return jc;
                        }
                    }
                }
                return joinColumns[0];
            }

            // fall back to introducing a constant
            return Expression.Constant(1, typeof(int?));
        }

        public virtual ProjectionExpression AddOuterJoinTest(ProjectionExpression proj)
        {
            var test = this.GetOuterJoinTest(proj.Select);
            var select = proj.Select;
            ColumnExpression testCol = null;
            // look to see if test expression exists in columns already
            foreach (var col in select.Columns)
            {
                if (test.Equals(col.Expression))
                {
                    var colType = this.TypeSystem.GetColumnType(test.Type);
                    testCol = new ColumnExpression(test.Type, colType, select.Alias, col.Name);
                    break;
                }
            }
            if (testCol == null)
            {
                // add expression to projection
                testCol = test as ColumnExpression;
                string colName = (testCol != null) ? testCol.Name : "Test";
                colName = proj.Select.Columns.GetAvailableColumnName(colName);
                var colType = this.TypeSystem.GetColumnType(test.Type);
                select = select.AddColumn(new ColumnDeclaration(colName, test, colType));
                testCol = new ColumnExpression(test.Type, colType, select.Alias, colName);
            }
            var newProjector = new OuterJoinedExpression(testCol, proj.Projector);
            return new ProjectionExpression(select, newProjector, proj.Aggregator);
        }

        class JoinColumnGatherer
        {
            HashSet<TableAlias> aliases;
            HashSet<ColumnExpression> columns = new HashSet<ColumnExpression>();

            private JoinColumnGatherer(HashSet<TableAlias> aliases)
            {
                this.aliases = aliases;
            }

            public static HashSet<ColumnExpression> Gather(HashSet<TableAlias> aliases, SelectExpression select)
            {
                var gatherer = new JoinColumnGatherer(aliases);
                gatherer.Gather(select.Where);
                return gatherer.columns;
            }

            private void Gather(Expression expression)
            {
                BinaryExpression b = expression as BinaryExpression;
                if (b != null)
                {
                    switch (b.NodeType)
                    {
                        case ExpressionType.Equal:
                        case ExpressionType.NotEqual:
                            if (IsExternalColumn(b.Left) && GetColumn(b.Right) != null)
                            {
                                this.columns.Add(GetColumn(b.Right));
                            }
                            else if (IsExternalColumn(b.Right) && GetColumn(b.Left) != null)
                            {
                                this.columns.Add(GetColumn(b.Left));
                            }
                            break;
                        case ExpressionType.And:
                        case ExpressionType.AndAlso:
                            if (b.Type == typeof(bool) || b.Type == typeof(bool?))
                            {
                                this.Gather(b.Left);
                                this.Gather(b.Right);
                            }
                            break;
                    }
                }
            }

            private ColumnExpression GetColumn(Expression exp)
            {
                while (exp.NodeType == ExpressionType.Convert)
                    exp = ((UnaryExpression)exp).Operand;
                return exp as ColumnExpression;
            }

            private bool IsExternalColumn(Expression exp)
            {
                var col = GetColumn(exp);
                if (col != null && !this.aliases.Contains(col.Alias))
                    return true;
                return false;
            }
        }

        /// <summary>
        /// Determines whether the CLR type corresponds to a scalar data type in the query language
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public virtual bool IsScalar(Type type)
        {
            type = TypeHelper.GetNonNullableType(type);
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Empty:
                case TypeCode.DBNull:
                    return false;
                case TypeCode.Object:
                    return
                        type == typeof(DateTimeOffset) ||
                        type == typeof(TimeSpan) ||
                        type == typeof(Guid) ||
                        type == typeof(byte[]);
                default:
                    return true;
            }
        }

        public virtual bool IsAggregate(MemberInfo member)
        {
            var method = member as MethodInfo;
            if (method != null)
            {
                if (method.DeclaringType == typeof(Queryable)
                    || method.DeclaringType == typeof(Enumerable))
                {
                    switch (method.Name)
                    {
                        case "Count":
                        case "LongCount":
                        case "Sum":
                        case "Min":
                        case "Max":
                        case "Average":
                            return true;
                    }
                }
            }
            var property = member as PropertyInfo;
            if (property != null
                && property.Name == "Count"
                && typeof(IEnumerable).IsAssignableFrom(property.DeclaringType))
            {
                return true;
            }
            return false;
        }

        public virtual bool AggregateArgumentIsPredicate(string aggregateName)
        {
            return aggregateName == "Count" || aggregateName == "LongCount";
        }

        /// <summary>
        /// Determines whether the given expression can be represented as a column in a select expressionss
        /// </summary>
        public virtual bool CanBeColumn(Expression expression)
        {
            return this.MustBeColumn(expression) || this.IsScalar(expression.Type);
        }

        /// <summary>
        /// Determines whether the given expression must be represented as a column in a SELECT column list
        /// </summary>
        public virtual bool MustBeColumn(Expression expression)
        {
            switch (expression.NodeType)
            {
                case (ExpressionType)DbExpressionType.Column:
                case (ExpressionType)DbExpressionType.Scalar:
                case (ExpressionType)DbExpressionType.Exists:
                case (ExpressionType)DbExpressionType.AggregateSubquery:
                case (ExpressionType)DbExpressionType.Aggregate:
                    return true;
                default:
                    return false;
            }
        }

        public virtual QueryLinguist CreateLinguist(QueryTranslator translator)
        {
            return new QueryLinguist(this, translator);
        }
    }

    public class QueryCache
    {
        MostRecentlyUsedCache<QueryCompiler.CompiledQuery> cache;
        static readonly Func<QueryCompiler.CompiledQuery, QueryCompiler.CompiledQuery, bool> fnCompareQueries = CompareQueries;
        static readonly Func<object, object, bool> fnCompareValues = CompareConstantValues;

        public QueryCache(int maxSize)
        {
            this.cache = new MostRecentlyUsedCache<QueryCompiler.CompiledQuery>(maxSize, fnCompareQueries);
        }

        private static bool CompareQueries(QueryCompiler.CompiledQuery x, QueryCompiler.CompiledQuery y)
        {
            return ExpressionComparer.AreEqual(x.Query, y.Query, fnCompareValues);
        }

        private static bool CompareConstantValues(object x, object y)
        {
            if (x == y) return true;
            if (x == null || y == null) return false;
            if (x is IQueryable && y is IQueryable && x.GetType() == y.GetType()) return true;
            return object.Equals(x, y);
        }

        public object Execute(Expression query)
        {
            object[] args;
            var cached = this.Find(query, true, out args);
            return cached.Invoke(args);
        }

        public object Execute(IQueryable query)
        {
            return this.Equals(query.Expression);
        }

        public IEnumerable<T> Execute<T>(IQueryable<T> query)
        {
            return (IEnumerable<T>)this.Execute(query.Expression);
        }

        public int Count
        {
            get { return this.cache.Count; }
        }

        public void Clear()
        {
            this.cache.Clear();
        }

        public bool Contains(Expression query)
        {
            object[] args;
            return this.Find(query, false, out args) != null;
        }

        public bool Contains(IQueryable query)
        {
            return this.Contains(query.Expression);
        }

        private QueryCompiler.CompiledQuery Find(Expression query, bool add, out object[] args)
        {
            var pq = this.Parameterize(query, out args);
            var cq = new QueryCompiler.CompiledQuery(pq);
            QueryCompiler.CompiledQuery cached;
            this.cache.Lookup(cq, add, out cached);
            return cached;
        }

        private LambdaExpression Parameterize(Expression query, out object[] arguments)
        {
            IQueryProvider provider = this.FindProvider(query);
            if (provider == null)
            {
                throw new ArgumentException("Cannot deduce query provider from query");
            }

            var ep = provider as IEntityProvider;
            Func<Expression, bool> fn = ep != null ? (Func<Expression, bool>)ep.CanBeEvaluatedLocally : null;
            List<ParameterExpression> parameters = new List<ParameterExpression>();
            List<object> values = new List<object>();

            var body = PartialEvaluator.Eval(query, fn, c =>
            {
                bool isQueryRoot = c.Value is IQueryable;
                if (!isQueryRoot && ep != null && !ep.CanBeParameter(c))
                    return c;
                var p = Expression.Parameter(c.Type, "p" + parameters.Count);
                parameters.Add(p);
                values.Add(c.Value);
                // if query root then parameterize but don't replace in the tree 
                if (isQueryRoot)
                    return c;
                return p;
            });

            if (body.Type != typeof(object))
                body = Expression.Convert(body, typeof(object));

            arguments = values.ToArray();
            if (arguments.Length < 5)
            {
                return Expression.Lambda(body, parameters.ToArray());
            }
            else
            {
                arguments = new object[] { arguments };
                return ExplicitToObjectArray.Rewrite(body, parameters);
            }
        }

        private IQueryProvider FindProvider(Expression expression)
        {
            ConstantExpression root = TypedSubtreeFinder.Find(expression, typeof(IQueryProvider)) as ConstantExpression;
            if (root == null)
            {
                root = TypedSubtreeFinder.Find(expression, typeof(IQueryable)) as ConstantExpression;
            }
            if (root != null)
            {
                IQueryProvider provider = root.Value as IQueryProvider;
                if (provider == null)
                {
                    IQueryable query = root.Value as IQueryable;
                    if (query != null)
                    {
                        provider = query.Provider;
                    }
                }
                return provider;
            }
            return null;
        }


        class ExplicitToObjectArray : ExpressionVisitor
        {
            IList<ParameterExpression> parameters;
            ParameterExpression array = Expression.Parameter(typeof(object[]), "array");

            private ExplicitToObjectArray(IList<ParameterExpression> parameters)
            {
                this.parameters = parameters;
            }

            internal static LambdaExpression Rewrite(Expression body, IList<ParameterExpression> parameters)
            {
                var visitor = new ExplicitToObjectArray(parameters);
                return Expression.Lambda(visitor.Visit(body), visitor.array);                  
            }

            protected override Expression VisitParameter(ParameterExpression p)
            {
                for (int i = 0, n = this.parameters.Count; i < n; i++)
                {
                    if (this.parameters[i] == p)
                    {
                        return Expression.Convert(Expression.ArrayIndex(this.array, Expression.Constant(i)), p.Type);
                    }
                }
                return p;
            }
        }
    }

    public class QueryLinguist
    {
        QueryLanguage language;
        QueryTranslator translator;

        public QueryLinguist(QueryLanguage language, QueryTranslator translator)
        {
            this.language = language;
            this.translator = translator;
        }

        public QueryLanguage Language 
        {
            get { return this.language; }
        }

        public QueryTranslator Translator
        {
            get { return this.translator; }
        }

        /// <summary>
        /// Provides language specific query translation.  Use this to apply language specific rewrites or
        /// to make assertions/validations about the query.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public virtual Expression Translate(Expression expression)
        {
            // remove redundant layers again before cross apply rewrite
            expression = UnusedColumnRemover.Remove(expression);
            expression = RedundantColumnRemover.Remove(expression);
            expression = RedundantSubqueryRemover.Remove(expression);

            // convert cross-apply and outer-apply joins into inner & left-outer-joins if possible
            var rewritten = CrossApplyRewriter.Rewrite(this.language, expression);

            // convert cross joins into inner joins
            rewritten = CrossJoinRewriter.Rewrite(rewritten);

            if (rewritten != expression)
            {
                expression = rewritten;
                // do final reduction
                expression = UnusedColumnRemover.Remove(expression);
                expression = RedundantSubqueryRemover.Remove(expression);
                expression = RedundantJoinRemover.Remove(expression);
                expression = RedundantColumnRemover.Remove(expression);
            }

            return expression;
        }

        /// <summary>
        /// Converts the query expression into text of this query language
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public virtual string Format(Expression expression)
        {
            // use common SQL formatter by default
            return SqlFormatter.Format(expression);
        }

        /// <summary>
        /// Determine which sub-expressions must be parameters
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public virtual Expression Parameterize(Expression expression)
        {
            return Parameterizer.Parameterize(this.language, expression);
        }
    }

    /// <summary>
    /// Defines query execution and materialization policies. 
    /// </summary>
    public class QueryPolicy
    {
        public QueryPolicy()
        {
        }

        /// <summary>
        /// Determines if a relationship property is to be included in the results of the query
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual bool IsIncluded(MemberInfo member)
        {
            return false;
        }

        /// <summary>
        /// Determines if a relationship property is included, but the query for the related data is 
        /// deferred until the property is first accessed.
        /// </summary>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual bool IsDeferLoaded(MemberInfo member)
        {
            return false;
        }

        public virtual QueryPolice CreatePolice(QueryTranslator translator)
        {
            return new QueryPolice(this, translator);
        }

        public static readonly QueryPolicy Default = new QueryPolicy();
    }

    public class QueryPolice
    {
        QueryPolicy policy;
        QueryTranslator translator;

        public QueryPolice(QueryPolicy policy, QueryTranslator translator)
        {
            this.policy = policy;
            this.translator = translator;
        }

        public QueryPolicy Policy
        {
            get { return this.policy; }
        }

        public QueryTranslator Translator
        {
            get { return this.translator; }
        }

        public virtual Expression ApplyPolicy(Expression expression, MemberInfo member)
        {
            return expression;
        }

        /// <summary>
        /// Provides policy specific query translations.  This is where choices about inclusion of related objects and how
        /// heirarchies are materialized affect the definition of the queries.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public virtual Expression Translate(Expression expression)
        {
            // add included relationships to client projection
            var rewritten = RelationshipIncluder.Include(this.translator.Mapper, expression);
            if (rewritten != expression)
            {
                expression = rewritten;
                expression = UnusedColumnRemover.Remove(expression);
                expression = RedundantColumnRemover.Remove(expression);
                expression = RedundantSubqueryRemover.Remove(expression);
                expression = RedundantJoinRemover.Remove(expression);
            }

            // convert any singleton (1:1 or n:1) projections into server-side joins (cardinality is preserved)
            rewritten = SingletonProjectionRewriter.Rewrite(this.translator.Linguist.Language, expression);
            if (rewritten != expression)
            {
                expression = rewritten;
                expression = UnusedColumnRemover.Remove(expression);
                expression = RedundantColumnRemover.Remove(expression);
                expression = RedundantSubqueryRemover.Remove(expression);
                expression = RedundantJoinRemover.Remove(expression);
            }

            // convert projections into client-side joins
            rewritten = ClientJoinedProjectionRewriter.Rewrite(this.policy, this.translator.Linguist.Language, expression);
            if (rewritten != expression)
            {
                expression = rewritten;
                expression = UnusedColumnRemover.Remove(expression);
                expression = RedundantColumnRemover.Remove(expression);
                expression = RedundantSubqueryRemover.Remove(expression);
                expression = RedundantJoinRemover.Remove(expression);
            }

            return expression;
        }

        /// <summary>
        /// Converts a query into an execution plan.  The plan is an function that executes the query and builds the
        /// resulting objects.
        /// </summary>
        public virtual Expression BuildExecutionPlan(Expression query, Expression provider)
        {
            return ExecutionBuilder.Build(this.translator.Linguist, this.policy, query, provider);
        }
    }

    /// <summary>
    /// Duplicate the query expression by making a copy with new table aliases
    /// </summary>
    public class QueryDuplicator : DbExpressionVisitor
    {
        Dictionary<TableAlias, TableAlias> map = new Dictionary<TableAlias, TableAlias>();

        public static Expression Duplicate(Expression expression)
        {
            return new QueryDuplicator().Visit(expression);
        }

        protected override Expression VisitTable(TableExpression table)
        {
            TableAlias newAlias = new TableAlias();
            this.map[table.Alias] = newAlias;
            return new TableExpression(newAlias, table.Entity, table.Name);
        }

        protected override Expression VisitSelect(SelectExpression select)
        {
            TableAlias newAlias = new TableAlias();
            this.map[select.Alias] = newAlias;
            select = (SelectExpression)base.VisitSelect(select);
            return new SelectExpression(newAlias, select.Columns, select.From, select.Where, select.OrderBy, select.GroupBy, select.IsDistinct, select.Skip, select.Take, select.IsReverse);
        }

        protected override Expression VisitColumn(ColumnExpression column)
        {
            TableAlias newAlias;
            if (this.map.TryGetValue(column.Alias, out newAlias))
            {
                return new ColumnExpression(column.Type, column.QueryType, newAlias, column.Name);
            }
            return column;
        }
    }

    public abstract class QueryExecutor
    {
        // called from compiled execution plan
        public abstract int RowsAffected { get; }
        public abstract object Convert(object value, Type type);
        public abstract IEnumerable<T> Execute<T>(QueryCommand command, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues);
        public abstract IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize, bool stream);
        public abstract IEnumerable<T> ExecuteBatch<T>(QueryCommand query, IEnumerable<object[]> paramSets, Func<FieldReader, T> fnProjector, MappingEntity entity, int batchSize, bool stream);
        public abstract IEnumerable<T> ExecuteDeferred<T>(QueryCommand query, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues);
        public abstract int ExecuteCommand(QueryCommand query, object[] paramValues);
    }

    public abstract class QueryType
    {
        public abstract bool NotNull { get; }
        public abstract int Length { get; }
        public abstract short Precision { get; }
        public abstract short Scale { get; }
    }

    public abstract class QueryTypeSystem
    {
        public abstract QueryType Parse(string typeDeclaration);
        public abstract QueryType GetColumnType(Type type);
        public abstract string GetVariableDeclaration(QueryType type, bool suppressSize);
    }

    public class Query<T> : IQueryable<T>, IQueryable, IEnumerable<T>, IEnumerable, IOrderedQueryable<T>, IOrderedQueryable
    {
        IQueryProvider provider;
        Expression expression;

        public Query(IQueryProvider provider)
            : this(provider, null)
        {
        }

        public Query(IQueryProvider provider, Type staticType)
        {
            if (provider == null)
            {
                throw new ArgumentNullException("Provider");
            }
            this.provider = provider;
            this.expression = staticType != null ? Expression.Constant(this, staticType) : Expression.Constant(this);
        }

        public Query(BaseProvider provider, Expression expression)
        {
            if (provider == null)
            {
                throw new ArgumentNullException("Provider");
            }
            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }
            if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
            {
                throw new ArgumentOutOfRangeException("expression");
            }
            this.provider = provider;
            this.expression = expression;
        }

        public Expression Expression
        {
            get { return this.expression; }
        }

        public Type ElementType
        {
            get { return typeof(T); }
        }

        public IQueryProvider Provider
        {
            get { return this.provider; }
        }

        public IEnumerator<T> GetEnumerator()
        {
            return ((IEnumerable<T>)this.provider.Execute(this.expression)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)this.provider.Execute(this.expression)).GetEnumerator();
        }

        public override string ToString()
        {
            if (this.expression.NodeType == ExpressionType.Constant &&
                ((ConstantExpression)this.expression).Value == this)
            {
                return "Query(" + typeof(T) + ")";
            }
            else
            {
                return this.expression.ToString();
            }
        }

        public string QueryText
        {
            get
            {
                IQueryText iqt = this.provider as IQueryText;
                if (iqt != null)
                {
                    return iqt.GetQueryText(this.expression);
                }
                return "";
            }
        }
    }
}
