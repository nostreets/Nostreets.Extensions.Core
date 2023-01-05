using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

namespace Nostreets.Extensions.Helpers.Data.QueryProvider
{
    public static class Updatable
    {
        public static object Insert(IUpdatable collection, object instance, LambdaExpression resultSelector)
        {
            var callMyself = Expression.Call(
                null,
                (MethodInfo)MethodInfo.GetCurrentMethod(),
                collection.Expression,
                Expression.Constant(instance),
                resultSelector != null ? (Expression)Expression.Quote(resultSelector) : Expression.Constant(null, typeof(LambdaExpression))
                );
            return collection.Provider.Execute(callMyself);
        }

        /// <summary>
        /// Insert an copy of the instance into the updatable collection and produce a result if the insert succeeds.
        /// </summary>
        /// <typeparam name="T">The type of the instance.</typeparam>
        /// <typeparam name="S">The type of the result.</typeparam>
        /// <param name="collection">The updatable collection.</param>
        /// <param name="instance">The instance to insert.</param>
        /// <param name="resultSelector">The function that produces the result.</param>
        /// <returns>The value of the result if the insert succeed, otherwise null.</returns>
        public static S Insert<T, S>(this IUpdatable<T> collection, T instance, Expression<Func<T, S>> resultSelector)
        {
            var callMyself = Expression.Call(
                null,
                ((MethodInfo)MethodInfo.GetCurrentMethod()).MakeGenericMethod(typeof(T), typeof(S)),
                collection.Expression,
                Expression.Constant(instance),
                resultSelector != null ? (Expression)Expression.Quote(resultSelector) : Expression.Constant(null, typeof(Expression<Func<T, S>>))
                );
            return (S)collection.Provider.Execute(callMyself);
        }

        /// <summary>
        /// Insert a copy of the instance into an updatable collection.
        /// </summary>
        /// <typeparam name="T">The type of the instance.</typeparam>
        /// <param name="collection">The updatable collection.</param>
        /// <param name="instance">The instance to insert.</param>
        /// <returns>The value 1 if the insert succeeds, otherwise 0.</returns>
        public static int Insert<T>(this IUpdatable<T> collection, T instance)
        {
            return Insert<T, int>(collection, instance, null);
        }

        public static object Update(IUpdatable collection, object instance, LambdaExpression updateCheck, LambdaExpression resultSelector)
        {
            var callMyself = Expression.Call(
                null,
                (MethodInfo)MethodInfo.GetCurrentMethod(),
                collection.Expression,
                Expression.Constant(instance),
                updateCheck != null ? (Expression)Expression.Quote(updateCheck) : Expression.Constant(null, typeof(LambdaExpression)),
                resultSelector != null ? (Expression)Expression.Quote(resultSelector) : Expression.Constant(null, typeof(LambdaExpression))
                );
            return collection.Provider.Execute(callMyself);
        }

        /// <summary>
        /// Update the object in the updatable collection with the values in this instance only if the update check passes and produce
        /// a result based on the updated object if the update succeeds.
        /// </summary>
        /// <typeparam name="T">The type of the instance.</typeparam>
        /// <typeparam name="S">The type of the result.</typeparam>
        /// <param name="collection">The updatable collection</param>
        /// <param name="instance">The instance to update.</param>
        /// <param name="updateCheck">A predicate testing the suitability of the object in the collection (often used that make sure assumptions have not changed.)</param>
        /// <param name="resultSelector">A function that produces a result based on the object in the collection after the update succeeds.</param>
        /// <returns>The value of the result function if the update succeeds, otherwise null.</returns>
        public static S Update<T, S>(this IUpdatable<T> collection, T instance, Expression<Func<T, bool>> updateCheck, Expression<Func<T, S>> resultSelector)
        {
            var callMyself = Expression.Call(
                null,
                ((MethodInfo)MethodInfo.GetCurrentMethod()).MakeGenericMethod(typeof(T), typeof(S)),
                collection.Expression,
                Expression.Constant(instance),
                updateCheck != null ? (Expression)Expression.Quote(updateCheck) : Expression.Constant(null, typeof(Expression<Func<T, bool>>)),
                resultSelector != null ? (Expression)Expression.Quote(resultSelector) : Expression.Constant(null, typeof(Expression<Func<T, S>>))
                );
            return (S)collection.Provider.Execute(callMyself);
        }

        /// <summary>
        /// Update the object in the updatable collection with the values in this instance only if the update check passes.
        /// </summary>
        /// <typeparam name="T">The type of the instance</typeparam>
        /// <param name="collection">The updatable collection.</param>
        /// <param name="instance">The instance to update.</param>
        /// <param name="updateCheck">A predicate testing the suitability of the object in the collection.</param>
        /// <returns>The value 1 if the update succeeds, otherwise 0.</returns>
        public static int Update<T>(this IUpdatable<T> collection, T instance, Expression<Func<T, bool>> updateCheck)
        {
            return Update<T, int>(collection, instance, updateCheck, null);
        }

        /// <summary>
        /// Update the object in the updatable collection with the values in this instance.
        /// </summary>
        /// <typeparam name="T">The type of the instance.</typeparam>
        /// <param name="collection">The updatable collection.</param>
        /// <param name="instance">The instance to update.</param>
        /// <returns>The value 1 if the update succeeds, otherwise 0.</returns>
        public static int Update<T>(this IUpdatable<T> collection, T instance)
        {
            return Update<T, int>(collection, instance, null, null);
        }

        public static object InsertOrUpdate(IUpdatable collection, object instance, LambdaExpression updateCheck, LambdaExpression resultSelector)
        {
            var callMyself = Expression.Call(
                null,
                (MethodInfo)MethodInfo.GetCurrentMethod(),
                collection.Expression,
                Expression.Constant(instance),
                updateCheck != null ? (Expression)Expression.Quote(updateCheck) : Expression.Constant(null, typeof(LambdaExpression)),
                resultSelector != null ? (Expression)Expression.Quote(resultSelector) : Expression.Constant(null, typeof(LambdaExpression))
                );
            return collection.Provider.Execute(callMyself);
        }

        /// <summary>
        /// Insert a copy of the instance if it does not exist in the collection or update the object in the collection with the values in this instance. 
        /// Produce a result based on the object in the collection after the insert or update succeeds.
        /// </summary>
        /// <typeparam name="T">The type of the instance.</typeparam>
        /// <typeparam name="S">The type of the result.</typeparam>
        /// <param name="collection">The updatable collection.</param>
        /// <param name="instance">The instance to insert or update.</param>
        /// <param name="updateCheck">A predicate testing the suitablilty of the object in the collection if an update is required.</param>
        /// <param name="resultSelector">A function producing a result based on the object in the collection after the insert or update succeeds.</param>
        /// <returns>The value of the result if the insert or update succeeds, otherwise null.</returns>
        public static S InsertOrUpdate<T, S>(this IUpdatable<T> collection, T instance, Expression<Func<T, bool>> updateCheck, Expression<Func<T, S>> resultSelector)
        {
            var callMyself = Expression.Call(
                null,
                ((MethodInfo)MethodInfo.GetCurrentMethod()).MakeGenericMethod(typeof(T), typeof(S)),
                collection.Expression,
                Expression.Constant(instance),
                updateCheck != null ? (Expression)Expression.Quote(updateCheck) : Expression.Constant(null, typeof(Expression<Func<T, bool>>)),
                resultSelector != null ? (Expression)Expression.Quote(resultSelector) : Expression.Constant(null, typeof(Expression<Func<T, S>>))
                );
            return (S)collection.Provider.Execute(callMyself);
        }

        /// <summary>
        /// Insert a copy of the instance if it does not exist in the collection or update the object in the collection with the values in this instance. 
        /// </summary>
        /// <typeparam name="T">The type of the instance.</typeparam>
        /// <param name="collection">The updatable collection.</param>
        /// <param name="instance">The instance to insert or update.</param>
        /// <param name="updateCheck">A function producing a result based on the object in the collection after the insert or update succeeds.</param>
        /// <returns>The value 1 if the insert or update succeeds, otherwise 0.</returns>
        public static int InsertOrUpdate<T>(this IUpdatable<T> collection, T instance, Expression<Func<T, bool>> updateCheck)
        {
            return InsertOrUpdate<T, int>(collection, instance, updateCheck, null);
        }

        /// <summary>
        /// Insert a copy of the instance if it does not exist in the collection or update the object in the collection with the values in this instance. 
        /// </summary>
        /// <typeparam name="T">The type of the instance.</typeparam>
        /// <param name="collection">The updatable collection.</param>
        /// <param name="instance">The instance to insert or update.</param>
        /// <returns>The value 1 if the insert or update succeeds, otherwise 0.</returns>
        public static int InsertOrUpdate<T>(this IUpdatable<T> collection, T instance)
        {
            return InsertOrUpdate<T, int>(collection, instance, null, null);
        }

        public static object Delete(IUpdatable collection, object instance, LambdaExpression deleteCheck)
        {
            var callMyself = Expression.Call(
                null,
                (MethodInfo)MethodInfo.GetCurrentMethod(),
                collection.Expression,
                Expression.Constant(instance),
                deleteCheck != null ? (Expression)Expression.Quote(deleteCheck) : Expression.Constant(null, typeof(LambdaExpression))
                );
            return collection.Provider.Execute(callMyself);
        }

        /// <summary>
        /// Delete the object in the collection that matches the instance only if the delete check passes.
        /// </summary>
        /// <typeparam name="T">The type of the instance.</typeparam>
        /// <param name="collection">The updatable collection.</param>
        /// <param name="instance">The instance to delete.</param>
        /// <param name="deleteCheck">A predicate testing the suitability of the corresponding object in the collection.</param>
        /// <returns>The value 1 if the delete succeeds, otherwise 0.</returns>
        public static int Delete<T>(this IUpdatable<T> collection, T instance, Expression<Func<T, bool>> deleteCheck)
        {
            var callMyself = Expression.Call(
                null,
                ((MethodInfo)MethodInfo.GetCurrentMethod()).MakeGenericMethod(typeof(T)),
                collection.Expression,
                Expression.Constant(instance),
                deleteCheck != null ? (Expression)Expression.Quote(deleteCheck) : Expression.Constant(null, typeof(Expression<Func<T, bool>>))
                );
            return (int)collection.Provider.Execute(callMyself);
        }

        /// <summary>
        /// Delete the object in the collection that matches the instance.
        /// </summary>
        /// <typeparam name="T">The type of the instance.</typeparam>
        /// <param name="collection">The updatable collection.</param>
        /// <param name="instance">The instance to delete.</param>
        /// <returns>The value 1 if the Delete succeeds, otherwise 0.</returns>
        public static int Delete<T>(this IUpdatable<T> collection, T instance)
        {
            return Delete<T>(collection, instance, null);
        }

        public static int Delete(IUpdatable collection, LambdaExpression predicate)
        {
            var callMyself = Expression.Call(
                null,
                ((MethodInfo)MethodInfo.GetCurrentMethod()),
                collection.Expression,
                predicate != null ? (Expression)Expression.Quote(predicate) : Expression.Constant(null, typeof(LambdaExpression))
                );
            return (int)collection.Provider.Execute(callMyself);
        }

        /// <summary>
        /// Delete all the objects in the collection that match the predicate.
        /// </summary>
        /// <typeparam name="T">The type of the instance.</typeparam>
        /// <param name="collection">The updatable collection.</param>
        /// <param name="predicate">The predicate.</param>
        /// <returns>The number of objects deleted.</returns>
        public static int Delete<T>(this IUpdatable<T> collection, Expression<Func<T, bool>> predicate)
        {
            var callMyself = Expression.Call(
                null,
                ((MethodInfo)MethodInfo.GetCurrentMethod()).MakeGenericMethod(typeof(T)),
                collection.Expression,
                predicate != null ? (Expression)Expression.Quote(predicate) : Expression.Constant(null, typeof(Expression<Func<T, bool>>))
                );
            return (int)collection.Provider.Execute(callMyself);
        }

        public static IEnumerable Batch(IUpdatable collection, IEnumerable items, LambdaExpression fnOperation, int batchSize, bool stream)
        {
            var callMyself = Expression.Call(
                null,
                ((MethodInfo)MethodInfo.GetCurrentMethod()),
                collection.Expression,
                Expression.Constant(items),
                fnOperation != null ? (Expression)Expression.Quote(fnOperation) : Expression.Constant(null, typeof(LambdaExpression)),
                Expression.Constant(batchSize),
                Expression.Constant(stream)
                );
            return (IEnumerable)collection.Provider.Execute(callMyself);
        }

        /// <summary>
        /// Apply an Insert, Update, InsertOrUpdate or Delete operation over a set of items and produce a set of results per invocation.
        /// </summary>
        /// <typeparam name="U">The type of the collection.</typeparam>
        /// <typeparam name="T">The type of the instances.</typeparam>
        /// <typeparam name="S">The type of each result</typeparam>
        /// <param name="collection">The updatable collection.</param>
        /// <param name="instances">The instances to apply the operation to.</param>
        /// <param name="fnOperation">The operation to apply.</param>
        /// <param name="batchSize">The maximum size of each batch.</param>
        /// <param name="stream">If true then execution is deferred until the resulting sequence is enumerated.</param>
        /// <returns>A sequence of results cooresponding to each invocation.</returns>
        public static IEnumerable<S> Batch<U, T, S>(this IUpdatable<U> collection, IEnumerable<T> instances, Expression<Func<IUpdatable<U>, T, S>> fnOperation, int batchSize, bool stream)
        {
            var callMyself = Expression.Call(
                null,
                ((MethodInfo)MethodInfo.GetCurrentMethod()).MakeGenericMethod(typeof(U), typeof(T), typeof(S)),
                collection.Expression,
                Expression.Constant(instances),
                fnOperation != null ? (Expression)Expression.Quote(fnOperation) : Expression.Constant(null, typeof(Expression<Func<IUpdatable<U>, T, S>>)),
                Expression.Constant(batchSize),
                Expression.Constant(stream)
                );
            return (IEnumerable<S>)collection.Provider.Execute(callMyself);
        }

        /// <summary>
        /// Apply an Insert, Update, InsertOrUpdate or Delete operation over a set of items and produce a set of result per invocation.
        /// </summary>
        /// <typeparam name="U">The type of the collection.</typeparam>
        /// <typeparam name="T">The type of the items.</typeparam>
        /// <typeparam name="S">The type of each result.</typeparam>
        /// <param name="collection">The updatable collection.</param>
        /// <param name="instances">The instances to apply the operation to.</param>
        /// <param name="fnOperation">The operation to apply.</param>
        /// <returns>A sequence of results corresponding to each invocation.</returns>
        public static IEnumerable<S> Batch<U, T, S>(this IUpdatable<U> collection, IEnumerable<T> instances, Expression<Func<IUpdatable<U>, T, S>> fnOperation)
        {
            return Batch<U, T, S>(collection, instances, fnOperation, 50, false);
        }
    }

    /// <summary>
    /// Result from calling ColumnProjector.ProjectColumns
    /// </summary>
    public sealed class ProjectedColumns
    {
        Expression projector;
        ReadOnlyCollection<ColumnDeclaration> columns;

        public ProjectedColumns(Expression projector, ReadOnlyCollection<ColumnDeclaration> columns)
        {
            this.projector = projector;
            this.columns = columns;
        }

        /// <summary>
        /// The expression to computed on the client.
        /// </summary>
        public Expression Projector
        {
            get { return this.projector; }
        }

        /// <summary>
        /// The columns to be computed on the server.
        /// </summary>
        public ReadOnlyCollection<ColumnDeclaration> Columns
        {
            get { return this.columns; }
        }
    }

    public enum ProjectionAffinity
    {
        /// <summary>
        /// Prefer expression computation on the client
        /// </summary>
        Client,

        /// <summary>
        /// Prefer expression computation on the server
        /// </summary>
        Server
    }

    /// <summary>
    /// Splits an expression into two parts
    ///   1) a list of column declarations for sub-expressions that must be evaluated on the server
    ///   2) a expression that describes how to combine/project the columns back together into the correct result
    /// </summary>
    public class ColumnProjector : DbExpressionVisitor
    {
        QueryLanguage language;
        Dictionary<ColumnExpression, ColumnExpression> map;
        List<ColumnDeclaration> columns;
        HashSet<string> columnNames;
        HashSet<Expression> candidates;
        HashSet<TableAlias> existingAliases;
        TableAlias newAlias;
        int iColumn;

        private ColumnProjector(QueryLanguage language, ProjectionAffinity affinity, Expression expression, IEnumerable<ColumnDeclaration> existingColumns, TableAlias newAlias, IEnumerable<TableAlias> existingAliases)
        {
            this.language = language;
            this.newAlias = newAlias;
            this.existingAliases = new HashSet<TableAlias>(existingAliases);
            this.map = new Dictionary<ColumnExpression, ColumnExpression>();
            if (existingColumns != null)
            {
                this.columns = new List<ColumnDeclaration>(existingColumns);
                this.columnNames = new HashSet<string>(existingColumns.Select(c => c.Name));
            }
            else
            {
                this.columns = new List<ColumnDeclaration>();
                this.columnNames = new HashSet<string>();
            }
            this.candidates = Nominator.Nominate(language, affinity, expression);
        }

        public static ProjectedColumns ProjectColumns(QueryLanguage language, ProjectionAffinity affinity, Expression expression, IEnumerable<ColumnDeclaration> existingColumns, TableAlias newAlias, IEnumerable<TableAlias> existingAliases)
        {
            ColumnProjector projector = new ColumnProjector(language, affinity, expression, existingColumns, newAlias, existingAliases);
            Expression expr = projector.Visit(expression);
            return new ProjectedColumns(expr, projector.columns.AsReadOnly());
        }

        public static ProjectedColumns ProjectColumns(QueryLanguage language, Expression expression, IEnumerable<ColumnDeclaration> existingColumns, TableAlias newAlias, IEnumerable<TableAlias> existingAliases)
        {
            return ProjectColumns(language, ProjectionAffinity.Client, expression, existingColumns, newAlias, existingAliases);
        }

        public static ProjectedColumns ProjectColumns(QueryLanguage language, ProjectionAffinity affinity, Expression expression, IEnumerable<ColumnDeclaration> existingColumns, TableAlias newAlias, params TableAlias[] existingAliases)
        {
            return ProjectColumns(language, affinity, expression, existingColumns, newAlias, (IEnumerable<TableAlias>)existingAliases);
        }

        public static ProjectedColumns ProjectColumns(QueryLanguage language, Expression expression, IEnumerable<ColumnDeclaration> existingColumns, TableAlias newAlias, params TableAlias[] existingAliases)
        {
            return ProjectColumns(language, expression, existingColumns, newAlias, (IEnumerable<TableAlias>)existingAliases);
        }

        protected override Expression Visit(Expression expression)
        {
            if (this.candidates.Contains(expression))
            {
                if (expression.NodeType == (ExpressionType)DbExpressionType.Column)
                {
                    ColumnExpression column = (ColumnExpression)expression;
                    ColumnExpression mapped;
                    if (this.map.TryGetValue(column, out mapped))
                    {
                        return mapped;
                    }
                    // check for column that already refers to this column
                    foreach (ColumnDeclaration existingColumn in this.columns)
                    {
                        ColumnExpression cex = existingColumn.Expression as ColumnExpression;
                        if (cex != null && cex.Alias == column.Alias && cex.Name == column.Name)
                        {
                            // refer to the column already in the column list
                            return new ColumnExpression(column.Type, column.QueryType, this.newAlias, existingColumn.Name);
                        }
                    }
                    if (this.existingAliases.Contains(column.Alias))
                    {
                        int ordinal = this.columns.Count;
                        string columnName = this.GetUniqueColumnName(column.Name);
                        this.columns.Add(new ColumnDeclaration(columnName, column, column.QueryType));
                        mapped = new ColumnExpression(column.Type, column.QueryType, this.newAlias, columnName);
                        this.map.Add(column, mapped);
                        this.columnNames.Add(columnName);
                        return mapped;
                    }
                    // must be referring to outer scope
                    return column;
                }
                else
                {
                    string columnName = this.GetNextColumnName();
                    var colType = this.language.TypeSystem.GetColumnType(expression.Type);
                    this.columns.Add(new ColumnDeclaration(columnName, expression, colType));
                    return new ColumnExpression(expression.Type, colType, this.newAlias, columnName);
                }
            }
            else
            {
                return base.Visit(expression);
            }
        }

        private bool IsColumnNameInUse(string name)
        {
            return this.columnNames.Contains(name);
        }

        private string GetUniqueColumnName(string name)
        {
            string baseName = name;
            int suffix = 1;
            while (this.IsColumnNameInUse(name))
            {
                name = baseName + (suffix++);
            }
            return name;
        }

        private string GetNextColumnName()
        {
            return this.GetUniqueColumnName("c" + (iColumn++));
        }

        /// <summary>
        /// Nominator is a class that walks an expression tree bottom up, determining the set of 
        /// candidate expressions that are possible columns of a select expression
        /// </summary>
        class Nominator : DbExpressionVisitor
        {
            private readonly QueryLanguage language;
            private readonly HashSet<Expression> candidates;
            private readonly ProjectionAffinity affinity;
            private bool isBlocked;

            private Nominator(QueryLanguage language, ProjectionAffinity affinity)
            {
                this.language = language;
                this.affinity = affinity;
                this.candidates = new HashSet<Expression>();
                this.isBlocked = false;
            }

            internal static HashSet<Expression> Nominate(QueryLanguage language, ProjectionAffinity affinity, Expression expression)
            {
                Nominator nominator = new Nominator(language, affinity);
                nominator.Visit(expression);
                return nominator.candidates;
            }

            protected override Expression Visit(Expression expression)
            {
                if (expression != null)
                {
                    bool saveIsBlocked = this.isBlocked;
                    this.isBlocked = false;
                    if (this.language.MustBeColumn(expression))
                    {
                        this.candidates.Add(expression);
                        // don't merge saveIsBlocked
                    }
                    else
                    {
                        base.Visit(expression);
                        if (!this.isBlocked)
                        {
                            if (this.language.MustBeColumn(expression)
                                || (this.affinity == ProjectionAffinity.Server && this.language.CanBeColumn(expression)))
                            {
                                this.candidates.Add(expression);
                            }
                            else
                            {
                                this.isBlocked = true;
                            }
                        }
                        this.isBlocked |= saveIsBlocked;
                    }
                }
                return expression;
            }

            protected override Expression VisitProjection(ProjectionExpression proj)
            {
                this.Visit(proj.Projector);
                return proj;
            }
        }
    }

    /// <summary>
    /// Rewrite all column references to one or more aliases to a new single alias
    /// </summary>
    public class ColumnMapper : DbExpressionVisitor
    {
        HashSet<TableAlias> oldAliases;
        TableAlias newAlias;

        private ColumnMapper(IEnumerable<TableAlias> oldAliases, TableAlias newAlias)
        {
            this.oldAliases = new HashSet<TableAlias>(oldAliases);
            this.newAlias = newAlias;
        }

        public static Expression Map(Expression expression, TableAlias newAlias, IEnumerable<TableAlias> oldAliases)
        {
            return new ColumnMapper(oldAliases, newAlias).Visit(expression);
        }

        public static Expression Map(Expression expression, TableAlias newAlias, params TableAlias[] oldAliases)
        {
            return Map(expression, newAlias, (IEnumerable<TableAlias>)oldAliases);
        }

        protected override Expression VisitColumn(ColumnExpression column)
        {
            if (this.oldAliases.Contains(column.Alias))
            {
                return new ColumnExpression(column.Type, column.QueryType, this.newAlias, column.Name);
            }
            return column;
        }
    }

    public enum SubmitAction
    {
        None,
        Update,
        PossibleUpdate,
        Insert,
        InsertOrUpdate,
        Delete
    }

    public class EnumerateOnce<T> : IEnumerable<T>, IEnumerable
    {
        IEnumerable<T> enumerable;

        public EnumerateOnce(IEnumerable<T> enumerable)
        {
            this.enumerable = enumerable;
        }

        public IEnumerator<T> GetEnumerator()
        {
            var en = Interlocked.Exchange(ref enumerable, null);
            if (en != null)
            {
                return en.GetEnumerator();
            }
            throw new Exception("Enumerated more than once.");
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }

    /// <summary>
    /// Simple implementation of the <see cref="IGrouping{TKey, TElement}"/> interface
    /// </summary>
    public class Grouping<TKey, TElement> : IGrouping<TKey, TElement>
    {
        TKey key;
        IEnumerable<TElement> group;

        public Grouping(TKey key, IEnumerable<TElement> group)
        {
            this.key = key;
            this.group = group;
        }

        public TKey Key
        {
            get { return this.key; }
        }

        public IEnumerator<TElement> GetEnumerator()
        {
            if (!(group is List<TElement>))
                group = group.ToList();
            return this.group.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.group.GetEnumerator();
        }
    }

    /// <summary>
    /// Make a strongly-typed delegate to a weakly typed method (one that takes single object[] argument)
    /// (up to 8 arguments)
    /// </summary>
    public class StrongDelegate
    {
        Func<object[], object> fn;

        private StrongDelegate(Func<object[], object> fn)
        {
            this.fn = fn;
        }

        private static MethodInfo[] _meths;

        static StrongDelegate()
        {
            _meths = new MethodInfo[9];

            var meths = typeof(StrongDelegate).GetMethods();
            for (int i = 0, n = meths.Length; i < n; i++)
            {
                var gm = meths[i];
                if (gm.Name.StartsWith("M"))
                {
                    var tas = gm.GetGenericArguments();
                    _meths[tas.Length - 1] = gm;
                }
            }
        }

        /// <summary>
        /// Create a strongly typed delegate over a method with a weak signature
        /// </summary>
        /// <param name="delegateType">The strongly typed delegate's type</param>
        /// <param name="target"></param>
        /// <param name="method">Any method that takes a single array of objects and returns an object.</param>
        /// <returns></returns>
        public static Delegate CreateDelegate(Type delegateType, object target, MethodInfo method)
        {
            return CreateDelegate(delegateType, (Func<object[], object>)Delegate.CreateDelegate(typeof(Func<object[], object>), target, method));
        }

        /// <summary>
        /// Create a strongly typed delegate over a Func delegate with weak signature
        /// </summary>
        /// <param name="delegateType"></param>
        /// <param name="fn"></param>
        /// <returns></returns>
        public static Delegate CreateDelegate(Type delegateType, Func<object[], object> fn)
        {
            MethodInfo invoke = delegateType.GetMethod("Invoke");
            var parameters = invoke.GetParameters();
            Type[] typeArgs = new Type[1 + parameters.Length];
            for (int i = 0, n = parameters.Length; i < n; i++)
            {
                typeArgs[i] = parameters[i].ParameterType;
            }
            typeArgs[typeArgs.Length - 1] = invoke.ReturnType;
            if (typeArgs.Length <= _meths.Length)
            {
                var gm = _meths[typeArgs.Length - 1];
                var m = gm.MakeGenericMethod(typeArgs);
                return Delegate.CreateDelegate(delegateType, new StrongDelegate(fn), m);
            }
            throw new NotSupportedException("Delegate has too many arguments");
        }

        public R M<R>()
        {
            return (R)fn(null);
        }

        public R M<A1, R>(A1 a1)
        {
            return (R)fn(new object[] { a1 });
        }

        public R M<A1, A2, R>(A1 a1, A2 a2)
        {
            return (R)fn(new object[] { a1, a2 });
        }

        public R M<A1, A2, A3, R>(A1 a1, A2 a2, A3 a3)
        {
            return (R)fn(new object[] { a1, a2, a3 });
        }

        public R M<A1, A2, A3, A4, R>(A1 a1, A2 a2, A3 a3, A4 a4)
        {
            return (R)fn(new object[] { a1, a2, a3, a4 });
        }

        public R M<A1, A2, A3, A4, A5, R>(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5)
        {
            return (R)fn(new object[] { a1, a2, a3, a4, a5 });
        }

        public R M<A1, A2, A3, A4, A5, A6, R>(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6)
        {
            return (R)fn(new object[] { a1, a2, a3, a4, a5, a6 });
        }

        public R M<A1, A2, A3, A4, A5, A6, A7, R>(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7)
        {
            return (R)fn(new object[] { a1, a2, a3, a4, a5, a6, a7 });
        }

        public R M<A1, A2, A3, A4, A5, A6, A7, A8, R>(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8)
        {
            return (R)fn(new object[] { a1, a2, a3, a4, a5, a6, a7, a8 });
        }
    }

    public class ScopedDictionary<TKey, TValue>
    {
        ScopedDictionary<TKey, TValue> previous;
        Dictionary<TKey, TValue> map;

        public ScopedDictionary(ScopedDictionary<TKey, TValue> previous)
        {
            this.previous = previous;
            this.map = new Dictionary<TKey, TValue>();
        }

        public ScopedDictionary(ScopedDictionary<TKey, TValue> previous, IEnumerable<KeyValuePair<TKey, TValue>> pairs)
            : this(previous)
        {
            foreach (var p in pairs)
            {
                this.map.Add(p.Key, p.Value);
            }
        }

        public void Add(TKey key, TValue value)
        {
            this.map.Add(key, value);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            for (ScopedDictionary<TKey, TValue> scope = this; scope != null; scope = scope.previous)
            {
                if (scope.map.TryGetValue(key, out value))
                    return true;
            }
            value = default(TValue);
            return false;
        }

        public bool ContainsKey(TKey key)
        {
            for (ScopedDictionary<TKey, TValue> scope = this; scope != null; scope = scope.previous)
            {
                if (scope.map.ContainsKey(key))
                    return true;
            }
            return false;
        }
    }

    public static class TopologicalSorter
    {
        public static IEnumerable<T> Sort<T>(this IEnumerable<T> items, Func<T, IEnumerable<T>> fnItemsBeforeMe)
        {
            return Sort<T>(items, fnItemsBeforeMe, null);
        }

        public static IEnumerable<T> Sort<T>(this IEnumerable<T> items, Func<T, IEnumerable<T>> fnItemsBeforeMe, IEqualityComparer<T> comparer)
        {
            HashSet<T> seen = comparer != null ? new HashSet<T>(comparer) : new HashSet<T>();
            HashSet<T> done = comparer != null ? new HashSet<T>(comparer) : new HashSet<T>();
            List<T> result = new List<T>();
            foreach (var item in items)
            {
                SortItem(item, fnItemsBeforeMe, seen, done, result);
            }
            return result;
        }

        private static void SortItem<T>(T item, Func<T, IEnumerable<T>> fnItemsBeforeMe, HashSet<T> seen, HashSet<T> done, List<T> result)
        {
            if (!done.Contains(item))
            {
                if (seen.Contains(item))
                {
                    throw new InvalidOperationException("Cycle in topological sort");
                }
                seen.Add(item);
                var itemsBefore = fnItemsBeforeMe(item);
                if (itemsBefore != null)
                {
                    foreach (var itemBefore in itemsBefore)
                    {
                        SortItem(itemBefore, fnItemsBeforeMe, seen, done, result);
                    }
                }
                result.Add(item);
                done.Add(item);
            }
        }
    }

    public class CompoundKey : IEquatable<CompoundKey>, IEnumerable<object>, IEnumerable
    {
        object[] values;
        int hc;

        public CompoundKey(params object[] values)
        {
            this.values = values;
            for (int i = 0, n = values.Length; i < n; i++)
            {
                object value = values[i];
                if (value != null)
                {
                    hc ^= (value.GetHashCode() + i);
                }
            }
        }

        public override int GetHashCode()
        {
            return hc;
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public bool Equals(CompoundKey other)
        {
            if (other == null || other.values.Length != values.Length)
                return false;
            for (int i = 0, n = other.values.Length; i < n; i++)
            {
                if (!object.Equals(this.values[i], other.values[i]))
                    return false;
            }
            return true;
        }

        public IEnumerator<object> GetEnumerator()
        {
            return ((IEnumerable<object>)values).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }

    /// <summary>
    /// A list implementation that is loaded the first time the contents are examined
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DeferredList<T> : IDeferredList<T>, ICollection<T>, IEnumerable<T>, IList, ICollection, IEnumerable, IDeferLoadable
    {
        IEnumerable<T> source;
        List<T> values;

        public DeferredList(IEnumerable<T> source)
        {
            this.source = source;
        }

        public void Load()
        {
            this.values = new List<T>(this.source);
        }

        public bool IsLoaded
        {
            get { return this.values != null; }
        }

        private void Check()
        {
            if (!this.IsLoaded)
            {
                this.Load();
            }
        }

        #region IList<T> Members

        public int IndexOf(T item)
        {
            this.Check();
            return this.values.IndexOf(item);
        }

        public void Insert(int index, T item)
        {
            this.Check();
            this.values.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            this.Check();
            this.values.RemoveAt(index);
        }

        public T this[int index]
        {
            get
            {
                this.Check();
                return this.values[index];
            }
            set
            {
                this.Check();
                this.values[index] = value;
            }
        }

        #endregion

        #region ICollection<T> Members

        public void Add(T item)
        {
            this.Check();
            this.values.Add(item);
        }

        public void Clear()
        {
            this.Check();
            this.values.Clear();
        }

        public bool Contains(T item)
        {
            this.Check();
            return this.values.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            this.Check();
            this.values.CopyTo(array, arrayIndex);
        }

        public int Count
        {
            get { this.Check(); return this.values.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(T item)
        {
            this.Check();
            return this.values.Remove(item);
        }

        #endregion

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            this.Check();
            return this.values.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        #endregion

        #region IList Members

        public int Add(object value)
        {
            this.Check();
            return ((IList)this.values).Add(value);
        }

        public bool Contains(object value)
        {
            this.Check();
            return ((IList)this.values).Contains(value);
        }

        public int IndexOf(object value)
        {
            this.Check();
            return ((IList)this.values).IndexOf(value);
        }

        public void Insert(int index, object value)
        {
            this.Check();
            ((IList)this.values).Insert(index, value);
        }

        public bool IsFixedSize
        {
            get { return false; }
        }

        public void Remove(object value)
        {
            this.Check();
            ((IList)this.values).Remove(value);
        }

        object IList.this[int index]
        {
            get
            {
                this.Check();
                return ((IList)this.values)[index];
            }
            set
            {
                this.Check();
                ((IList)this.values)[index] = value;
            }
        }

        #endregion

        #region ICollection Members

        public void CopyTo(Array array, int index)
        {
            this.Check();
            ((IList)this.values).CopyTo(array, index);
        }

        public bool IsSynchronized
        {
            get { return false; }
        }

        public object SyncRoot
        {
            get { return null; }
        }

        #endregion
    }

    /// <summary>
    ///  returns the set of all aliases produced by a query source
    /// </summary>
    public class DeclaredAliasGatherer : DbExpressionVisitor
    {
        HashSet<TableAlias> aliases;

        private DeclaredAliasGatherer()
        {
            this.aliases = new HashSet<TableAlias>();
        }

        public static HashSet<TableAlias> Gather(Expression source)
        {
            var gatherer = new DeclaredAliasGatherer();
            gatherer.Visit(source);
            return gatherer.aliases;
        }

        protected override Expression VisitSelect(SelectExpression select)
        {
            this.aliases.Add(select.Alias);
            return select;
        }

        protected override Expression VisitTable(TableExpression table)
        {
            this.aliases.Add(table.Alias);
            return table;
        }
    }

    public static class Aggregator
    {
        /// <summary>
        /// Get a function that coerces a sequence of one type into another type.
        /// This is primarily used for aggregators stored in ProjectionExpression's, which are used to represent the 
        /// final transformation of the entire result set of a query.
        /// </summary>
        public static LambdaExpression GetAggregator(Type expectedType, Type actualType)
        {
            Type actualElementType = TypeHelper.GetElementType(actualType);
            if (!expectedType.IsAssignableFrom(actualType))
            {
                Type expectedElementType = TypeHelper.GetElementType(expectedType);
                ParameterExpression p = Expression.Parameter(actualType, "p");
                Expression body = null;
                if (expectedType.IsAssignableFrom(actualElementType))
                {
                    body = Expression.Call(typeof(Enumerable), "SingleOrDefault", new Type[] { actualElementType }, p);
                }
                else if (expectedType.IsGenericType &&
                    (expectedType == typeof(IQueryable) ||
                     expectedType == typeof(IOrderedQueryable) ||
                     expectedType.GetGenericTypeDefinition() == typeof(IQueryable<>) ||
                     expectedType.GetGenericTypeDefinition() == typeof(IOrderedQueryable<>)))
                {
                    body = Expression.Call(typeof(Queryable), "AsQueryable", new Type[] { expectedElementType }, CoerceElement(expectedElementType, p));
                    if (body.Type != expectedType)
                    {
                        body = Expression.Convert(body, expectedType);
                    }
                }
                else if (expectedType.IsArray && expectedType.GetArrayRank() == 1)
                {
                    body = Expression.Call(typeof(Enumerable), "ToArray", new Type[] { expectedElementType }, CoerceElement(expectedElementType, p));
                }
                else if (expectedType.IsGenericType && expectedType.GetGenericTypeDefinition().IsAssignableFrom(typeof(IList<>)))
                {
                    var gt = typeof(DeferredList<>).MakeGenericType(expectedType.GetGenericArguments());
                    var cn = gt.GetConstructor(new Type[] { typeof(IEnumerable<>).MakeGenericType(expectedType.GetGenericArguments()) });
                    body = Expression.New(cn, CoerceElement(expectedElementType, p));
                }
                else if (expectedType.IsAssignableFrom(typeof(List<>).MakeGenericType(actualElementType)))
                {
                    // List<T> can be assigned to expectedType
                    body = Expression.Call(typeof(Enumerable), "ToList", new Type[] { expectedElementType }, CoerceElement(expectedElementType, p));
                }
                else
                {
                    // some other collection type that has a constructor that takes IEnumerable<T>
                    ConstructorInfo ci = expectedType.GetConstructor(new Type[] { actualType });
                    if (ci != null)
                    {
                        body = Expression.New(ci, p);
                    }
                }
                if (body != null)
                {
                    return Expression.Lambda(body, p);
                }
            }
            return null;
        }

        private static Expression CoerceElement(Type expectedElementType, Expression expression)
        {
            Type elementType = TypeHelper.GetElementType(expression.Type);
            if (expectedElementType != elementType && (expectedElementType.IsAssignableFrom(elementType) || elementType.IsAssignableFrom(expectedElementType)))
            {
                return Expression.Call(typeof(Enumerable), "Cast", new Type[] { expectedElementType }, expression);
            }
            return expression;
        }
    }

    /// <summary>
    /// Implements a cache over a most recently used list
    /// </summary>
    public class MostRecentlyUsedCache<T>
    {
        private readonly int maxSize;
        private readonly List<T> list;
        private readonly Func<T, T, bool> fnEquals;
        private readonly ReaderWriterLockSlim rwlock;
        private int version;

        public MostRecentlyUsedCache(int maxSize)
            : this(maxSize, EqualityComparer<T>.Default)
        {
        }

        public MostRecentlyUsedCache(int maxSize, IEqualityComparer<T> comparer)
            : this(maxSize, (x, y) => comparer.Equals(x, y))
        {
        }

        public MostRecentlyUsedCache(int maxSize, Func<T, T, bool> fnEquals)
        {
            this.list = new List<T>();
            this.maxSize = maxSize;
            this.fnEquals = fnEquals;
            this.rwlock = new ReaderWriterLockSlim();
        }

        public int Count
        {
            get
            {
                this.rwlock.EnterReadLock();
                try
                {
                    return this.list.Count;
                }
                finally
                {
                    this.rwlock.ExitReadLock();
                }
            }
        }

        public void Clear()
        {
            this.rwlock.EnterWriteLock();
            try
            {
                this.list.Clear();
                this.version++;
            }
            finally
            {
                this.rwlock.ExitWriteLock();
            }
        }

        public bool Lookup(T item, bool add, out T cached)
        {
            cached = default(T);
            int cacheIndex = -1;

            rwlock.EnterReadLock();
            int version = this.version;
            try
            {
                this.FindCachedItem(item, out cached, out cacheIndex);
            }
            finally
            {
                rwlock.ExitReadLock();
            }

            // now update item in the list (only if we need to change its position or add it)
            if (cacheIndex != 0 && add)
            {
                rwlock.EnterWriteLock();
                try
                {
                    // if list has changed find it again
                    this.FindCachedItem(item, out cached, out cacheIndex);

                    if (cacheIndex == -1)
                    {
                        // this is first time in list, put at start
                        this.list.Insert(0, item);
                        cached = item;
                    }
                    else
                    {
                        if (cacheIndex > 0)
                        {
                            // if item is not at start, move it to the start
                            this.list.RemoveAt(cacheIndex);
                            this.list.Insert(0, item);
                        }
                    }

                    // drop any items beyond max
                    if (this.list.Count > this.maxSize)
                    {
                        this.list.RemoveAt(this.list.Count - 1);
                    }

                    this.version++;
                }
                finally
                {
                    rwlock.ExitWriteLock();
                }
            }

            return cacheIndex >= 0;
        }

        private void FindCachedItem(T item, out T cached, out int index)
        {
            for (int i = 0, n = this.list.Count; i < n; i++)
            {
                cached = this.list[i];

                if (fnEquals(cached, item))
                {
                    index = i;
                    return;
                }
            }

            cached = default(T);
            index = -1;
        }
    }


    /// <summary>
    /// Finds the first sub-expression that is of a specified type
    /// </summary>
    public class TypedSubtreeFinder : ExpressionVisitor
    {
        Expression root;
        Type type;

        private TypedSubtreeFinder(Type type)
        {
            this.type = type;
        }

        public static Expression Find(Expression expression, Type type)
        {
            TypedSubtreeFinder finder = new TypedSubtreeFinder(type);
            finder.Visit(expression);
            return finder.root;
        }

        protected override Expression Visit(Expression exp)
        {
            Expression result = base.Visit(exp);

            // remember the first sub-expression that produces an IQueryable
            if (this.root == null && result != null)
            {
                if (this.type.IsAssignableFrom(result.Type))
                    this.root = result;
            }

            return result;
        }
    }

    public static class TypeHelper
    {
        public static Type FindIEnumerable(Type seqType)
        {
            if (seqType == null || seqType == typeof(string))
                return null;
            if (seqType.IsArray)
                return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType());
            if (seqType.IsGenericType)
            {
                foreach (Type arg in seqType.GetGenericArguments())
                {
                    Type ienum = typeof(IEnumerable<>).MakeGenericType(arg);
                    if (ienum.IsAssignableFrom(seqType))
                    {
                        return ienum;
                    }
                }
            }
            Type[] ifaces = seqType.GetInterfaces();
            if (ifaces != null && ifaces.Length > 0)
            {
                foreach (Type iface in ifaces)
                {
                    Type ienum = FindIEnumerable(iface);
                    if (ienum != null) return ienum;
                }
            }
            if (seqType.BaseType != null && seqType.BaseType != typeof(object))
            {
                return FindIEnumerable(seqType.BaseType);
            }
            return null;
        }

        public static Type GetSequenceType(Type elementType)
        {
            return typeof(IEnumerable<>).MakeGenericType(elementType);
        }

        public static Type GetElementType(Type seqType)
        {
            Type ienum = FindIEnumerable(seqType);
            if (ienum == null) return seqType;
            return ienum.GetGenericArguments()[0];
        }

        public static bool IsNullableType(Type type)
        {
            return type != null && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        public static bool IsNullAssignable(Type type)
        {
            return !type.IsValueType || IsNullableType(type);
        }

        public static Type GetNonNullableType(Type type)
        {
            if (IsNullableType(type))
            {
                return type.GetGenericArguments()[0];
            }
            return type;
        }

        public static Type GetNullAssignableType(Type type)
        {
            if (!IsNullAssignable(type))
            {
                return typeof(Nullable<>).MakeGenericType(type);
            }
            return type;
        }

        public static ConstantExpression GetNullConstant(Type type)
        {
            return Expression.Constant(null, GetNullAssignableType(type));
        }

        public static Type GetMemberType(MemberInfo mi)
        {
            FieldInfo fi = mi as FieldInfo;
            if (fi != null) return fi.FieldType;
            PropertyInfo pi = mi as PropertyInfo;
            if (pi != null) return pi.PropertyType;
            EventInfo ei = mi as EventInfo;
            if (ei != null) return ei.EventHandlerType;
            MethodInfo meth = mi as MethodInfo;  // property getters really
            if (meth != null) return meth.ReturnType;
            return null;
        }

        public static object GetDefault(Type type)
        {
            bool isNullable = !type.IsValueType || TypeHelper.IsNullableType(type);
            if (!isNullable)
                return Activator.CreateInstance(type);
            return null;
        }

        public static bool IsReadOnly(MemberInfo member)
        {
            switch (member.MemberType)
            {
                case MemberTypes.Field:
                    return (((FieldInfo)member).Attributes & FieldAttributes.InitOnly) != 0;
                case MemberTypes.Property:
                    PropertyInfo pi = (PropertyInfo)member;
                    return !pi.CanWrite || pi.GetSetMethod() == null;
                default:
                    return true;
            }
        }

        public static bool IsInteger(Type type)
        {
            Type nnType = GetNonNullableType(type);
            switch (Type.GetTypeCode(nnType))
            {
                case TypeCode.SByte:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Byte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    return true;
                default:
                    return false;
            }
        }
    }
}
