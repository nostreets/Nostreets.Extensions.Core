using System.Linq.Expressions;
using System;
using System.Collections.Generic;

namespace Nostreets.Extensions.Interfaces
{
    public interface IDBService
    {
        Task<List<object>> GetAll();
        Task<object> Get(object id);
        Task<object> Get(object id, Converter<object, object> converter);

        Task Insert(object model);
        Task Insert(object model, Converter<object, object> converter);
        Task InsertRange(IEnumerable<object> collection);
        Task InsertRange(IEnumerable<object> collection, Converter<object, object> converter);

        Task Update(object model);
        Task Update(object model, Converter<object, object> converter);
        Task UpdateRange(IEnumerable<object> collection);
        Task UpdateRange(IEnumerable<object> collection, Converter<object, object> converter);

        Task Delete(object id);
        Task DeleteRange(IEnumerable<object> ids);

        Task<List<object>> Where(Func<object, bool> predicate);
        Task<List<object>> Where(Func<object, bool> predicate, int pageSize, int pageOffset, string orderByKey = null, bool desc = false, IComparer<object> comparer = null);
        
        Task<object> FirstOrDefault(Func<object, bool> predicate);
        Task<List<dynamic>> QueryResults(string query, Dictionary<string, object> parameters = null);
    }


    public interface IDBService<T>
    {
        Task<List<T>> GetAll();
        Task<T> Get(object id);
        Task<T> Get(object id, Converter<T, T> converter);

        Task Insert(T model);
        Task Insert(T model, Converter<T, T> converter);
        Task InsertRange(IEnumerable<T> collection);
        Task InsertRange(IEnumerable<object> collection);
        Task InsertRange(IEnumerable<T> collection, Converter<T, T> converter);
        Task InsertRange(IEnumerable<object> collection, Converter<T, T> converter);
        
        Task Update(T model);
        Task Update(T model, Converter<T, T> converter);
        Task UpdateRange(IEnumerable<T> collection);
        Task UpdateRange(IEnumerable<object> collection);
        Task UpdateRange(IEnumerable<T> collection, Converter<T, T> converter);
        Task UpdateRange(IEnumerable<object> collection, Converter<T, T> converter);

        Task Delete(object id);

        /// <summary>
        /// Deletes the row if it is there; returns <c>false</c> instead of throwing when it is not.
        /// </summary>
        /// <remarks>
        /// Exists so <see cref="Delete(object)"/> can keep its STRICT contract. A delete for an id
        /// that does not exist means the caller's picture of the world is wrong, and for ordinary
        /// callers that should be loud — silently succeeding turns a typo'd id into a no-op.
        ///
        /// Rollback/compensation is the one caller that legitimately needs the opposite. A
        /// compensating delete may run twice: a rollback that got halfway and was retried, a restart
        /// finishing a rollback a dead process began, or two compensation paths racing the same
        /// aggregate. Under the strict contract the second run throws on rows the first already
        /// removed and can never finish — and the natural "fix" is to wrap it in a swallowing
        /// try/catch, which is exactly how <c>AppUserService.RollbackNewUserAsync</c> became dead
        /// code: its empty catch hides the throw, so the rollback fails silently precisely when it
        /// is needed.
        ///
        /// So the semantic is chosen at the CALL SITE rather than globally: ordinary code keeps
        /// strict deletes, compensation gets idempotency, and no existing caller changes behaviour.
        /// </remarks>
        /// <returns><c>true</c> if a row was found and deleted; <c>false</c> if none matched.</returns>
        Task<bool> DeleteIfExists(object id);

        Task DeleteRange(IEnumerable<object> ids);

        Task<List<T>> Where(Func<T, bool> predicate);
        Task<List<T>> Where(Func<T, bool> predicate, int pageSize, int pageOffset, string orderByKey = null, bool desc = false, IComparer<object> comparer = null);

        Task<T> FirstOrDefault(Func<T, bool> predicate);
        Task<int> Count(Func<T, bool> predicate = null);
        Task Backup(string disk = null);
        Task<List<TResult>> QueryResults<TResult>(string query, Dictionary<string, object> parameters = null);

        /// <summary>
        /// Runs raw parameterized SQL and materializes the rows as <typeparamref name="T"/> entities.
        /// The escape hatch for predicates the ORM cannot express — <see cref="Where(Func{T, bool})"/>
        /// takes a <c>Func</c>, so it filters IN MEMORY after loading every row. Use this when the
        /// filter must run in the database.
        /// </summary>
        /// <remarks>
        /// 🔴 The SQL MUST project EVERY mapped column of <typeparamref name="T"/>. A partial
        /// <c>SELECT</c> fails at materialization, not at compile time.
        /// 🔴 Pass values via <paramref name="parameters"/>. NEVER interpolate them into
        /// <paramref name="sql"/> — that is an injection hole, and this method cannot detect it.
        /// </remarks>

        /// <summary>
        /// Runs <paramref name="predicate"/> AS SQL. The generic-parameter difference from
        /// <see cref="Where(Func{T, bool})"/> is the whole point: <c>Queryable.Where</c> requires an
        /// <c>Expression</c>, and given a plain <c>Func</c> the compiler binds to <c>Enumerable.Where</c>
        /// instead — which enumerates the table, so EF issues <c>SELECT *</c> and filters in memory
        /// however simple the predicate is.
        /// </summary>
        /// <remarks>
        /// 🔑 Use this for ORDINARY predicates — comparisons, <c>&amp;&amp;</c>/<c>||</c>, <c>Contains</c>
        /// over a local collection (becomes <c>IN</c>), null checks. This is the default worth reaching
        /// for; <see cref="Where(Func{T, bool})"/> is the fallback, not the other way round.
        /// 🔴 It THROWS where the <c>Func</c> version silently succeeded. Anything EF cannot translate —
        /// a JSON deserialize, a custom helper, a C# method call over a column — raises
        /// <c>InvalidOperationException</c> rather than quietly running in memory. That is deliberate: a
        /// loud failure beats a hidden full-table scan. When a predicate genuinely cannot be expressed in
        /// SQL, drop to <see cref="WhereRaw"/>.
        /// ⚠️ A separate METHOD rather than an overload of <c>Where</c> on purpose. C# overload
        /// resolution prefers a delegate over an expression tree for a lambda literal, so an added
        /// overload would never be selected by existing call sites — and silently rebinding those ~197
        /// sites would turn every untranslatable predicate into a runtime throw.
        /// </remarks>
        Task<List<T>> WhereQueryable(Expression<Func<T, bool>> predicate);

        /// <summary>
        /// Paged counterpart to <see cref="WhereQueryable(Expression{Func{T, bool}})"/> — filters, orders
        /// and pages IN THE DATABASE, so only the requested rows cross the wire.
        /// </summary>
        /// <param name="pageOffset">
        /// A RAW ROW OFFSET, not a page index — the same contract as the <c>Func</c> overload (callers
        /// compute <c>PageIndex * PageSize</c> themselves).
        /// </param>
        /// <param name="orderByKey">
        /// Property NAME, translated via <c>EF.Property</c>. An unknown or blank key leaves the query
        /// unordered — and SQL Server does not guarantee row order without an ORDER BY, so a paged read
        /// with no key can return overlapping pages.
        /// </param>
        Task<List<T>> WhereQueryable(Expression<Func<T, bool>> predicate, int pageSize, int pageOffset,
                                     string orderByKey = null, bool desc = false);

        /// <summary>
        /// <c>COUNT(*)</c> with the filter in SQL. A null predicate counts every row.
        /// <para>
        /// Pairs with <see cref="WhereQueryable(Expression{Func{T, bool}}, int, int, string, bool)"/> for a
        /// paged read's total. The <c>Func</c> equivalents cost TWO full table materialisations per paged
        /// read — one to count, one to page.
        /// </para>
        /// </summary>
        Task<int> CountQueryable(Expression<Func<T, bool>> predicate = null);

        /// <summary>
        /// First match, as <c>TOP(1)</c> in SQL.
        /// <para>
        /// 🔴 Not equivalent to <see cref="FirstOrDefault(Func{T, bool})"/>, which is implemented as
        /// <c>(await Where(predicate)).FirstOrDefault()</c> — it materialises the ENTIRE filtered set to
        /// take one row.
        /// </para>
        /// </summary>
        Task<T> FirstOrDefaultQueryable(Expression<Func<T, bool>> predicate);

        Task<List<T>> WhereRaw(string sql, Dictionary<string, object> parameters = null);
    }

    public interface IDBService<T, IdType>
    {
        Task<List<T>> GetAll();
        Task<T> Get(IdType id);
        Task<T> Get(IdType id, Converter<T, T> converter);

        Task Insert(T model);
        Task Insert(T model, Converter<T, T> converter);
        Task InsertRange(IEnumerable<T> collection);
        Task InsertRange(IEnumerable<object> collection);
        Task InsertRange(IEnumerable<T> collection, Converter<T, T> converter);
        Task InsertRange(IEnumerable<object> collection, Converter<T, T> converter);

        Task Update(T model);
        Task Update(T model, Converter<T, T> converter);
        Task UpdateRange(IEnumerable<T> collection);
        Task UpdateRange(IEnumerable<object> collection);
        Task UpdateRange(IEnumerable<T> collection, Converter<T, T> converter);
        Task UpdateRange(IEnumerable<object> collection, Converter<T, T> converter);

        Task Delete(IdType id);

        /// <summary>
        /// Deletes the row if it is there; returns <c>false</c> instead of throwing when it is not.
        /// Lets <see cref="Delete(IdType)"/> keep its STRICT contract while rollback/compensation —
        /// the one caller that must survive running twice — gets idempotency. See the
        /// <c>IDBService&lt;T&gt;</c> overload for the full rationale.
        /// </summary>
        /// <returns><c>true</c> if a row was found and deleted; <c>false</c> if none matched.</returns>
        Task<bool> DeleteIfExists(IdType id);

        Task DeleteRange(IEnumerable<IdType> ids);

        Task<List<T>> Where(Func<T, bool> predicate);
        Task<List<T>> Where(Func<T, bool> predicate, int pageSize, int pageOffset, string orderByKey = null, bool desc = false, IComparer<object> comparer = null);

        Task<T> FirstOrDefault(Func<T, bool> predicate);
        Task<int> Count(Func<T, bool> predicate = null);
        Task Backup(string disk = null);
        Task<List<TResult>> QueryResults<TResult>(string query, Dictionary<string, object> parameters = null);

        /// <summary>
        /// Runs raw parameterized SQL and materializes the rows as <typeparamref name="T"/> entities.
        /// The escape hatch for predicates the ORM cannot express — <see cref="Where(Func{T, bool})"/>
        /// takes a <c>Func</c>, so it filters IN MEMORY after loading every row. Use this when the
        /// filter must run in the database.
        /// </summary>
        /// <remarks>
        /// 🔴 The SQL MUST project EVERY mapped column of <typeparamref name="T"/>. A partial
        /// <c>SELECT</c> fails at materialization, not at compile time.
        /// 🔴 Pass values via <paramref name="parameters"/>. NEVER interpolate them into
        /// <paramref name="sql"/> — that is an injection hole, and this method cannot detect it.
        /// </remarks>

        /// <summary>
        /// Runs <paramref name="predicate"/> AS SQL. The generic-parameter difference from
        /// <see cref="Where(Func{T, bool})"/> is the whole point: <c>Queryable.Where</c> requires an
        /// <c>Expression</c>, and given a plain <c>Func</c> the compiler binds to <c>Enumerable.Where</c>
        /// instead — which enumerates the table, so EF issues <c>SELECT *</c> and filters in memory
        /// however simple the predicate is.
        /// </summary>
        /// <remarks>
        /// 🔑 Use this for ORDINARY predicates — comparisons, <c>&amp;&amp;</c>/<c>||</c>, <c>Contains</c>
        /// over a local collection (becomes <c>IN</c>), null checks. This is the default worth reaching
        /// for; <see cref="Where(Func{T, bool})"/> is the fallback, not the other way round.
        /// 🔴 It THROWS where the <c>Func</c> version silently succeeded. Anything EF cannot translate —
        /// a JSON deserialize, a custom helper, a C# method call over a column — raises
        /// <c>InvalidOperationException</c> rather than quietly running in memory. That is deliberate: a
        /// loud failure beats a hidden full-table scan. When a predicate genuinely cannot be expressed in
        /// SQL, drop to <see cref="WhereRaw"/>.
        /// ⚠️ A separate METHOD rather than an overload of <c>Where</c> on purpose. C# overload
        /// resolution prefers a delegate over an expression tree for a lambda literal, so an added
        /// overload would never be selected by existing call sites — and silently rebinding those ~197
        /// sites would turn every untranslatable predicate into a runtime throw.
        /// </remarks>
        Task<List<T>> WhereQueryable(Expression<Func<T, bool>> predicate);

        /// <summary>
        /// Paged counterpart to <see cref="WhereQueryable(Expression{Func{T, bool}})"/> — filters, orders
        /// and pages IN THE DATABASE, so only the requested rows cross the wire.
        /// </summary>
        /// <param name="pageOffset">
        /// A RAW ROW OFFSET, not a page index — the same contract as the <c>Func</c> overload (callers
        /// compute <c>PageIndex * PageSize</c> themselves).
        /// </param>
        /// <param name="orderByKey">
        /// Property NAME, translated via <c>EF.Property</c>. An unknown or blank key leaves the query
        /// unordered — and SQL Server does not guarantee row order without an ORDER BY, so a paged read
        /// with no key can return overlapping pages.
        /// </param>
        Task<List<T>> WhereQueryable(Expression<Func<T, bool>> predicate, int pageSize, int pageOffset,
                                     string orderByKey = null, bool desc = false);

        /// <summary>
        /// <c>COUNT(*)</c> with the filter in SQL. A null predicate counts every row.
        /// <para>
        /// Pairs with <see cref="WhereQueryable(Expression{Func{T, bool}}, int, int, string, bool)"/> for a
        /// paged read's total. The <c>Func</c> equivalents cost TWO full table materialisations per paged
        /// read — one to count, one to page.
        /// </para>
        /// </summary>
        Task<int> CountQueryable(Expression<Func<T, bool>> predicate = null);

        /// <summary>
        /// First match, as <c>TOP(1)</c> in SQL.
        /// <para>
        /// 🔴 Not equivalent to <see cref="FirstOrDefault(Func{T, bool})"/>, which is implemented as
        /// <c>(await Where(predicate)).FirstOrDefault()</c> — it materialises the ENTIRE filtered set to
        /// take one row.
        /// </para>
        /// </summary>
        Task<T> FirstOrDefaultQueryable(Expression<Func<T, bool>> predicate);

        Task<List<T>> WhereRaw(string sql, Dictionary<string, object> parameters = null);
    }

    public interface IDBService<T, IdType, AddType, UpdateType>
    {
        Task<List<T>> GetAll();
        Task<T> Get(IdType id);
        Task<T> Get(IdType id, Converter<T, T> converter);

        Task Insert(T model);
        Task Insert(T model, Converter<T, T> converter);
        Task Insert(AddType model, Converter<AddType, T> converter);
        Task InsertRange(IEnumerable<T> collection);
        Task InsertRange(IEnumerable<object> collection);
        Task InsertRange(IEnumerable<T> collection, Converter<T, T> converter);
        Task InsertRange(IEnumerable<object> collection, Converter<T, T> converter);

        Task Update(UpdateType model, Converter<UpdateType, T> converter);
        Task Update(T model);
        Task Update(T model, Converter<T, T> converter);
        Task UpdateRange(IEnumerable<T> collection);
        Task UpdateRange(IEnumerable<object> collection);
        Task UpdateRange(IEnumerable<T> collection, Converter<T, T> converter);
        Task UpdateRange(IEnumerable<object> collection, Converter<T, T> converter);

        Task Delete(IdType id);

        /// <summary>
        /// Deletes the row if it is there; returns <c>false</c> instead of throwing when it is not.
        /// Lets <see cref="Delete(IdType)"/> keep its STRICT contract while rollback/compensation —
        /// the one caller that must survive running twice — gets idempotency. See the
        /// <c>IDBService&lt;T&gt;</c> overload for the full rationale.
        /// </summary>
        /// <returns><c>true</c> if a row was found and deleted; <c>false</c> if none matched.</returns>
        Task<bool> DeleteIfExists(IdType id);

        Task DeleteRange(IEnumerable<IdType> ids);

        Task<List<T>> Where(Func<T, bool> predicate);
        Task<List<T>> Where(Func<T, bool> predicate, int pageSize, int pageOffset, string orderByKey = null, bool desc = false, IComparer<object> comparer = null);

        Task<T> FirstOrDefault(Func<T, bool> predicate);
        Task<int> Count(Func<T, bool> predicate = null);
        Task Backup(string disk = null);
        Task<List<TResult>> QueryResults<TResult>(string query, Dictionary<string, object> parameters = null);

        /// <summary>
        /// Runs raw parameterized SQL and materializes the rows as <typeparamref name="T"/> entities.
        /// The escape hatch for predicates the ORM cannot express — <see cref="Where(Func{T, bool})"/>
        /// takes a <c>Func</c>, so it filters IN MEMORY after loading every row. Use this when the
        /// filter must run in the database.
        /// </summary>
        /// <remarks>
        /// 🔴 The SQL MUST project EVERY mapped column of <typeparamref name="T"/>. A partial
        /// <c>SELECT</c> fails at materialization, not at compile time.
        /// 🔴 Pass values via <paramref name="parameters"/>. NEVER interpolate them into
        /// <paramref name="sql"/> — that is an injection hole, and this method cannot detect it.
        /// </remarks>

        /// <summary>
        /// Runs <paramref name="predicate"/> AS SQL. The generic-parameter difference from
        /// <see cref="Where(Func{T, bool})"/> is the whole point: <c>Queryable.Where</c> requires an
        /// <c>Expression</c>, and given a plain <c>Func</c> the compiler binds to <c>Enumerable.Where</c>
        /// instead — which enumerates the table, so EF issues <c>SELECT *</c> and filters in memory
        /// however simple the predicate is.
        /// </summary>
        /// <remarks>
        /// 🔑 Use this for ORDINARY predicates — comparisons, <c>&amp;&amp;</c>/<c>||</c>, <c>Contains</c>
        /// over a local collection (becomes <c>IN</c>), null checks. This is the default worth reaching
        /// for; <see cref="Where(Func{T, bool})"/> is the fallback, not the other way round.
        /// 🔴 It THROWS where the <c>Func</c> version silently succeeded. Anything EF cannot translate —
        /// a JSON deserialize, a custom helper, a C# method call over a column — raises
        /// <c>InvalidOperationException</c> rather than quietly running in memory. That is deliberate: a
        /// loud failure beats a hidden full-table scan. When a predicate genuinely cannot be expressed in
        /// SQL, drop to <see cref="WhereRaw"/>.
        /// ⚠️ A separate METHOD rather than an overload of <c>Where</c> on purpose. C# overload
        /// resolution prefers a delegate over an expression tree for a lambda literal, so an added
        /// overload would never be selected by existing call sites — and silently rebinding those ~197
        /// sites would turn every untranslatable predicate into a runtime throw.
        /// </remarks>
        Task<List<T>> WhereQueryable(Expression<Func<T, bool>> predicate);

        /// <summary>
        /// Paged counterpart to <see cref="WhereQueryable(Expression{Func{T, bool}})"/> — filters, orders
        /// and pages IN THE DATABASE, so only the requested rows cross the wire.
        /// </summary>
        /// <param name="pageOffset">
        /// A RAW ROW OFFSET, not a page index — the same contract as the <c>Func</c> overload (callers
        /// compute <c>PageIndex * PageSize</c> themselves).
        /// </param>
        /// <param name="orderByKey">
        /// Property NAME, translated via <c>EF.Property</c>. An unknown or blank key leaves the query
        /// unordered — and SQL Server does not guarantee row order without an ORDER BY, so a paged read
        /// with no key can return overlapping pages.
        /// </param>
        Task<List<T>> WhereQueryable(Expression<Func<T, bool>> predicate, int pageSize, int pageOffset,
                                     string orderByKey = null, bool desc = false);

        /// <summary>
        /// <c>COUNT(*)</c> with the filter in SQL. A null predicate counts every row.
        /// <para>
        /// Pairs with <see cref="WhereQueryable(Expression{Func{T, bool}}, int, int, string, bool)"/> for a
        /// paged read's total. The <c>Func</c> equivalents cost TWO full table materialisations per paged
        /// read — one to count, one to page.
        /// </para>
        /// </summary>
        Task<int> CountQueryable(Expression<Func<T, bool>> predicate = null);

        /// <summary>
        /// First match, as <c>TOP(1)</c> in SQL.
        /// <para>
        /// 🔴 Not equivalent to <see cref="FirstOrDefault(Func{T, bool})"/>, which is implemented as
        /// <c>(await Where(predicate)).FirstOrDefault()</c> — it materialises the ENTIRE filtered set to
        /// take one row.
        /// </para>
        /// </summary>
        Task<T> FirstOrDefaultQueryable(Expression<Func<T, bool>> predicate);

        Task<List<T>> WhereRaw(string sql, Dictionary<string, object> parameters = null);
    }

    public interface IBasicService
    {
        Task<object> Get(string id);
        Task Insert(object model);
        Task Update(object model);
        Task Delete(string id);
    }
}
