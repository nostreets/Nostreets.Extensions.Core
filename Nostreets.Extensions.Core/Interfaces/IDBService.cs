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

        /// <summary>
        /// Filters the table. <b>Tries SQL first, falls back to in-memory only if EF cannot translate
        /// the expression.</b>
        /// </summary>
        /// <remarks>
        /// 🔑 The parameter is <c>Expression&lt;Func&lt;T,bool&gt;&gt;</c>, not <c>Func&lt;T,bool&gt;</c>,
        /// and that single difference is what makes the SQL path possible at all. <c>Queryable.Where</c>
        /// requires an expression tree; given a plain <c>Func</c> the compiler binds to
        /// <c>Enumerable.Where</c>, which enumerates the table — so EF issues <c>SELECT *</c> and filters
        /// client-side however simple the predicate is. A delegate cannot be turned back into a tree
        /// (the tree is discarded when the lambda is compiled at the call site), so the type has to
        /// carry it.
        /// <para>
        /// ✅ <b>Call sites do not change.</b> A lambda literal converts to either form, so
        /// <c>Where(a =&gt; !a.IsArchived)</c> compiles exactly as before and now runs in the database.
        /// Only a caller passing an already-compiled <c>Func</c> variable needs an edit, and the
        /// compiler finds every one of those.
        /// </para>
        /// <para>
        /// 🔴 <b>The fallback is a correctness net, not a performance one.</b> When EF reports the
        /// expression cannot be translated, the predicate is compiled and applied in memory — the old
        /// behaviour, so nothing that worked before stops working. It is silent by design, but it means
        /// a predicate calling a C# helper still costs a full table scan. If a read matters, verify it
        /// actually translated rather than assuming.
        /// </para>
        /// <para>
        /// 🔴 <b>The fallback is NOT semantically transparent, and string comparison is where it
        /// bites.</b> In SQL, <c>=</c> and <c>IN</c> use the column's collation —
        /// <c>SQL_Latin1_General_CP1_CI_AS</c> across this estate, i.e. case-INsensitive. In memory,
        /// .NET string comparison is ORDINAL, i.e. case-SENSITIVE. So the same predicate can return
        /// DIFFERENT ROWS depending on which path ran, and the fallback is the direction that silently
        /// narrows the match.
        /// <para>
        /// That is not theoretical. Rewriting the BUG-67 email-uniqueness guard as
        /// <c>Count(a =&gt; candidates.Contains(a.Email))</c> is correct in SQL and WRONG in memory —
        /// it would report <c>Bob@x.com</c> as available while <c>bob@x.com</c> is live, i.e. a
        /// duplicate live address, which is the exact harm that guard exists to prevent. It stays on
        /// <see cref="WhereRaw"/> for that reason, not because it is complex.
        /// </para>
        /// <para>
        /// 🔑 <b>The rule:</b> if a predicate's CORRECTNESS depends on SQL collation rather than merely
        /// its speed, do not let it be fallback-eligible — put it in <see cref="WhereRaw"/>, where
        /// there is only one path.
        /// </para>
        /// </para>
        /// </remarks>
        Task<List<T>> Where(Expression<Func<T, bool>> predicate);
        /// <summary>
        /// Paged filter. Filters, orders and pages IN THE DATABASE when the expression translates.
        /// <para>
        /// ⚠️ Passing <paramref name="comparer"/> forces the in-memory path — a .NET
        /// <c>IComparer</c> has no SQL equivalent, so ordering by it cannot be translated. Order by
        /// <paramref name="orderByKey"/> alone to stay in the database.
        /// </para>
        /// </summary>
        /// <param name="pageOffset">A RAW ROW OFFSET, not a page index.</param>
        Task<List<T>> Where(Expression<Func<T, bool>> predicate, int pageSize, int pageOffset, string orderByKey = null, bool desc = false, IComparer<object> comparer = null);

        /// <summary>
        /// First match. Emits <c>TOP(1)</c> when the expression translates, instead of
        /// materialising the entire filtered set to take one row.
        /// </summary>
        Task<T> FirstOrDefault(Expression<Func<T, bool>> predicate);
        /// <summary><c>COUNT(*)</c> in SQL when the expression translates.</summary>
        Task<int> Count(Expression<Func<T, bool>> predicate = null);
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

        /// <summary>
        /// Filters the table. <b>Tries SQL first, falls back to in-memory only if EF cannot translate
        /// the expression.</b>
        /// </summary>
        /// <remarks>
        /// 🔑 The parameter is <c>Expression&lt;Func&lt;T,bool&gt;&gt;</c>, not <c>Func&lt;T,bool&gt;</c>,
        /// and that single difference is what makes the SQL path possible at all. <c>Queryable.Where</c>
        /// requires an expression tree; given a plain <c>Func</c> the compiler binds to
        /// <c>Enumerable.Where</c>, which enumerates the table — so EF issues <c>SELECT *</c> and filters
        /// client-side however simple the predicate is. A delegate cannot be turned back into a tree
        /// (the tree is discarded when the lambda is compiled at the call site), so the type has to
        /// carry it.
        /// <para>
        /// ✅ <b>Call sites do not change.</b> A lambda literal converts to either form, so
        /// <c>Where(a =&gt; !a.IsArchived)</c> compiles exactly as before and now runs in the database.
        /// Only a caller passing an already-compiled <c>Func</c> variable needs an edit, and the
        /// compiler finds every one of those.
        /// </para>
        /// <para>
        /// 🔴 <b>The fallback is a correctness net, not a performance one.</b> When EF reports the
        /// expression cannot be translated, the predicate is compiled and applied in memory — the old
        /// behaviour, so nothing that worked before stops working. It is silent by design, but it means
        /// a predicate calling a C# helper still costs a full table scan. If a read matters, verify it
        /// actually translated rather than assuming.
        /// </para>
        /// <para>
        /// 🔴 <b>The fallback is NOT semantically transparent, and string comparison is where it
        /// bites.</b> In SQL, <c>=</c> and <c>IN</c> use the column's collation —
        /// <c>SQL_Latin1_General_CP1_CI_AS</c> across this estate, i.e. case-INsensitive. In memory,
        /// .NET string comparison is ORDINAL, i.e. case-SENSITIVE. So the same predicate can return
        /// DIFFERENT ROWS depending on which path ran, and the fallback is the direction that silently
        /// narrows the match.
        /// <para>
        /// That is not theoretical. Rewriting the BUG-67 email-uniqueness guard as
        /// <c>Count(a =&gt; candidates.Contains(a.Email))</c> is correct in SQL and WRONG in memory —
        /// it would report <c>Bob@x.com</c> as available while <c>bob@x.com</c> is live, i.e. a
        /// duplicate live address, which is the exact harm that guard exists to prevent. It stays on
        /// <see cref="WhereRaw"/> for that reason, not because it is complex.
        /// </para>
        /// <para>
        /// 🔑 <b>The rule:</b> if a predicate's CORRECTNESS depends on SQL collation rather than merely
        /// its speed, do not let it be fallback-eligible — put it in <see cref="WhereRaw"/>, where
        /// there is only one path.
        /// </para>
        /// </para>
        /// </remarks>
        Task<List<T>> Where(Expression<Func<T, bool>> predicate);
        /// <summary>
        /// Paged filter. Filters, orders and pages IN THE DATABASE when the expression translates.
        /// <para>
        /// ⚠️ Passing <paramref name="comparer"/> forces the in-memory path — a .NET
        /// <c>IComparer</c> has no SQL equivalent, so ordering by it cannot be translated. Order by
        /// <paramref name="orderByKey"/> alone to stay in the database.
        /// </para>
        /// </summary>
        /// <param name="pageOffset">A RAW ROW OFFSET, not a page index.</param>
        Task<List<T>> Where(Expression<Func<T, bool>> predicate, int pageSize, int pageOffset, string orderByKey = null, bool desc = false, IComparer<object> comparer = null);

        /// <summary>
        /// First match. Emits <c>TOP(1)</c> when the expression translates, instead of
        /// materialising the entire filtered set to take one row.
        /// </summary>
        Task<T> FirstOrDefault(Expression<Func<T, bool>> predicate);
        /// <summary><c>COUNT(*)</c> in SQL when the expression translates.</summary>
        Task<int> Count(Expression<Func<T, bool>> predicate = null);
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

        /// <summary>
        /// Filters the table. <b>Tries SQL first, falls back to in-memory only if EF cannot translate
        /// the expression.</b>
        /// </summary>
        /// <remarks>
        /// 🔑 The parameter is <c>Expression&lt;Func&lt;T,bool&gt;&gt;</c>, not <c>Func&lt;T,bool&gt;</c>,
        /// and that single difference is what makes the SQL path possible at all. <c>Queryable.Where</c>
        /// requires an expression tree; given a plain <c>Func</c> the compiler binds to
        /// <c>Enumerable.Where</c>, which enumerates the table — so EF issues <c>SELECT *</c> and filters
        /// client-side however simple the predicate is. A delegate cannot be turned back into a tree
        /// (the tree is discarded when the lambda is compiled at the call site), so the type has to
        /// carry it.
        /// <para>
        /// ✅ <b>Call sites do not change.</b> A lambda literal converts to either form, so
        /// <c>Where(a =&gt; !a.IsArchived)</c> compiles exactly as before and now runs in the database.
        /// Only a caller passing an already-compiled <c>Func</c> variable needs an edit, and the
        /// compiler finds every one of those.
        /// </para>
        /// <para>
        /// 🔴 <b>The fallback is a correctness net, not a performance one.</b> When EF reports the
        /// expression cannot be translated, the predicate is compiled and applied in memory — the old
        /// behaviour, so nothing that worked before stops working. It is silent by design, but it means
        /// a predicate calling a C# helper still costs a full table scan. If a read matters, verify it
        /// actually translated rather than assuming.
        /// </para>
        /// <para>
        /// 🔴 <b>The fallback is NOT semantically transparent, and string comparison is where it
        /// bites.</b> In SQL, <c>=</c> and <c>IN</c> use the column's collation —
        /// <c>SQL_Latin1_General_CP1_CI_AS</c> across this estate, i.e. case-INsensitive. In memory,
        /// .NET string comparison is ORDINAL, i.e. case-SENSITIVE. So the same predicate can return
        /// DIFFERENT ROWS depending on which path ran, and the fallback is the direction that silently
        /// narrows the match.
        /// <para>
        /// That is not theoretical. Rewriting the BUG-67 email-uniqueness guard as
        /// <c>Count(a =&gt; candidates.Contains(a.Email))</c> is correct in SQL and WRONG in memory —
        /// it would report <c>Bob@x.com</c> as available while <c>bob@x.com</c> is live, i.e. a
        /// duplicate live address, which is the exact harm that guard exists to prevent. It stays on
        /// <see cref="WhereRaw"/> for that reason, not because it is complex.
        /// </para>
        /// <para>
        /// 🔑 <b>The rule:</b> if a predicate's CORRECTNESS depends on SQL collation rather than merely
        /// its speed, do not let it be fallback-eligible — put it in <see cref="WhereRaw"/>, where
        /// there is only one path.
        /// </para>
        /// </para>
        /// </remarks>
        Task<List<T>> Where(Expression<Func<T, bool>> predicate);
        /// <summary>
        /// Paged filter. Filters, orders and pages IN THE DATABASE when the expression translates.
        /// <para>
        /// ⚠️ Passing <paramref name="comparer"/> forces the in-memory path — a .NET
        /// <c>IComparer</c> has no SQL equivalent, so ordering by it cannot be translated. Order by
        /// <paramref name="orderByKey"/> alone to stay in the database.
        /// </para>
        /// </summary>
        /// <param name="pageOffset">A RAW ROW OFFSET, not a page index.</param>
        Task<List<T>> Where(Expression<Func<T, bool>> predicate, int pageSize, int pageOffset, string orderByKey = null, bool desc = false, IComparer<object> comparer = null);

        /// <summary>
        /// First match. Emits <c>TOP(1)</c> when the expression translates, instead of
        /// materialising the entire filtered set to take one row.
        /// </summary>
        Task<T> FirstOrDefault(Expression<Func<T, bool>> predicate);
        /// <summary><c>COUNT(*)</c> in SQL when the expression translates.</summary>
        Task<int> Count(Expression<Func<T, bool>> predicate = null);
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
