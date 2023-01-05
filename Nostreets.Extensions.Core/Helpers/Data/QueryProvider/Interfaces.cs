using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Nostreets.Extensions.Helpers.Data.QueryProvider
{
    public interface IEntitySession
    {
        IEntityProvider Provider { get; }
        ISessionTable<T> GetTable<T>(string tableId);
        ISessionTable GetTable(Type elementType, string tableId);
        void SubmitChanges();
    }

    public interface ISessionTable : IQueryable
    {
        IEntitySession Session { get; }
        IEntityTable ProviderTable { get; }
        object GetById(object id);
        void SetSubmitAction(object instance, SubmitAction action);
        SubmitAction GetSubmitAction(object instance);
    }

    public interface ISessionTable<T> : IQueryable<T>, ISessionTable
    {
        new IEntityTable<T> ProviderTable { get; }
        new T GetById(object id);
        void SetSubmitAction(T instance, SubmitAction action);
        SubmitAction GetSubmitAction(T instance);
    }

   

    /// <summary>
    /// Common interface for controlling defer-loadable types
    /// </summary>
    public interface IDeferLoadable
    {
        bool IsLoaded { get; }
        void Load();
    }

    public interface IDeferredList : IList, IDeferLoadable
    {
    }

    public interface IDeferredList<T> : IList<T>, IDeferredList
    {
    }
    public interface ICreateExecutor
    {
        QueryExecutor CreateExecutor();
    }
    public interface IQueryText
    {
        string GetQueryText(Expression expression);
    }

    public interface IEntityProvider : IQueryProvider
    {
        IEntityTable<T> GetTable<T>(string tableId);
        IEntityTable GetTable(Type type, string tableId);
        bool CanBeEvaluatedLocally(Expression expression);
        bool CanBeParameter(Expression expression);
    }

    public interface IEntityTable : IQueryable, IUpdatable
    {
        new IEntityProvider Provider { get; }
        string TableId { get; }
        object GetById(object id);
        int Insert(object instance);
        int Update(object instance);
        int Delete(object instance);
        int InsertOrUpdate(object instance);
    }

    public interface IEntityTable<T> : IQueryable<T>, IEntityTable, IUpdatable<T>
    {
        new T GetById(object id);
        int Insert(T instance);
        int Update(T instance);
        int Delete(T instance);
        int InsertOrUpdate(T instance);
    }

    public interface IUpdatable : IQueryable
    {
    }

    public interface IUpdatable<T> : IUpdatable, IQueryable<T>
    {
    }

}
