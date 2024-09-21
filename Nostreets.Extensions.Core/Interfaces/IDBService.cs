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
        Task DeleteRange(IEnumerable<object> ids);

        Task<List<T>> Where(Func<T, bool> predicate);
        Task<List<T>> Where(Func<T, bool> predicate, int pageSize, int pageOffset, string orderByKey = null, bool desc = false, IComparer<object> comparer = null);

        Task<T> FirstOrDefault(Func<T, bool> predicate);
        Task<int> Count(Func<T, bool> predicate = null);
        Task Backup(string disk = null);
        Task<List<TResult>> QueryResults<TResult>(string query, Dictionary<string, object> parameters = null);
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
        Task DeleteRange(IEnumerable<IdType> ids);

        Task<List<T>> Where(Func<T, bool> predicate);
        Task<List<T>> Where(Func<T, bool> predicate, int pageSize, int pageOffset, string orderByKey = null, bool desc = false, IComparer<object> comparer = null);

        Task<T> FirstOrDefault(Func<T, bool> predicate);
        Task<int> Count(Func<T, bool> predicate = null);
        Task Backup(string disk = null);
        Task<List<TResult>> QueryResults<TResult>(string query, Dictionary<string, object> parameters = null);
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
        Task DeleteRange(IEnumerable<IdType> ids);

        Task<List<T>> Where(Func<T, bool> predicate);
        Task<List<T>> Where(Func<T, bool> predicate, int pageSize, int pageOffset, string orderByKey = null, bool desc = false, IComparer<object> comparer = null);

        Task<T> FirstOrDefault(Func<T, bool> predicate);
        Task<int> Count(Func<T, bool> predicate = null);
        Task Backup(string disk = null);
        Task<List<TResult>> QueryResults<TResult>(string query, Dictionary<string, object> parameters = null);
    }

    public interface IBasicService
    {
        Task<object> Get(string id);
        Task Insert(object model);
        Task Update(object model);
        Task Delete(string id);
    }
}
