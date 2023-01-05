using System;
using System.Collections.Generic;

namespace Nostreets.Extensions.Interfaces
{
    public interface IDBService
    {
        List<object> GetAll();
        object Get(object id);
        object Get(object id, Converter<object, object> converter);
        object Insert(object model);
        object Insert(object model, Converter<object, object> converter);
        object[] Insert(IEnumerable<object> collection);
        object[] Insert(IEnumerable<object> collection, Converter<object, object> converter);
        void Update(IEnumerable<object> collection);
        void Update(IEnumerable<object> collection, Converter<object, object> converter);
        void Update(object model);
        void Update(object model, Converter<object, object> converter);
        void Delete(object id);
        void Delete(IEnumerable<object> ids);
        List<object> Where(Func<object, bool> predicate);
        object FirstOrDefault(Func<object, bool> predicate);
        List<dynamic> QueryResults(string query, Dictionary<string, object> parameters = null);
    }


    public interface IDBService<T>
    {
        List<T> GetAll();
        T Get(object id);
        T Get(object id, Converter<T, T> converter);
        object Insert(T model);
        object Insert(T model, Converter<T, T> converter);
        object[] Insert(IEnumerable<T> collection);
        object[] Insert(IEnumerable<T> collection, Converter<T, T> converter);
        void Update(IEnumerable<T> collection);
        void Update(IEnumerable<T> collection, Converter<T, T> converter);
        void Update(T model);
        void Update(T model, Converter<T, T> converter);
        void Delete(object id);
        void Delete(IEnumerable<object> ids);
        List<T> Where(Func<T, bool> predicate);
        T FirstOrDefault(Func<T, bool> predicate);
        void Backup(string disk = null);
        List<TResult> QueryResults<TResult>(string query, Dictionary<string, object> parameters = null);
    }

    public interface IDBService<T, IdType>
    {

        List<T> GetAll();
        T Get(IdType id);
        T Get(IdType id, Converter<T, T> converter);
        IdType Insert(T model);
        IdType Insert(T model, Converter<T, T> converter);
        IdType[] Insert(IEnumerable<T> collection);
        IdType[] Insert(IEnumerable<T> collection, Converter<T, T> converter);
        void Update(IEnumerable<T> collection);
        void Update(IEnumerable<T> collection, Converter<T, T> converter);
        void Update(T model);
        void Update(T model, Converter<T, T> converter);
        void Delete(IdType id);
        List<T> Where(Func<T, bool> predicate);
        void Delete(IEnumerable<IdType> ids);
        T FirstOrDefault(Func<T, bool> predicate);
        void Backup(string disk = null);
        List<TResult> QueryResults<TResult>(string query, Dictionary<string, object> parameters = null);
    }

    public interface IDBService<T, IdType, AddType, UpdateType>
    {
        List<T> GetAll();
        T Get(IdType id);
        T Get(IdType id, Converter<T, T> converter);
        IdType Insert(T model);
        IdType Insert(T model, Converter<T, T> converter);
        IdType Insert(AddType model, Converter<AddType, T> converter);
        IdType[] Insert(IEnumerable<T> collection);
        IdType[] Insert(IEnumerable<T> collection, Converter<T, T> converter);
        void Update(IEnumerable<T> collection);
        void Update(IEnumerable<T> collection, Converter<T, T> converter);
        void Update(UpdateType model, Converter<UpdateType, T> converter);
        void Update(T model);
        void Update(T model, Converter<T, T> converter);
        void Delete(IdType id);
        void Delete(IEnumerable<IdType> ids);
        List<T> Where(Func<T, bool> predicate);
        T FirstOrDefault(Func<T, bool> predicate);
        void Backup(string disk = null);
        List<TResult> QueryResults<TResult>(string query, Dictionary<string, object> parameters = null);
    }
}
