using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using Nostreets.Extensions.DataControl.Classes;
using Nostreets.Extensions.Extend.Basic;
using Nostreets.Extensions.Interfaces;
using Nostreets.Extensions.Utilities;
using System.Data.OleDb;
using System.Data.Entity.Infrastructure;
using System.Data.Entity;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Nostreets.Extensions.Extend.Data
{
    public static class Data
    {
        #region Static

        /// <summary>
        /// Maps the specified source.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source">The source.</param>
        /// <param name="mapper">The mapper. Key == Query Name / Value == Class Name </param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">source</exception>
        /// <exception cref="ArgumentException">
        /// not nullable
        /// or
        /// type mismatch
        /// or
        /// not nullable
        /// or
        /// type mismatch
        /// </exception>
        public static T Map<T>(ExpandoObject source, Dictionary<string, string> mapper = null)
        {
            if (source == null)
                throw new ArgumentNullException("source");

            T result = default(T);
            IDictionary<string, object> realSource = source;

            if (realSource.Count > 1)
            {

                result = result.Instantiate();
                Dictionary<string, PropertyInfo> _propertyMap = typeof(T).GetProperties().ToDictionary(p => p.Name.ToLower(), p => p);


                foreach (var kv in source)
                {
                    bool mapperHasKey = mapper != null && mapper.ContainsKey(kv.Key);

                    if (_propertyMap.TryGetValue(!mapperHasKey ? kv.Key.ToLower() : mapper[kv.Key].ToLower(), out PropertyInfo p))
                    {
                        Type propType = p.PropertyType;
                        bool doesTypesMatch = kv.Value.GetType() == propType,
                             canCast = kv.Value.TryCast(propType, out object value);

                        if (kv.Value == null && !propType.IsByRef && propType.Name != "Nullable`1")
                            throw new ArgumentException("not nullable");

                        else if (!doesTypesMatch && !canCast)
                            throw new ArgumentException("type mismatch");

                        p.SetValue(result, value, null);
                    }
                }
            }
            else
            {
                object value = realSource.ElementAt(0).Value;

                if (value == null && !typeof(T).IsByRef && typeof(T).Name != "Nullable`1")
                    throw new ArgumentException("not nullable");

                else if (value.GetType() != typeof(T))
                    throw new ArgumentException("type mismatch");

                result = (T)value;
            }

            return result;
        }

        #endregion

        #region Extension
        /// <summary>
        /// Gets the column names.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <param name="dataSouce">The data souce.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public static string[] GetColumnNames(this ISqlExecutor reader, Func<SqlConnection> dataSouce, string tableName)
        {
            Dictionary<string, Type> result = GetSchema(reader, dataSouce, tableName);
            return result.Select(a => a.Key).ToArray();
        }

        /// <summary>
        /// Gets the column names.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        public static string[] GetColumnNames(this IDataReader reader)
        {
            return reader.GetSchemaTable().Rows.Cast<DataRow>().Select(c => c["ColumnName"].ToString()).ToArray();
        }

        /// <summary>
        /// Gets the columns.
        /// </summary>
        /// <param name="dbContext">The database context.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static List<string> GetColumns(this DbContext dbContext, Type type)
        {
            string statment = String.Format("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME like N'{0}s'", type.Name);
            DbRawSqlQuery<string> result = dbContext.Database.SqlQuery<string>(statment);
            return result.ToList();
        }

        /// <summary>
        /// Gets the column types.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <param name="dataSouce">The data souce.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public static Type[] GetColumnTypes(this ISqlExecutor reader, Func<SqlConnection> dataSouce, string tableName)
        {
            Dictionary<string, Type> result = GetSchema(reader, dataSouce, tableName);
            return result.Select(a => a.Value).ToArray();
        }

        /// <summary>
        /// Gets the column types.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        public static Type[] GetColumnTypes(this IDataReader reader)
        {
            List<Type> result = new List<Type>();
            string[] columns = reader.GetColumnNames();
            for (int i = 0; i < columns.Length; i++)
            {
                result.Add(reader.GetValue(i).GetType());
            }

            return result.ToArray();
        }

        /// <summary>
        /// Gets the double.
        /// </summary>
        /// <param name="dr">The dr.</param>
        /// <param name="column_name">Name of the column.</param>
        /// <returns></returns>
        public static double GetDouble(this DataRow dr, string column_name)
        {
            double dbl = 0;
            double.TryParse(dr[column_name].ToString(), out dbl);
            return dbl;
        }

        /// <summary>
        /// Gets the double.
        /// </summary>
        /// <param name="dr">The dr.</param>
        /// <param name="column_index">Index of the column.</param>
        /// <returns></returns>
        public static double GetDouble(this DataRow dr, int column_index)
        {
            double dbl = 0;
            double.TryParse(dr[column_index].ToString(), out dbl);
            return dbl;
        }

        /// <summary>
        /// Gets the double.
        /// </summary>
        /// <param name="dr">The dr.</param>
        /// <param name="column_name">Name of the column.</param>
        /// <returns></returns>
        public static double GetDouble(this IDataReader dr, string column_name)
        {
            double dbl = 0;
            double.TryParse(dr[column_name].ToString(), out dbl);
            return dbl;
        }

        /// <summary>
        /// Gets the double.
        /// </summary>
        /// <param name="dr">The dr.</param>
        /// <param name="column_index">Index of the column.</param>
        /// <returns></returns>
        public static double GetDouble(this IDataReader dr, int column_index)
        {
            double dbl = 0;
            double.TryParse(dr[column_index].ToString(), out dbl);
            return dbl;
        }

        /// <summary>
        /// Determines whether [has key attribute].
        /// </summary>
        /// <param name="prop">The property.</param>
        /// <returns>
        ///   <c>true</c> if [has key attribute] [the specified property]; otherwise, <c>false</c>.
        /// </returns>
        public static bool HasKeyAttribute(this PropertyInfo prop)
        {
            return prop.GetCustomAttribute(typeof(KeyAttribute)) == null ? false : true;
        }

        /// <summary>
        /// Determines whether [has not mapped attribute].
        /// </summary>
        /// <param name="prop">The property.</param>
        /// <returns>
        ///   <c>true</c> if [has not mapped attribute] [the specified property]; otherwise, <c>false</c>.
        /// </returns>
        public static bool HasNotMappedAttribute(this PropertyInfo prop)
        {
            return prop.GetCustomAttribute(typeof(NotMappedAttribute)) == null ? false : true;
        }

        /// <summary>
        /// Determines whether [has foreign key attribute].
        /// </summary>
        /// <param name="prop">The property.</param>
        /// <returns>
        ///   <c>true</c> if [has foreign key attribute] [the specified property]; otherwise, <c>false</c>.
        /// </returns>
        public static bool HasForeignKeyAttribute(this PropertyInfo prop)
        {
            return prop.GetCustomAttribute(typeof(ForeignKeyAttribute)) == null ? false : true;
        }

        /// <summary>
        /// Determines whether [has key attribute].
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if [has key attribute] [the specified type]; otherwise, <c>false</c>.
        /// </returns>
        public static bool HasKeyAttribute(this Type type)
        {
            bool result = false;
            foreach (PropertyInfo prop in type.GetProperties())
            {
                result = prop.GetCustomAttribute(typeof(KeyAttribute)) == null ? false : true;

                if (result == true)
                    break;
            }

            return result;
        }

        /// <summary>
        /// Determines whether [has not mapped attribute].
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if [has not mapped attribute] [the specified type]; otherwise, <c>false</c>.
        /// </returns>
        public static bool HasNotMappedAttribute(this Type type)
        {
            bool result = false;
            foreach (PropertyInfo prop in type.GetProperties())
            {
                result = prop.GetCustomAttribute(typeof(NotMappedAttribute)) == null ? false : true;

                if (result == true)
                    break;
            }

            return result;
        }

        /// <summary>
        /// Determines whether [has foreign key attribute].
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if [has foreign key attribute] [the specified type]; otherwise, <c>false</c>.
        /// </returns>
        public static bool HasForeignKeyAttribute(this Type type)
        {
            bool result = false;
            foreach (PropertyInfo prop in type.GetProperties())
            {
                result = prop.GetCustomAttribute(typeof(ForeignKeyAttribute)) == null ? false : true;

                if (result == true)
                    break;
            }

            return result;
        }

        /// <summary>
        /// Gets the properties by key attribute.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static List<PropertyInfo> GetPropertiesByKeyAttribute(this Type type)
        {
            List<PropertyInfo> result = null;

            using (AttributeScanner<KeyAttribute> scanner = new AttributeScanner<KeyAttribute>())
            {
                foreach (var item in scanner.ScanForAttributes(Assembly.GetCallingAssembly(), ClassTypes.Properties, type))
                {
                    if (result == null)
                        result = new List<PropertyInfo>();

                    result.Add((PropertyInfo)item.Item2);
                }
            }

            return result;
        }

        /// <summary>
        /// Gets the properties by not mapped attribute.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static List<PropertyInfo> GetPropertiesByNotMappedAttribute(this Type type)
        {
            List<PropertyInfo> result = null;

            using (AttributeScanner<NotMappedAttribute> scanner = new AttributeScanner<NotMappedAttribute>())
            {
                foreach (var item in scanner.ScanForAttributes(Assembly.GetCallingAssembly(), ClassTypes.Properties, type))
                {
                    if (result == null)
                        result = new List<PropertyInfo>();

                    result.Add((PropertyInfo)item.Item2);
                }
            }

            return result;
        }

        /// <summary>
        /// Gets the properties by foreign key attribute.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static List<PropertyInfo> GetPropertiesByForeignKeyAttribute(this Type type)
        {
            List<PropertyInfo> result = null;

            using (AttributeScanner<ForeignKeyAttribute> scanner = new AttributeScanner<ForeignKeyAttribute>())
            {
                foreach (var item in scanner.ScanForAttributes(Assembly.GetCallingAssembly(), ClassTypes.Properties, type))
                {
                    if (result == null)
                        result = new List<PropertyInfo>();

                    result.Add((PropertyInfo)item.Item2);
                }
            }

            return result;
        }

        /// <summary>
        /// Gets the schema.
        /// </summary>
        /// <param name="srv">The SRV.</param>
        /// <param name="dataSouce">The data souce.</param>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        /// <exception cref="System.Exception">
        /// dataSouce param must not be null or return null...
        /// or
        /// dataSouce param must not be null or return null...
        /// </exception>
        /// <exception cref="Exception">dataSouce param must not be null or return null...
        /// or
        /// dataSouce param must not be null or return null...</exception>
        public static Dictionary<string, Type> GetSchema(this ISqlExecutor srv, Func<SqlConnection> dataSouce, string tableName)
        {
            SqlDataReader reader = null;
            SqlCommand cmd = null;
            SqlConnection conn = null;
            Dictionary<string, Type> result = null;

            try
            {
                if (dataSouce == null)
                    throw new Exception("dataSouce param must not be null or return null...");

                using (conn = dataSouce())
                {
                    if (conn == null)
                        throw new Exception("dataSouce param must not be null or return null...");

                    if (conn.State != ConnectionState.Open)
                        conn.Open();

                    string query = "SELECT * FROM {0}".FormatString(tableName);
                    cmd = srv.GetCommand(conn, query);
                    cmd.CommandType = CommandType.Text;

                    if (cmd != null)
                    {
                        reader = cmd.ExecuteReader();

                        IEnumerable<KeyValuePair<string, Type>> pairs = reader.GetSchemaTable().Rows.Cast<DataRow>().Select(c => new KeyValuePair<string, Type>(c["ColumnName"].ToString(), (Type)c["DataType"]));
                        foreach (var pair in pairs)
                        {
                            if (result == null)
                                result = new Dictionary<string, Type>();

                            result.Add(pair.Key, pair.Value);
                        }

                        reader.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (conn != null && conn.State != ConnectionState.Closed)
                    conn.Close();
            }

            return result;
        }

        public static Dictionary<string, Type> GetSchema(this IOleDbExecutor srv, Func<OleDbConnection> dataSouce, string tableName)
        {
            OleDbDataReader reader = null;
            OleDbCommand cmd = null;
            OleDbConnection conn = null;
            Dictionary<string, Type> result = null;

            try
            {
                if (dataSouce == null)
                    throw new Exception("dataSouce param must not be null or return null...");

                using (conn = dataSouce())
                {
                    if (conn == null)
                        throw new Exception("dataSouce param must not be null or return null...");

                    if (conn.State != ConnectionState.Open)
                        conn.Open();

                    string query = "SELECT * FROM {0}".FormatString(tableName);
                    cmd = srv.GetCommand(conn, query);
                    cmd.CommandType = CommandType.Text;

                    if (cmd != null)
                    {
                        reader = cmd.ExecuteReader();

                        IEnumerable<KeyValuePair<string, Type>> pairs = reader.GetSchemaTable().Rows.Cast<DataRow>().Select(c => new KeyValuePair<string, Type>(c["ColumnName"].ToString(), (Type)c["DataType"]));
                        foreach (var pair in pairs)
                        {
                            if (result == null)
                                result = new Dictionary<string, Type>();

                            result.Add(pair.Key, pair.Value);
                        }

                        reader.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (conn != null && conn.State != ConnectionState.Closed)
                    conn.Close();
            }

            return result;
        }

        /// <summary>
        /// Gets the schema.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        public static Dictionary<string, Type> GetSchema(this IDataReader reader)
        {
            Dictionary<string, Type> result = null;
            IEnumerable<KeyValuePair<string, Type>> pairs = reader.GetSchemaTable().Rows.Cast<DataRow>().Select(c => new KeyValuePair<string, Type>(c["ColumnName"].ToString(), (Type)c["DataType"]));
            foreach (var pair in pairs)
            {
                if (result == null)
                    result = new Dictionary<string, Type>();

                result.Add(pair.Key, pair.Value);
            }

            return result;
        }

        /// <summary>
        /// To the data table.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="iList">The i list.</param>
        /// <returns></returns>
        public static DataTable ToDataTable<T>(this List<T> iList)
        {
            DataTable dataTable = new DataTable();
            List<PropertyDescriptor> propertyDescriptorCollection = TypeDescriptor.GetProperties(typeof(T)).Cast<PropertyDescriptor>().ToList();

            for (int i = 0; i < propertyDescriptorCollection.Count; i++)
            {
                PropertyDescriptor propertyDescriptor = propertyDescriptorCollection[i];

                Type type = propertyDescriptor.PropertyType ?? typeof(int);

                if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    type = Nullable.GetUnderlyingType(type);

                dataTable.Columns.Add(propertyDescriptor.Name);
                dataTable.Columns[i].AllowDBNull = true;
            }

            int id = 0;
            foreach (object iListItem in iList)
            {
                ArrayList values = new ArrayList();
                for (int i = 0; i < propertyDescriptorCollection.Count; i++)
                {
                    values.Add(
                        propertyDescriptorCollection[i].GetValue(iListItem) == null && propertyDescriptorCollection[i].PropertyType == typeof(string)
                        ? String.Empty
                        : (i == 0 && propertyDescriptorCollection[i].Name.Contains("Id") && propertyDescriptorCollection[i].PropertyType == typeof(int))
                        ? id += 1
                        : propertyDescriptorCollection[i].GetValue(iListItem) == null
                        ? DBNull.Value
                        : propertyDescriptorCollection[i].GetValue(iListItem));
                }
                dataTable.Rows.Add(values.ToArray());

                values = null;
            }

            return dataTable;
        }

        /// <summary>
        /// To the data table.
        /// </summary>
        /// <param name="iList">The i list.</param>
        /// <param name="objType">Type of the object.</param>
        /// <returns></returns>
        public static DataTable ToDataTable(this List<object> iList, Type objType)
        {
            DataTable dataTable = new DataTable();
            List<PropertyDescriptor> propertyDescriptorCollection = TypeDescriptor.GetProperties(objType).Cast<PropertyDescriptor>().ToList();

            for (int i = 0; i < propertyDescriptorCollection.Count; i++)
            {
                PropertyDescriptor propertyDescriptor = propertyDescriptorCollection[i];

                Type type = propertyDescriptor.PropertyType ?? typeof(int);

                if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    type = Nullable.GetUnderlyingType(type);

                dataTable.Columns.Add(propertyDescriptor.Name);
                dataTable.Columns[i].AllowDBNull = true;
            }

            int id = 0;
            foreach (object iListItem in iList)
            {
                ArrayList values = new ArrayList();
                for (int i = 0; i < propertyDescriptorCollection.Count; i++)
                {
                    values.Add(
                        propertyDescriptorCollection[i].GetValue(iListItem) == null && propertyDescriptorCollection[i].PropertyType == typeof(string)
                        ? String.Empty
                        : (i == 0 && propertyDescriptorCollection[i].Name.Contains("Id") && propertyDescriptorCollection[i].PropertyType == typeof(int))
                        ? id += 1
                        : propertyDescriptorCollection[i].GetValue(iListItem) == null
                        ? DBNull.Value
                        : propertyDescriptorCollection[i].GetValue(iListItem));
                }
                dataTable.Rows.Add(values.ToArray());

                values = null;
            }

            return dataTable;
        }

        /// <summary>
        /// Gets the type of the database.
        /// </summary>
        /// <param name="giveType">Type of the give.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentException"></exception>
        public static SqlDbType GetDbType(this Type giveType)
        {
            Dictionary<Type, SqlDbType> _typeMap = new Dictionary<Type, SqlDbType>() {
                { typeof(string), SqlDbType.NVarChar },
                { typeof(char[]), SqlDbType.NVarChar },
                { typeof(byte), SqlDbType.TinyInt },
                { typeof(short), SqlDbType.SmallInt },
                { typeof(int), SqlDbType.Int },
                { typeof(long), SqlDbType.BigInt },
                { typeof(byte[]), SqlDbType.Image },
                { typeof(bool), SqlDbType.Bit },
                { typeof(DateTime), SqlDbType.DateTime2 },
                { typeof(DateTimeOffset), SqlDbType.DateTimeOffset },
                { typeof(decimal), SqlDbType.Money },
                { typeof(float), SqlDbType.Real },
                { typeof(double), SqlDbType.Float },
                { typeof(TimeSpan), SqlDbType.Time }
        };


            giveType = Nullable.GetUnderlyingType(giveType) ?? giveType;

            if (_typeMap.ContainsKey(giveType))
            {
                return _typeMap[giveType];
            }

            throw new ArgumentException($"{giveType.FullName} is not a supported .NET class");
        }

        /// <summary>
        /// Gets the type of the database.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static SqlDbType GetDbType<T>(this T type)
        {

            return GetDbType(typeof(T));
        }

        /// <summary>
        /// Gets the database connection.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connectionKey">The connection key.</param>
        /// <returns></returns>
        public static T GetDbConnection<T>(this string connectionKey) where T : IDbConnection, new()
        {
            return (T)typeof(T).Instantiate(ConfigurationManager.ConnectionStrings[connectionKey].ConnectionString);
        }

        /// <summary>
        /// Types the with ef attributes.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <returns></returns>
        public static Type TypeWithEFAttributes(this Type context)
        {
            Type annotatedType(Type type)
            {

                type = checkOrAddPK(type);
                type = checkOrAddFKs(type);

                return type;
            }

            IEnumerable<PropertyInfo> getPropertiesThatNeedFKs(Type type)
            {

                return type.GetProperties().Where(
                    a =>
                    {
                        if (a.GetCustomAttribute(typeof(ForeignKeyAttribute)) != null)
                            return false;


                        return (a.PropertyType.IsSystemType())
                          ? false
                          : (a.PropertyType.IsCollection())
                          ? false
                          : (a.PropertyType.IsClass || a.PropertyType.IsEnum);

                    });

            }

            Type checkOrAddPK(Type type)
            {
                PropertyInfo pk = type.GetPropertiesByKeyAttribute()?.SingleOrDefault();

                if (!type.IsClass)
                    throw new Exception("Generic Type has to be a custom class...");

                if (type.IsSystemType())
                    throw new Exception("Generic Type cannot be a system type...");

                if (pk != null && !pk.Name.ToLower().Contains("id") && !(pk.PropertyType == typeof(int) || pk.PropertyType == typeof(Guid) || pk.PropertyType == typeof(string)))
                    throw new Exception("Primary Key must be the data type of Int32, Guid, or String and the Name needs ID in it...");


                if (pk == null)
                {
                    pk = type.GetProperties().FirstOrDefault(a => a.Name.ToLower().Contains("id") && !(a.PropertyType == typeof(int) || a.PropertyType == typeof(Guid) || a.PropertyType == typeof(string)));

                    if (pk == null)
                        type = type.AddProperty(typeof(int), "Id", 0, new Dictionary<Type, object[]>() { { typeof(KeyAttribute), null } });


                    //type = type.AddOrSetAttribute(new Dictionary<string, Dictionary<Type, object[]>>() {
                    //    { "Id", new Dictionary<Type, object[]>(){ { typeof(KeyAttribute), null } } }
                    //});
                }

                return type;
            }

            Type checkOrAddFKs(Type type)
            {
                Dictionary<string, Dictionary<Type, object[]>> props = new Dictionary<string, Dictionary<Type, object[]>>();

                //Set or Add ForeignKeyAttributes and Flatten Enums for Properties
                foreach (PropertyInfo prop in getPropertiesThatNeedFKs(type))
                {
                    if (prop.PropertyType.IsEnum)
                        type = type.SetProperty(prop, typeof(FlatEnum<>).IntoGenericConstructorAsT(prop.PropertyType, type.GetPropertyValue(prop.Name)).GetType(), "Flat_" + prop.Name);

                    props.Add(prop.PropertyType.IsEnum ? "Flat_" + prop.Name : prop.Name, new Dictionary<Type, object[]>() { { typeof(ForeignKeyAttribute), new[] { prop.Name + "Id" } } });
                }


                //Add All FK Attributes
                type = type.AddOrSetAttribute(props);


                //Check and Add KeyAttributes for Property Types
                if (type.HasForeignKeyAttribute())
                    foreach (PropertyInfo fk in type.GetPropertiesByForeignKeyAttribute())
                    {
                        Type newPropType = checkOrAddPK(fk.PropertyType);
                        type = type.SetProperty(fk, newPropType);
                    }


                return type;
            }

            return annotatedType(context);

        }

        /// <summary>
        /// Queries the results.
        /// </summary>
        /// <param name="sql">The SQL.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public static List<dynamic> QueryResults(this string sql, string connectionString, Dictionary<string, object> parameters = null, CommandType commandType = CommandType.Text, short set = 1)
        {
            List<dynamic> result = new List<dynamic>();
            int resultSet = 1;

            void getRow(IDataReader reader)
            {
                var dataRow = new ExpandoObject() as IDictionary<string, object>;
                for (var fieldCount = 0; fieldCount < reader.FieldCount; fieldCount++)
                    dataRow.Add(reader.GetName(fieldCount), reader[fieldCount]);

                result.Add(dataRow);
            }

            using (SqlCommand cmd = new SqlConnection(connectionString).CreateCommand())
            {

                if (cmd.Connection.State != ConnectionState.Open)
                    cmd.Connection.Open();

                if (parameters != null)
                    foreach (KeyValuePair<string, object> param in parameters)
                        cmd.Parameters.AddWithValue(param.Key[0] == '@' ? param.Key : '@' + param.Key, param.Value);


                cmd.CommandText = sql;
                cmd.CommandType = commandType;
                IDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);


                while (true)
                {

                    if (set == resultSet)
                        while (reader.Read())
                            getRow(reader);


                    resultSet += 1;


                    if (reader.IsClosed || !reader.NextResult())
                        break;

                }

                reader.Close();

                if (cmd.Connection.State != ConnectionState.Closed)
                    cmd.Connection.Close();
            }

            return result;
        }

        public static string GetSqlType(this Type type, bool isKey = false)
        {
            if (type == typeof(int))
                return "INT";
            else if (type == typeof(string))
            {
                if (isKey)
                    return "NVARCHAR(450)";
                else
                    return "VARCHAR(MAX)";
            }
            else if (type == typeof(long))
                return "BIGINT";
            else if (type == typeof(short))
                return "SMALLINT";
            else if (type == typeof(byte))
                return "TINYINT";
            else if (type == typeof(float))
                return "FLOAT";
            else if (type == typeof(double))
                return "DOUBLE";
            else if (type == typeof(decimal))
                return "DECIMAL(18, 2)";
            else if (type == typeof(bool))
                return "BIT";
            else if (type == typeof(DateOnly))
                return "DATE";
            else if (type == typeof(DateTime))
                return "DATETIME";
            else if (type == typeof(DateTimeOffset))
                return "DATETIMEOFFSET";
            else if (type == typeof(TimeOnly))
                return "TIME";
            else if (type == typeof(TimeSpan))
                return "TIME";
            else if (type == typeof(Guid))
                return "UNIQUEIDENTIFIER";
            else if (type == typeof(byte[]))
                return "VARBINARY(MAX)";
            else if (type == typeof(char))
                return "CHAR(1)";
            else if (type.IsEnum)
                return "INT"; // Assuming the enum values are stored as integers
            else if (Nullable.GetUnderlyingType(type) != null)
                return GetSqlType(Nullable.GetUnderlyingType(type));
            // Add more supported types as needed

            throw new NotSupportedException($"SQL type mapping not defined for {type.Name}.");
        }

        public static bool MatchDotNetToSqlType(this Type propType, string sqlGeneralType)
        {
            sqlGeneralType = sqlGeneralType.ToUpper();
            Type realPropType = !propType.IsEnum ? propType : Enum.GetUnderlyingType(propType);
            string sqlTranslatorValue = realPropType.GetSqlType();
            sqlTranslatorValue = sqlTranslatorValue.Contains('(') ? sqlTranslatorValue.Split('(')[0] : sqlTranslatorValue;

            sqlGeneralType = sqlGeneralType switch
            {
                "NVARCHAR" => "VARCHAR",
                "DATETIME2" => "DATETIME",
                _ => sqlGeneralType
            };

            return sqlGeneralType == sqlTranslatorValue;
        }

        public static DateOnly ToDateOnly(this DateTime dateTime) => new DateOnly(dateTime.Year, dateTime.Month, dateTime.Day);

        public static TimeOnly ToTimeOnly(this DateTime dateTime) => new TimeOnly(dateTime.Hour, dateTime.Minute, dateTime.Second, dateTime.Millisecond, dateTime.Microsecond);

        #endregion

    }
}