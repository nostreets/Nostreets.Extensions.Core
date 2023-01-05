using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Nostreets.Extensions.Helpers.Data.QueryProvider
{
    public class SqlQueryProvider : DbEntityProvider
    {
        bool? allowMulitpleActiveResultSets;

        public SqlQueryProvider(SqlConnection connection, QueryMapping mapping, QueryPolicy policy)
            : base(connection, TSqlLanguage.Default, mapping, policy)
        {
        }

        public override DbEntityProvider New(DbConnection connection, QueryMapping mapping, QueryPolicy policy)
        {

            return new SqlQueryProvider((SqlConnection)connection, mapping, policy);
        }

        public static SqlQueryProvider Create(SqlConnection connection, string mappingId)
        {
            return new SqlQueryProvider(connection, GetMapping(mappingId), QueryPolicy.Default);
        }

        public static SqlQueryProvider Create(string connectionString, string mappingId)
        {
            SqlConnection sqlConnection = new SqlConnection();
            if (!connectionString.Contains('='))
                connectionString = GetConnectionString(connectionString);

            sqlConnection.ConnectionString = connectionString;

            return new SqlQueryProvider(sqlConnection, GetMapping(mappingId), QueryPolicy.Default);
        }

        public static string GetConnectionString(string databaseFile)
        {
            if (databaseFile.EndsWith(".mdf"))
            {
                databaseFile = Path.GetFullPath(databaseFile);
            }

            return string.Format(@"Data Source=.\SQLEXPRESS;Integrated Security=True;Connect Timeout=30;User Instance=True;MultipleActiveResultSets=true;AttachDbFilename='{0}'", databaseFile);
        }

        public bool AllowsMultipleActiveResultSets
        {
            get
            {
                if (this.allowMulitpleActiveResultSets == null)
                {
                    var builder = new SqlConnectionStringBuilder(this.Connection.ConnectionString);
                    var result = builder["MultipleActiveResultSets"];
                    this.allowMulitpleActiveResultSets = (result != null && result.GetType() == typeof(bool) && (bool)result);
                }
                return (bool)this.allowMulitpleActiveResultSets;
            }
        }

        protected override QueryExecutor CreateExecutor()
        {
            return new Executor(this);
        }

        new class Executor : DbEntityProvider.Executor
        {
            SqlQueryProvider provider;

            public Executor(SqlQueryProvider provider)
                : base(provider)
            {
                this.provider = provider;
            }

            protected override bool BufferResultRows
            {
                get { return !this.provider.AllowsMultipleActiveResultSets; }
            }

            protected override void AddParameter(DbCommand command, QueryParameter parameter, object value)
            {
                DbQueryType sqlType = (DbQueryType)parameter.QueryType;
                if (sqlType == null)
                {
                    sqlType = (DbQueryType)this.Provider.Language.TypeSystem.GetColumnType(parameter.Type);
                }

                int len = sqlType.Length;
                if (len == 0 && DbTypeSystem.IsVariableLength(sqlType.SqlDbType))
                {
                    len = Int32.MaxValue;
                }

                var p = ((SqlCommand)command).Parameters.Add("@" + parameter.Name, sqlType.SqlDbType, len);
                if (sqlType.Precision != 0)
                {
                    p.Precision = (byte)sqlType.Precision;
                }

                if (sqlType.Scale != 0)
                {
                    p.Scale = (byte)sqlType.Scale;
                }

                p.Value = value ?? DBNull.Value;
            }

            public override IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize, bool stream)
            {
                this.StartUsingConnection();
                try
                {
                    var result = this.ExecuteBatch(query, paramSets, batchSize);
                    if (!stream || this.ActionOpenedConnection)
                    {
                        return result.ToList();
                    }
                    else
                    {
                        return new EnumerateOnce<int>(result);
                    }
                }
                finally
                {
                    this.StopUsingConnection();
                }
            }

            private IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize)
            {
                SqlCommand cmd = (SqlCommand)this.GetCommand(query, null);
                DataTable dataTable = new DataTable();

                for (int i = 0, n = query.Parameters.Count; i < n; i++)
                {
                    var qp = query.Parameters[i];
                    cmd.Parameters[i].SourceColumn = qp.Name;
                    dataTable.Columns.Add(qp.Name, TypeHelper.GetNonNullableType(qp.Type));
                }

                SqlDataAdapter dataAdapter = new SqlDataAdapter();
                dataAdapter.InsertCommand = cmd;
                dataAdapter.InsertCommand.UpdatedRowSource = UpdateRowSource.None;
                dataAdapter.UpdateBatchSize = batchSize;

                this.LogMessage("-- Start SQL Batching --");
                this.LogMessage("");
                this.LogCommand(query, null);

                IEnumerator<object[]> en = paramSets.GetEnumerator();
                using (en)
                {
                    bool hasNext = true;
                    while (hasNext)
                    {
                        int count = 0;
                        for (; count < dataAdapter.UpdateBatchSize && (hasNext = en.MoveNext()); count++)
                        {
                            var paramValues = en.Current;
                            dataTable.Rows.Add(paramValues);
                            this.LogParameters(query, paramValues);
                            this.LogMessage("");
                        }

                        if (count > 0)
                        {
                            int n = dataAdapter.Update(dataTable);
                            for (int i = 0; i < count; i++)
                            {
                                yield return (i < n) ? 1 : 0;
                            }

                            dataTable.Rows.Clear();
                        }
                    }
                }

                this.LogMessage(string.Format("-- End SQL Batching --"));
                this.LogMessage("");
            }
        }
    }


    public class DbEntityProvider : EntityProvider
    {
        DbConnection connection;
        DbTransaction transaction;
        IsolationLevel isolation = IsolationLevel.ReadCommitted;

        int nConnectedActions = 0;
        bool actionOpenedConnection = false;

        public DbEntityProvider(DbConnection connection, QueryLanguage language, QueryMapping mapping, QueryPolicy policy)
            : base(language, mapping, policy)
        {
            if (connection == null)
                throw new InvalidOperationException("Connection not specified");
            this.connection = connection;
        }

        public virtual DbConnection Connection
        {
            get { return this.connection; }
        }

        public virtual DbTransaction Transaction
        {
            get { return this.transaction; }
            set
            {
                if (value != null && value.Connection != this.connection)
                    throw new InvalidOperationException("Transaction does not match connection.");
                this.transaction = value;
            }
        }

        public IsolationLevel Isolation
        {
            get { return this.isolation; }
            set { this.isolation = value; }
        }

        public virtual DbEntityProvider New(DbConnection connection, QueryMapping mapping, QueryPolicy policy)
        {
            return (DbEntityProvider)Activator.CreateInstance(this.GetType(), new object[] { connection, mapping, policy });
        }

        public virtual DbEntityProvider New(DbConnection connection)
        {
            var n = New(connection, this.Mapping, this.Policy);
            n.Log = this.Log;
            return n;
        }

        public virtual DbEntityProvider New(QueryMapping mapping)
        {
            var n = New(this.Connection, mapping, this.Policy);
            n.Log = this.Log;
            return n;
        }

        public virtual DbEntityProvider New(QueryPolicy policy)
        {
            var n = New(this.Connection, this.Mapping, policy);
            n.Log = this.Log;
            return n;
        }

        public static DbEntityProvider FromApplicationSettings()
        {
            var provider = System.Configuration.ConfigurationManager.AppSettings["Provider"];
            var connection = System.Configuration.ConfigurationManager.AppSettings["Connection"];
            var mapping = System.Configuration.ConfigurationManager.AppSettings["Mapping"];
            return From(provider, connection, mapping);
        }

        public static DbEntityProvider FromApplicationSettings(string connectionKey)
        {
            var provider = System.Configuration.ConfigurationManager.AppSettings["Provider"];
            var connection = System.Configuration.ConfigurationManager.AppSettings[connectionKey];
            var mapping = System.Configuration.ConfigurationManager.AppSettings["Mapping"];
            return From(provider, connection, mapping);
        }

        public static DbEntityProvider From(string connectionString, string mappingId)
        {
            return From(connectionString, mappingId, QueryPolicy.Default);
        }

        public static DbEntityProvider From(string connectionString, string mappingId, QueryPolicy policy)
        {
            return From(null, connectionString, mappingId, policy);
        }

        public static DbEntityProvider From(string connectionString, QueryMapping mapping, QueryPolicy policy)
        {
            return From((string)null, connectionString, mapping, policy);
        }

        public static DbEntityProvider From(string provider, string connectionString, string mappingId)
        {
            return From(provider, connectionString, mappingId, QueryPolicy.Default);
        }

        public static DbEntityProvider From(string provider, string connectionString, string mappingId, QueryPolicy policy)
        {
            return From(provider, connectionString, GetMapping(mappingId), policy);
        }

        public static DbEntityProvider From(string provider, string connectionString, QueryMapping mapping, QueryPolicy policy)
        {
            if (provider == null)
            {
                var clower = connectionString.ToLower();
                // try sniffing connection to figure out provider
                if (clower.Contains(".mdb") || clower.Contains(".accdb"))
                {
                    provider = "IQToolkit.Data.Access";
                }
                else if (clower.Contains(".sdf"))
                {
                    provider = "IQToolkit.Data.SqlServerCe";
                }
                else if (clower.Contains(".sl3") || clower.Contains(".db3"))
                {
                    provider = "IQToolkit.Data.SQLite";
                }
                else if (clower.Contains(".mdf"))
                {
                    provider = "IQToolkit.Data.SqlClient";
                }
                else
                {
                    throw new InvalidOperationException(string.Format("Query provider not specified and cannot be inferred."));
                }
            }

            Type providerType = GetProviderType(provider);
            if (providerType == null)
                throw new InvalidOperationException(string.Format("Unable to find query provider '{0}'", provider));

            return From(providerType, connectionString, mapping, policy);
        }

        public static DbEntityProvider From(Type providerType, string connectionString, QueryMapping mapping, QueryPolicy policy)
        {
            Type adoConnectionType = GetAdoConnectionType(providerType);
            if (adoConnectionType == null)
                throw new InvalidOperationException(string.Format("Unable to deduce ADO provider for '{0}'", providerType.Name));
            DbConnection connection = (DbConnection)Activator.CreateInstance(adoConnectionType);

            // is the connection string just a filename?
            if (!connectionString.Contains('='))
            {
                MethodInfo gcs = providerType.GetMethod("GetConnectionString", BindingFlags.Static | BindingFlags.Public, null, new Type[] { typeof(string) }, null);
                if (gcs != null)
                {
                    var getConnectionString = (Func<string, string>)Delegate.CreateDelegate(typeof(Func<string, string>), gcs);
                    connectionString = getConnectionString(connectionString);
                }
            }

            connection.ConnectionString = connectionString;

            return (DbEntityProvider)Activator.CreateInstance(providerType, new object[] { connection, mapping, policy });
        }

        private static Type GetAdoConnectionType(Type providerType)
        {
            // sniff constructors 
            foreach (var con in providerType.GetConstructors())
            {
                foreach (var arg in con.GetParameters())
                {
                    if (arg.ParameterType.IsSubclassOf(typeof(DbConnection)))
                        return arg.ParameterType;
                }
            }
            return null;
        }

        protected bool ActionOpenedConnection
        {
            get { return this.actionOpenedConnection; }
        }

        protected void StartUsingConnection()
        {
            if (this.connection.State == ConnectionState.Closed)
            {
                this.connection.Open();
                this.actionOpenedConnection = true;
            }
            this.nConnectedActions++;
        }

        protected void StopUsingConnection()
        {
            System.Diagnostics.Debug.Assert(this.nConnectedActions > 0);
            this.nConnectedActions--;
            if (this.nConnectedActions == 0 && this.actionOpenedConnection)
            {
                this.connection.Close();
                this.actionOpenedConnection = false;
            }
        }

        public override void DoConnected(Action action)
        {
            this.StartUsingConnection();
            try
            {
                action();
            }
            finally
            {
                this.StopUsingConnection();
            }
        }

        public override void DoTransacted(Action action)
        {
            this.StartUsingConnection();
            try
            {
                if (this.Transaction == null)
                {
                    var trans = this.Connection.BeginTransaction(this.Isolation);
                    try
                    {
                        this.Transaction = trans;
                        action();
                        trans.Commit();
                    }
                    finally
                    {
                        this.Transaction = null;
                        trans.Dispose();
                    }
                }
                else
                {
                    action();
                }
            }
            finally
            {
                this.StopUsingConnection();
            }
        }

        public override int ExecuteCommand(string commandText)
        {
            if (this.Log != null)
            {
                this.Log.WriteLine(commandText);
            }
            this.StartUsingConnection();
            try
            {
                DbCommand cmd = this.Connection.CreateCommand();
                cmd.CommandText = commandText;
                return cmd.ExecuteNonQuery();
            }
            finally
            {
                this.StopUsingConnection();
            }
        }

        protected override QueryExecutor CreateExecutor()
        {
            return new Executor(this);
        }

        public class Executor : QueryExecutor
        {
            DbEntityProvider provider;
            int rowsAffected;

            public Executor(DbEntityProvider provider)
            {
                this.provider = provider;
            }

            public DbEntityProvider Provider
            {
                get { return this.provider; }
            }

            public override int RowsAffected
            {
                get { return this.rowsAffected; }
            }

            protected virtual bool BufferResultRows
            {
                get { return false; }
            }

            protected bool ActionOpenedConnection
            {
                get { return this.provider.actionOpenedConnection; }
            }

            protected void StartUsingConnection()
            {
                this.provider.StartUsingConnection();
            }

            protected void StopUsingConnection()
            {
                this.provider.StopUsingConnection();
            }

            public override object Convert(object value, Type type)
            {
                if (value == null)
                {
                    return TypeHelper.GetDefault(type);
                }
                type = TypeHelper.GetNonNullableType(type);
                Type vtype = value.GetType();
                if (type != vtype)
                {
                    if (type.IsEnum)
                    {
                        if (vtype == typeof(string))
                        {
                            return Enum.Parse(type, (string)value);
                        }
                        else
                        {
                            Type utype = Enum.GetUnderlyingType(type);
                            if (utype != vtype)
                            {
                                value = System.Convert.ChangeType(value, utype);
                            }
                            return Enum.ToObject(type, value);
                        }
                    }
                    return System.Convert.ChangeType(value, type);
                }
                return value;
            }

            public override IEnumerable<T> Execute<T>(QueryCommand command, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues)
            {
                this.LogCommand(command, paramValues);
                this.StartUsingConnection();
                try
                {
                    DbCommand cmd = this.GetCommand(command, paramValues);
                    DbDataReader reader = this.ExecuteReader(cmd);
                    var result = Project(reader, fnProjector, entity, true);
                    if (this.provider.ActionOpenedConnection)
                    {
                        result = result.ToList();
                    }
                    else
                    {
                        result = new EnumerateOnce<T>(result);
                    }
                    return result;
                }
                finally
                {
                    this.StopUsingConnection();
                }
            }

            protected virtual DbDataReader ExecuteReader(DbCommand command)
            {
                var reader = command.ExecuteReader();
                if (this.BufferResultRows)
                {
                    // use data table to buffer results
                    var ds = new DataSet();
                    ds.EnforceConstraints = false;
                    var table = new DataTable();
                    ds.Tables.Add(table);
                    ds.EnforceConstraints = false;
                    table.Load(reader);
                    reader = table.CreateDataReader();
                }
                return reader;
            }

            protected virtual IEnumerable<T> Project<T>(DbDataReader reader, Func<FieldReader, T> fnProjector, MappingEntity entity, bool closeReader)
            {
                var freader = new DbFieldReader(this, reader);
                try
                {
                    while (reader.Read())
                    {
                        yield return fnProjector(freader);
                    }
                }
                finally
                {
                    if (closeReader)
                    {
                        reader.Close();
                    }
                }
            }

            public override int ExecuteCommand(QueryCommand query, object[] paramValues)
            {
                this.LogCommand(query, paramValues);
                this.StartUsingConnection();
                try
                {
                    DbCommand cmd = this.GetCommand(query, paramValues);
                    this.rowsAffected = cmd.ExecuteNonQuery();
                    return this.rowsAffected;
                }
                finally
                {
                    this.StopUsingConnection();
                }
            }

            public override IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets, int batchSize, bool stream)
            {
                this.StartUsingConnection();
                try
                {
                    var result = this.ExecuteBatch(query, paramSets);
                    if (!stream || this.ActionOpenedConnection)
                    {
                        return result.ToList();
                    }
                    else
                    {
                        return new EnumerateOnce<int>(result);
                    }
                }
                finally
                {
                    this.StopUsingConnection();
                }
            }

            private IEnumerable<int> ExecuteBatch(QueryCommand query, IEnumerable<object[]> paramSets)
            {
                this.LogCommand(query, null);
                DbCommand cmd = this.GetCommand(query, null);
                foreach (var paramValues in paramSets)
                {
                    this.LogParameters(query, paramValues);
                    this.LogMessage("");
                    this.SetParameterValues(query, cmd, paramValues);
                    this.rowsAffected = cmd.ExecuteNonQuery();
                    yield return this.rowsAffected;
                }
            }

            public override IEnumerable<T> ExecuteBatch<T>(QueryCommand query, IEnumerable<object[]> paramSets, Func<FieldReader, T> fnProjector, MappingEntity entity, int batchSize, bool stream)
            {
                this.StartUsingConnection();
                try
                {
                    var result = this.ExecuteBatch(query, paramSets, fnProjector, entity);
                    if (!stream || this.ActionOpenedConnection)
                    {
                        return result.ToList();
                    }
                    else
                    {
                        return new EnumerateOnce<T>(result);
                    }
                }
                finally
                {
                    this.StopUsingConnection();
                }
            }

            private IEnumerable<T> ExecuteBatch<T>(QueryCommand query, IEnumerable<object[]> paramSets, Func<FieldReader, T> fnProjector, MappingEntity entity)
            {
                this.LogCommand(query, null);
                DbCommand cmd = this.GetCommand(query, null);
                cmd.Prepare();
                foreach (var paramValues in paramSets)
                {
                    this.LogParameters(query, paramValues);
                    this.LogMessage("");
                    this.SetParameterValues(query, cmd, paramValues);
                    var reader = this.ExecuteReader(cmd);
                    var freader = new DbFieldReader(this, reader);
                    try
                    {
                        if (reader.HasRows)
                        {
                            reader.Read();
                            yield return fnProjector(freader);
                        }
                        else
                        {
                            yield return default(T);
                        }
                    }
                    finally
                    {
                        reader.Close();
                    }
                }
            }

            public override IEnumerable<T> ExecuteDeferred<T>(QueryCommand query, Func<FieldReader, T> fnProjector, MappingEntity entity, object[] paramValues)
            {
                this.LogCommand(query, paramValues);
                this.StartUsingConnection();
                try
                {
                    DbCommand cmd = this.GetCommand(query, paramValues);
                    var reader = this.ExecuteReader(cmd);
                    var freader = new DbFieldReader(this, reader);
                    try
                    {
                        while (reader.Read())
                        {
                            yield return fnProjector(freader);
                        }
                    }
                    finally
                    {
                        reader.Close();
                    }
                }
                finally
                {
                    this.StopUsingConnection();
                }
            }

            /// <summary>
            /// Get an ADO command object initialized with the command-text and parameters
            /// </summary>
            protected virtual DbCommand GetCommand(QueryCommand query, object[] paramValues)
            {
                // create command object (and fill in parameters)
                DbCommand cmd = this.provider.Connection.CreateCommand();
                cmd.CommandText = query.CommandText;
                if (this.provider.Transaction != null)
                    cmd.Transaction = this.provider.Transaction;
                this.SetParameterValues(query, cmd, paramValues);
                return cmd;
            }

            protected virtual void SetParameterValues(QueryCommand query, DbCommand command, object[] paramValues)
            {
                if (query.Parameters.Count > 0 && command.Parameters.Count == 0)
                {
                    for (int i = 0, n = query.Parameters.Count; i < n; i++)
                    {
                        this.AddParameter(command, query.Parameters[i], paramValues != null ? paramValues[i] : null);
                    }
                }
                else if (paramValues != null)
                {
                    for (int i = 0, n = command.Parameters.Count; i < n; i++)
                    {
                        DbParameter p = command.Parameters[i];
                        if (p.Direction == System.Data.ParameterDirection.Input
                         || p.Direction == System.Data.ParameterDirection.InputOutput)
                        {
                            p.Value = paramValues[i] ?? DBNull.Value;
                        }
                    }
                }
            }

            protected virtual void AddParameter(DbCommand command, QueryParameter parameter, object value)
            {
                DbParameter p = command.CreateParameter();
                p.ParameterName = parameter.Name;
                p.Value = value ?? DBNull.Value;
                command.Parameters.Add(p);
            }

            protected virtual void GetParameterValues(DbCommand command, object[] paramValues)
            {
                if (paramValues != null)
                {
                    for (int i = 0, n = command.Parameters.Count; i < n; i++)
                    {
                        if (command.Parameters[i].Direction != System.Data.ParameterDirection.Input)
                        {
                            object value = command.Parameters[i].Value;
                            if (value == DBNull.Value)
                                value = null;
                            paramValues[i] = value;
                        }
                    }
                }
            }

            protected virtual void LogMessage(string message)
            {
                if (this.provider.Log != null)
                {
                    this.provider.Log.WriteLine(message);
                }
            }

            /// <summary>
            /// Write a command and parameters to the log
            /// </summary>
            /// <param name="command"></param>
            /// <param name="paramValues"></param>
            protected virtual void LogCommand(QueryCommand command, object[] paramValues)
            {
                if (this.provider.Log != null)
                {
                    this.provider.Log.WriteLine(command.CommandText);
                    if (paramValues != null)
                    {
                        this.LogParameters(command, paramValues);
                    }
                    this.provider.Log.WriteLine();
                }
            }

            protected virtual void LogParameters(QueryCommand command, object[] paramValues)
            {
                if (this.provider.Log != null && paramValues != null)
                {
                    for (int i = 0, n = command.Parameters.Count; i < n; i++)
                    {
                        var p = command.Parameters[i];
                        var v = paramValues[i];

                        if (v == null || v == DBNull.Value)
                        {
                            this.provider.Log.WriteLine("-- {0} = NULL", p.Name);
                        }
                        else
                        {
                            this.provider.Log.WriteLine("-- {0} = [{1}]", p.Name, v);
                        }
                    }
                }
            }
        }

        protected class DbFieldReader : FieldReader
        {
            QueryExecutor executor;
            DbDataReader reader;

            public DbFieldReader(QueryExecutor executor, DbDataReader reader)
            {
                this.executor = executor;
                this.reader = reader;
                this.Init();
            }

            protected override int FieldCount
            {
                get { return this.reader.FieldCount; }
            }

            protected override Type GetFieldType(int ordinal)
            {
                return this.reader.GetFieldType(ordinal);
            }

            protected override bool IsDBNull(int ordinal)
            {
                return this.reader.IsDBNull(ordinal);
            }

            protected override T GetValue<T>(int ordinal)
            {
                return (T)this.executor.Convert(this.reader.GetValue(ordinal), typeof(T));
            }

            protected override Byte GetByte(int ordinal)
            {
                return this.reader.GetByte(ordinal);
            }

            protected override Char GetChar(int ordinal)
            {
                return this.reader.GetChar(ordinal);
            }

            protected override DateTime GetDateTime(int ordinal)
            {
                return this.reader.GetDateTime(ordinal);
            }

            protected override Decimal GetDecimal(int ordinal)
            {
                return this.reader.GetDecimal(ordinal);
            }

            protected override Double GetDouble(int ordinal)
            {
                return this.reader.GetDouble(ordinal);
            }

            protected override Single GetSingle(int ordinal)
            {
                return this.reader.GetFloat(ordinal);
            }

            protected override Guid GetGuid(int ordinal)
            {
                return this.reader.GetGuid(ordinal);
            }

            protected override Int16 GetInt16(int ordinal)
            {
                return this.reader.GetInt16(ordinal);
            }

            protected override Int32 GetInt32(int ordinal)
            {
                return this.reader.GetInt32(ordinal);
            }

            protected override Int64 GetInt64(int ordinal)
            {
                return this.reader.GetInt64(ordinal);
            }

            protected override String GetString(int ordinal)
            {
                return this.reader.GetString(ordinal);
            }
        }
    }


    /// <summary>
    /// A LINQ IQueryable query provider that executes database queries over a DbConnection
    /// </summary>
    public abstract class EntityProvider : BaseProvider, IEntityProvider, ICreateExecutor
    {
        QueryLanguage language;
        QueryMapping mapping;
        QueryPolicy policy;
        TextWriter log;
        Dictionary<MappingEntity, IEntityTable> tables;
        QueryCache cache;

        public EntityProvider(QueryLanguage language, QueryMapping mapping, QueryPolicy policy)
        {
            if (language == null)
                throw new InvalidOperationException("Language not specified");
            if (mapping == null)
                throw new InvalidOperationException("Mapping not specified");
            if (policy == null)
                throw new InvalidOperationException("Policy not specified");
            this.language = language;
            this.mapping = mapping;
            this.policy = policy;
            this.tables = new Dictionary<MappingEntity, IEntityTable>();
        }

        public QueryMapping Mapping
        {
            get { return this.mapping; }
        }

        public QueryLanguage Language
        {
            get { return this.language; }
        }

        public QueryPolicy Policy
        {
            get { return this.policy; }

            set
            {
                if (value == null)
                {
                    this.policy = QueryPolicy.Default;
                }
                else
                {
                    this.policy = value;
                }
            }
        }

        public TextWriter Log
        {
            get { return this.log; }
            set { this.log = value; }
        }

        public QueryCache Cache
        {
            get { return this.cache; }
            set { this.cache = value; }
        }

        public IEntityTable GetTable(MappingEntity entity)
        {
            IEntityTable table;
            if (!this.tables.TryGetValue(entity, out table))
            {
                table = this.CreateTable(entity);
                this.tables.Add(entity, table);
            }
            return table;
        }

        protected virtual IEntityTable CreateTable(MappingEntity entity)
        {
            return (IEntityTable)Activator.CreateInstance(
                typeof(EntityTable<>).MakeGenericType(entity.ElementType),
                new object[] { this, entity }
                );
        }

        public virtual IEntityTable<T> GetTable<T>()
        {
            return GetTable<T>(null);
        }

        public virtual IEntityTable<T> GetTable<T>(string tableId)
        {
            return (IEntityTable<T>)this.GetTable(typeof(T), tableId);
        }

        public virtual IEntityTable GetTable(Type type)
        {
            return GetTable(type, null);
        }

        public virtual IEntityTable GetTable(Type type, string tableId)
        {
            return this.GetTable(this.Mapping.GetEntity(type, tableId));
        }

        public bool CanBeEvaluatedLocally(Expression expression)
        {
            return this.Mapping.CanBeEvaluatedLocally(expression);
        }

        public virtual bool CanBeParameter(Expression expression)
        {
            Type type = TypeHelper.GetNonNullableType(expression.Type);
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Object:
                    if (expression.Type == typeof(Byte[]) ||
                        expression.Type == typeof(Char[]))
                        return true;
                    return false;
                default:
                    return true;
            }
        }

        protected abstract QueryExecutor CreateExecutor();

        QueryExecutor ICreateExecutor.CreateExecutor()
        {
            return this.CreateExecutor();
        }

        public class EntityTable<T> : Query<T>, IEntityTable<T>, IHaveMappingEntity
        {
            MappingEntity entity;
            EntityProvider provider;

            public EntityTable(EntityProvider provider, MappingEntity entity)
                : base(provider, typeof(IEntityTable<T>))
            {
                this.provider = provider;
                this.entity = entity;
            }

            public MappingEntity Entity
            {
                get { return this.entity; }
            }

            new public IEntityProvider Provider
            {
                get { return this.provider; }
            }

            public string TableId
            {
                get { return this.entity.TableId; }
            }

            public Type EntityType
            {
                get { return this.entity.EntityType; }
            }

            public T GetById(object id)
            {
                var dbProvider = this.Provider;
                if (dbProvider != null)
                {
                    IEnumerable<object> keys = id as IEnumerable<object>;
                    if (keys == null)
                        keys = new object[] { id };
                    Expression query = ((EntityProvider)dbProvider).Mapping.GetPrimaryKeyQuery(this.entity, this.Expression, keys.Select(v => Expression.Constant(v)).ToArray());
                    return this.Provider.Execute<T>(query);
                }
                return default(T);
            }

            object IEntityTable.GetById(object id)
            {
                return this.GetById(id);
            }

            public int Insert(T instance)
            {
                return Updatable.Insert(this, instance);
            }

            int IEntityTable.Insert(object instance)
            {
                return this.Insert((T)instance);
            }

            public int Delete(T instance)
            {
                return Updatable.Delete(this, instance);
            }

            int IEntityTable.Delete(object instance)
            {
                return this.Delete((T)instance);
            }

            public int Update(T instance)
            {
                return Updatable.Update(this, instance);
            }

            int IEntityTable.Update(object instance)
            {
                return this.Update((T)instance);
            }

            public int InsertOrUpdate(T instance)
            {
                return Updatable.InsertOrUpdate(this, instance);
            }

            int IEntityTable.InsertOrUpdate(object instance)
            {
                return this.InsertOrUpdate((T)instance);
            }
        }

        public override string GetQueryText(Expression expression)
        {
            Expression plan = this.GetExecutionPlan(expression);
            var commands = CommandGatherer.Gather(plan).Select(c => c.CommandText).ToArray();
            return string.Join("\n\n", commands);
        }

        class CommandGatherer : DbExpressionVisitor
        {
            List<QueryCommand> commands = new List<QueryCommand>();

            public static ReadOnlyCollection<QueryCommand> Gather(Expression expression)
            {
                var gatherer = new CommandGatherer();
                gatherer.Visit(expression);
                return gatherer.commands.AsReadOnly();
            }

            protected override Expression VisitConstant(ConstantExpression c)
            {
                QueryCommand qc = c.Value as QueryCommand;
                if (qc != null)
                {
                    this.commands.Add(qc);
                }
                return c;
            }
        }

        public string GetQueryPlan(Expression expression)
        {
            Expression plan = this.GetExecutionPlan(expression);
            return DbExpressionWriter.WriteToString(this.Language, plan);
        }

        protected virtual QueryTranslator CreateTranslator()
        {
            return new QueryTranslator(this.language, this.mapping, this.policy);
        }

        public abstract void DoTransacted(Action action);
        public abstract void DoConnected(Action action);
        public abstract int ExecuteCommand(string commandText);

        /// <summary>
        /// Execute the query expression (does translation, etc.)
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public override object Execute(Expression expression)
        {
            LambdaExpression lambda = expression as LambdaExpression;

            if (lambda == null && this.cache != null && expression.NodeType != ExpressionType.Constant)
            {
                return this.cache.Execute(expression);
            }

            Expression plan = this.GetExecutionPlan(expression);

            if (lambda != null)
            {
                // compile & return the execution plan so it can be used multiple times
                LambdaExpression fn = Expression.Lambda(lambda.Type, plan, lambda.Parameters);
#if NOREFEMIT
                    return ExpressionEvaluator.CreateDelegate(fn);
#else
                return fn.Compile();
#endif
            }
            else
            {
                // compile the execution plan and invoke it
                Expression<Func<object>> efn = Expression.Lambda<Func<object>>(Expression.Convert(plan, typeof(object)));
#if NOREFEMIT
                    return ExpressionEvaluator.Eval(efn, new object[] { });
#else
                Func<object> fn = efn.Compile();
                return fn();
#endif
            }
        }

        /// <summary>
        /// Convert the query expression into an execution plan
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public virtual Expression GetExecutionPlan(Expression expression)
        {
            // strip off lambda for now
            LambdaExpression lambda = expression as LambdaExpression;
            if (lambda != null)
                expression = lambda.Body;

            QueryTranslator translator = this.CreateTranslator();

            // translate query into client & server parts
            Expression translation = translator.Translate(expression);

            var parameters = lambda != null ? lambda.Parameters : null;
            Expression provider = this.Find(expression, parameters, typeof(EntityProvider));
            if (provider == null)
            {
                Expression rootQueryable = this.Find(expression, parameters, typeof(IQueryable));
                provider = Expression.Property(rootQueryable, typeof(IQueryable).GetProperty("Provider"));
            }

            return translator.Police.BuildExecutionPlan(translation, provider);
        }

        private Expression Find(Expression expression, IList<ParameterExpression> parameters, Type type)
        {
            if (parameters != null)
            {
                Expression found = parameters.FirstOrDefault(p => type.IsAssignableFrom(p.Type));
                if (found != null)
                    return found;
            }

            return TypedSubtreeFinder.Find(expression, type);
        }

        public static QueryMapping GetMapping(string mappingId)
        {
            if (mappingId != null)
            {
                Type type = FindLoadedType(mappingId);
                if (type != null)
                {
                    return new AttributeMapping(type);
                }

                if (File.Exists(mappingId))
                {
                    return XmlMapping.FromXml(File.ReadAllText(mappingId));
                }
            }

            return new ImplicitMapping();
        }

        public static Type GetProviderType(string providerName)
        {
            if (!string.IsNullOrEmpty(providerName))
            {
                var type = FindInstancesIn(typeof(EntityProvider), providerName).FirstOrDefault();
                if (type != null)
                    return type;
            }
            return null;
        }

        private static Type FindLoadedType(string typeName)
        {
            foreach (var assem in AppDomain.CurrentDomain.GetAssemblies())
            {
                var type = assem.GetType(typeName, false, true);
                if (type != null)
                    return type;
            }
            return null;
        }

        private static IEnumerable<Type> FindInstancesIn(Type type, string assemblyName)
        {
            Assembly assembly = GetAssemblyForNamespace(assemblyName);
            if (assembly != null)
            {
                foreach (var atype in assembly.GetTypes())
                {
                    if (string.Compare(atype.Namespace, assemblyName, true) == 0
                        && type.IsAssignableFrom(atype))
                    {
                        yield return atype;
                    }
                }
            }
        }

        private static Assembly GetAssemblyForNamespace(string nspace)
        {
            foreach (var assem in AppDomain.CurrentDomain.GetAssemblies())
            {
                if (assem.FullName.Contains(nspace))
                {
                    return assem;
                }
            }

            return Load(nspace + ".dll");
        }

        private static Assembly Load(string name)
        {
            // try to load it.
            try
            {
                var fullName = Path.GetFullPath(name);
                return Assembly.LoadFrom(fullName);
            }
            catch
            {
            }
            return null;
        }
    }


    public abstract class BaseProvider : IQueryProvider, IQueryText
    {
        protected BaseProvider()
        {
        }

        IQueryable<S> IQueryProvider.CreateQuery<S>(Expression expression)
        {
            return new Query<S>(this, expression);
        }

        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            Type elementType = TypeHelper.GetElementType(expression.Type);
            try
            {
                return (IQueryable)Activator.CreateInstance(typeof(Query<>).MakeGenericType(elementType), new object[] { this, expression });
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        S IQueryProvider.Execute<S>(Expression expression)
        {
            return (S)this.Execute(expression);
        }

        object IQueryProvider.Execute(Expression expression)
        {
            return this.Execute(expression);
        }

        public abstract string GetQueryText(Expression expression);
        public abstract object Execute(Expression expression);
    }



}
