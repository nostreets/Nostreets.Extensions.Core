using Nostreets.Extensions.Interfaces;
using System;
using System.Data;
using System.Data.SqlClient;

namespace Nostreets.Extensions.Helpers.Data
{
    internal sealed class SqlExecutor : ISqlExecutor
    {
        private static SqlExecutor _instance = null;
        private int _numOfCalls = 0;

        private SqlExecutor() { }

        static SqlExecutor()
        {
            _instance = new SqlExecutor();
        }

        public static SqlExecutor Instance
        {
            get
            {
                return _instance;
            }
        }

        public int NumberOfSqlCalls { get { return _numOfCalls; } }

       public void ExecuteCmd(Func<SqlConnection> dataSouce,
            string storedProc,
            Action<SqlParameterCollection> inputParamMapper,
            Action<IDataReader, short> map,
            Action<SqlParameterCollection> returnParameters = null,
            Action<SqlCommand> cmdModifier = null,
            CommandBehavior cmdBehavior = default(CommandBehavior),
            int? timeOutSpan = null)
        {
            if (map == null)
                throw new NullReferenceException("ObjectMapper is required.");

            SqlDataReader reader = null;
            SqlCommand cmd = null;
            SqlConnection conn = null;
            short resultSet = 0;
            try
            {

                using (conn = dataSouce())
                {
                    if (conn != null)
                    {

                        if (conn.State != ConnectionState.Open)
                            conn.Open();

                        cmd = GetCommand(conn, storedProc, inputParamMapper);
                        if (cmd != null)
                        {
                            if (cmdModifier != null)
                                cmdModifier(cmd);


                            if (cmdBehavior == default(CommandBehavior)) { cmdBehavior = CommandBehavior.CloseConnection;  }
                            reader = cmd.ExecuteReader(cmdBehavior);
                            ++_numOfCalls;


                            while (true)
                            {

                                while (reader.Read())
                                {
                                    if (map != null)
                                        map(reader, resultSet);
                                }

                                resultSet += 1;

                                if (reader.IsClosed || !reader.NextResult())
                                    break;

                                if (resultSet > 10)
                                {
                                    throw new Exception("Too many result sets returned");
                                }
                            }

                            reader.Close();

                            if (returnParameters != null)
                                returnParameters(cmd.Parameters);

                            if (conn.State != ConnectionState.Closed)
                                conn.Close();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (reader != null && !reader.IsClosed)
                    reader.Close();

                if (conn != null && conn.State != ConnectionState.Closed)
                    conn.Close();
            }


        }


        public int ExecuteNonQuery(Func<SqlConnection> dataSouce, string storedProc,
            Action<SqlParameterCollection> inputParamMapper,
            Action<SqlParameterCollection> returnParameters = null,
            Action<SqlCommand> cmdModifier = null,
            int? timeOutSpan = null)
        {
            SqlCommand cmd = null;
            SqlConnection conn = null;
            SqlDataAdapter adapter = null;
            try
            {

                using (conn = dataSouce())
                {
                    if (conn != null)
                    {

                        if (conn.State != ConnectionState.Open)
                            conn.Open();

                        cmd = GetCommand(conn, storedProc, inputParamMapper);
                        cmdModifier?.Invoke(cmd);

                        if (timeOutSpan != null)
                        {

                            adapter = new SqlDataAdapter(cmd);
                            adapter.SelectCommand.CommandTimeout = timeOutSpan.Value;
                        }

                        if (cmd != null)
                        {
                            int returnValue = cmd.ExecuteNonQuery();
                            ++_numOfCalls;


                            if (conn.State != ConnectionState.Closed)
                                conn.Close();

                            if (returnParameters != null)
                                returnParameters(cmd.Parameters);

                            return returnValue;
                        }
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

            return -1;

        }

        public SqlCommand GetCommand(SqlConnection conn, string cmdText = null, Action<SqlParameterCollection> paramMapper = null)
        {
            SqlCommand cmd = null;

            if (conn != null)
                cmd = conn.CreateCommand();

            if (cmd != null)
            {
                if (!String.IsNullOrEmpty(cmdText))
                {
                    cmd.CommandText = cmdText;
                    cmd.CommandType = CommandType.StoredProcedure;
                }

                if (paramMapper != null)
                    paramMapper(cmd.Parameters);
            }

            return cmd;

        }

        public IDbCommand GetCommand(IDbConnection conn, string cmdText = null, Action<IDataParameterCollection> paramMapper = null)
        {
            IDbCommand cmd = null;

            if (conn != null)
                cmd = conn.CreateCommand();

            if (cmd != null)
            {
                if (!String.IsNullOrEmpty(cmdText))
                {
                    cmd.CommandText = cmdText;
                    cmd.CommandType = CommandType.StoredProcedure;
                }

                if (paramMapper != null)
                    paramMapper(cmd.Parameters);
            }

            return cmd;

        }




    }

}
