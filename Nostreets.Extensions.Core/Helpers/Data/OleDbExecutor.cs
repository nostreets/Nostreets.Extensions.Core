using Nostreets.Extensions.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nostreets.Extensions.Helpers.Data
{
    internal sealed class OleDbExecutor : IOleDbExecutor
    {
        private static OleDbExecutor _instance = null;
        private const string LOG_CAT = "DAO";

        private OleDbExecutor() { }

        static OleDbExecutor()
        {
            _instance = new OleDbExecutor();
        }

        public static OleDbExecutor Instance
        {
            get
            {
                return _instance;
            }
        }

        public void ExecuteCmd(Func<OleDbConnection> dataSouce,
            string cmdText,
            Action<OleDbParameterCollection> inputParamMapper,
            Action<IDataReader, short> map,
            Action<OleDbParameterCollection> returnParameters = null,
            Action<OleDbCommand> cmdModifier = null,
            int? timeOutSpan = null)
        {
            if (map == null)
                throw new NullReferenceException("ObjectMapper is required.");

            OleDbDataReader reader = null;
            OleDbCommand cmd = null;
            OleDbConnection conn = null;
            short resultSet = 0;
            try
            {

                using (conn = dataSouce())
                {
                    if (conn != null)
                    {

                        if (conn.State != ConnectionState.Open)
                            conn.Open();

                        cmd = GetCommand(conn, cmdText, inputParamMapper);

                        if (cmd != null)
                        {
                            reader = cmd.ExecuteReader();

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


        public int ExecuteNonQuery(Func<OleDbConnection> dataSouce, 
            string cmdText,
            Action<OleDbParameterCollection> inputParamMapper, 
            Action<OleDbParameterCollection> returnParameters = null,
            Action<OleDbCommand> cmdModifier = null,
            int? timeOutSpan = null)
        {
            OleDbCommand cmd = null;
            OleDbConnection conn = null;
            try
            {

                using (conn = dataSouce())
                {
                    if (conn != null)
                    {
                        if (conn.State != ConnectionState.Open)
                            conn.Open();

                        cmd = GetCommand(conn, cmdText, inputParamMapper);

                        if (cmd != null)
                        {
                            int returnValue = cmd.ExecuteNonQuery();

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

        public OleDbCommand GetCommand(OleDbConnection conn, string cmdText = null, Action<OleDbParameterCollection> paramMapper = null)
        {
            OleDbCommand cmd = null;

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

            cmd.CommandType = CommandType.Text;

            return cmd;

        }

    }
}
