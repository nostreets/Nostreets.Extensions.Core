using System;
using System.Data;
using System.Data.OleDb;
using System.Data.SqlClient;

namespace Nostreets.Extensions.Interfaces
{
    public interface ISqlExecutor
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataSouce">The Connection we use to get to the database we want</param>
        /// <param name="storedProc">The name of the procedure we want to execute</param>
        /// <param name="inputParamMapper"></param>
        /// <param name="map"></param>
        /// <param name="returnParameters"></param>
        /// <param name="cmdModifier"></param>
        void ExecuteCmd(
            Func<SqlConnection> dataSouce,
            string storedProc,
            Action<SqlParameterCollection> inputParamMapper,
             Action<IDataReader, short> map,

            Action<SqlParameterCollection> returnParameters = null,
            Action<SqlCommand> cmdModifier = null,
            CommandBehavior cmdBehavior = default(CommandBehavior),
            int? timeOutSpan = null);

        int ExecuteNonQuery(Func<SqlConnection> dataSouce, string query,
            Action<SqlParameterCollection> inputParamMapper,
            Action<SqlParameterCollection> returnParameters = null,
            Action<SqlCommand> cmdModifier = null,
            int? timeOutSpan = null);

        SqlCommand GetCommand(SqlConnection conn, string cmdText = null, Action<SqlParameterCollection> paramMapper = null);

        IDbCommand GetCommand(IDbConnection conn, string cmdText = null, Action<IDataParameterCollection> paramMapper = null);

    }

    public interface IOleDbExecutor
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataSouce">The Connection we use to get to the database we want</param>
        /// <param name="cmdText">The SQL text we want to execute</param>
        /// <param name="inputParamMapper"></param>
        /// <param name="map"></param>
        /// <param name="returnParameters"></param>
        /// <param name="cmdModifier"></param>
        void ExecuteCmd(
            Func<OleDbConnection> dataSouce,
            string cmdText,
            Action<OleDbParameterCollection> inputParamMapper,
            Action<IDataReader, short> map,
            Action<OleDbParameterCollection> returnParameters = null,
            Action<OleDbCommand> cmdModifier = null,
            int? timeOutSpan = null);

        int ExecuteNonQuery(Func<OleDbConnection> dataSouce, string cmdText,
            Action<OleDbParameterCollection> inputParamMapper,
            Action<OleDbParameterCollection> returnParameters = null,
            Action<OleDbCommand> cmdModifier = null,
            int? timeOutSpan = null);

        OleDbCommand GetCommand(OleDbConnection conn, string cmdText = null, Action<OleDbParameterCollection> paramMapper = null);

    }
}
