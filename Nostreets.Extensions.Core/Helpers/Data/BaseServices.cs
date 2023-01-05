using Nostreets.Extensions.Interfaces;
using Nostreets.Extensions.Utilities;
using System.Configuration;
using System.Data.OleDb;
using System.Data.SqlClient;
using System.Linq;

namespace Nostreets.Extensions.Helpers.Data
{
    public abstract class SqlService : Disposable
    {
        public SqlService()
        {
            _connectionString = ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;
        }

        public SqlService(string connectionKey)
        {
            _connectionString = ConfigurationManager.ConnectionStrings[connectionKey] != null ? ConfigurationManager.ConnectionStrings[connectionKey].ConnectionString : connectionKey;
        }


        private string _connectionString = null;
        private IQueryProvider _queryProvider = null;

        public SqlConnection Connection => new SqlConnection(_connectionString);
        public static ISqlExecutor Query => DataProvider.SqlInstance;
        public IQueryProvider QueryProvider { get => _queryProvider; set => _queryProvider = value; }


        public SqlConnection ChangeSqlConnection(string connectionKey)
        {
            _connectionString = ConfigurationManager.ConnectionStrings[connectionKey].ConnectionString;
            return Connection;
        }

    }

    public abstract class OleDbService : Disposable
    {
        public OleDbService(string filePath, string OLEDBType = "ACE")
        {
            string[] splitPath = filePath.Split('.');


            if (splitPath[splitPath.Length - 1].Contains("xlsx") || OLEDBType == "ACE")
                _connectionString = string.Format("Provider=Microsoft.ACE.OLEDB.12.0; Data Source='{0}'; Extended Properties=\"Excel 12.0;HDR=YES;\"", filePath);
            else
                _connectionString = string.Format("Provider=Microsoft.Jet.OLEDB.4.0; Data Source='{0}'; Extended Properties=\"Excel 8.0;HDR=YES;\"", filePath);

        }

        private string _connectionString;
        private IQueryProvider _queryProvider = null;

        public OleDbConnection Connection => new OleDbConnection(_connectionString); 
        public IQueryProvider QueryProvider { get => _queryProvider; set => _queryProvider = value; }
        protected static IOleDbExecutor Query => DataProvider.OleDbInstance;

        public OleDbConnection ChangeSqlConnection(string connectionKey)
        {
            _connectionString = ConfigurationManager.ConnectionStrings[connectionKey].ConnectionString;
            return Connection;

        }
    }
}