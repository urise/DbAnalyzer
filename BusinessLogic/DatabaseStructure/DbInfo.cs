using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sampo.Generics.DAL;

namespace BusinessLogic.DatabaseStructure
{
    public class DbInfo
    {
        #region Fields and Properties

        private string _connectionString;

        #endregion

        #region Constructors

        public DbInfo(string connectionString)
        {
            _connectionString = connectionString;
        }

        #endregion

        #region Public Methods

        public int GetTablesCount()
        {
            return DALManager.Connect(_connectionString).ExecuteScalar<int>("select count(*) from sys.tables", CommandType.Text);
        }

        public DataTable GetTables()
        {
            return DALManager.Connect(_connectionString).ExecuteDataTable("select name from sys.tables", CommandType.Text);
        }

        public int GetRecordsCount(string tableName)
        {
            return DALManager.Connect(_connectionString).ExecuteScalar<int>(
                "select count(*) from " + tableName, CommandType.Text);
        }

        public List<TableInfo> GetTableInfos()
        {
            var dt = GetTables();
            var result = new List<TableInfo>();
            foreach (DataRow row in dt.Rows)
            {
                var tableInfo = new TableInfo {TableName = row["name"].ToString()};
                tableInfo.RecordsCount = GetRecordsCount(tableInfo.TableName);
                result.Add(tableInfo);
            }
            return result.OrderByDescending(r => r.RecordsCount).ToList();
        }

        #endregion

        #region Private Methods

        #endregion
    }
}
