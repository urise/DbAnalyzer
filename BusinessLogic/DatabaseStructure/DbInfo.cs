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

        public IDataReader GetTables()
        {
            return DALManager.Connect(_connectionString).ExecuteReader("select * from sys.tables", CommandType.Text);
        }

        #endregion

        #region Private Methods

        #endregion
    }
}
