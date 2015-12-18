using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace Sampo.Generics.DAL
{
    public sealed class DALManager :
        DALDbManager<DALManager, SqlConnection, SqlCommand, SqlParameter, SqlDataAdapter, SqlDataReader>
    {
        public DALManager(string connectionString)
        {
            this.ConnectionString = connectionString;
        }

        protected override string LoggerLocation { get { return "SQL"; } }

        /// <summary>
        ///     Create instance of DALManager class and opens connection to the database;
        /// </summary>
        /// <param name="connectionString">Optional parameter for connection string to the db</param>
        /// <param name="throwExceptions">Determinate whether to re-throw exceptions or only to log them.</param>
        /// <returns>instance of DALManager class</returns>
        public static DALManager Connect(string connectionString = "", bool throwExceptions = false)
        {
            return new DALManager(connectionString).Init(connectionString, throwExceptions);
        }

        public DALManager AddParameter<T>(string parameterName, T parameterValue, SqlDbType type, string typeName = "")
        {
            SqlParameter newParam = new SqlParameter
            {
                ParameterName = parameterName,
                SqlDbType = type,
                Value = parameterValue,
                Direction = ParameterDirection.Input
            };
            if (typeName != String.Empty)
            {
                newParam.TypeName = typeName;
            }

            this.AddParameterToCommand(newParam);

            return this;
        }

        public DALManager AddOutputParameter(string parameterName, SqlDbType type, int size = 32)
        {
            SqlParameter newParam = new SqlParameter
            {
                ParameterName = parameterName,
                SqlDbType = type,
                Size = size,
                Direction = ParameterDirection.Output
            };
            this.AddParameterToCommand(newParam);

            return this;
        }
    }
}