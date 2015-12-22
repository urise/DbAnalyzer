using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Sampo.Generics.DAL
{
    public abstract class DALDbManager<TManager, TConnection, TCommand, TParameter, TDataAdapter, TDataReader> : IDisposable
        where TManager : DALDbManager<TManager, TConnection, TCommand, TParameter, TDataAdapter, TDataReader>
        where TConnection : DbConnection, new()
        where TCommand : DbCommand, new()
        where TParameter : DbParameter, new()
        where TDataAdapter : DbDataAdapter, new()
        where TDataReader : DbDataReader
    {
        // ReSharper disable once StaticMemberInGenericType
        private static int _activeConnectionsCount;
        public static int ActiveConnectionsCount { get { return _activeConnectionsCount; } }

        protected abstract string LoggerLocation { get; }
        protected TConnection Connection { get; private set; }
        protected TCommand Command { get; private set; }

        private Dictionary<string, TParameter> outputParameters = new Dictionary<string, TParameter>();
        private bool executed = false;

        private Guid _connectionID;

        // ReSharper disable once StaticMemberInGenericType
        private static readonly ConcurrentDictionary<Guid, string> OpenedConnectionNames =
            new ConcurrentDictionary<Guid, string>();

        public static IEnumerable<string> OpenedConnections { get { return OpenedConnectionNames.Values; } }

        protected DALDbManager()
        {
            this.Command = new TCommand();
        }

        public string ConnectionString { get; set; }

        public bool ThrowExceptions { get; set; }

        public TDataReader DataReader { get; set; }

        protected TManager Init(string connectionString, bool throwExceptions)
        {
            if (connectionString != string.Empty)
            {
                this.ConnectionString = connectionString;
            }
            this.ThrowExceptions = throwExceptions;
            this.OpenConnection();
            return this as TManager;
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            if (this.Connection != null && this.Connection.State != ConnectionState.Closed)
            {
                this.Connection.Close();
                Interlocked.Decrement(ref _activeConnectionsCount);
                string connName;
                OpenedConnectionNames.TryRemove(this._connectionID, out connName);
            }
            if (this.Command != null)
            {
                this.Command.Parameters.Clear();
            }
            this.Command = null;
            this.Connection = null;
            this.executed = true;
        }

        /// <summary>
        /// Add parameter to current command, if last command is executed - creates new command
        /// </summary>
        public TManager AddParameter<T>(string parameterName, T parameterValue,
            ParameterDirection direction = ParameterDirection.Input)
        {
            TParameter newParam = new TParameter
            {
                ParameterName = parameterName,
                Value = parameterValue,
                Direction = direction
            };

            this.AddParameterToCommand(newParam);

            return this as TManager;
        }

        public TManager AddParameter<T>(string parameterName, T parameterValue, Predicate<T> condition,
            ParameterDirection direction = ParameterDirection.Input)
        {
            if (condition(parameterValue))
            {
                this.AddParameter(parameterName, parameterValue, direction);
            }

            return this as TManager;
        }

        public TManager AddParameterIfExists<T>(string parameterName, T parameterValue,
            ParameterDirection direction = ParameterDirection.Input) where T : class
        {
            return this.AddParameter(parameterName, parameterValue, p => p != null);
        }

        /// <summary>
        /// Add parameter to current command, if last command is executed - creates new command
        /// </summary>
        public TManager AddParameter<T>(T newParam)
            where T : TParameter
        {
            if (newParam.ParameterName == null)
            {
                throw new NoNullAllowedException("ParameterName can not be null");
            }

            this.AddParameterToCommand(newParam);

            return this as TManager;
        }

        public T GetOutputParameter<T>(string parameterName)
        {
            if (!executed)
            {
                throw new InvalidOperationException("Can't get output parameter before the query is executed.");
            }

            var val = this.outputParameters[parameterName].Value;
            return val != DBNull.Value ? (T)val : default(T);
        }

        /// <summary>
        /// Safe executes procedure, so try-catch is not needed. After execution the
        /// connection is closed.
        /// </summary>
        /// <param name="storedProcedureName">Name of the procedure</param>
        /// <param name="shouldCloseConnection"></param>
        public int ExecuteNonQuery(string storedProcedureName, bool shouldCloseConnection = true)
        {
            this.PrepareCommand(storedProcedureName);
            var result = 0;
            this.SafeExecute(() =>
            {
                result = this.Command.ExecuteNonQuery();
                this.FillOutputParameters();
            }, shouldCloseConnection);
            return result;
        }

        /// <summary>
        /// Safe executes procedure, so try-catch is not needed. After execution the
        /// connection is closed.
        /// </summary>
        /// <param name="storedProcedureName">Name of the procedure</param>
        /// <param name="shouldCloseConnection"></param>
        public async Task<int> ExecuteNonQueryAsync(string storedProcedureName, bool shouldCloseConnection = true)
        {
            this.PrepareCommand(storedProcedureName);
            var result = 0;
            await this.SafeExecuteAsync(async () =>
            {
                result = await this.Command.ExecuteNonQueryAsync();
                this.FillOutputParameters();
            }, shouldCloseConnection);
            return result;
        }

        /// <summary>
        /// Safe Executes the procedure, and returns the first column of the first row.
        /// Try-catch is not needed. After execution the connection is closed.
        /// </summary>
        public T ExecuteScalar<T>(string storedProcedureName, bool shouldCloseConnection = true)
        {
            return ExecuteScalar<T>(storedProcedureName, CommandType.StoredProcedure, shouldCloseConnection);
        }

        /// <summary>
        /// Safe Executes the command, and returns the first column of the first row.
        /// Try-catch is not needed. After execution the connection is closed.
        /// </summary>
        public T ExecuteScalar<T>(string command, CommandType commandType, bool shouldCloseConnection = true)
        {
            this.PrepareCommand(command, commandType);
            T result = default(T);
            this.SafeExecute(() =>
            {
                    result = (T)this.Command.ExecuteScalar();
                    this.FillOutputParameters();
            }, shouldCloseConnection);
            return result;
        }

        /// <summary>
        /// Safe Executes the procedure, and returns the first column of the first row.
        /// Try-catch is not needed. After execution the connection is closed.
        /// </summary>
        public async Task<T> ExecuteScalarAsync<T>(string storedProcedureName, bool shouldCloseConnection = true)
            where T : class
        {
            this.PrepareCommand(storedProcedureName);
            T result = default(T);
            await this.SafeExecuteAsync(async () =>
            {
                    result = await this.Command.ExecuteScalarAsync() as T;
                    this.FillOutputParameters();
            }, shouldCloseConnection);
            return result;
        }

        /// <summary>
        /// Safe executes procedure, so try-catch is not needed. After execution the 
        /// connection is closed.
        /// </summary>
        public DataSet ExecuteDataSet(string storedProcedureName, int commandTimeout = 30)
        {
            this.PrepareCommand(storedProcedureName, commandTimeout: commandTimeout);
            var dataAdapter = new TDataAdapter { SelectCommand = this.Command };
            var dataSet = new DataSet();
            this.SafeExecute(() =>
            {
                    dataAdapter.Fill(dataSet);
                    this.FillOutputParameters();
            });
            return dataSet;
        }

        public DataTable ExecuteDataTable(string commandName, CommandType commandType, int commandTimeout = 30)
        {
            this.PrepareCommand(commandName, commandType, commandTimeout);
            var dataAdapter = new TDataAdapter { SelectCommand = this.Command };
            var dataTable = new DataTable();
            this.SafeExecute(() =>
            {
                dataAdapter.Fill(dataTable);
                this.FillOutputParameters();
            });
            return dataTable;
        }

        public T ExecuteDataSet<T>(string storedProcedureName, string tblName, T result) where T : DataSet
        {
            this.PrepareCommand(storedProcedureName);
            var dataAdapter = new TDataAdapter { SelectCommand = this.Command };
            this.SafeExecute(() =>
            {
                    dataAdapter.Fill(result, tblName);
                    this.FillOutputParameters();
            });
            return result;
        }

        /// <summary>
        /// Unsafe executes command Reader.
        /// </summary>
        public TDataReader ExecuteReader(string commandName,
            CommandType commandType = CommandType.StoredProcedure,
            int commandTimeout = 30)
        {
            this.PrepareCommand(commandName, commandType, commandTimeout);
            this.DataReader = (TDataReader)this.Command.ExecuteReader();
            this.executed = true;
            this.FillOutputParameters();
            return this.DataReader;
        }

        /// <summary>
        /// Unsafe executes command Reader.
        /// </summary>
        public async Task<TDataReader> ExecuteReaderAsync(string commandName,
            CommandType commandType = CommandType.StoredProcedure, int commandTimeout = 30)
        {
            this.PrepareCommand(commandName, commandType, commandTimeout);
            this.DataReader = (TDataReader)await this.Command.ExecuteReaderAsync();
            this.executed = true;
            this.FillOutputParameters();
            return this.DataReader;
        }

        /// <summary>
        /// Executes a reader as enumerable and applies the given selector function.
        /// </summary>
        public IEnumerable<T> ExecuteEnumerableReader<T>(string commandName, Func<IDataRecord, T> selector,
            CommandType commandType = CommandType.StoredProcedure, int commandTimeout = 30)
        {
            using (TDataReader reader = this.ExecuteReader(commandName, commandType, commandTimeout))
            {
                    while (reader.Read())
                    {
                        yield return selector(reader);
                    }
            }
            this.Command.Parameters.Clear();
        }

        /// <summary>
        /// Executes a reader as enumerable.
        /// </summary>
        public IEnumerable<IDataRecord> ExecuteEnumerableReader(string commandName,
            CommandType commandType = CommandType.StoredProcedure, int commandTimeout = 30)
        {
            using (TDataReader reader = this.ExecuteReader(commandName, commandType, commandTimeout))
            {
                    while (reader.Read())
                    {
                        yield return reader;
                    }
            }
            this.Command.Parameters.Clear();
        }

        /// <summary>
        /// Executes a reader as enumerable.
        /// </summary>
        public async Task<IEnumerable<T>> ExecuteEnumerableReaderAsync<T>(string commandName, Func<IDataRecord, T> selector,
            CommandType commandType = CommandType.StoredProcedure, int commandTimeout = 30)
        {
            var res = new List<T>();
            using (TDataReader reader = await this.ExecuteReaderAsync(commandName, commandType, commandTimeout))
            {
                    while (await reader.ReadAsync())
                    {
                        res.Add(selector(reader));
                    }
            }

            this.Command.Parameters.Clear();
            return res;
        }

        /// <summary>
        /// Close opened Reader.
        /// </summary>
        public void CloseReader()
        {
            if (this.DataReader != null)
            {
                this.DataReader.Close();
            }
            this.Dispose();
        }

        private void OpenConnection()
        {
            this.Connection = new TConnection { ConnectionString = this.ConnectionString };
            if (this.Connection.State != ConnectionState.Open)
            {
                this.SafeExecute(() =>
                {
                    this.Connection.Open();
                    Interlocked.Increment(ref _activeConnectionsCount);
                }, false);
            }
        }

        protected void PrepareCommand(string commandName, CommandType commandType = CommandType.StoredProcedure,
            int commandTimeout = 30)
        {
            if (this.Command == null)
            {
                // Command is possible to be null if last command is executed and the connection is closed,
                // in this case we need to create new connection and command.
                // And the new command do not have parameters.
                this.OpenConnection();
                this.Command = new TCommand();
            }
            this.Command.Connection = this.Connection;
            this.Command.CommandText = commandName;
            this.Command.CommandType = commandType;
            this.Command.CommandTimeout = commandTimeout;
            OpenedConnectionNames.TryAdd(this._connectionID, commandName);
        }

        protected void AddParameterToCommand(TParameter parameter)
        {
            if ((parameter.Direction == ParameterDirection.InputOutput) &&
                (parameter.Value == null))
            {
                parameter.Value = DBNull.Value;
            }

            if (this.Command == null)
            {
                // Before execute we add parameters(if needed), so it is possible command
                // to be null if last command is executed and the connection is closed,
                // in this case we need to create new connection and command.
                this.OpenConnection();
                this.Command = new TCommand();
            }

            this.Command.Parameters.Add(parameter);
        }

        protected async Task SafeExecuteAsync(Func<Task> code, bool shouldCloseConnection = true)
        {
            try
            {
                await code();
            }
            catch (Exception ex)
            {
                if (!this.ThrowExceptions)
                {
                    //this.Log.Fatal(this.Command.CommandText + ":", ex);
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                if (shouldCloseConnection)
                {
                    this.Dispose();
                }
                else
                {
                    if (this.Command != null)
                    {
                        this.Command.Parameters.Clear();
                    }
                }
            }
        }

        protected void SafeExecute(Action code, bool shouldCloseConnection = true)
        {
            try
            {
                code();
            }
            catch (Exception ex)
            {
                if (!this.ThrowExceptions)
                {
                    //this.Log.Fatal(this.Command.CommandText + ":", ex);
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                if (shouldCloseConnection)
                {
                    this.Dispose();
                }
                else
                {
                    if (this.Command != null)
                    {
                        this.Command.Parameters.Clear();
                    }
                }
            }
        }

        private void FillOutputParameters()
        {
            this.outputParameters = this.Command.Parameters
                .Cast<TParameter>()
                .Where(p => p.Direction == ParameterDirection.Output)
                .ToDictionary(p => p.ParameterName);
        }
    }
}