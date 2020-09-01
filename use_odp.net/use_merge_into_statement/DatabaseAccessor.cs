using Microsoft.SqlServer.Server;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace use_merge_into_statement
{
    class Utils
    {
        // https://stackoverflow.com/questions/1583150/c-oracle-data-type-equivalence-with-oracledbtype
        public static OracleDbType GetOracleDbType(object o)
        {
            if (o is string) return OracleDbType.Varchar2;
            if (o is DateTime) return OracleDbType.Date;
            if (o is Int64) return OracleDbType.Int64;
            if (o is Int32) return OracleDbType.Int32;
            if (o is Int16) return OracleDbType.Int16;
            if (o is sbyte) return OracleDbType.Byte;
            if (o is byte) return OracleDbType.Int16;
            if (o is decimal) return OracleDbType.Decimal;
            if (o is float) return OracleDbType.Single;
            if (o is double) return OracleDbType.Double;
            if (o is byte[]) return OracleDbType.Blob;

            return OracleDbType.Varchar2;
        }

        public static OracleDbType GetOracleDbType(Type t)
        {
            if (t == typeof(string)) return OracleDbType.Varchar2;
            if (t == typeof(DateTime)) return OracleDbType.Date;
            if (t == typeof(Int64)) return OracleDbType.Int64;
            if (t == typeof(Int32)) return OracleDbType.Int32;
            if (t == typeof(Int16)) return OracleDbType.Int16;
            if (t == typeof(sbyte)) return OracleDbType.Byte;
            if (t == typeof(byte)) return OracleDbType.Int16;
            if (t == typeof(decimal)) return OracleDbType.Decimal;
            if (t == typeof(float)) return OracleDbType.Single;
            if (t == typeof(double)) return OracleDbType.Double;
            if (t == typeof(byte[])) return OracleDbType.Blob;

            return OracleDbType.Varchar2;
        }
    }

    class DatabaseAccessor
    {
        OracleConnection connection = null;

        public DatabaseAccessor()
        {
            InitDbConnection();
        }

        void InitDbConnection()
        {
            OracleConnectionStringBuilder connectionStringBuilder = new OracleConnectionStringBuilder
            {
                UserID = Config.Instance.dbUser,
                Password = Config.Instance.dbPasswd,
                DataSource = Config.Instance.dbUrl,
            };
            connection = new OracleConnection(connectionStringBuilder.ConnectionString);
        }

        void OpenDbConnection()
        {
            if (connection != null && connection.State != System.Data.ConnectionState.Open)
            {
                connection.Open();
            }
        }

        void CloseDbConnection()
        {
            if (connection != null)
            {
                connection.Close();
            }
        }

        bool IsTableExist(string tableName)
        {
            using (OracleCommand command = connection.CreateCommand())
            {
                command.CommandText = string.Format("select table_name from user_tables where table_name = '{0}'", tableName);
                command.CommandType = System.Data.CommandType.Text;
                OracleDataReader reader = command.ExecuteReader();
                return reader.HasRows;
            }
        }

        void DropTable(string tableName)
        {
            using (OracleCommand command = connection.CreateCommand())
            {
                command.CommandText = string.Format("drop table {0}", tableName);
                command.CommandType = System.Data.CommandType.Text;
                command.ExecuteNonQuery();
            }
        }

        void CreateTable(string createTableSql)
        {
            using (OracleCommand command = connection.CreateCommand())
            {
                command.CommandText = createTableSql;
                command.CommandType = System.Data.CommandType.Text;
                command.ExecuteNonQuery();
            }
        }

        void DropCreateTable(string tableName, string createTableSql)
        {
            OpenDbConnection();
            OracleTransaction trans = connection.BeginTransaction();
            if (IsTableExist(tableName))
            {
                DropTable(tableName);
            }
            CreateTable(createTableSql);

            trans.Commit();
        }

        Dictionary<string, OracleParameter> GetInsertParams(DataTable dt)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            Dictionary<string, OracleParameter> paramsMap = new Dictionary<string, OracleParameter>();
            foreach (DataColumn dc in dt.Columns)
            {
                OracleDbType dataType = Utils.GetOracleDbType(dc.DataType);
                object datas = dt.AsEnumerable().Select(c => c[dc.ColumnName]).ToArray();
                paramsMap.Add(dc.ColumnName, new OracleParameter($":{dc.ColumnName}", dataType, datas, ParameterDirection.Input));
            }
            stopwatch.Stop();
            Console.WriteLine($"preparing parameter consumes: {stopwatch.ElapsedMilliseconds}ms");
            return paramsMap;
        }

        private static string GetColumnNames(List<string> columnNames) => string.Join(", ", (from string column in columnNames select column));

        private static string GetParamPlaceholders(List<string> columnNames) => string.Join(", ", (from string column in columnNames select $":{column}"));

        private static string GetMergeSelectParamPlaceholders(List<string> columnNames) => string.Join(", ", (from string column in columnNames select $":{column} as {column}"));

        private static string GetMergeUpdateParam(string tableA, string tableB, HashSet<string> keys, List<string> columnNames) => string.Join(", ", columnNames.Where(c => !keys.Contains(c)).Select(c => $"{tableA}.{c}={tableB}.{c}"));

        private static string GetMergeInsertParam(string table, List<string> columnNames) => string.Join(", ", columnNames.Select(c => $"{table}.{c}"));

        private static string GetUpdateParam(string table, HashSet<string> keys, List<string> columnNames) => string.Join(", ", columnNames.Where(c => !keys.Contains(c)).Select(c => $"{table}.{c}=:{c}"));

        void ExecuteInsert(string insertCommandText, Dictionary<string, OracleParameter> insertParams, int dataCount)
        {
            using (OracleCommand command = connection.CreateCommand())
            {
                command.CommandText = insertCommandText;
                command.ArrayBindCount = dataCount;
                command.CommandType = CommandType.Text;
                command.BindByName = true;
                foreach (var e in insertParams)
                {
                    command.Parameters.Add(e.Value);
                }
                command.ExecuteNonQuery();
            }
        }

        public void InsertIntoTable(string tableName, Dictionary<string, OracleParameter> insertParams, int count)
        {
            string insertCommandText = $"insert into {tableName} ({GetColumnNames(insertParams.Keys.ToList())}) values ({GetParamPlaceholders(insertParams.Keys.ToList())})";
            OpenDbConnection();
            OracleTransaction trans = connection.BeginTransaction();
            ExecuteInsert(insertCommandText, insertParams, count);
            trans.Commit();
        }

        public void MergeIntoTable(string tableName, Dictionary<string, OracleParameter> insertParams, int count, List<string> keys)
        {
            HashSet<string> keySet = new HashSet<string>(keys);
            string mergeCommandText = $"merge into {tableName} a using (select {GetMergeSelectParamPlaceholders(insertParams.Keys.ToList())} from dual) b " +
                $"on ({string.Join(" and ", keys.Select(c => $"a.{c}=b.{c}"))}) " +
                $"when matched then " +
                $"update set {GetMergeUpdateParam("a", "b", keySet, insertParams.Keys.ToList())} " +
                $"when not matched then " +
                $"insert ({GetMergeInsertParam("a", insertParams.Keys.ToList())}) " +
                $"values ({GetMergeInsertParam("b", insertParams.Keys.ToList())})";

            OpenDbConnection();
            OracleTransaction trans = connection.BeginTransaction();
            ExecuteInsert(mergeCommandText, insertParams, count);
            trans.Commit();
        }

        public void CreateTable(string tableName, string createTableSql)
        {
            DropCreateTable(tableName, createTableSql);
        }

        public void InsertIntoTable(DataTable dt)
        {
            Dictionary<string, OracleParameter> insertParams = GetInsertParams(dt);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            InsertIntoTable(dt.TableName, insertParams, dt.Rows.Count);
            stopwatch.Stop();
            Console.WriteLine($"inserting table consumes: {stopwatch.ElapsedMilliseconds}ms");
        }

        public void MergeIntoTable(DataTable dt, List<string> keys)
        {
            Dictionary<string, OracleParameter> insertParams = GetInsertParams(dt);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            MergeIntoTable(dt.TableName, insertParams, dt.Rows.Count, keys);
            stopwatch.Stop();
            Console.WriteLine($"merging table consumes: {stopwatch.ElapsedMilliseconds}ms");
        }

        public void UpdateTable(string tableName, Dictionary<string, OracleParameter> insertParams, int count, List<string> keys)
        {
            HashSet<string> keySet = new HashSet<string>(keys);
            string updateCommandText = $"update {tableName} a " +
                $"set {GetUpdateParam("a", keySet, insertParams.Keys.ToList())} " +
                $"where {string.Join(" and ", keys.Select(c => $"a.{c}=:{c}"))}";

            OpenDbConnection();
            OracleTransaction trans = connection.BeginTransaction();
            ExecuteInsert(updateCommandText, insertParams, count);
            trans.Commit();
        }

        public void UpdateTable(DataTable dt, List<string> keys)
        {
            Dictionary<string, OracleParameter> insertParams = GetInsertParams(dt);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            UpdateTable(dt.TableName, insertParams, dt.Rows.Count, keys);
            stopwatch.Stop();
            Console.WriteLine($"update table consumes: {stopwatch.ElapsedMilliseconds}ms");
        }

        void DeleteFromTable(string tableName)
        {
            using (OracleCommand command = connection.CreateCommand())
            {
                command.CommandText = string.Format("delete from {0}", tableName);
                command.CommandType = System.Data.CommandType.Text;
                command.ExecuteNonQuery();
            }
        }

        public void DeleteAndInsertTable(string tableName, Dictionary<string, OracleParameter> insertParams, int count)
        {
            string insertCommandText = $"insert into {tableName} ({GetColumnNames(insertParams.Keys.ToList())}) values ({GetParamPlaceholders(insertParams.Keys.ToList())})";
            OpenDbConnection();
            OracleTransaction trans = connection.BeginTransaction();
            DeleteFromTable(tableName);
            ExecuteInsert(insertCommandText, insertParams, count);
            trans.Commit();
        }

        public void DeleteAndInsertTable(DataTable dt)
        {
            Dictionary<string, OracleParameter> insertParams = GetInsertParams(dt);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            DeleteAndInsertTable(dt.TableName, insertParams, dt.Rows.Count);
            stopwatch.Stop();
            Console.WriteLine($"inserting table consumes: {stopwatch.ElapsedMilliseconds}ms");
        }

        Dictionary<string, OracleParameter> GetInsertParams(List<object> datas)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            Dictionary<string, List<object>> dataMap = new Dictionary<string, List<object>>();

            Type t = datas[0].GetType();
            foreach (var p in t.GetProperties())
            {
                dataMap[p.Name] = new List<object>();
            }
            
            foreach (object d in datas)
            {
                foreach (var p in t.GetProperties())
                {
                    dataMap[p.Name].Add(p.GetValue(d));
                }
            }

            Dictionary<string, OracleParameter> paramsMap = new Dictionary<string, OracleParameter>();
            foreach (var k in dataMap)
            {
                OracleDbType dataType = Utils.GetOracleDbType(k.Value[0]);
                paramsMap.Add(k.Key, new OracleParameter($":{k.Key}", dataType, k.Value.ToArray(), ParameterDirection.Input));
            }

            stopwatch.Stop();
            Console.WriteLine($"preparing parameter consumes: {stopwatch.ElapsedMilliseconds}ms");
            return paramsMap;
        }

        public void InsertIntoTable(List<object> datas, string tableName)
        {
            Dictionary<string, OracleParameter> insertParams = GetInsertParams(datas);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            InsertIntoTable(tableName, insertParams, datas.Count);
            stopwatch.Stop();
            Console.WriteLine($"inserting table consumes: {stopwatch.ElapsedMilliseconds}ms");
        }

        public void MergeIntoTable(List<object> datas, string tableName, List<string> keys)
        {
            Dictionary<string, OracleParameter> insertParams = GetInsertParams(datas);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            MergeIntoTable(tableName, insertParams, datas.Count, keys);
            stopwatch.Stop();
            Console.WriteLine($"merging table consumes: {stopwatch.ElapsedMilliseconds}ms");
        }
        
        public void ExecuteSql(string sql)
        {
            OpenDbConnection();
            OracleTransaction trans = connection.BeginTransaction();
            using (OracleCommand command = connection.CreateCommand())
            {
                command.CommandText = sql;
                command.CommandType = System.Data.CommandType.Text;
                command.ExecuteNonQuery();
            }
            trans.Commit();
        }
    }
}

