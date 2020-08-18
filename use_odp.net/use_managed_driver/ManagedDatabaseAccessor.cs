using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace use_managed_driver
{
    struct DataRow
    {
        public int id;
        public string name;
    }

    class ManagedDatabaseAccessor
    {
        const string tableName = "T_TEST_ODP";
        const string createTableSql = "create table t_test_odp (" +
            "id number," +
            "name varchar2(200))";

        OracleConnection connection = null;

        public ManagedDatabaseAccessor()
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

        bool IsTableExist()
        {
            using (OracleCommand command = connection.CreateCommand())
            {
                command.CommandText = string.Format("select table_name from user_tables where table_name = '{0}'", tableName);
                command.CommandType = System.Data.CommandType.Text;
                OracleDataReader reader = command.ExecuteReader();
                return reader.HasRows;
            }
        }

        void DropTable()
        {
            using (OracleCommand command = connection.CreateCommand())
            {
                command.CommandText = string.Format("drop table {0}", tableName);
                command.CommandType = System.Data.CommandType.Text;
                command.ExecuteNonQuery();
            }
        }

        void CreateTable()
        {
            using (OracleCommand command = connection.CreateCommand())
            {
                command.CommandText = createTableSql;
                command.CommandType = System.Data.CommandType.Text;
                command.ExecuteNonQuery();
            }
        }

        void DropCreateTable()
        {
            OpenDbConnection();
            OracleTransaction trans = connection.BeginTransaction();
            if (IsTableExist())
            {
                DropTable();
            }
            CreateTable();

            trans.Commit();
        }

        List<DataRow> GenerateData(int n)
        {
            List<DataRow> d = new List<DataRow>();
            for (int i = 0; i < n; i++)
            {
                d.Add(new DataRow
                {
                    id = i,
                    name = string.Format("name_{0}", i)
                });
            }
            return d;
        }

        Dictionary<string, OracleParameter> GetInsertParams(List<DataRow> d)
        {
            Dictionary<string, OracleParameter> paramsMap = new Dictionary<string, OracleParameter>();
            paramsMap.Add("id", new OracleParameter(":id", OracleDbType.Int32, d.Select(c => c.id).ToArray(), ParameterDirection.Input));
            paramsMap.Add("name", new OracleParameter(":name", OracleDbType.Varchar2, d.Select(c => c.name).ToArray(), ParameterDirection.Input));
            return paramsMap;
        }

        private static string GetColumnNames(List<string> columnNames) => string.Join(", ", (from string column in columnNames select column));

        private static string GetParamPlaceholders(List<string> columnNames) => string.Join(", ", (from string column in columnNames select $":{column}"));

        void InsertIntoTable(string insertCommandText, Dictionary<string, OracleParameter> insertParams, int dataCount)
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

        void WriteData()
        {
            List<DataRow> d = GenerateData(10);
            Dictionary<string, OracleParameter> insertParams = GetInsertParams(d);
            string insertCommandText = $"insert into {tableName} ({GetColumnNames(insertParams.Keys.ToList())}) values ({GetParamPlaceholders(insertParams.Keys.ToList())})";
            OpenDbConnection();
            OracleTransaction trans = connection.BeginTransaction();
            InsertIntoTable(insertCommandText, insertParams, d.Count);
            trans.Commit();
        }

        public void WriteTable()
        {
            DropCreateTable();
            WriteData();
        }
    }
}

