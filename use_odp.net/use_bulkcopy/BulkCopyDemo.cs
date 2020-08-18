using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace use_bulkcopy
{
    public class BulkCopyDemo
    {
        const string tableName = "T_TEST_ODP";
        const string createTableSql = "create table t_test_odp (" +
            "ID number," +
            "NAME varchar2(200))";

        OracleConnection connection = null;

        public BulkCopyDemo()
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

        DataTable GenerateData(int n)
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("ID");
            dt.Columns.Add("NAME");
            dt.Columns["ID"].DataType = typeof(Int32);

            for (int i = 0; i < n; i++)
            {
                DataRow dr = dt.NewRow();
                dr["ID"] = i;
                dr["NAME"] = "name_" + i;
                dt.Rows.Add(dr);
            }
            return dt;
        }

        void WriteData()
        {
            DataTable dt = GenerateData(10000);
            OpenDbConnection();
            Stopwatch stopwatch = new Stopwatch();
            using (var bulkCopy = new Oracle.DataAccess.Client.OracleBulkCopy(connection, Oracle.DataAccess.Client.OracleBulkCopyOptions.UseInternalTransaction))
            {
                bulkCopy.DestinationTableName = tableName;
                bulkCopy.BulkCopyTimeout = 600;
                stopwatch.Start();
                bulkCopy.WriteToServer(dt);
                stopwatch.Stop();
            }
            Console.WriteLine("time consumed: {0}ms", stopwatch.ElapsedMilliseconds);
        }

        public void WriteTable()
        {
            DropCreateTable();
            WriteData();
        }
    }
}
