using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace use_merge_into_statement
{
    class Program
    {
        static string GetCreateTableSql(string tableName, int columnCount)
        {
            string createTableSql = $"create table {tableName} (";
            for (int i = 0; i < columnCount; i++)
            {
                createTableSql += $"column_{i + 1} varchar2(200)";
                if (i + 1 < columnCount)
                {
                    createTableSql += ",";
                }
            }
            createTableSql += ")";
            return createTableSql;
        }

        static DataTable CreateData(string tableName, int dataCount, int columnCount, int startDataIndex)
        {
            DataTable dt = new DataTable(tableName);
            for (int i = 0; i < columnCount; i++)
            {
                dt.Columns.Add(new DataColumn($"column_{i + 1}", System.Type.GetType("System.String")));
            }

            for (int i = 0; i < dataCount; i++)
            {
                DataRow dr = dt.NewRow();
                for (int j = 0; j < columnCount; j++)
                {
                    dr[$"column_{j + 1}"] = $"data_{i + startDataIndex}_column_{j + 1}";
                }
                dt.Rows.Add(dr);
            }
            return dt;
        }

        static void ChangeDataForTestMerge(DataTable dt, HashSet<string> keys)
        {
            foreach (DataColumn dc in dt.Columns)
            {
                if (keys.Contains(dc.ColumnName))
                {
                    continue;
                }
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    dt.Rows[i][dc.ColumnName] += "_merge";
                }
            }
        }

        static void Main(string[] args)
        {
            const string tableName = "T_TEST_MERGE_INTO";
            const int columnCount = 3;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            DataTable dt = CreateData(tableName, 10, columnCount, 1);
            stopwatch.Stop();
            Console.WriteLine($"preparing data consumes: {stopwatch.ElapsedMilliseconds}ms");

            DatabaseAccessor dba = new DatabaseAccessor();
            dba.CreateTable(tableName, GetCreateTableSql(tableName, columnCount));
            dba.InsertIntoTable(dt);

            DataTable dtToMerge = CreateData(tableName, 10, columnCount, 2);
            string[] mergeOnKeys = { "column_1", "column_2" };
            ChangeDataForTestMerge(dtToMerge, new HashSet<string>(mergeOnKeys));
            dba.MergeIntoTable(dtToMerge, mergeOnKeys.ToList());
        }
    }
}
