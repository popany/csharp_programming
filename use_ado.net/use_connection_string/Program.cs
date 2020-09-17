using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings
namespace use_connection_string
{
    class Program
    {
        static void ShowDataSource()
        {
            using (SqlConnection connection = new SqlConnection(Config.ConnectionString))
            {
                connection.Open();
                Console.WriteLine("ServerVersion: {0}", connection.ServerVersion);
                Console.WriteLine("DataSource: {0}", connection.DataSource);
            }
        }

        static void Main(string[] args)
        {
            ShowDataSource();
            Console.Read();
        }
    }
}
