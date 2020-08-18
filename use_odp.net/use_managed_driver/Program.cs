using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace use_managed_driver
{
    class Program
    {
        static void Main(string[] args)
        {
            ManagedDatabaseAccessor managedDatabaseAccessor = new ManagedDatabaseAccessor();
            managedDatabaseAccessor.WriteTable();
        }
    }
}
