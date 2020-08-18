using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace use_unmanaged_driver
{
    class Program
    {
        static void Main(string[] args)
        {
            UnManagedDatabaseAccessor unManagedDatabaseAccessor = new UnManagedDatabaseAccessor();
            unManagedDatabaseAccessor.WriteTable();
        }
    }
}
