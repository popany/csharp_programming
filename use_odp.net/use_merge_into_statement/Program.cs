using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace use_merge_into_statement
{
    // https://www.c-sharpcorner.com/UploadFile/87b416/dynamically-create-a-class-at-runtime/
    public class MyClassBuilder
    {
        AssemblyName asemblyName;
        public MyClassBuilder(string ClassName)
        {
            this.asemblyName = new AssemblyName(ClassName);
        }
        public object CreateObject(string[] PropertyNames, Type[] Types)
        {
            if (PropertyNames.Length != Types.Length)
            {
                Console.WriteLine("The number of property names should match their corresopnding types number");
            }

            TypeBuilder DynamicClass = this.CreateClass();
            this.CreateConstructor(DynamicClass);
            for (int ind = 0; ind < PropertyNames.Count(); ind++)
                CreateProperty(DynamicClass, PropertyNames[ind], Types[ind]);
            Type type = DynamicClass.CreateType();

            return Activator.CreateInstance(type);
        }
        private TypeBuilder CreateClass()
        {
            AssemblyBuilder assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(this.asemblyName, AssemblyBuilderAccess.Run);
            ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule("MainModule");
            TypeBuilder typeBuilder = moduleBuilder.DefineType(this.asemblyName.FullName
                                , TypeAttributes.Public |
                                TypeAttributes.Class |
                                TypeAttributes.AutoClass |
                                TypeAttributes.AnsiClass |
                                TypeAttributes.BeforeFieldInit |
                                TypeAttributes.AutoLayout
                                , null);
            return typeBuilder;
        }
        private void CreateConstructor(TypeBuilder typeBuilder)
        {
            typeBuilder.DefineDefaultConstructor(MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.RTSpecialName);
        }
        private void CreateProperty(TypeBuilder typeBuilder, string propertyName, Type propertyType)
        {
            FieldBuilder fieldBuilder = typeBuilder.DefineField("_" + propertyName, propertyType, FieldAttributes.Private);

            PropertyBuilder propertyBuilder = typeBuilder.DefineProperty(propertyName, System.Reflection.PropertyAttributes.HasDefault, propertyType, null);
            MethodBuilder getPropMthdBldr = typeBuilder.DefineMethod("get_" + propertyName, MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig, propertyType, Type.EmptyTypes);
            ILGenerator getIl = getPropMthdBldr.GetILGenerator();

            getIl.Emit(OpCodes.Ldarg_0);
            getIl.Emit(OpCodes.Ldfld, fieldBuilder);
            getIl.Emit(OpCodes.Ret);

            MethodBuilder setPropMthdBldr = typeBuilder.DefineMethod("set_" + propertyName,
                  MethodAttributes.Public |
                  MethodAttributes.SpecialName |
                  MethodAttributes.HideBySig,
                  null, new[] { propertyType });

            ILGenerator setIl = setPropMthdBldr.GetILGenerator();
            Label modifyProperty = setIl.DefineLabel();
            Label exitSet = setIl.DefineLabel();

            setIl.MarkLabel(modifyProperty);
            setIl.Emit(OpCodes.Ldarg_0);
            setIl.Emit(OpCodes.Ldarg_1);
            setIl.Emit(OpCodes.Stfld, fieldBuilder);

            setIl.Emit(OpCodes.Nop);
            setIl.MarkLabel(exitSet);
            setIl.Emit(OpCodes.Ret);

            propertyBuilder.SetGetMethod(getPropMthdBldr);
            propertyBuilder.SetSetMethod(setPropMthdBldr);
        }
    }

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

        static void TestMergeIntoTableUseDataTable()
        {
            const string tableName = "T_TEST_MERGE_INTO";
            const int columnCount = 50;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            DataTable dt = CreateData(tableName, 10000, columnCount, 1);
            stopwatch.Stop();
            Console.WriteLine($"preparing data consumes: {stopwatch.ElapsedMilliseconds}ms");

            DatabaseAccessor dba = new DatabaseAccessor();
            dba.CreateTable(tableName, GetCreateTableSql(tableName, columnCount));
            dba.InsertIntoTable(dt);

            DataTable dtToMerge = CreateData(tableName, 10000, columnCount, 2);
            string[] mergeOnKeys = { "column_1", "column_2" };

            stopwatch.Start();
            dba.ExecuteSql(CreateIndexSql(tableName, mergeOnKeys));
            stopwatch.Stop();
            Console.WriteLine($"creating index consumes: {stopwatch.ElapsedMilliseconds}ms");

            ChangeDataForTestMerge(dtToMerge, new HashSet<string>(mergeOnKeys));
            dba.MergeIntoTable(dtToMerge, mergeOnKeys.ToList());
        }

        static object CreateDateType(int fieldCount)
        {
            List<string> fieldNames = new List<string>();
            List<Type> fieldTypes = new List<Type>();

            for (int i = 0; i < fieldCount; i++)
            {
                fieldNames.Add($"column_{i + 1}");
                fieldTypes.Add(typeof(string));
            }

            MyClassBuilder cb = new MyClassBuilder("DataType");
            return cb.CreateObject(fieldNames.ToArray(), fieldTypes.ToArray());
        }

        static List<object> CreateData(int dataCount, int fieldCount, int startDataIndex)
        {
            object d = CreateDateType(fieldCount);
            Type t = d.GetType();
            List<object> datas = new List<object>();

            for (int i = 0; i < dataCount; i++)
            {
                object data = Activator.CreateInstance(d.GetType());
                foreach (var p in t.GetProperties())
                {
                    p.SetValue(data, $"data_{i + startDataIndex}_{p.Name}");
                }
                datas.Add(data);
            }
            return datas;
        }

        static void ChangeDataForTestMerge(List<object> datas, HashSet<string> keys)
        {
            Type t = datas[0].GetType();
            
            foreach (var p in t.GetProperties())
            {
                if (keys.Contains(p.Name))
                {
                    continue;
                }
                
                foreach (object d in datas)
                {
                    p.SetValue(d, $"{p.GetValue(d)}_merge_2");
                }
            }
        }

        static void TestMergeIntoTableUseReflection()
        {
            const int columnCount = 50;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            List<object> datas = CreateData(10000, columnCount, 1);
            stopwatch.Stop();
            Console.WriteLine($"preparing data consumes: {stopwatch.ElapsedMilliseconds}ms");

            const string tableName = "T_TEST_MERGE_INTO";
            DatabaseAccessor dba = new DatabaseAccessor();
            dba.CreateTable(tableName, GetCreateTableSql(tableName, columnCount));
            dba.InsertIntoTable(datas, tableName);

            List<object> datasToMerge = CreateData(10000, columnCount, 2);
            string[] mergeOnKeys = { "column_1", "column_2" };

            stopwatch.Start();
            dba.ExecuteSql(CreateIndexSql(tableName, mergeOnKeys));
            stopwatch.Stop();
            Console.WriteLine($"creating index consumes: {stopwatch.ElapsedMilliseconds}ms");

            ChangeDataForTestMerge(datasToMerge, new HashSet<string>(mergeOnKeys));
            dba.MergeIntoTable(datasToMerge, tableName, mergeOnKeys.ToList());
        }

        static void TestUpdateTableUseDataTable()
        {
            const string tableName = "T_TEST_MERGE_INTO";
            const int columnCount = 50;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            DataTable dt = CreateData(tableName, 10000, columnCount, 1);
            stopwatch.Stop();
            Console.WriteLine($"preparing data consumes: {stopwatch.ElapsedMilliseconds}ms");

            DatabaseAccessor dba = new DatabaseAccessor();
            dba.CreateTable(tableName, GetCreateTableSql(tableName, columnCount));
            dba.InsertIntoTable(dt);

            DataTable dtToUpdate = CreateData(tableName, 10000, columnCount, 1);
            string[] mergeOnKeys = { "column_1", "column_2" };

            stopwatch.Start();
            dba.ExecuteSql(CreateIndexSql(tableName, mergeOnKeys));
            stopwatch.Stop();
            Console.WriteLine($"creating index consumes: {stopwatch.ElapsedMilliseconds}ms");

            ChangeDataForTestMerge(dtToUpdate, new HashSet<string>(mergeOnKeys));
            dba.UpdateTable(dtToUpdate, mergeOnKeys.ToList());
        }

        static void TestDeleteInsertTableUseDataTable()
        {
            const string tableName = "T_TEST_MERGE_INTO";
            const int columnCount = 50;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            DataTable dt = CreateData(tableName, 10000, columnCount, 1);
            stopwatch.Stop();
            Console.WriteLine($"preparing data consumes: {stopwatch.ElapsedMilliseconds}ms");

            DatabaseAccessor dba = new DatabaseAccessor();
            dba.CreateTable(tableName, GetCreateTableSql(tableName, columnCount));
            dba.InsertIntoTable(dt);

            DataTable dtToInsert = CreateData(tableName, 10000, columnCount, 1);
            string[] mergeOnKeys = { "column_1", "column_2" };

            stopwatch.Start();
            dba.ExecuteSql(CreateIndexSql(tableName, mergeOnKeys));
            stopwatch.Stop();
            Console.WriteLine($"creating index consumes: {stopwatch.ElapsedMilliseconds}ms");

            ChangeDataForTestMerge(dtToInsert, new HashSet<string>(mergeOnKeys));
            dba.DeleteAndInsertTable(dtToInsert);
        }

        static string CreateIndexSql(string tableName, string[] keys)
        {
            string sql = $"create index {tableName}_index on {tableName} ({string.Join(", ", keys.Select(c=>$"{c}"))})";
            return sql;
        }

        static void Main(string[] args)
        {
#if true
            TestMergeIntoTableUseDataTable();
#endif

#if false
            TestMergeIntoTableUseReflection();
#endif

#if false
            TestUpdateTableUseDataTable();
#endif

#if false
            TestDeleteInsertTableUseDataTable();
#endif
        }
    }
}

