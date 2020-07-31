using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace monitor_file_change
{
    class Program
    {
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        static void Run()
        {
            Configuration configuration = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            var appSettings = configuration.AppSettings;

            OnFileChangeCopier onFileChangeCopier = new OnFileChangeCopier();
            onFileChangeCopier.Logger = logger;
            onFileChangeCopier.SourceFilePath = appSettings.Settings["source_file_path"].Value.ToString();
            onFileChangeCopier.TargetFilePath = appSettings.Settings["target_file_path"].Value.ToString();
            onFileChangeCopier.Start();

            while (true)
            {
                string s = Console.ReadLine();
                if (s == "exit")
                {
                    onFileChangeCopier.Close();
                    break;
                }
            }
        }

        static void Main(string[] args)
        {
            Run();
        }
    }
}
