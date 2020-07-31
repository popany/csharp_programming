using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace change_file
{
    class Program
    {
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        static void Run()
        {

            Configuration configuration = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            var appSettings = configuration.AppSettings;

            FileToucher fileToucher = new FileToucher();
            fileToucher.Logger = logger;
            fileToucher.FilePath = appSettings.Settings["file_path"].Value.ToString();
            fileToucher.LoopIntervalMs = int.Parse(appSettings.Settings["loop_interval_ms"].Value.ToString());

            fileToucher.Start();

            while (true)
            {
                string s = Console.ReadLine();
                if (s == "exit")
                {
                    fileToucher.Close();
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
