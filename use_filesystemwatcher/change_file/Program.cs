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

            FileChanger fileChanger = new FileChanger();
            fileChanger.Logger = logger;
            fileChanger.FilePath = appSettings.Settings["file_path"].Value.ToString();
            fileChanger.LoopIntervalMs = int.Parse(appSettings.Settings["loop_interval_ms"].Value.ToString());
            fileChanger.SetMode(int.Parse(appSettings.Settings["mode"].Value.ToString()));

            fileChanger.Start();

            while (true)
            {
                string s = Console.ReadLine();
                if (s == "exit")
                {
                    fileChanger.Close();
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
