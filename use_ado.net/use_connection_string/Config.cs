using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace use_connection_string
{
    class Config
    {
        private static readonly Config instance = new Config();

        private Config()
        {
            Reload();
        }

        public static Config Instance
        {
            get
            {
                return instance;
            }
        }

        public void Reload()
        {
            Configuration configuration = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            var appSettings = configuration.AppSettings;

            connectionString = appSettings.Settings["connection_string"].Value.ToString();
        }

        static public string ConnectionString
        {
            get
            {
                return Instance.connectionString;
            }
        }

        public string connectionString;
    }
}
