using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace use_managed_driver
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

            dbUser = appSettings.Settings["db_user"].Value.ToString();
            dbPasswd = appSettings.Settings["db_passwd"].Value.ToString();
            dbUrl = appSettings.Settings["db_url"].Value.ToString();
        }

        public string dbUser;
        public string dbPasswd;
        public string dbUrl;
    }
}
