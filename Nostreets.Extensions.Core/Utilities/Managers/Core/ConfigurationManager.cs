using System.Linq;
using Microsoft.Extensions.Configuration;
using Nostreets.Extensions.Extend.Basic;
using Nostreets.Extensions.Extend.Core.Config;
using System.Collections.Specialized;


namespace Nostreets.Extensions.Utilities.Managers.Core
{
    public class ConfigurationManager
    {
        public static void SetUp(IConfiguration configuration)
        {

            var appSettings = configuration.GetSection("AppSettings");
            var connctionStrings = configuration.GetSection("ConnctionStrings");

            if (appSettings != default(IConfigurationSection))
                AppSettings = appSettings.ToDictionary().ToNameValueCollection();

            if (appSettings != default(IConfigurationSection))
                ConnectionStrings = connctionStrings.ToDictionary().ToNameValueCollection();

        }

        public static NameValueCollection AppSettings { get; set; }
        public static NameValueCollection ConnectionStrings { get; set; }


    }
}
