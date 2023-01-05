using System;
using Newtonsoft.Json.Linq;
using Nostreets.Extensions.Extend.Basic;
using StackExchange.Redis;
using Microsoft.Extensions.Caching.Memory;

namespace Nostreets.Extensions.Utilities.Managers.Core
{

    public static class CacheManager
    {

        static CacheManager()
        {
            _instance = new MemoryCache(new MemoryCacheOptions());

            if (ConfigurationManager.AppSettings["Redis.Host"] != null)
                _redisCache = new RedisCache(RedisCache.GetConfigurationOptions());

        }

        private static MemoryCache _instance = null;
        private static RedisCache _redisCache = null;


        private static void Contains(string key, out bool instanceContains, out bool redisContains)
        {
            instanceContains = _instance.TryGetValue(key, out object data);
            redisContains = (_redisCache != null) ? _redisCache.Contains(key) : false;


            if (key != null)
                if (instanceContains)
                {
                    if (_redisCache != null && !redisContains)
                    {
                        _redisCache.Set(key, data, TimeSpan.FromMinutes(180));
                        redisContains = true;
                    }
                }
                else if (_redisCache != null && redisContains)
                {
                    data = _redisCache.Get(key);
                    _instance.Set(key, data, new MemoryCacheEntryOptions() { AbsoluteExpiration = DateTimeOffset.Now.AddMinutes(180) });
                    instanceContains = true;
                }

        }

        public static T Get<T>(string key)
        {
            T result = default(T);
            Contains(key, out bool instanceContains, out bool redisContains);

            if (instanceContains || redisContains)
            {
                if (instanceContains)
                {
                    object obj = _instance.Get(key);

                    if (obj.GetType() == typeof(JObject))
                        result = ((JObject)obj).ToObject<T>();

                    else if (obj.GetType() == typeof(T))
                        result = (T)obj;


                    if (_redisCache != null && !redisContains)
                        _redisCache.Set(key, result, TimeSpan.FromMinutes(180));
                }
                else
                {
                    result = _redisCache.Get<T>(key);
                    if (!instanceContains)
                        _instance.Set(key, result, new MemoryCacheEntryOptions() { AbsoluteExpiration = DateTimeOffset.Now.AddMinutes(180) });
                }
            }

            return result;
        }

        public static void Set(string key, object data, int minsTillExp = 180)
        {
            Contains(key, out bool instanceContains, out bool redisContains);


            if (_redisCache != null)
                _redisCache.Set(key, data, TimeSpan.FromMinutes(minsTillExp));


            if (!instanceContains)
                _instance.Set(key, data, new MemoryCacheEntryOptions() { AbsoluteExpiration = DateTimeOffset.Now.AddMinutes(minsTillExp) });

        }

        public static void Remove(string key)
        {
            Contains(key, out bool instanceContains, out bool redisContains);


            if (instanceContains)
                _instance.Remove(key);

            if (_redisCache != null && redisContains)
                _redisCache.Remove(key);
        }

        public static bool Contains(string key)
        {
            Contains(key, out bool instanceContains, out bool redisContains);

            return instanceContains || redisContains;
        }

    }

    #region Redis Cache

    public class RedisCache
    {
        public RedisCache(ConfigurationOptions configurationOptions)
        {
            _redisConfig = configurationOptions ?? throw new ArgumentNullException(nameof(configurationOptions));

            _multiplexer = CreateMultiplexer(_redisConfig);
        }

        public RedisCache(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));

            _multiplexer = CreateMultiplexer(_connectionString);
        }

        public IDatabase Cache => GetCache();
        public IServer Server => GetServer();
        public static ConnectionMultiplexer Connection
        {
            get
            {
                return GetRedisConnection();
            }
        }

        private static Lazy<ConnectionMultiplexer> _multiplexer = null;
        private static ConfigurationOptions _redisConfig = null;
        private static string _connectionString = null;

        public T Get<T>(string key)
        {
            T result = default(T);

            RedisValue value = Cache.StringGet(key);

            if (value.HasValue)
                result = ((string)value).JsonDeserialize<T>();

            return result;
        }

        public object Get(string key)
        {
            object result = null;

            RedisValue value = Cache.StringGet(key);
            if (value.HasValue)
                result = ((string)value).JsonDeserialize();

            return result;
        }

        public void Set(string key, object data, TimeSpan cacheTime)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            if (cacheTime == default(TimeSpan))
                throw new ArgumentNullException(nameof(cacheTime));

            if (data == null)
                return;

            Cache.StringSet(key, data.JsonSerialize(), cacheTime);
        }

        public bool Contains(string key)
        {
            bool result = false;
            result = Cache.KeyExists(key);
            return result;
        }

        public void Remove(string key)
        {
            Cache.KeyDelete(key);
        }

        public void RemoveByPattern(string pattern)
        {
            if (int.TryParse(ConfigurationManager.AppSettings["Redis.Port"], out int redisPort))
                throw new ArgumentException("Redis.Port needs to equal an int to be able to RemoveByPatternRedis()");

            var keysToRemove = Server.Keys(pattern: "*" + pattern + "*");
            foreach (var key in keysToRemove)
                Remove(key);
        }

        public void Clear()
        {
            if (int.TryParse(ConfigurationManager.AppSettings["Redis.Port"], out int redisPort))
                throw new ArgumentException("Redis.Port needs to equal an int to be able to ClearRedis()");

            var keysToRemove = Server.Keys();
            foreach (var key in keysToRemove)
                Remove(key);
        }

        public static ConfigurationOptions GetConfigurationOptions()
        {
            if (!int.TryParse(ConfigurationManager.AppSettings["Redis.Port"], out int redisPort))
                throw new ArgumentException("Redis.Port needs to equal an int to be able to GetRedisConfigurationOptions()");


            ConfigurationOptions options = new ConfigurationOptions();

            string redisHost = ConfigurationManager.AppSettings["Redis.Host"];
            string redisPassword = ConfigurationManager.AppSettings["Redis.Password"];

            options.EndPoints.Add(redisHost, redisPort);
            options.Password = redisPassword;
            options.AllowAdmin = true;
            options.Ssl = true;
            options.AbortOnConnectFail = false;
            options.SyncTimeout = int.MaxValue;
            options.ConnectRetry = 10;
            options.KeepAlive = 180;

            return options;
        }

        private static ConnectionMultiplexer GetRedisConnection()
        {
            return _multiplexer.Value;
        }

        private Lazy<ConnectionMultiplexer> CreateMultiplexer(ConfigurationOptions options)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            return new Lazy<ConnectionMultiplexer>(
                () =>
                {
                    return ConnectionMultiplexer.Connect(options);
                    //return ConnectionMultiplexer.ConnectAsync(_redisConfig).Complete();
                }
            );
        }

        private Lazy<ConnectionMultiplexer> CreateMultiplexer(string connectionString)
        {
            if (connectionString == null)
                throw new ArgumentNullException(nameof(connectionString));

            return new Lazy<ConnectionMultiplexer>(
                () =>
                {
                    return ConnectionMultiplexer.Connect(_connectionString);
                }
            );
        }

        private IDatabase GetCache()
        {
            return Connection.GetDatabase();
        }

        private IServer GetServer()
        {
            if (int.TryParse(ConfigurationManager.AppSettings["Redis.Port"], out int redisPort))
                throw new ArgumentException("Redis.Port needs to equal an int to be able to ClearRedis()");

            IServer server = Connection.GetServer(_redisConfig.SslHost, redisPort);
            return server;
        }
    }

    #endregion

}