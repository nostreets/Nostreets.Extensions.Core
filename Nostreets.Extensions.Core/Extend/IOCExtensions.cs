using Castle.Windsor;
using Nostreets.Extensions.Extend.Basic;
using System;
using System.Linq;
using System.Reflection;
using Unity;
using Unity.Resolution;

namespace Nostreets.Extensions.Extend.IOC
{
    public static class IOCExtensions
    {
        public static object SafeResolve(this Type type, IUnityContainer containter)
        {
            bool canResovle = false;
            ParameterInfo[] parameters = type.GetConstructors()[0].GetParameters();
            foreach (var par in parameters)
            {
                bool matched = false;
                foreach (var reg in containter.Registrations)
                    if (!matched && par.ParameterType == reg.RegisteredType)
                        matched = true;

                if (!matched)
                    break;
                else if (parameters[parameters.Length - 1] == par)
                    canResovle = true;
            }

            return (canResovle || parameters.Length == 0) ? containter.Resolve(type) : type.Instantiate();
        }

        public static object WindsorResolve(this Type type, IWindsorContainer containter)
        {
            return containter.Resolve(type);
        }

        public static object WindsorResolve(this object obj, IWindsorContainer containter)
        {
            bool resolved = obj.GetType().TryWindsorResolve(containter, out object instance);
            return resolved ? instance : null;
        }

        public static T WindsorResolve<T>(this T obj, IWindsorContainer containter)
        {
            bool resolved = typeof(T).TryWindsorResolve(containter, out object instance);
            return resolved ? (T)instance : default(T);
        }

        public static T WindsorResolve<T>(this T obj, Assembly assembly)
        {
            IWindsorContainer containter = assembly.GetWindsorContainer();
            bool resolved = typeof(T).TryWindsorResolve(containter, out object instance);
            return resolved ? (T)instance : default(T);
        }

        public static object WindsorResolve(this Type type, Assembly assembly)
        {
            IWindsorContainer containter = assembly.GetWindsorContainer();
            return containter.Resolve(type);
        }

        public static object WindsorResolve(this object obj, Assembly assembly)
        {
            IWindsorContainer containter = assembly.GetWindsorContainer();
            bool resolved = obj.GetType().TryWindsorResolve(containter, out object instance);
            return resolved ? instance : null;
        }

        public static T WindsorResolve<T>(this T obj, string assemblyName)
        {
            IWindsorContainer containter = assemblyName.GetWindsorContainer();
            bool resolved = typeof(T).TryWindsorResolve(containter, out object instance);
            return resolved ? (T)instance : default(T);
        }

        public static object WindsorResolve(this Type type, string assemblyName)
        {
            IWindsorContainer containter = assemblyName.GetWindsorContainer();
            return containter.Resolve(type);
        }

        public static object WindsorResolve(this object obj, string assemblyName)
        {
            IWindsorContainer containter = assemblyName.GetWindsorContainer();
            bool resolved = obj.GetType().TryWindsorResolve(containter, out object instance);
            return resolved ? instance : null;
        }

        public static bool TryWindsorResolve(this Type type, IWindsorContainer containter, out object instance)
        {
            bool result = false;
            try
            {
                instance = containter.Resolve(type);
                result = true;
            }
#pragma warning disable CS0168 // Variable is declared but never used
            catch (Exception ex)
#pragma warning restore CS0168 // Variable is declared but never used
            {
                instance = null;
            }
            return result;
        }

        public static bool TryUnityResolve(this Type type, IUnityContainer containter, out object instance)
        {
            bool result = false;
            try
            {
                instance = containter.Resolve(type);
                result = true;
            }
            catch (Exception)
            {
                instance = null;
            }
            return result;
        }

        public static object UnityResolve(this Type type, IUnityContainer containter)
        {
            return containter.Resolve(type);
        }

        public static object UntityResolve(this Type type, IUnityContainer container, object overrideObject = null)
        {
            PropertyInfo[] properties = overrideObject?.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);

            ResolverOverride[] overridesArray = properties?.Select(p => new ParameterOverride(p.Name, p.GetValue(overrideObject, null))).Cast<ResolverOverride>().ToArray();

            return (overrideObject != null)
                    ? container.Resolve(type, overridesArray)
                    : container.Resolve(type);
        }

        public static IUnityContainer GetUnityContainer(this string assemblyName)
        {
            MethodInfo methodInfo = (MethodInfo)"UnityConfig.GetContainer".ScanAssembliesForObject(out Assembly assembly, assemblyName);
            object unityConfig = assembly.GetTypeWithAssembly("UnityConfig");
            UnityContainer result = (UnityContainer)methodInfo?.Invoke(unityConfig, null);
            return result;
        }

        public static IUnityContainer GetUnityContainer(this Assembly assembly)
        {
            MethodInfo methodInfo = (MethodInfo)"UnityConfig.GetContainer".ScanAssembliesForObject(out Assembly _assembly, assembly.GetName().Name);
            object unityConfig = assembly.GetTypeWithAssembly("UnityConfig");
            UnityContainer result = (UnityContainer)methodInfo?.Invoke(unityConfig, null);
            return result;
        }

        public static IWindsorContainer GetWindsorContainer(this string assemblyName)
        {
            MethodInfo methodInfo = (MethodInfo)"WindsorConfig.GetContainer".ScanAssembliesForObject(out Assembly assembly, assemblyName);
            object windsorConfig = assembly.GetTypeWithAssembly("WindsorConfig");
            WindsorContainer result = (WindsorContainer)methodInfo?.Invoke(windsorConfig, null);
            return result;
        }

        public static IWindsorContainer GetWindsorContainer(this Assembly assembly)
        {
            MethodInfo methodInfo = (MethodInfo)"WindsorConfig.GetContainer".ScanAssembliesForObject(out Assembly _assembly, assembly.GetName().Name);
            object windsorConfig = assembly.GetTypeWithAssembly("WindsorConfig");
            WindsorContainer result = (WindsorContainer)methodInfo?.Invoke(windsorConfig, null);
            return result;
        }
    }
}