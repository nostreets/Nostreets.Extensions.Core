using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Web.Http;
using System.Web.Http.Controllers;
using System.Web.Http.Dispatcher;

namespace Nostreets.Extensions.Helpers.Web
{
    public class AssemblyResolver : IAssembliesResolver
    {
        private string _assemblyName;

        public AssemblyResolver(string assemblyName) {
            _assemblyName = assemblyName;
        }

        public ICollection<Assembly> GetAssemblies()
        {
            List<Assembly> assemblies = AppDomain.CurrentDomain.GetAssemblies().ToList();
            Assembly controllersAssembly = Assembly.Load(_assemblyName);
            assemblies.Add(controllersAssembly);


            return assemblies;
        }
    }

    public class ControllerSelector : DefaultHttpControllerSelector
    {
        private HttpConfiguration _configuration;
        private string _assemblyName;

        public ControllerSelector(HttpConfiguration configuration, string assemblyName) : base(configuration)
        {
            _configuration = configuration;
            _assemblyName = assemblyName;
        }


        public override HttpControllerDescriptor SelectController(HttpRequestMessage request)
        {
            Assembly assembly = Assembly.LoadFile(_assemblyName);
            Type[] types = assembly.GetTypes();
            List<Type> matchedTypes = types.Where(i => typeof(IHttpController).IsAssignableFrom(i)).ToList();

            string controllerName = base.GetControllerName(request);
            var matchedController = matchedTypes.FirstOrDefault(i => i.Name.ToLower() == controllerName.ToLower() + "controller");

            return new HttpControllerDescriptor(_configuration, controllerName, matchedController);
        }
    }

}
