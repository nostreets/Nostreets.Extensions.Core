using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

using Nostreets.Extensions.Extend.Basic;

namespace Nostreets.Extensions.Utilities
{
    public enum ClassTypes
    {
        Any = 1,
        Assembly = 2,
        Methods = 4,
        Constructors = 8,
        Properties = 16,
        OtherFields = 32,
        Type = 64,
        Parameters = 128
    }

    public class AssemblyScanner : Disposable
    {
        private List<string> skipAssemblies = null;

        public AssemblyScanner()
        {
            skipAssemblies = new List<string>();

            foreach (Assembly assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                if (assembly.FullName.Contains("System") || assembly.FullName.Contains("Microsoft")) { skipAssemblies.Add(assembly.FullName); }
            }

            skipAssemblies.Add("Unity.Mvc5");
        }

        public Dictionary<object,Assembly> ScanAssembliesForObjects(
            string nameToCheckFor
            , string[] assembliesToLookFor = null
            , string[] assembliesToSkip = null
            , ClassTypes classType = ClassTypes.Any
            , Func<dynamic, bool> predicate = null)
        {
            Dictionary<object,Assembly> result = null;

            foreach (Assembly assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                SearchForObject(assembly, nameToCheckFor, out object obj, assembliesToLookFor, assembliesToSkip, classType, predicate);

                if (result != null)
                {
                    result.Add(obj, assembly);
                }
            }

            return result;

        }


        public object ScanAssembliesForObject(
              string nameToCheckFor
            , out Assembly assembly
            , string[] assembliesToLookFor = null
            , string[] assembliesToSkip = null
            , ClassTypes classType = ClassTypes.Any
            , Func<dynamic, bool> predicate = null)
        {
            assembly = null;
            object result = null;

            foreach (Assembly _assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                SearchForObject(_assembly, nameToCheckFor, out result, assembliesToLookFor, assembliesToSkip, classType, predicate);

                if (result != null)
                {
                    assembly = _assembly;
                    break;
                }
            }

            return result;
        }

        private void SearchForObject(
              Assembly assembly
            , string nameToCheckFor
            , out object result
            , string[] assembliesToLookFor
            , string[] assembliesToSkip
            , ClassTypes classType = ClassTypes.Any
            , Func<dynamic, bool> predicate = null)
        {
            result = null;
            string[] namesToCheckFor = null;
            const BindingFlags memberInfoBinding = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance;
            bool shouldSkip = false,
                 hasPredicate = predicate != null;

            if (assembliesToLookFor == null)
                assembliesToLookFor = new string[0];

            if (assembliesToSkip == null)
                assembliesToSkip = new string[0];

            if (assembliesToSkip != null)
                foreach (var assemble in assembliesToSkip)
                    if (skipAssemblies.Find(a => a.Contains(assemble)) == null)
                        skipAssemblies.AddRange(assembliesToSkip);

            if (nameToCheckFor.Contains('.'))
                namesToCheckFor = nameToCheckFor.Split('.');
            else
                namesToCheckFor = new[] { nameToCheckFor };

            foreach (string skippedAssembly in skipAssemblies)
                if (assembly.FullName.Contains(skippedAssembly))
                    shouldSkip = true;
                else if (assembliesToLookFor.Length > 0 && !assembliesToLookFor.Any(a => assembly.FullName.Contains(a)))
                    shouldSkip = true;

            if (!shouldSkip)
            {
                //+Find Assembly
                if ((classType == ClassTypes.Assembly || classType == ClassTypes.Any) && namesToCheckFor.Any(a => assembly.FullName.Contains(a)))
                    result = assembly;

                foreach (Type type in assembly.GetTypes())
                {
                    if (namesToCheckFor.Any(a => a == type.Name))
                    {
                        #region Find Method

                        if (classType == ClassTypes.Methods || classType == ClassTypes.Any)
                            foreach (MethodInfo method in type.GetMethods(memberInfoBinding))
                            {
                                if (classType == ClassTypes.Parameters || classType == ClassTypes.Any)
                                    foreach (ParameterInfo parameter in method.GetParameters())
                                    {
                                        if (result != null)
                                            break;

                                        if (namesToCheckFor.Any(a => a == parameter.Name))
                                            result = parameter;
                                    }

                                if (result != null)
                                    break;

                                if (namesToCheckFor.Any(a => a == method.Name))
                                    result = method;
                            }

                        #endregion Find Method

                        #region Find Property

                        if (classType == ClassTypes.Properties || classType == ClassTypes.Any)
                            foreach (PropertyInfo prop in type.GetProperties())
                            {
                                if (result != null)
                                    break;

                                if (namesToCheckFor.Any(a => a == prop.Name))
                                    result = prop;
                            }

                        #endregion Find Property

                        #region Find Constructor

                        if (classType == ClassTypes.Constructors || classType == ClassTypes.Any)
                            foreach (ConstructorInfo construct in type.GetConstructors())
                            {
                                if (result != null)
                                    break;

                                if (namesToCheckFor.Any(a => a == construct.Name))
                                    result = construct;
                            }

                        #endregion Find Constructor

                        if (result != null)
                            break;

                        //+Find Type
                        if (namesToCheckFor.Any(a => a == type.Name) && classType == ClassTypes.Type || classType == ClassTypes.Any)
                            result = type;
                    }
                }

                //+Predicate Check
                if (result != null && hasPredicate)
                    result = predicate(result) ? result : null;
            }
        }
    }

    public class AttributeScanner<TAttribute> : Disposable where TAttribute : Attribute
    {
        private List<Tuple<TAttribute, object, Type, Assembly>> _targetMap;

        public AttributeScanner()
        {
            _targetMap = new List<Tuple<TAttribute, object, Type, Assembly>>();
        }

        public IEnumerable<Tuple<TAttribute, object, Type, Assembly>> ScanForAttributes(
              Assembly assembly
            , ClassTypes section = ClassTypes.Any
            , Type type = null)
        {
            if (assembly == null)
                throw new ArgumentException(nameof(assembly));

            var props = _targetMap.Where(a => type != null && a.Item3 == type);

            if (props.Count() == 0)
                if (type == null)
                    ScanAssembly(assembly, section);
                else
                    ScanType(type, section);

            return (props.Count() == 0)
                   ? _targetMap
                   : _targetMap.Where(a => type != null && a.Item3 == type);
        }

        private void Add(TAttribute attribute, object item, Type type, Assembly assembly)
        {
            _targetMap.Add(new Tuple<TAttribute, object, Type, Assembly>(attribute, item, type, assembly));
        }

        private void ScanAssembly(Assembly assembly, ClassTypes classPart = ClassTypes.Any)
        {
            if (assembly == null)
                throw new ArgumentException(nameof(assembly));

            SearchForAttributes(assembly, classPart);
        }

        private void ScanType(Type typeToScan, ClassTypes classPart)
        {
            const BindingFlags memberInfoBinding = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance;

            if (classPart == ClassTypes.Any || classPart == ClassTypes.Type)
                foreach (TAttribute attr in typeToScan.GetCustomAttributes(typeof(TAttribute)))
                    Add(attr, typeToScan, typeToScan, typeToScan.Assembly);

            foreach (MemberInfo member in typeToScan.GetMembers(memberInfoBinding))
            {
                if (member.MemberType == MemberTypes.Property && (classPart == ClassTypes.Properties | classPart == ClassTypes.Any))
                    foreach (TAttribute attr in member.GetCustomAttributes(typeof(TAttribute)))
                        Add(attr, member, typeToScan, typeToScan.Assembly);

                if (member.MemberType == MemberTypes.Method && (classPart == ClassTypes.Methods | classPart == ClassTypes.Any))
                    foreach (TAttribute attr in member.GetCustomAttributes(typeof(TAttribute)))
                        Add(attr, member, typeToScan, typeToScan.Assembly);

                if (member.MemberType == MemberTypes.Method && (classPart == ClassTypes.Parameters | classPart == ClassTypes.Any))
                    foreach (ParameterInfo parameter in ((MethodInfo)member).GetParameters())
                        foreach (TAttribute attr in parameter.GetCustomAttributes(typeof(TAttribute)))
                            Add(attr, parameter, typeToScan, typeToScan.Assembly);
            }
        }

        private void SearchForAttributes(
            Assembly assembly
            , ClassTypes classPart = ClassTypes.Any
            , Type typeToCheck = null)
        {
            bool shouldSkip = false;

            try
            {
                if (typeToCheck != null)
                    ScanType(typeToCheck, classPart);
                else if (!shouldSkip)
                {
                    if (classPart == ClassTypes.Any || classPart == ClassTypes.Assembly)
                        foreach (TAttribute attr in assembly.GetCustomAttributes(typeof(TAttribute)))
                            Add(attr, assembly, typeof(Assembly), assembly);

                    foreach (Type type in assembly.GetTypes())
                        ScanType(type, classPart);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }

    public class DirectoryScanner : Disposable
    {
        private Dictionary<string, Assembly> _backedUpAssemblies = new Dictionary<string, Assembly>();
        private List<string> _checkedDirectories = new List<string>();

        public DirectoryScanner()
        {
        }

        public Dictionary<string, Assembly> BackedUpAssemblies { get { return _backedUpAssemblies; } }
        private string BaseDirectory { get { return AppDomain.CurrentDomain.BaseDirectory; } }
        private List<string> CheckedDirectories { get { return _checkedDirectories; } }

        #region Private Methods

        private bool HasBackupFolder()
        {
            throw new NotImplementedException();
        }

        private void LoadBackupAssemblies(params string[] assemblies)
        {
            Dictionary<string, Assembly> result = new Dictionary<string, Assembly>();
            List<string> list = ScanFolder("BACKUP", ".dll", true, assemblies);

            foreach (string backup in assemblies)
            {
                if (list.Any(file => file.Contains(backup + ".dll")))
                {
                    string assemblyPath = list.FirstOrDefault(file => file.Contains(backup + ".dll"));
                    Assembly assembly = Assembly.LoadFile(assemblyPath);
                    result.Add(assembly.GetName().Name, assembly);
                }
            }

            _backedUpAssemblies = result;
        }

        private List<string> ScanFolder(string path, string fileExtension, bool searchRecursively, params string[] fileNames)
        {
            if (fileNames == null)
                throw new ArgumentNullException(nameof(fileNames));

            if (path == null)
                throw new ArgumentNullException(nameof(path));

            List<string> result = new List<string>();
            bool extendHasDot = (fileExtension != null && fileExtension.Contains('.')) ? true : false;
            string targetedDirectory = BaseDirectory,
                   startingDirectory = null;
            Tuple<string, bool>[] filePairs = (fileNames.Length < 0)
                                               ? null
                                               : fileNames.Select(a => new Tuple<string, bool>(a, false)).ToArray();

            if (!path.IsValidUri(out Uri uri))
                targetedDirectory = targetedDirectory.StepIntoDirectory(path, true);
            else
                targetedDirectory = uri.LocalPath;

            startingDirectory = targetedDirectory;

            do
            {
                string[] filesInFolder = Directory.GetFiles(targetedDirectory),
                         subDirectories = Directory.GetDirectories(targetedDirectory);

                if (fileNames.Length > 0)
                {
                    foreach (string name in fileNames)

                        if (fileExtension != null)
                        {
                            if (filesInFolder.Any(file => file.Contains(name + fileExtension)))
                            {
                                result.AddRange(
                                    filesInFolder.Where(
                                        file => file.Contains(name + fileExtension) || (file.Contains(name) && file.Contains(fileExtension))
                                    ));

                                filePairs[filePairs.Index(a => a.Item1 == name)] = new Tuple<string, bool>(name, true);
                            }
                        }
                        else if (filesInFolder.Any(file => file.Contains(name)))
                        {
                            result.AddRange(filesInFolder.Where(file => file.Contains(name)));

                            filePairs[filePairs.Index(a => a.Item1 == name)] = new Tuple<string, bool>(name, true);
                        }
                }
                else
                    foreach (string file in filesInFolder)
                        if ((extendHasDot ? ('.' + file.FileExtention()) : file.FileExtention()) == fileExtension)
                            result.Add(file);

                if (searchRecursively)
                {
                    if (subDirectories.Length > 0)
                    {
                        string intialDirectory = targetedDirectory;

                        foreach (string dir in subDirectories)
                            if (!CheckedDirectories.Contains(dir))
                            {
                                targetedDirectory = targetedDirectory.StepIntoDirectory(dir);
                                CheckedDirectories.Add(targetedDirectory);
                                break;
                            }
                            else
                                continue;

                        if (intialDirectory == targetedDirectory)
                            if (targetedDirectory == startingDirectory)
                                searchRecursively = false;
                            else
                                targetedDirectory = targetedDirectory.StepOutOfDirectory(1);
                    }
                    else
                        targetedDirectory = targetedDirectory.StepOutOfDirectory(1);
                }
                //else
                //    searchRecursively = false;

                if (result.Count > 0)
                    if (fileNames.Length == 0)
                        searchRecursively = false;
                    else
                        searchRecursively = filePairs.All(a => a.Item2 == true);
            }
            while (searchRecursively);

            return result;
        }

        #endregion Private Methods

        #region Public Methods

        public Assembly GetBackedUpAssembly(Assembly assembly)
        {
            return BackedUpAssemblies.FirstOrDefault(a => a.Value == assembly).Value;
        }

        public Assembly GetBackedUpAssembly(string assemblyName)
        {
            return BackedUpAssemblies.FirstOrDefault(a => a.Key == assemblyName).Value;
        }

        public ResolveEventHandler LoadBackUpDirectoryOnEvent()
        {
            return new ResolveEventHandler(
                (a, b) =>
                    {
                        string assembly = b.Name.Split(',')[0];
                        LoadBackupAssemblies(assembly);
                        return AppDomain.CurrentDomain.GetAssembly(assembly);
                    });
        }

        public FileInfo SearchForFile(string fileName)
        {
            if (fileName == null)
                throw new ArgumentNullException(nameof(fileName));

            string filePath = ScanFolder(null, null, true, (fileName == null) ? new string[0] : new[] { fileName }).SingleOrDefault();
            return (filePath == null) ? null : new FileInfo(filePath);
        }

        public FileInfo SearchForFile(string fileName, string dirPath)
        {
            if (fileName == null)
                throw new ArgumentNullException(nameof(fileName));

            string filePath = ScanFolder(dirPath, null, true, (fileName == null) ? new string[0] : new[] { fileName }).SingleOrDefault();
            return (filePath == null) ? null : new FileInfo(filePath);
        }

        public FileInfo SearchForFile(string fileName, string dirPath, string fileExtension)
        {
            if (fileName == null && fileExtension == null)
                if (fileName == null)
                    throw new ArgumentNullException(nameof(fileName));
                else
                    throw new ArgumentNullException(nameof(fileExtension));

            string filePath = ScanFolder(dirPath, fileExtension, true, (fileName == null) ? new string[0] : new[] { fileName }).SingleOrDefault();
            return (filePath == null) ? null : new FileInfo(filePath);
        }

        public FileInfo[] SearchForFiles(string dirPath, string fileExtension, params string[] fileNames)
        {
            if (fileExtension == null && fileNames == null)
                if (fileNames == null)
                    throw new ArgumentNullException(nameof(fileNames));
                else
                    throw new ArgumentNullException(nameof(fileExtension));

            List<FileInfo> result = new List<FileInfo>();
            List<string> filePaths = ScanFolder(dirPath, fileExtension, true, fileNames);
            foreach (string path in filePaths)
                result.Add(new FileInfo(path));

            return result.ToArray();
        }

        public FileInfo[] SearchForFiles(params string[] fileNames)
        {
            if (fileNames == null)
                throw new ArgumentNullException(nameof(fileNames));

            List<FileInfo> result = new List<FileInfo>();
            List<string> filePaths = ScanFolder(null, null, true, fileNames);
            foreach (string path in filePaths)
                result.Add(new FileInfo(path));

            return result.ToArray();
        }

        public FileInfo[] SearchForFiles(string fileExtension)
        {
            if (fileExtension == null)
                throw new ArgumentNullException(nameof(fileExtension));

            List<FileInfo> result = new List<FileInfo>();
            List<string> filePaths = ScanFolder(null, fileExtension, true, null);
            foreach (string path in filePaths)
                result.Add(new FileInfo(path));

            return result.ToArray();
        }

        #endregion Public Methods
    }
}