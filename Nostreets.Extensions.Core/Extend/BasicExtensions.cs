﻿using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

using Nostreets.Extensions.Utilities;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Diagnostics;
using System.Dynamic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web.WebPages;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Serialization;

namespace Nostreets.Extensions.Extend.Basic
{
    public static class Basic
    {
        #region Static
        public static string RandomNumber(int count)
        {
            Random random = new Random();
            List<int> randomNumbers = new List<int>();

            for (int i = 0; i < count; i++)
                randomNumbers.Add(random.Next(0, 9));

            return string.Join("", randomNumbers);
        }

        public static bool AreValuesEqual(object value1, object value2)
        {
            if (value1 == null && value2 == null)
                return true;

            if (value1 == null || value2 == null)
                return false;

            var settings = new JsonSerializerSettings
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore
            };

            return JsonConvert.SerializeObject(value1, settings) == JsonConvert.SerializeObject(value2, settings);
        }

        public static IEnumerable<T> GetEnumList<T>() where T : Enum
        {
            var vals = Enum.GetValues(typeof(T));
            var list = Enumerable.Cast<T>(vals);
            return list;
        }

        public static Assembly GetAssembly(this string assemblyName)
        {
            Assembly result = null;

            foreach (Assembly assemble in AppDomain.CurrentDomain.GetAssemblies())
            {
                if (assemble.FullName.Contains(assemblyName) || assemble.GetName().Name == assemblyName) { result = assemble; break; }
            }

            return result;
        }

        /// <summary>
        /// Creates the class.
        /// </summary>
        /// <param name="props">The props.</param>
        /// <param name="methods">The methods.</param>
        /// <returns></returns>
        public static Type CreateClass(List<Tuple<string, Type, Dictionary<Type, object[]>>> props, List<Tuple<string, Type, MethodAttributes, List<Tuple<Type, ParameterAttributes>>, Dictionary<Type, object[]>>> methods)
        {
            return CreateClass(null, props, methods);
        }

        /// <summary>
        /// Creates the class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="props">The props.</param>
        /// <param name="methods">The methods.</param>
        /// <returns></returns>
        public static Type CreateClass(string name, List<Tuple<string, Type, Dictionary<Type, object[]>>> props, List<Tuple<string, Type, MethodAttributes, List<Tuple<Type, ParameterAttributes>>, Dictionary<Type, object[]>>> methods)
        {
            return ClassBuilder.CreateType(name ?? "Class" + Guid.NewGuid().ToString(), props, methods);
        }

        /// <summary>
        /// Gets the local path.
        /// </summary>
        /// <returns></returns>
        public static string GetLocalPath()
        {
            return Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
        }

        /// <summary>
        /// Gets the methods by attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="obj">The object.</param>
        /// <param name="type">The type.</param>
        /// <param name="assembliesToSkip">The assemblies to skip.</param>
        /// <returns></returns>
        public static List<MethodInfo> GetMethodsByAttribute<TAttribute>(Func<Assembly, bool> predicate) where TAttribute : Attribute
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            List<MethodInfo> result = new List<MethodInfo>();
            IEnumerable<Assembly> assemblies = AppDomain.CurrentDomain.GetAssemblies().Where(predicate);

            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
                foreach (Assembly ass in assemblies)
                    foreach (var item in scanner.ScanForAttributes(ass, ClassTypes.Methods))
                        result.Add((MethodInfo)item.Item2);

            return result.Distinct().ToList();
        }

        /// <summary>
        /// Gets the objects by attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="obj">The object.</param>
        /// <param name="section">The section.</param>
        /// <param name="type">The type.</param>
        /// <param name="assembliesToSkip">The assemblies to skip.</param>
        /// <returns></returns>
        public static List<object> GetObjectsByAttribute<TAttribute>(Func<Assembly, bool> predicate, ClassTypes section) where TAttribute : Attribute
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            List<object> result = new List<object>();
            IEnumerable<Assembly> assemblies = AppDomain.CurrentDomain.GetAssemblies().Where(predicate);

            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
                foreach (Assembly ass in assemblies)
                    foreach (var item in scanner.ScanForAttributes(ass, section))
                        result.Add(item.Item2);

            return result.Distinct().ToList();
        }

        /// <summary>
        /// Gets the objects with attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="obj">The object.</param>
        /// <param name="types">The types.</param>
        /// <param name="assembliesToSkip">The assemblies to skip.</param>
        /// <returns></returns>
        public static List<Tuple<TAttribute, object, Assembly>> GetObjectsWithAttribute<TAttribute>(Func<Assembly, bool> predicate, ClassTypes section) where TAttribute : Attribute
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            List<Tuple<TAttribute, object, Assembly>> result = new List<Tuple<TAttribute, object, Assembly>>();
            IEnumerable<Assembly> assemblies = AppDomain.CurrentDomain.GetAssemblies().Where(predicate);

            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
                foreach (Assembly ass in assemblies)
                    foreach (var item in scanner.ScanForAttributes(ass, section))
                        result.Add(new Tuple<TAttribute, object, Assembly>(item.Item1, item.Item2, item.Item4));

            return result.Distinct().ToList();
        }

        /// <summary>
        /// Gets the os drive.
        /// </summary>
        /// <returns></returns>
        public static string GetOSDrive()
        {
            return Path.GetPathRoot(Environment.SystemDirectory);
        }

        /// <summary>
        /// Gets the properties by attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="obj">The object.</param>
        /// <param name="type">The type.</param>
        /// <param name="assembliesToSkip">The assemblies to skip.</param>
        /// <returns></returns>
        public static List<PropertyInfo> GetPropertiesByAttribute<TAttribute>(Func<Assembly, bool> predicate) where TAttribute : Attribute
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            List<PropertyInfo> result = null;
            IEnumerable<Assembly> assemblies = AppDomain.CurrentDomain.GetAssemblies().Where(predicate);

            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
                foreach (Assembly ass in assemblies)
                    foreach (var item in scanner.ScanForAttributes(ass, ClassTypes.Properties))
                        result.Add((PropertyInfo)item.Item2);

            return result.Distinct().ToList();
        }

        /// <summary>
        /// Gets the solution.
        /// </summary>
        /// <returns></returns>
        public static Solution GetSolution()
        {
            return new Solution(SolutionPath());
        }

        /// <summary>
        /// Gets the solution projects.
        /// </summary>
        /// <returns></returns>
        public static List<Project> GetSolutionProjects()
        {
            return GetSolution().Projects;
        }

        /// <summary>
        /// Gets the types by attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="obj">The object.</param>
        /// <param name="type">The type.</param>
        /// <param name="assembliesToSkip">The assemblies to skip.</param>
        /// <returns></returns>
        public static List<Type> GetTypesByAttribute<TAttribute>(Func<Assembly, bool> predicate) where TAttribute : Attribute
        {
            if (predicate == null)
                throw new ArgumentNullException(nameof(predicate));

            List<Type> result = new List<Type>();
            IEnumerable<Assembly> assemblies = AppDomain.CurrentDomain.GetAssemblies().Where(predicate);

            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
                foreach (Assembly ass in assemblies)
                    foreach (var item in scanner.ScanForAttributes(ass, ClassTypes.Type))
                        result.Add((Type)item.Item2);

            return result.Distinct().ToList();
        }

        /// <summary>
        /// Globals the stack trace.
        /// </summary>
        /// <param name="removeLines">The remove lines.</param>
        /// <param name="trim">The trim.</param>
        /// <returns></returns>
        public static string[] GlobalStackTrace(int removeLines = 0, int trim = 6)
        {
            string[] stack = Environment.StackTrace.Split(
                new string[] { Environment.NewLine },
                StringSplitOptions.RemoveEmptyEntries
            );

            if (stack.Length <= removeLines)
                return new string[0];

            string[] actualResult = new string[stack.Length - removeLines];

            for (int i = removeLines; i < stack.Length; i++)
                // Remove 6 characters (e.g. "  at ") from the beginning of the line
                // This might be different for other languages and platforms
                actualResult[i - removeLines] = stack[i].Substring(trim);

            return actualResult;
        }

        /// <summary>
        /// Maps the specified source.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source">The source.</param>
        /// <exception cref="ArgumentNullException">source</exception>
        /// <exception cref="ArgumentException">
        /// not nullable
        /// or
        /// type mismatch
        /// </exception>
        public static void Map<T>(ExpandoObject source)
        {
            Dictionary<string, PropertyInfo> _propertyMap =
                typeof(T)
                .GetProperties()
                .ToDictionary(
                    p => p.Name.ToLower(),
                    p => p
                );

            // Might as well take care of null references early.
            if (source == null)
                throw new ArgumentNullException("source");

            // By iterating the KeyValuePair<string, object> of
            // source we can avoid manually searching the keys of
            // source as we see in your original code.
            foreach (var kv in source)
            {
                PropertyInfo p;
                if (_propertyMap.TryGetValue(kv.Key.ToLower(), out p))
                {
                    var propType = p.PropertyType;
                    if (kv.Value == null)
                    {
                        if (!propType.IsByRef && propType.Name != "Nullable`1")
                        {
                            // Throw if type is a value type
                            // but not Nullable<>
                            throw new ArgumentException("not nullable");
                        }
                    }
                    else if (kv.Value.GetType() != propType)
                    {
                        // You could make this a bit less strict
                        // but I don't recommend it.
                        throw new ArgumentException("type mismatch");
                    }
                    p.SetValue(default(T), kv.Value, null);
                }
            }
        }

        /// <summary>
        /// Gets the solution path.
        /// </summary>
        /// <returns></returns>
        public static string SolutionPath()
        {
            string solutionDirPath = Assembly.GetCallingAssembly().CodeBase.StepOutOfDirectory(3);

            return solutionDirPath.ScanForFilePath(null, "sln");
        }

        public static string GetMethodNameThruStack(string txtInStack, bool simplfyMethodName = true)
        {
            string result = null;
            string stackTrace = Environment.StackTrace;
            string[] stackFrames = stackTrace.Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var callingFrame in stackFrames) 
            {
                if (!callingFrame.Contains(txtInStack))
                    continue;

                int methodNameStartIndex = callingFrame.IndexOf("at ") + 3;
                int methodNameEndIndex = callingFrame.LastIndexOf(" in ");

                if (methodNameEndIndex == -1)
                    continue;

                string executingMethodName = callingFrame.Substring(methodNameStartIndex, methodNameEndIndex - methodNameStartIndex);

                result = executingMethodName;
                break;
            }

            if (!string.IsNullOrEmpty(result) && simplfyMethodName) 
            {
                string pattern = @"(?<=\.)[\w\d_]+\(.*\)$";
                Match match = Regex.Match(result, pattern);

                if (match.Success)
                {
                    result = match.Value;
                }
            }

            return result;
        }

        public static string GetContrastingColor(string hexColor)
        {
            // Remove the '#' if it's present
            if (hexColor.StartsWith("#"))
            {
                hexColor = hexColor.Substring(1);
            }

            // Parse the hex string to get the RGB values
            int r = Convert.ToInt32(hexColor.Substring(0, 2), 16);
            int g = Convert.ToInt32(hexColor.Substring(2, 2), 16);
            int b = Convert.ToInt32(hexColor.Substring(4, 2), 16);

            // Calculate the perceived brightness
            double brightness = (0.299 * r + 0.587 * g + 0.114 * b);

            // Return white for dark colors, black for light colors
            return
                brightness < 128
                ? "#ffffff" //white
                : "#000000"; // black
        }

        public static string AdjustColorBrightness(string hexColor, decimal adjustment)
        {
            // Remove the '#' if present
            if (hexColor.StartsWith("#"))
            {
                hexColor = hexColor.Substring(1);
            }

            // Parse the hex string to get the RGB values
            int r = Convert.ToInt32(hexColor.Substring(0, 2), 16);
            int g = Convert.ToInt32(hexColor.Substring(2, 2), 16);
            int b = Convert.ToInt32(hexColor.Substring(4, 2), 16);

            // Calculate the adjustment factor
            decimal factor = adjustment > 0 ? 1 + adjustment * 0.1m : 1 + adjustment * 0.1m;

            // Adjust RGB values
            r = (int)Math.Clamp(r * factor, 0, 255);
            g = (int)Math.Clamp(g * factor, 0, 255);
            b = (int)Math.Clamp(b * factor, 0, 255);

            // Convert back to hex
            return $"#{r:X2}{g:X2}{b:X2}";
        }
        #endregion Static

        #region Extensions
        /// <summary>
        /// Adds the attribute.
        /// </summary>
        /// <param name="obj">The affected object.</param>
        /// <param name="attributeParams">ITEM 1 : Field's Name
        /// ITEM 2 : Attribute To Add
        /// ITEM 3 : Attribute Params To Construct With</param>
        /// <returns></returns>
        /// <exception cref="InvalidDataException">attributeParams must only have Type's of Attributes...</exception>
        public static object AddOrSetAttribute(this object obj, Dictionary<string, Dictionary<Type, object[]>> attributeParams)
        {
            object result = null;
            if (attributeParams.Any(a => !a.Value.Any(b => b.Key.IsSubclassOf(typeof(Attribute)))))
                throw new InvalidDataException("attributeParams must only have Type's of Attributes...");

            Type type = obj as Type ?? obj.GetType();
            List<Tuple<string, Type, Dictionary<Type, object[]>>> props = new List<Tuple<string, Type, Dictionary<Type, object[]>>>();
            List<Tuple<string, Type, MethodAttributes, List<Tuple<Type, ParameterAttributes>>, Dictionary<Type, object[]>>> methods = new List<Tuple<string, Type, MethodAttributes, List<Tuple<Type, ParameterAttributes>>, Dictionary<Type, object[]>>>();

            foreach (PropertyInfo prop in type.GetProperties())
                props.Add(
                    new Tuple<string, Type, Dictionary<Type, object[]>>(
                        prop.Name,
                        prop.PropertyType,
                        attributeParams.Keys.Any(a => a == prop.Name) ? attributeParams.Single(a => a.Key == prop.Name).Value : null
                    )
                );

            foreach (MethodInfo method in type.GetMethods())
                methods.Add(
                    new Tuple<string, Type, MethodAttributes, List<Tuple<Type, ParameterAttributes>>, Dictionary<Type, object[]>>(
                        method.Name,
                        method.ReturnType,
                        method.Attributes,
                        method.GetParameters().Select(a => new Tuple<Type, ParameterAttributes>(a.ParameterType, a.Attributes)).ToList(),
                        attributeParams.Keys.Any(a => a == method.Name) ? attributeParams.Single(a => a.Key == method.Name).Value : null
                    )
                );

            result = ClassBuilder.CreateObject(type.Name, props, methods);

            foreach (var prop in props)
                result.SetPropertyValue(prop.Item1, obj.GetPropertyValue(prop.Item1));

            return result;
        }

        /// <summary>
        /// Adds the attribute.
        /// </summary>
        /// <param name="obj">The affected object.</param>
        /// <param name="attributeParams">ITEM 1 : Field's Name
        /// ITEM 2 : Attribute To Add
        /// ITEM 3 : Attribute Params To Construct With</param>
        /// <returns></returns>
        /// <exception cref="InvalidDataException">attributeParams must only have Type's of Attributes...</exception>
        public static Type AddOrSetAttribute(this Type obj, Dictionary<string, Dictionary<Type, object[]>> attributeParams)
        {
            if (attributeParams.Any(a => !a.Value.Any(b => b.Key.IsSubclassOf(typeof(Attribute)))))
                throw new InvalidDataException("attributeParams must only have Type's of Attributes...");

            Type type = obj;
            PropertyInfo[] props = type.GetProperties();
            MethodInfo[] methods = type.GetMethods();

            return ClassBuilder.CreateType(
                                type.Name
                                , props.Select(a => new Tuple<string, Type, Dictionary<Type, object[]>>(a.Name, a.PropertyType,
                                    attributeParams.Any(b => b.Key == a.Name) ? attributeParams.FirstOrDefault(b => b.Key == a.Name).Value : null)
                                ).ToList()
                                , methods.Select(a => new Tuple<string, Type, MethodAttributes, List<Tuple<Type, ParameterAttributes>>, Dictionary<Type, object[]>>(a.Name, a.ReturnType, a.Attributes,
                                    a.GetParameters().Select(b => new Tuple<Type, ParameterAttributes>(b.ParameterType, b.Attributes)).ToList(),
                                    attributeParams.Any(b => b.Key == a.Name) ? attributeParams.FirstOrDefault(b => b.Key == a.Name).Value : null)
                              ).ToList()
                            );
        }

        /// <summary>
        /// Adds the or set property.
        /// </summary>
        /// <param name="objType">Type of the object.</param>
        /// <param name="oldPropName">Old name of the property.</param>
        /// <param name="newPropType">New type of the property.</param>
        /// <param name="newPropName">New name of the property.</param>
        /// <param name="propIndex">Index of the property.</param>
        /// <param name="attributes">The attributes.</param>
        /// <returns></returns>
        public static Type AddOrSetProperty(this Type objType, string oldPropName = null, Type newPropType = null, string newPropName = null, int propIndex = -1, Dictionary<Type, object[]> attributes = null)
        {
            List<Tuple<string, Type, Dictionary<Type, object[]>>> properties = new List<Tuple<string, Type, Dictionary<Type, object[]>>>();
            List<PropertyInfo> baseProps = objType.GetProperties().ToList();
            PropertyInfo[] props = baseProps.Where(a => a.Name != oldPropName).ToArray();
            int i = 0;

            foreach (PropertyInfo prop in props)
            {
                if (i == propIndex)
                {
                    properties.Add(new Tuple<string, Type, Dictionary<Type, object[]>>(newPropName, newPropType, attributes));
                    properties.Add(new Tuple<string, Type, Dictionary<Type, object[]>>(prop.Name, prop.PropertyType, attributes));
                }
                else if (prop.Name == oldPropName)
                    properties.Add(new Tuple<string, Type, Dictionary<Type, object[]>>(newPropName ?? oldPropName, newPropType, attributes));
                else
                    properties.Add(new Tuple<string, Type, Dictionary<Type, object[]>>(prop.Name, prop.PropertyType, attributes));

                i++;
            }

            List<Tuple<string, Type, MethodAttributes, List<Tuple<Type, ParameterAttributes>>, Dictionary<Type, object[]>>> methods = new List<Tuple<string, Type, MethodAttributes, List<Tuple<Type, ParameterAttributes>>, Dictionary<Type, object[]>>>();
            foreach (MethodInfo method in objType.GetMethods())
                methods.Add(new Tuple<string, Type, MethodAttributes, List<Tuple<Type, ParameterAttributes>>, Dictionary<Type, object[]>>(
                    method.Name, method.ReturnType, method.Attributes, method.GetParameters().Select(a => new Tuple<Type, ParameterAttributes>(a.ParameterType, a.Attributes)).ToList(), null));

            return ClassBuilder.CreateType(objType.Name, properties, methods);
        }

        /// <summary>
        /// Adds the property.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="newPropType">Type of the property.</param>
        /// <param name="newPropName">Name of the property.</param>
        /// <param name="propIndex">Index of the property.</param>
        /// <param name="attributes">The attributes.</param>
        /// <returns></returns>
        public static Type AddProperty(this object obj, Type newPropType, string newPropName, int propIndex = 0, Dictionary<Type, object[]> attributes = null)
        {
            Type objType = obj as Type ?? obj.GetType();
            return AddOrSetProperty(objType, null, newPropType, newPropName, (propIndex >= 0) ? propIndex : 0, attributes);
        }

        /// <summary>
        /// Adds the specified values.
        /// </summary>
        /// <param name="collection">The collection.</param>
        /// <param name="values">The values.</param>
        /// <returns></returns>
        public static IEnumerable AddValues(this IEnumerable collection, params object[] values)
        {
            IEnumerable result = null;
            if (values.Length > 0)
            {
                List<object> list = collection.OfType<object>().ToList();
                list.AddRange(values);

                if (collection.GetType().IsArray)
                    result = list.ToArray();
                else
                    result = list;
            }

            return result;
        }

        /// <summary>
        /// Appends the specified chars.
        /// </summary>
        /// <param name="char">The character.</param>
        /// <param name="chars">The chars.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">chars</exception>
        public static string Append(this char @char, params char[] chars)
        {
            if (chars == null)
                throw new ArgumentNullException("chars");

            if (chars.Length == 0)
                return @char.ToString();

            List<char> arrOfChars = new List<char>();
            arrOfChars.Add(@char);

            foreach (char character in chars)
                arrOfChars.Add(character);

            return new string(arrOfChars.ToArray());
        }

        /// <summary>
        /// Casts the specified value.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">value</exception>
        public static object Cast(this object value, Type type)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            object obj = null;
            obj = Convert.ChangeType(value, type);
            return obj;
        }

        /// <summary>
        /// Casts the specified value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        public static T Cast<T>(this object value)
        {
            return (T)value.Cast(typeof(T));
        }

        /// <summary>
        /// Tries the cast.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="type">The type.</param>
        /// <param name="convertedObj">The converted object.</param>
        /// <returns></returns>
        public static bool TryCast(this object value, Type type, out object convertedObj)
        {
            bool result = false;
            convertedObj = null;

            try
            {
                convertedObj = value.Cast(type);
            }
            finally
            {
                result = convertedObj != null;
            }

            return result;
        }

        /// <summary>
        /// Tries the cast.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value">The value.</param>
        /// <param name="convertedObj">The converted object.</param>
        /// <returns></returns>
        public static bool TryCast<T>(this object value, out T convertedObj)
        {
            bool result = false;
            convertedObj = default(T);

            try
            {
                convertedObj = value.Cast<T>();
            }
            finally
            {
                result = convertedObj != null;
            }

            return result;
        }

        /// <summary>
        /// Casts the specified type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collection">The collection.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        /// <exception cref="Exception">type cannot be null to be able to Cast
        /// or
        /// All entities Type in the collection have to match to type to be able to Cast</exception>
        public static IEnumerable Cast<T>(this IEnumerable<T> collection, Type type)
        {
            IEnumerable result = null;
            if (type == null)
                throw new Exception("type cannot be null to be able to Cast");

            if (collection != null)
            {
                bool entitiesMatch = collection.All(a => a.GetType() == type);
                if (type != typeof(object) && !entitiesMatch)
                    throw new Exception("All entities Type in the collection have to match to type to be able to Cast");

                dynamic genericList = typeof(List<>).MakeGenericType(type).Instantiate();
                Type genericListType = (Type)genericList.GetType();

                if (genericList != null)
                {
                    if (result == null)
                        result = new List<object>();

                    foreach (var item in collection)
                        ((object)genericList).IntoMethod(genericListType, "Add", false, item);

                    result = genericList;
                }
            }

            return result;
        }

        /// <summary>
        /// Casts the specified type.
        /// </summary>
        /// <param name="collection">The collection.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        /// <exception cref="Exception">type cannot be null to be able to Cast
        /// or
        /// All entities Type in the collection have to match to type to be able to Cast</exception>
        public static IEnumerable Cast(this IEnumerable collection, Type type)
        {
            IEnumerable result = null;
            if (type == null)
                throw new Exception("type cannot be null to be able to Cast");

            if (collection != null)
            {
                bool entitiesMatch = true;
                foreach (var item in collection)
                    entitiesMatch = (item.GetType() == type) ? true : false;

                if (!entitiesMatch)
                    throw new Exception("All entities Type in the collection have to match to type to be able to Cast");

                dynamic genericList = typeof(List<>).MakeGenericType(type).Instantiate();
                Type genericListType = (Type)genericList.GetType();

                if (genericList != null)
                {
                    if (result == null)
                        result = new List<object>();

                    foreach (var item in collection)
                        ((object)genericList).IntoMethod(genericListType, "Add", false, item);

                    result = genericList;
                }
            }

            return result;
        }

        /// <summary>
        /// Catches the reflection type load exception.
        /// </summary>
        /// <param name="ex">The ex.</param>
        /// <returns></returns>
        public static string CatchReflectionTypeLoadException(this ReflectionTypeLoadException ex)
        {
            StringBuilder sb = new StringBuilder();
            foreach (Exception exSub in ex.LoaderExceptions)
            {
                sb.AppendLine(exSub.Message);
                FileNotFoundException exFileNotFound = exSub as FileNotFoundException;
                if (exFileNotFound != null)
                {
                    if (!string.IsNullOrEmpty(exFileNotFound.FusionLog))
                    {
                        sb.AppendLine("Fusion Log:");
                        sb.AppendLine(exFileNotFound.FusionLog);
                    }
                }
                sb.AppendLine();
            }

            string errorMessage = sb.ToString();
            return errorMessage;
        }

        /// <summary>
        /// Determines whether [contains] [the specified values].
        /// </summary>
        /// <param name="list">The list.</param>
        /// <param name="values">The values.</param>
        /// <returns>
        ///   <c>true</c> if [contains] [the specified values]; otherwise, <c>false</c>.
        /// </returns>
        public static bool Contains(this IEnumerable<string> list, params string[] values)
        {
            bool result = false;
            foreach (string item in list)
            {
                result = (values.Any(a => a == item)) ? true : false;
            }

            return result;
        }

        /// <summary>
        /// Creates the folder.
        /// </summary>
        /// <param name="path">The path.</param>
        public static void CreateFolder(this string path)
        {
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            return;
        }

        /// <summary>
        /// Decrypts the specified key.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <param name="key">The key.</param>
        /// <param name="encoding">The encoding.</param>
        /// <returns></returns>
        /// <exception cref="Exception">
        /// key cannot be null to be able to Decrypt...
        /// or
        /// this cannot be null to be able to Decrypt...
        /// </exception>
        public static string Decrypt(this string data, string key, Encoding encoding = null)
        {
            if (key == null)
                throw new Exception("key cannot be null to be able to Decrypt...");

            if (data == null)
                throw new Exception("this cannot be null to be able to Decrypt...");

            if (encoding == null)
                encoding = Encoding.Unicode;

            return Encryption.Decrypt(data, key);
        }

        /// <summary>
        /// Directories the exists.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        public static bool DirectoryExists(this string path)
        {
            if (path.IndexOfAny(Path.GetInvalidPathChars()) != -1) { return false; }

            DirectoryInfo directoryInfo = new DirectoryInfo(Path.GetFullPath(path));
            if (!directoryInfo.Exists) { return false; }

            return true;
        }

        /// <summary>
        /// Distincts the by.
        /// </summary>
        /// <typeparam name="TSource">The type of the source.</typeparam>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <param name="source">The source.</param>
        /// <param name="keySelector">The key selector.</param>
        /// <returns></returns>
        public static IEnumerable<TSource> DistinctBy<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector)
        {
            HashSet<TKey> seenKeys = new HashSet<TKey>();
            foreach (TSource element in source)
            {
                if (seenKeys.Add(keySelector(element)))
                {
                    yield return element;
                }
            }
        }

        /// <summary>
        /// Doeses the inherit.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="inheritanceType">Type of the inheritance.</param>
        /// <returns></returns>
        public static bool DoesInherit(this Type type, Type inheritanceType)
        {
            return type.IsAssignableFrom(inheritanceType);
        }

        /// <summary>
        /// Encrypts the specified key.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <param name="key">The key.</param>
        /// <param name="encoding">The encoding.</param>
        /// <returns></returns>
        /// <exception cref="Exception">
        /// key cannot be null to be able to Encrypt...
        /// or
        /// this cannot be null to be able to Encrypt...
        /// </exception>
        public static string Encrypt(this string data, string key, Encoding encoding = null)
        {
            if (key == null)
                throw new Exception("key cannot be null to be able to Encrypt...");

            if (data == null)
                throw new Exception("this cannot be null to be able to Encrypt...");

            if (encoding == null)
                encoding = Encoding.Unicode;

            return Encryption.Encrypt(data, key);
        }

        /// <summary>
        /// Ends the of week.
        /// </summary>
        /// <param name="dt">The dt.</param>
        /// <returns></returns>
        public static DateTime EndOfWeek(this DateTime dt)
        {
            DateTime start = StartOfWeek(dt);
            return start.AddDays(6);
        }

        /// <summary>
        /// To the dictionary.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="enumType">Type of the enum.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">Type must be an enum</exception>
        public static Dictionary<int, string> EnumToDictionary<T>(this T enumType) where T : struct, IConvertible
        {
            if (!typeof(T).IsEnum)
                throw new ArgumentNullException("Type must be an enum");

            return typeof(T).EnumToDictionary();
        }

        /// <summary>
        /// To the dictionary.
        /// </summary>
        /// <param name="enumType">Type of the enum.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">Type must be an enum</exception>
        public static Dictionary<int, string> EnumToDictionary(this Type enumType)
        {
            Dictionary<int, string> result = null;
            if (!enumType.IsEnum)
                throw new ArgumentNullException("Type must be an enum");

            string[] arr = Enum.GetNames(enumType);

            foreach (string enumName in arr)
            {
                if (result == null)
                    result = new Dictionary<int, string>();

                result.Add(enumType.GetEnumValue(enumName), enumName);
            }

            return result;
        }

        /// <summary>
        /// Extends the path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="extension">The extension.</param>
        /// <returns></returns>
        public static string ExtendPath(this string path, string extension)
        {
            return Path.GetFullPath(Path.Combine(path, extension));
        }

        /// <summary>
        /// Files the extention.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        public static string FileExtention(this string path)
        {
            string[] split = path.Split('.');
            return split[split.Length - 1];
        }

        /// <summary>
        /// Formats the string.
        /// </summary>
        /// <param name="template">The template.</param>
        /// <param name="txt">The text.</param>
        /// <returns></returns>
        public static string FormatString(this string template, params string[] txt)
        {
            try { return String.Format(template, txt); }
            catch (Exception)
            {
                for (int i = 0; i < txt.Length; i++)
                    template.Replace("{" + i + "}", txt[i]);

                return template;
            }
        }

        /// <summary>
        /// Gets the assembly.
        /// </summary>
        /// <param name="appDomain">The assembly.</param>
        /// <param name="assemblyName">Name of the assembly.</param>
        /// <returns></returns>
        public static Assembly GetAssembly(this AppDomain appDomain, string assemblyName)
        {
            Assembly result = null;

            foreach (Assembly assemble in appDomain.GetAssemblies())
            {
                if (assemble.FullName.Contains(assemblyName) || assemble.GetName().Name == assemblyName) { result = assemble; break; }
            }

            return result;
        }

        public static Type GetTypeWithAssembly(this Assembly assembly, string typeName)
        {
            return assembly.GetTypes().FirstOrDefault(t => t.Name == typeName);
        }

        /// <summary>
        /// Gets the constructor parameters.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="param">The parameter.</param>
        public static void GetConstructorParams(this Type type, object[] param = null)
        {
            if (param == null)
                param = new object[0];

            ConstructorInfo result = null;
            ConstructorInfo[] constructors = type.GetConstructors();
            foreach (ConstructorInfo con in constructors)
            {
                ParameterInfo[] parameters = con.GetParameters();
                if (parameters.Length == param.Length)
                {
                    for (int i = 0; i < param.Length; i++)
                    {
                        if (param[i].GetType() != parameters[i].ParameterType)
                            break;

                        if (i == param.Length - 1)
                            result = con;
                    }
                }
            }
        }

        /// <summary>
        /// Gets the default constructor.
        /// </summary>
        /// <param name="t">The t.</param>
        /// <returns></returns>
        public static ConstructorInfo GetDefaultConstructor(this Type t)
        {
            return t.GetConstructor(Type.EmptyTypes);
        }

        /// <summary>
        /// Gets the duplicates.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="enumerable">The enumerable.</param>
        /// <param name="propertyName">Name of the property.</param>
        /// <returns></returns>
        public static Dictionary<object, T[]> GetDuplicates<T>(this IEnumerable<T> enumerable, string propertyName) where T : class
        {
            Dictionary<object, List<T>> dict = new Dictionary<object, List<T>>();

            foreach (T item in enumerable)
            {
                object key = item.GetPropertyValue(propertyName);
                if (!dict.ContainsKey(key))
                {
                    dict.Add(key, new List<T> { item });
                }
                else
                {
                    dict[item.GetPropertyValue(propertyName)].Add(item);
                }
            }

            Dictionary<object, T[]> duplicates = new Dictionary<object, T[]>();

            foreach (var value in dict)
            {
                if (value.Value.Count > 1)
                {
                    duplicates.Add(value.Key, value.Value.ToArray());
                }
            }

            return duplicates;
        }

        /// <summary>
        /// Gets the enum value.
        /// </summary>
        /// <param name="enumType">Type of the enum.</param>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">Type must be an enum</exception>
        public static int GetEnumValue(this Type enumType, string name)
        {
            if (!enumType.IsEnum)
                throw new ArgumentNullException("Type must be an enum");

            return (int)Enum.Parse(enumType, name);
        }

        /// <summary>
        /// Gets the enum value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">Type must be an enum</exception>
        public static int GetEnumValue<T>(this string name)
        {
            if (!typeof(T).IsEnum)
                throw new ArgumentNullException("Type must be an enum");

            return (int)Enum.Parse(typeof(T), name);
        }

        /// <summary>
        /// Gets the enum description.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="name">The name.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">Type must be an enum</exception>
        public static string GetEnumDescription<T>(this T value) where T : struct, IConvertible
        {
            if (!typeof(T).IsEnum)
            {
                throw new ArgumentException("T must be an enumerated type");
            }

            DescriptionAttribute attribute = value.GetType()
                    .GetField(value.ToString())
                    ?.GetCustomAttributes(typeof(DescriptionAttribute), false)
                    .SingleOrDefault() as DescriptionAttribute;

            return attribute == null ? value.ToString() : attribute.Description;
        }

        /// <summary>
        /// Gets the event.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="eventName">Name of the event.</param>
        /// <returns></returns>
        public static EventInfo GetEvent(this object obj, string eventName)
        {
            return obj.GetType().GetEvent(eventName);
        }

        /// <summary>
        /// Gets the name of the incremented.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="format">The format.</param>
        /// <returns></returns>
        public static string GetIncrementedName(this string name, string format = "{0}.v.{1}")
        {
            format = format.Contains("{0}") && format.Contains("{1}") ? format : "{0}.v.{1}";

            int numericIndex = 0;
            for (int i = name.Length - 1; i >= 0; i--)
                if (name[i].IsNumeric())
                    numericIndex++;
                else
                    break;

            string result = null;
            int nextVersion = !name[name.Length - 1].IsNumeric() ? 2 : int.Parse(name.Substring(name.Length - numericIndex).ToString()) + 1;

            if (nextVersion != 2)
                result = name.Substring(0, name.Length - numericIndex) + nextVersion;
            else
                result = format.FormatString(name, nextVersion.ToString());

            return result;
        }

        /// <summary>
        /// Gets the method information.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="T2">The type of the 2.</typeparam>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">Expression is not a method - expression</exception>
        public static MethodInfo GetMethod<T, T2>(this Expression<Func<T, T2>> expression)
        {
            var member = expression.Body as MethodCallExpression;

            if (member != null)
                return member.Method;

            throw new ArgumentNullException("Expression is not a method", "expression");
        }

        /// <summary>
        /// Gets the method information.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">Expression is not a method - expression</exception>
        public static MethodInfo GetMethod<T>(this Expression<Action<T>> expression)
        {
            var member = expression.Body as MethodCallExpression;

            if (member != null)
                return member.Method;

            throw new ArgumentNullException("Expression is not a method", "expression");
        }

        /// <summary>
        /// Gets the method information.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="methodName">Name of the method.</param>
        /// <param name="types">The types.</param>
        /// <param name="searchSettings">The search settings.</param>
        /// <returns></returns>
        public static MethodInfo GetMethod(this object obj, string methodName, Type[] types = null, Type[] generics = null, BindingFlags searchSettings = BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public | BindingFlags.Static)
        {
            return obj.GetType().GetMethod(methodName, searchSettings, Type.DefaultBinder, types ?? new Type[] { }, null);
        }

        /// <summary>
        /// Gets the method information.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="methodName">Name of the method.</param>
        /// <param name="paramTypes">The types.</param>
        /// <param name="searchSettings">The search settings.</param>
        /// <returns></returns>
        public static MethodInfo GetMethod(this Type type, string methodName, Type[] paramTypes = null, Type[] generics = null, BindingFlags searchSettings = BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public | BindingFlags.Static)
        {
            try
            {
                return type.GetMethod(methodName, searchSettings, Type.DefaultBinder, paramTypes ?? new Type[] { }, null);
            }
            catch (AmbiguousMatchException ex)
            {
                //In Deepth Scan
                MethodInfo result = null;
                var methods = type.GetMethods(searchSettings);
                foreach (var method in methods)
                {
                    bool match = true;
                    if (method.Name == methodName)
                    {
                        if (generics != null)
                        {
                            var isGeneric = method.IsGenericMethod;
                            if (isGeneric)
                            {
                                var genericArguements = method.GetGenericArguments();
                                if (genericArguements.Length == generics.Length)
                                {
                                    //Check Generic Type against genrics params
                                    for (var i = 0; i < genericArguements.Length; i++)
                                    {
                                        var arguement = genericArguements[i];
                                        if (arguement.BaseType != null)
                                        {
                                            var genericType = generics[i];
                                            var arguementIsInterface = arguement.BaseType.IsInterface;

                                            if (arguementIsInterface ? !genericType.HasInterface(arguement.BaseType) : arguement.BaseType != genericType.BaseType)
                                                match = false;
                                        }
                                    }
                                }
                                else
                                    match = false;
                            }
                            else
                                match = false;
                        }

                        var parameters = method.GetParameters();
                        if (parameters.Length == paramTypes.Length)
                        {
                            for (var i = 0; i < parameters.Length; i++)
                            {
                                var parameter = parameters[i];
                                var targetType = paramTypes[i];
                                var propIsInterface = parameter.ParameterType.IsInterface;

                                if (propIsInterface ? !targetType.HasInterface(parameter.ParameterType) : parameter.ParameterType != targetType)
                                    match = false;
                            }
                        }
                        else
                            match = false;
                    }
                    else
                        match = false;

                    if (match)
                    {
                        result = method;
                        break;
                    }
                }

                return result;
            }
        }

        /// <summary>
        /// Gets the method information.
        /// </summary>
        /// <param name="fullMethodName">Full name of the method.</param>
        /// <returns></returns>
        public static MethodInfo GetMethod(this string fullMethodName)
        {
            return (MethodInfo)fullMethodName.ScanAssembliesForObject();
        }

        /// <summary>
        /// Gets the methods by attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="assembly">The assembly.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static List<MethodInfo> GetMethodsByAttribute<TAttribute>(this Assembly assembly, Type type = null) where TAttribute : Attribute
        {
            List<MethodInfo> result = new List<MethodInfo>();
            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
            {
                foreach (var item in scanner.ScanForAttributes(assembly, ClassTypes.Methods, type))
                    result.Add((MethodInfo)item.Item2);
            }

            return result;
        }

        /// <summary>
        /// Gets the methods by attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="assembly">The assembly.</param>
        /// <param name="types">The types.</param>
        /// <returns></returns>
        public static List<MethodInfo> GetMethodsByAttribute<TAttribute>(this Assembly assembly, IEnumerable<Type> types = null) where TAttribute : Attribute
        {
            List<MethodInfo> result = new List<MethodInfo>();
            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
            {
                if (types != null)
                {
                    foreach (Type type in types)
                        foreach (var item in scanner.ScanForAttributes(assembly, ClassTypes.Methods))
                            result.Add((MethodInfo)item.Item2);
                }
                else
                    foreach (var item in scanner.ScanForAttributes(Assembly.GetCallingAssembly(), ClassTypes.Methods))
                        result.Add((MethodInfo)item.Item2);
            }

            return result;
        }

        /// <summary>
        /// Gets the methods by attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static List<MethodInfo> GetMethodsByAttribute<TAttribute>(this Type type) where TAttribute : Attribute
        {
            List<MethodInfo> result = new List<MethodInfo>();
            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
            {
                foreach (var item in scanner.ScanForAttributes(Assembly.GetCallingAssembly(), ClassTypes.Methods, type))
                    result.Add((MethodInfo)item.Item2);
            }

            return result;
        }

        /// <summary>
        /// Gets the methods by attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="types">The types.</param>
        /// <returns></returns>
        public static List<MethodInfo> GetMethodsByAttribute<TAttribute>(this IEnumerable<Type> types) where TAttribute : Attribute
        {
            List<MethodInfo> result = new List<MethodInfo>();
            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
            {
                foreach (Type type in types)
                    foreach (var item in scanner.ScanForAttributes(Assembly.GetCallingAssembly(), ClassTypes.Methods, type))
                        result.Add((MethodInfo)item.Item2);
            }

            return result;
        }

        /// <summary>
        /// Gets the objects by attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="assembly">The assembly.</param>
        /// <param name="section">The section.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static List<object> GetObjectsByAttribute<TAttribute>(this Assembly assembly, ClassTypes section, Type type = null) where TAttribute : Attribute
        {
            List<object> result = new List<object>();
            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
            {
                foreach (var item in scanner.ScanForAttributes(assembly, section, type))
                    result.Add(item.Item2);
            }

            return result;
        }

        /// <summary>
        /// Gets the objects with attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="assembly">The assembly.</param>
        /// <param name="section">The section.</param>
        /// <returns></returns>
        public static List<Tuple<TAttribute, object, Assembly>> GetObjectsWithAttribute<TAttribute>(this Assembly assembly, ClassTypes section) where TAttribute : Attribute
        {
            List<Tuple<TAttribute, object, Assembly>> result = new List<Tuple<TAttribute, object, Assembly>>();
            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
                foreach (var item in scanner.ScanForAttributes(assembly, section))
                    result.Add(new Tuple<TAttribute, object, Assembly>(item.Item1, item.Item2, item.Item4));

            return result;
        }

        /// <summary>
        /// Gets the properties by attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="assembly">The assembly.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static List<PropertyInfo> GetPropertiesByAttribute<TAttribute>(this Assembly assembly, Type type = null) where TAttribute : Attribute
        {
            List<PropertyInfo> result = new List<PropertyInfo>();

            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
            {
                foreach (var item in scanner.ScanForAttributes(assembly, ClassTypes.Properties, type))
                    result.Add((PropertyInfo)item.Item2);
            }

            return result;
        }

        /// <summary>
        /// Gets the properties by attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static List<PropertyInfo> GetPropertiesByAttribute<TAttribute>(this Type type) where TAttribute : Attribute
        {
            List<PropertyInfo> result = null;

            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
            {
                foreach (var item in scanner.ScanForAttributes(Assembly.GetCallingAssembly(), ClassTypes.Properties, type))
                {
                    if (result == null)
                        result = new List<PropertyInfo>();

                    result.Add((PropertyInfo)item.Item2);
                }
            }

            return result;
        }

        public static Type GetPropertyType(this object obj, int ordinal)
        {
            if (obj.GetType() == typeof(Type).GetType())
                throw new Exception("obj cannot be a Type its self to be able to GetPropertyValue...");

            return obj.GetType().GetProperties().Where((a, b) => b == ordinal).First().PropertyType;
        }

        public static Type GetPropertyType(this object obj, string propertyName)
        {
            if (obj.GetType() == typeof(Type).GetType())
                throw new Exception("obj cannot be a Type its self to be able to GetPropertyValue...");

            return obj.GetType().GetProperties().Single(pi => pi.Name == propertyName).PropertyType;
        }

        public static Type GetPropertyType(this Type obj, int ordinal)
        {
            return obj.GetProperties().Where((a, b) => b == ordinal).First().PropertyType;
        }

        public static Type GetPropertyType(this Type obj, string propertyName)
        {
            return obj.GetProperties().Single(pi => pi.Name == propertyName).PropertyType;
        }

        /// <summary>
        /// Gets the property value.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="propertyName">Name of the property.</param>
        /// <returns></returns>
        /// <exception cref="Exception">obj cannot be a Type its self to be able to GetPropertyValue...</exception>
        public static object GetPropertyValue(this object obj, string propertyName)
        {
            if (obj.GetType() == typeof(Type).GetType())
                throw new Exception("obj cannot be a Type its self to be able to GetPropertyValue...");

            return obj.GetType().GetProperties().Single(pi => pi.Name == propertyName).GetValue(obj);
        }

        /// <summary>
        /// Gets the property value.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="ordinal">The ordinal.</param>
        /// <returns></returns>
        /// <exception cref="Exception">obj cannot be a Type its self to be able to GetPropertyValue...</exception>
        public static object GetPropertyValue(this object obj, int ordinal)
        {
            if (obj.GetType() == typeof(Type).GetType())
                throw new Exception("obj cannot be a Type its self to be able to GetPropertyValue...");

            return obj.GetType().GetProperties().Where((a, b) => b == ordinal).First().GetValue(obj);
        }

        /// <summary>
        /// Gets the type of t.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">type</exception>
        /// <exception cref="InvalidDataException">type does not implements IEnumerable</exception>
        public static Type GetTypeOfT(this Type type)
        {
            Type result = null;

            if (type == null)
                throw new ArgumentNullException("type");

            if (!type.HasInterface<IEnumerable>())
                throw new InvalidDataException("type is not a generic type...");

            if (type.BaseType == typeof(Array))
                result = type.GetElementType();
            if (result == null)
                result = type.GetGenericArguments()[0];

            return result;
        }

        /// <summary>
        /// Gets the type of t.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">obj</exception>
        /// <exception cref="InvalidDataException">obj's Type does not implements IEnumerable</exception>
        public static Type GetTypeOfT(this object obj)
        {
            if (obj == null)
                throw new ArgumentNullException("obj");

            Type type = obj.GetType();

            if (!type.HasInterface<IEnumerable>())
                throw new InvalidDataException("type is not a generic type...");

            return type.GetGenericArguments()[0];
        }

        /// <summary>
        /// Gets the types by attribute.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="assembly">The assembly.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static List<Type> GetTypesByAttribute<TAttribute>(this Assembly assembly, Type type = null) where TAttribute : Attribute
        {
            List<Type> result = new List<Type>();

            using (AttributeScanner<TAttribute> scanner = new AttributeScanner<TAttribute>())
            {
                foreach (var item in scanner.ScanForAttributes(assembly, ClassTypes.Type, type))
                    result.Add((Type)item.Item2);
            }

            return result;
        }

        /// <summary>
        /// Gets the types with.
        /// </summary>
        /// <typeparam name="TAttribute">The type of the attribute.</typeparam>
        /// <param name="app">The application.</param>
        /// <param name="searchDervied">if set to <c>true</c> [search dervied].</param>
        /// <returns></returns>
        public static List<Type> GetTypesWith<TAttribute>(this AppDomain app, bool searchDervied) where TAttribute : Attribute
        {
            //return from a in AppDomain.CurrentDomain.GetAssemblies()
            //       from t in a.GetTypes()
            //       where t.IsDefined(typeof(TAttribute), searchDervied)
            //       select t;

            List<Type> result = new List<Type>();
            var assemblies = app.GetAssemblies();

            foreach (var assembly in assemblies)
            {
                var types = assembly.GetTypes();
                foreach (var item in types)
                {
                    if (Attribute.GetCustomAttribute(item, typeof(TAttribute)) != null) { result.Add(item); }
                }
            }

            return result;
        }

        /// <summary>
        /// Gets the week of month.
        /// </summary>
        /// <param name="time">The time.</param>
        /// <returns></returns>
        public static int GetWeekOfMonth(this DateTime time)
        {
            DateTime first = new DateTime(time.Year, time.Month, 1);
            return time.GetWeekOfYear() - first.GetWeekOfYear() + 1;
        }

        /// <summary>
        /// Gets the week of year.
        /// </summary>
        /// <param name="time">The time.</param>
        /// <returns></returns>
        public static int GetWeekOfYear(this DateTime time)
        {
            GregorianCalendar _gc = new GregorianCalendar();
            return _gc.GetWeekOfYear(time, CalendarWeekRule.FirstDay, DayOfWeek.Sunday);
        }

        public static int GetQuarter(this DateTime dateTime)
        {
            return (dateTime.Month - 1) / 3 + 1;
        }

        public static int GetHalfYear(this DateTime dateTime)
        {
            return dateTime.Month >= 6 ? 1 : 2;
        }

        /// <summary>
        /// Determines whether this instance has attribute.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="prop">The property.</param>
        /// <returns>
        ///   <c>true</c> if the specified property has attribute; otherwise, <c>false</c>.
        /// </returns>
        /// <exception cref="InvalidDataException">T must have a base Type of Attribute...</exception>
        public static bool HasAttribute<T>(this PropertyInfo prop)
        {
            if (!typeof(Attribute).IsAssignableFrom(typeof(T)))
                throw new InvalidDataException("T must have a base Type of Attribute...");

            return prop.GetCustomAttribute(typeof(T)) == null ? false : true;
        }

        /// <summary>
        /// Determines whether this instance has attribute.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if the specified property has attribute; otherwise, <c>false</c>.
        /// </returns>
        /// <exception cref="InvalidDataException">T must have a base Type of Attribute...</exception>
        public static bool HasAttribute<T>(this Type type) where T : Attribute
        {
            return type.GetCustomAttribute<T>() != null;
        }

        /// <summary>
        /// Determines whether this instance has attribute.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if the specified type has attribute; otherwise, <c>false</c>.
        /// </returns>
        /// <exception cref="InvalidDataException">T must have a base Type of Attribute...</exception>
        public static bool HasAttributeInProperties<T>(this Type type)
        {
            if (typeof(T).IsSubclassOf(typeof(Attribute)))
                throw new InvalidDataException("T must have a base Type of Attribute...");

            bool result = false;
            foreach (PropertyInfo prop in type.GetProperties())
            {
                result = prop.GetCustomAttribute(typeof(T)) == null ? false : true;

                if (result == true)
                    break;
            }

            return result;
        }

        /// <summary>
        /// Determines whether the specified constructor has constuctor.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="constructor">The constructor.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns>
        ///   <c>true</c> if the specified constructor has constuctor; otherwise, <c>false</c>.
        /// </returns>
        public static bool HasConstuctor(this Type type, out ConstructorInfo constructor, params object[] parameters)
        {
            bool doParamsMatch = true;
            constructor = null;

            if (parameters.Length > 0)
            {
                List<Type> argTypes = new List<Type>();
                ConstructorInfo[] constructors = type.GetConstructors();

                foreach (object par in parameters)
                {
                    if (par != null)
                        argTypes.Add(par.GetType());
                    else
                        argTypes.Add(null);
                }

                foreach (ConstructorInfo constuct in constructors)
                {
                    ParameterInfo[] constuctorParams = constuct.GetParameters();
                    // parameters can be null for default constructors so use argTypes
                    if (argTypes.Count == constuctorParams.Length)
                    {
                        for (int i = 0; i < parameters.Length; i++)
                        {
                            if (argTypes[i] == null)
                            {
                                if (constuctorParams[i].ParameterType.IsValueType)
                                {
                                    //fill the defaults for value type if not supplied
                                    parameters[i] = Instantiate(constuctorParams[i].ParameterType, null);
                                    argTypes[i] = parameters[i].GetType();
                                }
                                else
                                {
                                    argTypes[i] = constuctorParams[i].ParameterType;
                                }
                            }
                            if (!constuctorParams[i].ParameterType.IsAssignableFrom(argTypes[i]))
                            {
                                doParamsMatch = false;
                                break;
                            }
                        }
                        if (doParamsMatch)
                            constructor = constuct;
                    }
                }
            }
            else
            {
                constructor = GetDefaultConstructor(type);

                if (constructor == null)
                    doParamsMatch = false;
            }

            return doParamsMatch;
        }

        /// <summary>
        /// Determines whether the specified property name has duplicates.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="enumerable">The enumerable.</param>
        /// <param name="propertyName">Name of the property.</param>
        /// <returns>
        ///   <c>true</c> if the specified property name has duplicates; otherwise, <c>false</c>.
        /// </returns>
        public static bool HasDuplicates<T>(this IEnumerable<T> enumerable, string propertyName) where T : class
        {
            List<object> dict = new List<object>();
            foreach (var item in enumerable)
            {
                object key = item.GetPropertyValue(propertyName);

                if (!dict.Contains(key))
                    dict.Add(key);
                else
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Determines whether the specified field has field.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="field">The field.</param>
        /// <returns>
        ///   <c>true</c> if the specified field has field; otherwise, <c>false</c>.
        /// </returns>
        public static bool HasField(this Type type, FieldInfo field)
        {
            return type.GetFields().FirstOrDefault(a => a == field) == null ? false : true;
        }

        /// <summary>
        /// Determines whether this instance has interface.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if the specified type has interface; otherwise, <c>false</c>.
        /// </returns>
        /// <exception cref="Exception">T has to be an interface</exception>
        public static bool HasInterface<T>(this Type type)
        {
            if (!typeof(T).IsInterface)
                throw new Exception("T has to be an interface");

            if (type.IsInterface)
                return false;

            if (typeof(T).IsAssignableFrom(type))
                return true;

            if (type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(T)))
                return true;

            return false;
        }

        /// <summary>
        /// Determines whether the specified inter has interface.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="inter">The inter.</param>
        /// <returns>
        ///   <c>true</c> if the specified inter has interface; otherwise, <c>false</c>.
        /// </returns>
        /// <exception cref="Exception">T has to be an interface</exception>
        public static bool HasInterface(this Type type, Type inter)
        {
            if (!inter.IsInterface)
                throw new Exception("T has to be an interface");

            if (type.IsInterface)
                return false;

            if (inter.IsAssignableFrom(type))
                return true;

            if (type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == inter))
                return true;

            return false;
        }

        /// <summary>
        /// Determines whether the specified method has method.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="method">The method.</param>
        /// <returns>
        ///   <c>true</c> if the specified method has method; otherwise, <c>false</c>.
        /// </returns>
        public static bool HasMethod(this Type type, MethodInfo method)
        {
            return type.GetMethods().FirstOrDefault(a => a == method) == null ? false : true;
        }

        /// <summary>
        /// Determines whether the specified property has property.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="prop">The property.</param>
        /// <returns>
        ///   <c>true</c> if the specified property has property; otherwise, <c>false</c>.
        /// </returns>
        public static bool HasProperty(this Type type, PropertyInfo prop)
        {
            return type.GetProperties().FirstOrDefault(a => a == prop) == null ? false : true;
        }

        /// <summary>
        /// Determines whether the specified property has property.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="prop">The property.</param>
        /// <returns>
        ///   <c>true</c> if the specified property has property; otherwise, <c>false</c>.
        /// </returns>
        public static bool HasProperty(this Type type, string propName)
        {
            return type.GetProperties().FirstOrDefault(a => a.Name == propName) == null ? false : true;
        }

        /// <summary>
        /// Determines whether the specified property has property.
        /// </summary>
        /// <param name="obj">The type.</param>
        /// <param name="prop">The property name.</param>
        /// <returns>
        ///   <c>true</c> if the specified property has property; otherwise, <c>false</c>.
        /// </returns>
        public static bool HasProperty(this object obj, string propName)
        {
            Type type = obj.GetType() == typeof(Type) ? (Type)obj : obj.GetType();
            return type.GetProperties().FirstOrDefault(a => a.Name == propName) == null ? false : true;
        }

        /// <summary>
        /// Ins the specified arguments.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj">The object.</param>
        /// <param name="args">The arguments.</param>
        /// <returns></returns>
        public static bool In<T>(this T obj, params T[] args)
        {
            return args.Contains(obj);
        }

        /// <summary>
        /// Finds the index bassed in the specified predicate.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="coll">The coll.</param>
        /// <param name="predicate">The predicate.</param>
        /// <returns></returns>
        public static int Index<T>(this IEnumerable<T> coll, Func<T, bool> predicate)
        {
            for (int i = 0; i < coll.Count(); i++)
                if (predicate(coll.ElementAt(i)))
                    return i;

            return -1;
        }

        /// <summary>
        /// Instantiates the specified parameters.
        /// </summary>
        /// <param name="t">The type.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public static object Instantiate(this Type t, params object[] parameters)
        {
            Func<object> func = null;

            if (t == typeof(string))
                func = Expression.Lambda<Func<object>>(Expression.Constant(string.Empty)).Compile();

            if (t.HasConstuctor(out ConstructorInfo constructor, parameters))
                func = Expression.Lambda<Func<object>>(Expression.New(constructor)).Compile();

            //if (parameters.Length == 0)
            //    func = Expression.Lambda<Func<object>>(Expression.New(t)).Compile();

            if (func == null)
                func = () => FormatterServices.GetUninitializedObject(t);

            return func.Invoke();
        }

        /// <summary>
        /// Instantiates the specified type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static T Instantiate<T>(this T type)
        {
            return (T)typeof(T).Instantiate();
        }

        /// <summary>
        /// Intoes the generic constructor as t.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="T">The t.</param>
        /// <param name="parms">The parms.</param>
        /// <returns></returns>
        public static object IntoGenericConstructorAsT(this Type type, Type T, params object[] parms)
        {
            return type.IntoGenericConstructorAsT(T).Instantiate(parms);
        }

        /// <summary>
        /// Intoes the generic constructor as t.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="T">The t.</param>
        /// <param name="parms">The parms.</param>
        /// <returns></returns>
        public static object IntoGenericConstructorAsT(this Type type, Type[] T, params object[] parms)
        {
            return type.IntoGenericConstructorAsT(T).Instantiate(parms);
        }

        /// <summary>
        /// Intoes the generic constructor as t.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="T">The t.</param>
        /// <returns></returns>
        public static Type IntoGenericConstructorAsT(this Type type, Type T)
        {
            return type.MakeGenericType(T);
        }

        /// <summary>
        /// Intoes the generic constructor as t.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="T">The t.</param>
        /// <returns></returns>
        public static Type IntoGenericConstructorAsT(this Type type, params Type[] T)
        {
            return type.MakeGenericType(T);
        }

        /// <summary>
        /// Intoes the generic method.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="methodHolder">The method holder.</param>
        /// <param name="methodName">Name of the method.</param>
        /// <param name="generics">The type.</param>
        /// <param name="isExtension">if set to <c>true</c> [is extension].</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public static object IntoGenericMethod(this object obj, Type methodHolder, string methodName, Type[] generics, bool isExtension = false, params object[] parameters)
        {
            if (obj == null)
                return null;

            object result = null;
            bool isStatic = false;

            if (isExtension)
            {
                var paramList = parameters.ToList();
                paramList.Insert(0, obj);
                parameters = paramList.ToArray();
            }

            if (methodHolder.GetMethods(BindingFlags.Static | BindingFlags.Public).Any(a => a.Name == methodName))
                isStatic = true;

            var paramTypes = parameters.Select(a => a.GetType()).ToArray();
            var methodInfo = GetMethod(methodHolder, methodName, paramTypes, generics);
            var method = methodInfo?.MakeGenericMethod(generics);

            result = method?.Invoke((isStatic) ? null : obj, parameters);

            return result;
        }

        /// <summary>
        /// Intoes the generic method.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="methodName">Name of the method.</param>
        /// <param name="generic">The generic.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public static object IntoGenericMethod(this object obj, string methodName, Type generic, params object[] parameters)
        {
            if (obj == null)
                return null;

            object result = null;
            bool isStatic = false;

            if (obj.GetType().GetMethods(BindingFlags.Static | BindingFlags.Public).Any(a => a.Name == methodName))
                isStatic = true;

            var paramTypes = parameters.Select(a => a.GetType()).ToArray();
            MethodInfo method = GetMethod(obj.GetType(), methodName, paramTypes)?.MakeGenericMethod(new Type[] { generic });
            result = method?.Invoke((isStatic) ? null : obj, parameters);
            return result;
        }

        /// <summary>
        /// Intoes the generic method.
        /// </summary>
        /// <param name="methodHolder">The method holder.</param>
        /// <param name="methodName">Name of the method.</param>
        /// <param name="generic">The generic.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public static object IntoGenericMethod(this Type methodHolder, string methodName, Type generic, params object[] parameters)
        {
            object result = null;
            bool isStatic = false;

            if (methodHolder.GetMethods(BindingFlags.Static | BindingFlags.Public).Any(a => a.Name == methodName))
                isStatic = true;

            var paramTypes = parameters.Select(a => a.GetType()).ToArray();
            MethodInfo method = GetMethod(methodHolder, methodName, paramTypes)?.MakeGenericMethod(new Type[] { generic });

            result = method?.Invoke((isStatic) ? null : methodHolder.Instantiate(), parameters);
            return result;
        }

        /// <summary>
        /// Intoes the generic method.
        /// </summary>
        /// <param name="methodHolder">The method holder.</param>
        /// <param name="methodName">Name of the method.</param>
        /// <param name="generics">The generics.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public static object IntoGenericMethod(this Type methodHolder, string methodName, Type[] generics, params object[] parameters)
        {
            object result = null;
            bool isStatic = false;

            if (methodHolder.GetMethods(BindingFlags.Static | BindingFlags.Public).Any(a => a.Name == methodName))
                isStatic = true;

            var paramTypes = parameters.Select(a => a.GetType()).ToArray();
            MethodInfo method = GetMethod(methodHolder, methodName, paramTypes)?.MakeGenericMethod(generics);

            result = method?.Invoke((isStatic) ? null : methodHolder.Instantiate(), parameters);
            return result;
        }

        /// <summary>
        /// Intoes the method.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="methodHolder">The method holder.</param>
        /// <param name="methodName">Name of the method.</param>
        /// <param name="isExtension">if set to <c>true</c> [is extension].</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public static object IntoMethod(this object obj, Type methodHolder, string methodName, bool isExtension = false, params object[] parameters)
        {
            if (obj == null)
                return null;

            object result = null;
            bool isStatic = false;

            if (isExtension)
                parameters = (object[])parameters.AddValues(obj);

            if (methodHolder.GetMethods(BindingFlags.Static | BindingFlags.Public).Any(a => a.Name == methodName))
                isStatic = true;

            var paramTypes = parameters.Select(a => a.GetType()).ToArray();
            MethodInfo method = GetMethod(methodHolder, methodName, paramTypes);
            result = method?.Invoke((isStatic) ? null : obj, parameters);
            return result;
        }

        /// <summary>
        /// Intoes the method.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="methodName">Name of the method.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public static async Task<object> IntoMethodAsync(this object obj, string methodName, params object[] parameters)
        {
            if (obj == null)
                return null;

            object result = null;
            bool isStatic = false;
            Type type = obj as Type ?? obj.GetType();

            if (type.GetMethods(BindingFlags.Static | BindingFlags.Public).Any(a => a.Name == methodName))
                isStatic = true;

            var paramTypes = parameters.Select(a => a.GetType()).ToArray();
            MethodInfo method = GetMethod(type, methodName, paramTypes);

            if (method != null)
            {
                if (method.ReturnType == typeof(Task))
                {
                    await (Task)method.Invoke((isStatic) ? null : obj, parameters);
                }
                else
                {
                    result = await (dynamic)method.Invoke((isStatic) ? null : obj, parameters);
                }
            }

            return result;
        }

        /// <summary>
        /// Intoes the method.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="methodName">Name of the method.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public static object IntoMethod(this object obj, string methodName, params object[] parameters)
        {
            if (obj == null)
                return null;

            object result = null;
            bool isStatic = false;
            Type type = obj as Type ?? obj.GetType();

            if (type.GetMethods(BindingFlags.Static | BindingFlags.Public).Any(a => a.Name == methodName))
                isStatic = true;

            var paramTypes = parameters.Select(a => a.GetType()).ToArray();
            MethodInfo method = GetMethod(type, methodName, paramTypes);
            result = method?.Invoke((isStatic) ? null : obj, parameters);
            return result;
        }

        /// <summary>
        /// Determines whether [is action delegate].
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source">The source.</param>
        /// <returns>
        ///   <c>true</c> if [is action delegate] [the specified source]; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsActionDelegate<T>(this T source)
        {
            return typeof(T).FullName.StartsWith("System.Action");
        }

        /// <summary>
        /// Determines whether the specified exclude strings is collection.
        /// </summary>
        /// <param name="item">The item.</param>
        /// <param name="excludeStrings">if set to <c>true</c> [exclude strings].</param>
        /// <returns>
        ///   <c>true</c> if the specified exclude strings is collection; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsCollection(this Type item, bool excludeStrings = true)
        {
            return (!excludeStrings)
                        ? (item.HasInterface<IEnumerable>() /*|| item.HasInterface<ICollection>() || item.HasInterface<IList>()*/)
                        : (item == typeof(string))
                        ? false
                        : (item.HasInterface<IEnumerable>() /*|| item.HasInterface<ICollection>() || item.HasInterface<IList>()*/);
        }

        /// <summary>
        /// Determines whether [is equal to] [the specified object].
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="enumerable">The enumerable.</param>
        /// <param name="obj">The object.</param>
        /// <returns>
        ///   <c>true</c> if [is equal to] [the specified object]; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsEqualTo<T>(this IEnumerable<T> enumerable, IEnumerable<T> obj)
        {
            bool result = false;

            if (enumerable.Count() == obj.Count())
                for (int i = 0; i < enumerable.Count(); i++)
                {
                    result = enumerable.ElementAt(i).Equals(obj.ElementAt(i));

                    if (!result)
                        break;
                }

            return result;
        }

        /// <summary>
        /// Determines whether this instance is even.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        ///   <c>true</c> if the specified value is even; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsEven(this int value)
        {
            return value % 2 == 0;
        }

        /// <summary>
        /// Determines whether this instance is nullable.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if the specified type is nullable; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsNullable(this Type type)
        {
            return Nullable.GetUnderlyingType(type) != null;
        }

        /// <summary>
        /// Determines whether this instance is nullable.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if the specified type is nullable; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsNullable(this Type type, out Type underlyingType)
        {
            underlyingType = Nullable.GetUnderlyingType(type);
            return underlyingType != null;
        }

        /// <summary>
        /// Determines whether this instance is numeric.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>
        ///   <c>true</c> if the specified object is numeric; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsNumeric(this object obj)
        {
            Type nnType = Nullable.GetUnderlyingType(obj.GetType()) ?? obj.GetType();
            switch (Type.GetTypeCode(nnType))
            {
                case TypeCode.SByte:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Byte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                    return true;

                case TypeCode.Char:
                    return int.TryParse(obj + "", out int x);

                case TypeCode.String:
                    return int.TryParse((string)obj, out int y);

                default:
                    return false;
            }
        }

        public static bool IsString(this object obj)
        {
            Type nnType = Nullable.GetUnderlyingType(obj.GetType()) ?? obj.GetType();
            return nnType == typeof(string) || nnType == typeof(char[]);
        }

        /// <summary>
        /// Determines whether this instance is odd.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns>
        ///   <c>true</c> if the specified value is odd; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsOdd(this int value)
        {
            return value % 2 != 0;
        }

        /// <summary>
        /// Determines whether this instance is plural.
        /// </summary>
        /// <param name="txt">The text.</param>
        /// <returns>
        ///   <c>true</c> if the specified text is plural; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsPlural(this string txt)
        {
            return ((txt[txt.Length - 1] == 's') ? true : false);
        }

        /// <summary>
        /// Determines whether [is runtime type].
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if [is runtime type] [the specified type]; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsRuntimeType(this Type type)
        {
            return type.Name.Contains("Runtime");
        }

        /// <summary>
        /// Determines whether [is system type].
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if [is system type] [the specified type]; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsSystemType(this Type type)
        {
            return type.Assembly == typeof(object).Assembly;
        }

        /// <summary>
        /// Determines whether the specified output is type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj">The object.</param>
        /// <param name="output">The output.</param>
        /// <returns>
        ///   <c>true</c> if the specified output is type; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsType<T>(this object obj, out T output) where T : class
        {
            bool result = false;
            output = null;
            if (typeof(T) == obj.GetType())
            {
                result = true;
                output = (T)obj;
            }

            return result;
        }

        /// <summary>
        /// Determines whether the specified type is type.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if the specified type is type; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsType(this object obj, Type type)
        {
            bool result = false;
            if (type == obj.GetType())
            {
                result = true;
            }

            return result;
        }

        /// <summary>
        /// Determines whether [is valid URI] [the specified URI].
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="uri">The URI.</param>
        /// <param name="predicate">The predicate.</param>
        /// <returns>
        ///   <c>true</c> if [is valid URI] [the specified URI]; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsValidUri(this string path, out Uri uri, Func<Uri, bool> predicate = null)
        {
            bool result = false;
            if (Uri.TryCreate(path, UriKind.Absolute, out uri))
                result = (predicate != null) ? predicate(uri) : true;

            return result;
        }

        /// <summary>
        /// Determines whether [is valid URL].
        /// </summary>
        /// <param name="url">The URL.</param>
        /// <returns>
        ///   <c>true</c> if [is valid URL] [the specified URL]; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsValidUrl(this string url)
        {
            return Uri.TryCreate(url, UriKind.Absolute, out Uri uriResult) && (uriResult.Scheme == Uri.UriSchemeHttp || uriResult.Scheme == Uri.UriSchemeHttps);
        }

        /// <summary>
        /// Determines whether [is week day].
        /// </summary>
        /// <param name="date">The date.</param>
        /// <returns>
        ///   <c>true</c> if [is week day] [the specified date]; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsWeekDay(this DateTime date)
        {
            return ((date.DayOfWeek == DayOfWeek.Saturday) || (date.DayOfWeek == DayOfWeek.Sunday)) ? false : true;
        }

        /// <summary>
        /// Determines whether [is week of month] [the specified pay day].
        /// </summary>
        /// <param name="week">The week.</param>
        /// <param name="payDay">The pay day.</param>
        /// <returns>
        ///   <c>true</c> if [is week of month] [the specified pay day]; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsWeekOfMonth(this int week, DateTime payDay)
        {
            bool result = false;
            int weekOfPay = GetWeekOfMonth(payDay);

            while (week >= weekOfPay)
            {
                week -= 4;
                if (week == weekOfPay) { result = true; }
            }

            return result;
        }

        /// <summary>
        /// Jsons the deserialize.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        public static object JsonDeserialize(this string obj)
        {
            return JsonConvert.DeserializeObject(obj);
        }

        /// <summary>
        /// Jsons the deserialize.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        public static T JsonDeserialize<T>(this string obj)
        {
            return JsonConvert.DeserializeObject<T>(obj);
        }

        /// <summary>
        /// Jsons the serialize.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        public static string JsonSerialize(this object obj, JsonSerializerSettings settings = null, Newtonsoft.Json.Formatting formatting = Newtonsoft.Json.Formatting.None)
        {
            settings = settings ?? new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() };
            return JsonConvert.SerializeObject(obj, formatting, settings);
        }

        /// <summary>
        /// Logs the specified text.
        /// </summary>
        /// <param name="txt">The text.</param>
        public static void LogInDebug(this string txt)
        {
            Debug.Write(txt + Environment.NewLine);
        }

        /// <summary>
        /// Maps the properties.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj">The object.</param>
        /// <param name="target">The target.</param>
        public static void MapProperties<T>(this object obj, ref T target, Dictionary<string, string> mapper = null) where T : new()
        {
            if (obj == null)
                throw new ArgumentNullException(nameof(obj));

            if (target == null)
                target = new T();

            if (obj.GetType() != typeof(Dictionary<string, object>))
            {
                PropertyInfo[] curProps = obj.GetType() == typeof(Type) ? ((Type)obj).GetProperties() : obj.GetType().GetProperties();
                PropertyInfo[] targetProps = target.GetType().GetProperties();

                foreach (PropertyInfo prop in curProps)
                {
                    PropertyInfo getProp(string propName) => targetProps.FirstOrDefault(a => a.Name == propName && a.PropertyType == prop.PropertyType);
                    var targetProp = getProp(prop.Name);

                    if (targetProp == null && mapper != null && mapper.ContainsKey(prop.Name))
                        targetProp = getProp(mapper[prop.Name]);

                    if (targetProp != null)
                        target.SetPropertyValue(prop.Name, obj.GetPropertyValue(targetProp.Name));
                }
            }
            else
            {
                var someObject = new T();
                var someObjectType = someObject.GetType();

                foreach (var item in (Dictionary<string, object>)obj)
                {
                    someObjectType
                             .GetProperty(item.Key)
                             .SetValue(someObject, item.Value, null);
                }

                target = someObject;
            }
        }

        /// <summary>
        /// Maps the properties.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj">The object.</param>
        /// <param name="target">The target.</param>
        public static T MapProperties<T>(this object obj, Dictionary<string, string> mapper = null) where T : new()
        {
            if (obj == null)
                throw new ArgumentNullException(nameof(obj));

            var target = new T();

            if (obj.GetType() != typeof(Dictionary<string, object>))
            {
                PropertyInfo[] curProps = obj.GetType() == typeof(Type) ? ((Type)obj).GetProperties() : obj.GetType().GetProperties();
                PropertyInfo[] targetProps = target.GetType().GetProperties();

                foreach (PropertyInfo prop in curProps)
                {
                    PropertyInfo getProp(string propName) => targetProps.FirstOrDefault(a => a.Name == propName && a.PropertyType == prop.PropertyType);
                    var targetProp = getProp(prop.Name);

                    if (targetProp == null && mapper != null && mapper.ContainsKey(prop.Name))
                        targetProp = getProp(mapper[prop.Name]);

                    if (targetProp != null)
                        target.SetPropertyValue(prop.Name, obj.GetPropertyValue(targetProp.Name));
                }
            }
            else
            {
                var someObject = new T();
                var someObjectType = someObject.GetType();

                foreach (var item in (Dictionary<string, object>)obj)
                {
                    someObjectType
                             .GetProperty(item.Key)
                             .SetValue(someObject, item.Value, null);
                }

                target = someObject;
            }

            return target;
        }

        /// <summary>
        /// Names the with parameters.
        /// </summary>
        /// <param name="method">The method.</param>
        /// <returns></returns>
        public static string NameWithParams(this MethodBase method)
        {
            return string.Format("{0}.{1}({2})",
                    method.ReflectedType.FullName,
                    method.Name,
                    string.Join(",", method.GetParameters().Select(o => string.Format("{0} {1}", o.ParameterType, o.Name)).ToArray())
                );
        }

        /// <summary>
        /// Numbers the of days in month.
        /// </summary>
        /// <param name="date">The date.</param>
        /// <param name="dayOfWeek">The day of week.</param>
        /// <returns></returns>
        public static int NumberOfDaysInMonth(this DateTime date, DayOfWeek dayOfWeek)
        {
            DateTime start = new DateTime(date.Year, date.Month, 1),
                     end = new DateTime(date.Year, date.Month, DateTime.DaysInMonth(date.Year, date.Month));

            TimeSpan ts = end - start;

            int totalDays = (int)Math.Floor(ts.TotalDays / 7);
            int remainder = (int)ts.TotalDays % 7;
            int sinceLastDay = end.DayOfWeek - dayOfWeek;

            if (sinceLastDay < 0)
                sinceLastDay += 7;

            if (remainder >= sinceLastDay)
                totalDays++;

            return totalDays;
        }

        /// <summary>
        /// Parses the int.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        public static int ParseInt(this string obj)
        {
            return int.Parse(obj);
        }

        /// <summary>
        /// Parses the long.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        public static long ParseLong(this string obj)
        {
            return long.Parse(obj);
        }

        /// <summary>
        /// Parses the short.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        public static short ParseShort(this string obj)
        {
            return short.Parse(obj);
        }

        /// <summary>
        /// Parses the u int.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        public static uint ParseUInt(this string obj)
        {
            return uint.Parse(obj);
        }

        /// <summary>
        /// Parses the u long.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        public static ulong ParseULong(this string obj)
        {
            return ulong.Parse(obj);
        }

        /// <summary>
        /// Parses the u short.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        public static ushort ParseUShort(this string obj)
        {
            return ushort.Parse(obj);
        }

        /// <summary>
        /// Prepends the specified values.
        /// </summary>
        /// <param name="collection">The collection.</param>
        /// <param name="values">The values.</param>
        /// <returns></returns>
        public static IEnumerable Prepend(this IEnumerable collection, params object[] values)
        {
            return values.Concat(collection.OfType<object>());
        }

        /// <summary>
        /// Prepends the specified values.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collection">The collection.</param>
        /// <param name="values">The values.</param>
        /// <returns></returns>
        public static IEnumerable<T> Prepend<T>(this IEnumerable<T> collection, params T[] values)
        {
            return values.Concat(collection);
        }

        /// <summary>
        /// Prepends the specified item.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collection">The collection.</param>
        /// <param name="item">The item.</param>
        public static void Prepend<T>(this IList<T> collection, T item)
        {
            collection.Insert(0, item);
        }

        /// <summary>
        /// Prepends the specified key.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="dic">The dic.</param>
        /// <param name="key">The key.</param>
        /// <param name="item">The item.</param>
        /// <returns></returns>
        public static Dictionary<TKey, TValue> Prepend<TKey, TValue>(this IDictionary<TKey, TValue> dic, TKey key, TValue item)
        {
            List<KeyValuePair<TKey, TValue>> list = dic.ToList();
            list.Insert(0, new KeyValuePair<TKey, TValue>(key, item));
            return list.ToDictionary();
        }

        /// <summary>
        /// Randoms the number.
        /// </summary>
        /// <param name="ran">The ran.</param>
        /// <param name="min">The minimum.</param>
        /// <param name="max">The maximum.</param>
        /// <returns></returns>
        public static int RandomNumber(this Random ran, int min, int max)
        {
            return ran.Next(min, max);
        }

        /// <summary>
        /// Randoms the string.
        /// </summary>
        /// <param name="ran">The ran.</param>
        /// <param name="length">The length.</param>
        /// <param name="includeNumbers">if set to <c>true</c> [include numbers].</param>
        /// <returns></returns>
        public static string RandomString(this Random ran, int length, bool includeUpperCase = true, bool includeLowerCase = true, bool includeNumbers = true)
        {
            if (!includeUpperCase && !includeLowerCase && !includeNumbers)
                throw new Exception($"{nameof(includeUpperCase)} or {nameof(includeLowerCase)} or {nameof(includeNumbers)} must be true");

            string capitalChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            string lowerChars = "abcdefghijklmnopqrstuvwxyz";
            string numbers = "1234567890";

            var chars = "";
            if (includeUpperCase)
                chars += capitalChars;
            if (includeLowerCase)
                chars += lowerChars;
            if (includeNumbers)
                chars += numbers;

            return new string(Enumerable.Repeat(chars, length).Select(s => s[ran.Next(s.Length)]).ToArray());
        }

        /// <summary>
        /// Reads the file.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <returns></returns>
        public static string ReadFile(this string path)
        {
            string result = null;
            using (StreamReader reader = new StreamReader(path))
            {
                result = reader.ReadToEnd();
            }
            return result;
        }

        /// <summary>
        /// Removes the specified strings to remove.
        /// </summary>
        /// <param name="txt">The text.</param>
        /// <param name="stringsToRemove">The strings to remove.</param>
        /// <returns></returns>
        public static string Remove(this string txt, params string[] stringsToRemove)
        {
            foreach (string s in stringsToRemove)
            {
                txt = txt.Replace(s, "");
            }

            return txt;
        }

        /// <summary>
        /// Removes the character.
        /// </summary>
        /// <param name="txt">The text.</param>
        /// <param name="charsToRemove">The chars to remove.</param>
        /// <returns></returns>
        public static string RemoveChar(this string txt, params char[] charsToRemove)
        {
            foreach (char c in charsToRemove)
            {
                txt = txt.Replace("" + c, "");
            }

            return txt;
        }

        public static string RemoveSpecialCharacters(this string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                return input; // Return the original string if it's null or empty
            }

            // Use a regular expression to remove special characters
            return Regex.Replace(input, @"[^a-zA-Z0-9]", "");
        }

        /// <summary>
        /// Safes the name.
        /// </summary>
        /// <param name="txt">The text.</param>
        /// <returns></returns>
        public static string SafeName(this string txt)
        {
            return txt?.Remove("`1")?.RemoveChar('<', '>', '@', '.', '{', '}', '[', ']', '_');
        }

        public static object ScanAssembliesForObject(this string nameToCheckFor
                                                    , string assemblyToLookFor = null
                                                    , ClassTypes classType = ClassTypes.Any
                                                    , Func<dynamic, bool> predicate = null)
        {
            return nameToCheckFor.ScanAssembliesForObject(out Assembly assembly, null, (assemblyToLookFor != null) ? new[] { assemblyToLookFor } : null, classType, predicate);
        }

        public static object ScanAssembliesForObject(this string nameToCheckFor
                                                    , string[] assembliesToLookFor
                                                    , ClassTypes classType = ClassTypes.Any
                                                    , Func<dynamic, bool> predicate = null)
        {
            return nameToCheckFor.ScanAssembliesForObject(out Assembly assembly, null, assembliesToLookFor, classType, predicate);
        }

        public static object ScanAssembliesForObject(this string nameToCheckFor
                                                    , string[] assembliesToSkip
                                                    , string[] assembliesToLookFor
                                                    , ClassTypes classType = ClassTypes.Any
                                                    , Func<dynamic, bool> predicate = null)
        {
            return nameToCheckFor.ScanAssembliesForObject(out Assembly assembly, assembliesToSkip, assembliesToLookFor, classType, predicate);
        }

        /// <summary>
        /// Scans the assemblies for object.
        /// </summary>
        /// <param name="nameToCheckFor">The name to check for.</param>
        /// <param name="assemblyToLookFor">The assembly to look for.</param>
        /// <param name="classType">Type of the class.</param>
        /// <param name="predicate">The predicate.</param>
        /// <returns></returns>
        public static object ScanAssembliesForObject(this string nameToCheckFor
                                                    , out Assembly assembly
                                                    , string assemblyToLookFor = null
                                                    , ClassTypes classType = ClassTypes.Any
                                                    , Func<dynamic, bool> predicate = null)
        {
            object result = null;
            using (AssemblyScanner scanner = new AssemblyScanner())
                result = scanner.ScanAssembliesForObject(nameToCheckFor, out assembly, (assemblyToLookFor != null) ? new[] { assemblyToLookFor } : null, null, classType, predicate);

            return result;
        }

        /// <summary>
        /// Scans the assemblies for object.
        /// </summary>
        /// <param name="nameToCheckFor">The name to check for.</param>
        /// <param name="assembliesToLookFor">The assemblies to look for.</param>
        /// <param name="classType">Type of the class.</param>
        /// <param name="predicate">The predicate.</param>
        /// <returns></returns>
        public static object ScanAssembliesForObject(this string nameToCheckFor
                                                    , out Assembly assembly
                                                    , string[] assembliesToLookFor
                                                    , ClassTypes classType = ClassTypes.Any
                                                    , Func<dynamic, bool> predicate = null)
        {
            object result = null;
            using (AssemblyScanner scanner = new AssemblyScanner())
                result = scanner.ScanAssembliesForObject(nameToCheckFor, out assembly, assembliesToLookFor, null, classType, predicate);

            return result;
        }

        /// <summary>
        /// Scans the assemblies for object.
        /// </summary>
        /// <param name="nameToCheckFor">The name to check for.</param>
        /// <param name="assembliesToSkip">The assemblies to skip.</param>
        /// <param name="assembliesToLookFor">The assemblies to look for.</param>
        /// <param name="classType">Type of the class.</param>
        /// <param name="predicate">The predicate.</param>
        /// <returns></returns>
        public static object ScanAssembliesForObject(this string nameToCheckFor
                                                    , out Assembly assembly
                                                    , string[] assembliesToSkip
                                                    , string[] assembliesToLookFor
                                                    , ClassTypes classType = ClassTypes.Any
                                                    , Func<dynamic, bool> predicate = null)
        {
            object result = null;
            using (AssemblyScanner scanner = new AssemblyScanner())
                result = scanner.ScanAssembliesForObject(nameToCheckFor, out assembly, assembliesToLookFor, assembliesToSkip, classType, predicate);

            return result;
        }

        public static Dictionary<object, Assembly> ScanAssembliesForObjects(this string nameToCheckFor
                                                                           , string assemblyToLookFor = null
                                                                           , ClassTypes classType = ClassTypes.Any
                                                                           , Func<dynamic, bool> predicate = null)
        {
            Dictionary<object, Assembly> result = null;
            using (AssemblyScanner scanner = new AssemblyScanner())
                result = scanner.ScanAssembliesForObjects(nameToCheckFor, (assemblyToLookFor != null) ? new[] { assemblyToLookFor } : null, null, classType, predicate);

            return result;
        }

        /// <summary>
        /// Scans the assemblies for object.
        /// </summary>
        /// <param name="nameToCheckFor">The name to check for.</param>
        /// <param name="assembliesToLookFor">The assemblies to look for.</param>
        /// <param name="classType">Type of the class.</param>
        /// <param name="predicate">The predicate.</param>
        /// <returns></returns>
        public static Dictionary<object, Assembly> ScanAssembliesForObjects(this string nameToCheckFor
                                                                           , string[] assembliesToLookFor
                                                                           , ClassTypes classType = ClassTypes.Any
                                                                           , Func<dynamic, bool> predicate = null)
        {
            Dictionary<object, Assembly> result = null;
            using (AssemblyScanner scanner = new AssemblyScanner())
                result = scanner.ScanAssembliesForObjects(nameToCheckFor, assembliesToLookFor, null, classType, predicate);

            return result;
        }

        /// <summary>
        /// Scans the assemblies for object.
        /// </summary>
        /// <param name="nameToCheckFor">The name to check for.</param>
        /// <param name="assembliesToSkip">The assemblies to skip.</param>
        /// <param name="assembliesToLookFor">The assemblies to look for.</param>
        /// <param name="classType">Type of the class.</param>
        /// <param name="predicate">The predicate.</param>
        /// <returns></returns>
        public static Dictionary<object, Assembly> ScanAssembliesForObjects(this string nameToCheckFor
                                                                           , string[] assembliesToSkip
                                                                           , string[] assembliesToLookFor
                                                                           , ClassTypes classType = ClassTypes.Any
                                                                           , Func<dynamic, bool> predicate = null)
        {
            Dictionary<object, Assembly> result = null;
            using (AssemblyScanner scanner = new AssemblyScanner())
                result = scanner.ScanAssembliesForObjects(nameToCheckFor, assembliesToLookFor, assembliesToSkip, classType, predicate);

            return result;
        }

        /// <summary>
        /// Scans for file.
        /// </summary>
        /// <param name="dirPath">The dir path.</param>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="fileExtension">The file extension.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">fileName</exception>
        public static FileInfo ScanForFile(this string dirPath, string fileName, string fileExtension)
        {
            if (fileName == null)
                throw new ArgumentNullException(nameof(fileName));

            FileInfo result = null;

            using (DirectoryScanner scanner = new DirectoryScanner())
                result = scanner.SearchForFile(fileName, dirPath, fileExtension);

            return result;
        }

        public static bool DoesAssemblyExist(this string nameToCheck)
        {
            return Assembly.GetExecutingAssembly().GetReferencedAssemblies().FirstOrDefault(c => c.FullName == nameToCheck) == null;
        }

        /// <summary>
        /// Scans for file path.
        /// </summary>
        /// <param name="dirPath">The dir path.</param>
        /// <param name="fileName">Name of the file.</param>
        /// <param name="fileExtension">The file extension.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">
        /// fileExtension
        /// or
        /// fileName
        /// </exception>
        public static string ScanForFilePath(this string dirPath, string fileName, string fileExtension)
        {
            if (fileName == null && fileExtension == null)
                if (fileExtension == null)
                    throw new ArgumentNullException(nameof(fileExtension));
                else
                    throw new ArgumentNullException(nameof(fileName));

            FileInfo result = null;

            using (DirectoryScanner scanner = new DirectoryScanner())
                result = scanner.SearchForFile(fileName, dirPath, fileExtension);

            return result.FullName;
        }

        /// <summary>
        /// Scans for file paths.
        /// </summary>
        /// <param name="dirPath">The dir path.</param>
        /// <param name="fileExtension">The file extension.</param>
        /// <param name="fileNames">The file names.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">
        /// fileExtension
        /// or
        /// fileNames
        /// </exception>
        public static string[] ScanForFilePaths(this string dirPath, string fileExtension, params string[] fileNames)
        {
            if (fileNames == null && fileExtension == null)
                if (fileExtension == null)
                    throw new ArgumentNullException(nameof(fileExtension));
                else
                    throw new ArgumentNullException(nameof(fileNames));

            List<string> result = new List<string>();
            FileInfo[] files = ScanForFiles(dirPath, fileExtension, fileNames);

            foreach (FileInfo f in files)
                result.Add(f.FullName);

            return result.ToArray();
        }

        /// <summary>
        /// Scans for files.
        /// </summary>
        /// <param name="dirPath">The dir path.</param>
        /// <param name="fileExtension">The file extension.</param>
        /// <param name="fileNames">The file names.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">
        /// fileExtension
        /// or
        /// fileNames
        /// </exception>
        public static FileInfo[] ScanForFiles(this string dirPath, string fileExtension, params string[] fileNames)
        {
            if (fileNames == null && fileExtension == null)
                if (fileExtension == null)
                    throw new ArgumentNullException(nameof(fileExtension));
                else
                    throw new ArgumentNullException(nameof(fileNames));

            FileInfo[] result = null;
            using (DirectoryScanner scanner = new DirectoryScanner())
                result = scanner.SearchForFiles(dirPath, fileExtension, fileNames);

            return result;
        }

        /// <summary>
        /// Semis the month date.
        /// </summary>
        /// <param name="date">The date.</param>
        /// <returns></returns>
        public static DateTime SemiMonthDate(this DateTime date)
        {
            int daysInMonth = DateTime.DaysInMonth(date.Year, date.Month);

            DateTime halfTimeDate = new DateTime(date.Year, date.Month, daysInMonth / 2),
                     endDate = new DateTime(date.Year, date.Month, daysInMonth);

            while (!halfTimeDate.IsWeekDay() || !endDate.IsWeekDay())
            {
                if (!halfTimeDate.IsWeekDay())
                    halfTimeDate = halfTimeDate.AddDays(-1);

                if (!endDate.IsWeekDay())
                    endDate = endDate.AddDays(-1);
            }

            return (date.Day <= halfTimeDate.Day)
                   ? halfTimeDate
                   : endDate;
        }

        /// <summary>
        /// Sets the property.
        /// </summary>
        /// <param name="objType">Type of the object.</param>
        /// <param name="property">The property.</param>
        /// <param name="newPropType">New type of the property.</param>
        /// <param name="newName">The new name.</param>
        /// <returns></returns>
        public static Type SetProperty(this Type objType, PropertyInfo property, Type newPropType = null, string newName = null)
        {
            return AddOrSetProperty(objType, property.Name, newPropType ?? property.PropertyType, newName ?? property.Name);
        }

        /// <summary>
        /// Sets the property.
        /// </summary>
        /// <param name="objType">Type of the object.</param>
        /// <param name="propName">Name of the property.</param>
        /// <param name="newPropType">New type of the property.</param>
        /// <param name="newName">The new name.</param>
        /// <returns></returns>
        /// <exception cref="Exception">There is no property that is named " + propName + "...</exception>
        public static Type SetProperty(this Type objType, string propName, Type newPropType = null, string newName = null)
        {
            PropertyInfo prop = objType.GetProperties().SingleOrDefault(a => a.Name == propName);
            if (prop == null)
                throw new Exception("There is no property that is named " + propName + "...");

            return SetProperty(objType, prop, newPropType ?? prop.PropertyType, newName ?? prop.Name);
        }

        /// <summary>
        /// Sets the property value.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="ordinal">The ordinal.</param>
        /// <param name="value">The value.</param>
        public static void SetPropertyValue(this object obj, int ordinal, object value)
        {
            if (obj.GetType().GetProperties().Where((a, b) => b == ordinal).Single().GetSetMethod() != null)
                obj.GetType().GetProperties().Where((a, b) => b == ordinal).Single().SetValue(obj, value);
        }

        /// <summary>
        /// Sets the property value.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="propertyName">Name of the property.</param>
        /// <param name="value">The value.</param>
        public static void SetPropertyValue(this object obj, string propertyName, object value)
        {
            if (obj.GetType().GetProperties().Single(pi => pi.Name == propertyName).GetSetMethod() != null)
                obj.GetType().GetProperties().Single(pi => pi.Name == propertyName).SetValue(obj, value);
        }

        /// <summary>
        /// Splits by the specified seperator.
        /// </summary>
        /// <param name="txt">The text.</param>
        /// <param name="seperator">The seperator.</param>
        /// <returns></returns>
        public static string[] Split(this string txt, string seperator)
        {
            return txt.Split(new string[] { seperator }, StringSplitOptions.None);
        }

        /// <summary>
        /// Stacks the trace to dictionary.
        /// </summary>
        /// <param name="ex">The ex.</param>
        /// <returns></returns>
        public static Dictionary<string, string> StackTraceToDictionary(this Exception ex)
        {
            Dictionary<string, string> result = new Dictionary<string, string>();
            Regex r = new Regex(@"at (?<namespace>.*)\.(?<class>.*)\.(?<method>.*(.*)) in (?<file>.*):line (?<line>\d*)");
            Match match = r.Match(ex.StackTrace);

            if (match.Success)
            {
                result.Add("namespace", match.Groups["namespace"].Value.ToString());
                result.Add("class", match.Groups["class"].Value.ToString());
                result.Add("method", match.Groups["method"].Value.ToString());
                result.Add("file", match.Groups["file"].Value.ToString());
                result.Add("line", match.Groups["line"].Value.ToString());
            }

            return result;
        }

        /// <summary>
        /// Starts the of week.
        /// </summary>
        /// <param name="dt">The dt.</param>
        /// <returns></returns>
        public static DateTime StartOfWeek(this DateTime dt)
        {
            DayOfWeek firstDay = CultureInfo.CurrentCulture.DateTimeFormat.FirstDayOfWeek;
            DateTime firstDayInWeek = dt.Date;
            while (firstDayInWeek.DayOfWeek != firstDay)
                firstDayInWeek = firstDayInWeek.AddDays(-1);

            return firstDayInWeek;
        }

        /// <summary>
        /// Steps the into directory.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="targetFile">The target file.</param>
        /// <param name="recursively">if set to <c>true</c> [recursively].</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">path</exception>
        public static string StepIntoDirectory(this string path, string targetFile, bool recursively = false)
        {
            if (path == null)
                throw new ArgumentNullException(nameof(path));

            string result = Path.GetDirectoryName(path);

            do
            {
                string[] subDirectories = Directory.GetDirectories(path),
                         filesInFolder = Directory.GetFiles(path);

                foreach (string file in filesInFolder)
                    if (file.Contains(targetFile))
                        return file;

                foreach (string dir in subDirectories)
                    if (dir == targetFile)
                        return dir;

                if (recursively && subDirectories.Length > 0)
                {
                    foreach (string dir in subDirectories)
                    {
                        dir.StepIntoDirectory(targetFile, true);
                    }
                }
                else
                {
                    recursively = false;
                }
            }
            while (recursively);

            return result;
        }

        /// <summary>
        /// Steps the out of directory.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="foldersBack">The folders back.</param>
        /// <returns></returns>
        public static string StepOutOfDirectory(this string path, int foldersBack = 1)
        {
            Uri uri = null;
            string p = path;

            if (path.Substring(0, 8) == "file:///")
                path = path.Substring(8);

            for (var i = 0; i < foldersBack; i++)
                p = Directory.GetParent(p.IsValidUri(out uri) ? uri.LocalPath : p).FullName;

            return p;
        }

        /// <summary>
        /// Synchronizes the task.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="task">The task.</param>
        /// <returns></returns>
        public static T Await<T>(this Func<Task<T>> task)
        {
            return Task.Run(task).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Timestamps the specified time.
        /// </summary>
        /// <param name="time">The time.</param>
        /// <param name="format">The format.</param>
        /// <returns></returns>
        public static string Timestamp(this DateTime time, string format = null)
        {
            return time.ToString(format ?? "hh.mm.ss.tt MMM_dd_yy");
        }

        /// <summary>
        /// To the array.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static Array ToArray(this IEnumerable source, Type type)
        {
            var param = Expression.Parameter(typeof(IEnumerable), "source");
            var cast = Expression.Call(typeof(Enumerable), "Cast", new[] { type }, param);
            var toArray = Expression.Call(typeof(Enumerable), "ToArray", new[] { type }, cast);
            var lambda = Expression.Lambda<Func<IEnumerable, Array>>(toArray, param).Compile();

            return lambda(source);
        }

        /// <summary>
        /// To the character.
        /// </summary>
        /// <param name="num">The number.</param>
        /// <returns></returns>
        public static char ToChar(this int num)
        {
            return Convert.ToChar(num);
        }

        /// <summary>
        /// To the date time.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="format">The format.</param>
        /// <returns></returns>
        public static DateTime ToDateTime(this string obj, string format = null)
        {
            if (format != null) { return DateTime.ParseExact(obj, format, CultureInfo.InvariantCulture); }
            else { return Convert.ToDateTime(obj); }
        }

        /// <summary>
        /// To the delegate.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="target">The target.</param>
        /// <returns></returns>
        public static Delegate ToDelegate(this MethodInfo obj, object target = null)
        {
            Type delegateType;

            var typeArgs = obj.GetParameters()
                .Select(p => p.ParameterType)
                .ToList();

            // builds a delegate type
            if (obj.ReturnType == typeof(void))
            {
                delegateType = Expression.GetActionType(typeArgs.ToArray());
            }
            else
            {
                typeArgs.Add(obj.ReturnType);
                delegateType = Expression.GetFuncType(typeArgs.ToArray());
            }

            // creates a binded delegate if target is supplied
            var result = (target == null)
                ? Delegate.CreateDelegate(delegateType, obj)
                : Delegate.CreateDelegate(delegateType, target, obj);

            return result;
        }

        public static Dictionary<string, object> ToDictionary(this object source, BindingFlags bindingAttr = BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance)
        {
            return source.GetType().GetProperties(bindingAttr).ToDictionary
            (
                propInfo => propInfo.Name,
                propInfo => propInfo.GetValue(source, null)
            );
        }

        /// <summary>
        /// To the dictionary.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="coll">The coll.</param>
        /// <returns></returns>
        public static Dictionary<TKey, TValue> ToDictionary<TKey, TValue>(this IEnumerable<KeyValuePair<TKey, TValue>> coll)
        {
            Dictionary<TKey, TValue> result = new Dictionary<TKey, TValue>();

            if (coll != null && coll.ToArray().Length > 0)
                foreach (KeyValuePair<TKey, TValue> pair in coll.ToArray())
                    result.Add(pair.Key, pair.Value);

            return result;
        }

        /// <summary>
        /// To the dictionary.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="coll">The coll.</param>
        /// <param name="converter">The converter.</param>
        /// <returns></returns>
        public static Dictionary<TKey, TValue> ToDictionary<T, TKey, TValue>(this IEnumerable<T> coll, Converter<T, KeyValuePair<TKey, TValue>> converter)
        {
            if (coll == null)
                return null;

            Dictionary<TKey, TValue> result = new Dictionary<TKey, TValue>();
            foreach (T data in coll)
            {
                var pair = converter(data);
                result.Add(pair.Key, pair.Value);
            }

            return result;
        }

        /// <summary>
        /// To the expression.
        /// </summary>
        /// <param name="method">The method.</param>
        /// <returns></returns>
        public static Expression<Action> ToExpression(this Action method)
        {
            return () => method();
        }

        /// <summary>
        /// To the expression.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="method">The method.</param>
        /// <returns></returns>
        public static Expression<Action<T>> ToExpression<T>(this Action<T> method)
        {
            return x => method(x);
        }

        /// <summary>
        /// To the expression.
        /// </summary>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="method">The method.</param>
        /// <returns></returns>
        public static Expression<Func<TResult>> ToExpression<TResult>(this Func<TResult> method)
        {
            return () => method();
        }

        /// <summary>
        /// To the expression.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="method">The method.</param>
        /// <returns></returns>
        public static Expression<Func<T, TResult>> ToExpression<T, TResult>(this Func<T, TResult> method)
        {
            return x => method(x);
        }

        /// <summary>
        /// To the expression.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TResult">The type of the result.</typeparam>
        /// <param name="method">The method.</param>
        /// <returns></returns>
        /// <exception cref="Exception">obj is not an method...</exception>
        public static Expression<Func<T, TResult>> ToExpression<T, TResult>(this object method)
        {
            if (method is Delegate)
                return x => ((Func<T, TResult>)method)(x);
            else
                throw new Exception("obj is not an method...");
        }

        /// <summary>
        /// To the name value collection.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="dict">The dictionary.</param>
        /// <returns></returns>
        public static NameValueCollection ToNameValueCollection<TKey, TValue>(this IDictionary<TKey, TValue> dict)
        {
            var nameValueCollection = new NameValueCollection();

            foreach (var kvp in dict)
            {
                string value = null;
                if (kvp.Value != null)
                    value = kvp.Value.ToString();

                nameValueCollection.Add(kvp.Key.ToString(), value);
            }

            return nameValueCollection;
        }

        /// <summary>
        /// To the upper case.
        /// </summary>
        /// <param name="char">The character.</param>
        /// <returns></returns>
        public static char ToUpperCase(this char @char)
        {
            return char.ToUpper(@char);
        }

        /// <summary>
        /// XMLs the deserialize.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        public static object XmlDeserialize(this string obj)
        {
            object result = null;
            string path = "cars.xml";

            XmlSerializer serializer = new XmlSerializer(obj.GetType());

            StreamReader reader = new StreamReader(path);
            result = serializer.Deserialize(reader);
            reader.Close();
            return result;
        }

        /// <summary>
        /// XMLs the deserialize.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        public static T XmlDeserialize<T>(this string obj)
        {
            return (T)XmlDeserialize(obj);
        }

        /// <summary>
        /// XMLs the serialize.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        /// <exception cref="Exception">An error occurred on XmlSerialize(this object obj)...</exception>
        public static string XmlSerialize(this object obj)
        {
            if (obj == null)
                return string.Empty;

            try
            {
                var xmlserializer = new XmlSerializer(obj.GetType());
                var stringWriter = new StringWriter();
                using (var writer = XmlWriter.Create(stringWriter))
                {
                    xmlserializer.Serialize(writer, obj);
                    return stringWriter.ToString();
                }
            }
            catch (Exception ex)
            {
                throw new Exception("An error occurred on XmlSerialize(this object obj)...", ex);
            }
        }

        public static bool IsJson(this string input)
        {
            input = input.Trim();
            bool IsWellFormed()
            {
                try
                {
                    JToken.Parse(input);
                }
                catch
                {
                    return false;
                }
                return true;
            }
            return (input.StartsWith("{") && input.EndsWith("}") || input.StartsWith("[") && input.EndsWith("]")) && IsWellFormed();
        }

        public static bool IsXml(this string input)
        {
            bool result = true;
            if (!string.IsNullOrEmpty(input) && input.TrimStart().StartsWith("<"))
            {
                try
                {
                    XDocument.Parse(input);
                }
                catch
                {
                    result = false;
                }
            }
            else
                result = false;

            return result;
        }

        public static bool AllEquals<T>(this IEnumerable<T> a, IEnumerable<T> b)
        {
            if (a == null)
                return b == null;
            if (b == null || a.Count() != b.Count())
                return false;

            int i = 0;
            foreach (T item in a)
            {
                if (!Equals(item, b.ElementAt(i)))
                    return false;
                i++;
            }
            return true;
        }

        public static Task ForEachAsync<T>(this IEnumerable<T> source, Func<T, Task> body)
        {
            return Task.WhenAll(
                from item in source
                select Task.Run(() => body(item)));
        }

        public static Task ForEachAsync<T>(this IEnumerable<T> source, Action<T> body)
        {
            return Task.WhenAll(
                from item in source
                select Task.Run(() => body(item)));
        }

        public static IEnumerable<List<T>> SplitList<T>(this List<T> list, int nSize = 25)
        {
            for (int i = 0; i < list.Count; i += nSize)
            {
                yield return list.GetRange(i, Math.Min(nSize, list.Count - i));
            }
        }

        public static bool IsFileLocked(this FileInfo file)
        {
            try
            {
                using (FileStream stream = file.Open(FileMode.Open, FileAccess.Read, FileShare.None))
                {
                    stream.Close();
                }
            }
            catch (IOException ex)
            {
                //the file is unavailable because it is:
                //still being written to
                //or being processed by another thread
                //or does not exist (has already been processed)
                return true;
            }

            //file is not locked
            return false;
        }

        public static bool IsFileLocked(this string path)
        {
            return IsFileLocked(new FileInfo(path));
        }

        public static dynamic ToDynamic<T>(this T obj)
        {
            IDictionary<string, object> expando = new ExpandoObject();

            foreach (var propertyInfo in typeof(T).GetProperties())
            {
                var currentValue = propertyInfo.GetValue(obj);
                expando.Add(propertyInfo.Name, currentValue);
            }
            return expando as ExpandoObject;
        }

        public static bool AreObjectsEqual<T>(this T obj1, T obj2)
        {
            var settings = new JsonSerializerSettings
            {
                ReferenceLoopHandling = ReferenceLoopHandling.Ignore
            };

            var serializedObj1 = JsonConvert.SerializeObject(obj1, settings);
            var serializedObj2 = JsonConvert.SerializeObject(obj2, settings);

            return serializedObj1 == serializedObj2;
        }

        public static bool IsEmail(this string email)
        {
            if (string.IsNullOrEmpty(email))
                return false;

            var trimmedEmail = email.Trim();

            if (!trimmedEmail.Contains("@") || !trimmedEmail.Contains("."))
                return false;

            if (trimmedEmail.EndsWith("."))
                return false;

            try
            {
                var addr = new System.Net.Mail.MailAddress(email);
                return addr.Address == trimmedEmail;
            }
            catch
            {
                return false;
            }
        }

        public static bool IsPhoneNumber(this string number)
        {
            if (string.IsNullOrEmpty(number))
                return false;

            Match match = Regex.Match(number, @"^[0-9]{10}$");
            return match.Success;
        }

        public static T CastViaReflection<T>(this object obj)
        {
            if (obj is null)
            {
                return default;
            }

            var result = Activator.CreateInstance<T>();
            var properties = typeof(T).GetProperties();

            foreach (var property in properties)
            {
                if (property.CanWrite)
                {
                    var propertyValue = GetPropertyValue(obj, property.Name);
                    if (propertyValue != null && property.PropertyType.IsAssignableFrom(propertyValue.GetType()))
                    {
                        property.SetValue(result, propertyValue);
                    }
                }
            }

            return result;
        }

        public static bool IsAlphanumeric(this string input)
        {
            Regex regex = new Regex("^[a-zA-Z0-9]+$");
            return regex.IsMatch(input);
        }

        public static string SpaceBetweenUpperCase(this string input)
        {
            return input == null ? null : Regex.Replace(input, "([A-Z])", " $1", RegexOptions.Compiled).Trim();
        }

        public static int[] GetQuarterMonths(this DateTime dateTime)
        {
            int quarter = (dateTime.Month - 1) / 3 + 1;
            int startMonth = (quarter - 1) * 3 + 1;

            int[] months = new int[3];
            for (int i = 0; i < 3; i++)
            {
                months[i] = startMonth + i;
            }

            return months;
        }

        public static DateTime AsDateTime(this DateOnly date, TimeOnly? time = null) 
        {
            return new DateTime(date.Year, date.Month, date.Day, time?.Hour ?? 0, time?.Minute ?? 0, time?.Second ?? 0);
        }

        public static bool TryParseJson<T>(this string @this, out T result)
        {
            bool success = true;
            var settings = new JsonSerializerSettings
            {
                Error = (sender, args) => { success = false; args.ErrorContext.Handled = true; },
                MissingMemberHandling = MissingMemberHandling.Error
            };
            result = JsonConvert.DeserializeObject<T>(@this, settings);
            return success;
        }

        public static string GetName(this object obj) => obj.GetType().Name;

        public static DateTime GetNextQuarterHrTime(this DateTime currentTime)
        {
            DateTime nextTime;
            int minutes = currentTime.Minute;

            if (minutes < 15)
            {
                nextTime = currentTime.Date.AddHours(currentTime.Hour).AddMinutes(15);
            }
            else if (minutes < 30)
            {
                nextTime = currentTime.Date.AddHours(currentTime.Hour).AddMinutes(30);
            }
            else if (minutes < 45)
            {
                nextTime = currentTime.Date.AddHours(currentTime.Hour).AddMinutes(45);
            }
            else
            {
                nextTime = currentTime.Date.AddHours(currentTime.Hour + 1).AddMinutes(0);
            }

            return nextTime;
        }

        public static TimeOnly ToTimeOnly(this TimeSpan timeSpan)
        {
            return new TimeOnly(timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds);
        }

        public static DateTime GetStartOfWeek(this DateTime date) => date.AddDays(-(int)date.DayOfWeek);

        public static DateTime GetEndOfWeek(this DateTime date) => GetStartOfWeek(date).AddDays(6);

        public static DateTime GetStartOfMonth(this DateTime date) => new DateTime(date.Year, date.Month, 1);

        public static DateTime GetEndOfMonth(this DateTime date) => GetStartOfMonth(date).AddMonths(1).AddDays(-1);

        public static DateTime GetStartOfQuarter(this DateTime date) => new DateTime(date.Year, ((date.Month - 1) / 3) * 3 + 1, 1);

        public static DateTime GetEndOfQuarter(this DateTime date) => GetStartOfQuarter(date).AddMonths(3).AddDays(-1);

        public static DateTime GetStartOfHalfYear(this DateTime date) => new DateTime(date.Year, (date.Month <= 6) ? 1 : 7, 1);

        public static DateTime GetEndOfHalfYear(this DateTime date) => GetStartOfHalfYear(date).AddMonths(6).AddDays(-1);

        public static DateTime GetStartOfYear(this DateTime date) => new DateTime(date.Year, 1, 1);

        public static DateTime GetEndOfYear(this DateTime date) => GetStartOfYear(date).AddYears(1).AddDays(-1);

        public static string GetDaySuffix(this DateTime date)
        {
            var day = date.Day;
            if (day >= 11 && day <= 13)
                return "th";

            switch (day % 10)
            {
                case 1: return "st";
                case 2: return "nd";
                case 3: return "rd";
                default: return "th";
            }
        }

        public static bool ContainsHTML(this string checkString)
        {
            return Regex.IsMatch(checkString, "<(.|\n)*?>");
        }

        public static int GetOccurrenceInQuarter(this DateTime date)
        {
            // Get the quarter (1, 2, 3, or 4) of the date
            int quarter = (date.Month - 1) / 3 + 1;

            // Get the total days in the quarter
            int totalDaysInQuarter = DateTime.DaysInMonth(date.Year, quarter * 3);

            // Get the occurrence (1st, 2nd, 3rd, or 4th) of the day in the quarter
            int occurrence = (date.Day - 1) / 7 + 1;

            // If the day is in the last week of the quarter, it might belong to the next quarter
            if (date.AddDays(7).Month != date.Month && occurrence != 1)
            {
                // Adjust the occurrence to the last week of the quarter
                occurrence = (totalDaysInQuarter - date.Day) / 7 + 1;
            }

            return occurrence;
        }

        public static int GetOccurrenceInHalfYear(this DateTime date)
        {
            // Get the half year (1 or 2) of the date
            int halfYear = date.Month <= 6 ? 1 : 2;

            // Get the total days in the half year
            int totalDaysInHalfYear = DateTime.DaysInMonth(date.Year, halfYear * 6);

            // Get the occurrence (1st, 2nd, or 3rd) of the day in the half year
            int occurrence = (date.Day - 1) / 7 + 1;

            // If the day is in the last week of the half year, it might belong to the next half year
            if (date.AddDays(7).Month != date.Month && occurrence != 1)
            {
                // Adjust the occurrence to the last week of the half year
                occurrence = (totalDaysInHalfYear - date.Day) / 7 + 1;
            }

            return occurrence;
        }

        public static int GetOccurrenceInYear(this DateTime date)
        {
            // Get the total days in the year
            int totalDaysInYear = DateTime.IsLeapYear(date.Year) ? 366 : 365;

            // Get the occurrence (1st, 2nd, 3rd, or 4th) of the day in the year
            int occurrence = (date.Day - 1) / 7 + 1;

            // If the day is in the last week of the year, it might belong to the next year
            if (date.AddDays(7).Year != date.Year && occurrence != 1)
            {
                // Adjust the occurrence to the last week of the year
                occurrence = (totalDaysInYear - date.Day) / 7 + 1;
            }

            return occurrence;
        }

        public static string GetOrdinalNumber(this int number, bool useSuffix)
        {
            if (useSuffix)
            {
                if (number >= 11 && number <= 13)
                {
                    return number.ToString() + "th";
                }
                switch (number % 10)
                {
                    case 1:
                        return number.ToString() + "st";
                    case 2:
                        return number.ToString() + "nd";
                    case 3:
                        return number.ToString() + "rd";
                    default:
                        return number.ToString() + "th";
                }
            }
            else
            {
                return number.ToString();
            }
        }

        public static T Clone<T>(this T obj, bool ignoreReferenceLoopHandling = true)
        {
            var settings = new JsonSerializerSettings
            {
                ReferenceLoopHandling = ignoreReferenceLoopHandling ? ReferenceLoopHandling.Ignore : ReferenceLoopHandling.Serialize
            };

            var seliarzedObj = JsonConvert.SerializeObject(obj, settings);
            return JsonConvert.DeserializeObject<T>(seliarzedObj, settings);
        }

        public static string GetFileExtension(this string fileName)
        {
            if (string.IsNullOrEmpty(fileName))
            {
                throw new ArgumentException("File name cannot be null or empty.");
            }

            int dotIndex = fileName.LastIndexOf('.');
            if (dotIndex >= 0 && dotIndex < fileName.Length - 1)
            {
                return fileName.Substring(dotIndex + 1);
            }

            return string.Empty;
        }

        public static string FirstCharToUpper(this string input) =>
        input switch
        {
            null => throw new ArgumentNullException(nameof(input)),
            "" => throw new ArgumentException($"{nameof(input)} cannot be empty", nameof(input)),
            _ => string.Concat(input[0].ToString().ToUpper(), input.AsSpan(1))
        };

        public static string Masked(this string source, char maskValue, int start, int count, params char[] excludedChars)
        {
            var firstPart = source.Substring(0, start);
            var lastPart = source.Substring(start + count, source.Length - start - count);
            var middlePart = new string(maskValue, count);

            // Exclude specified characters from being masked
            for (int i = 0; i < count; i++)
            {
                char currentChar = source[start + i];
                if (excludedChars.Contains(currentChar))
                {
                    middlePart = middlePart.Remove(i, 1).Insert(i, currentChar.ToString());
                }
            }

            return firstPart + middlePart + lastPart;
        }

        public static T SetPropertyValue<T, TValue>(this T obj, TValue value, Expression<Func<T, TValue>> selector)
        {
            var memberExpression = selector.Body as MemberExpression;
            var property = memberExpression.Member as PropertyInfo;

            if (property != default)
                property.SetValue(obj, value);

            return obj;
        }

        public static object SetPropertyValue<TValue>(this object obj, TValue value, Expression<Func<object, TValue>> selector)
        {
            var memberExpression = selector.Body as MemberExpression;
            var property = memberExpression.Member as PropertyInfo;

            if (property != default)
                property.SetValue(obj, value);

            return obj;
        }
        #endregion Extensions
    }
}