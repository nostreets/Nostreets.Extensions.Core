using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace Nostreets.Extensions.Utilities
{
    public class ClassBuilder
    {
        private ClassBuilder(string className)
        {
            _asemblyName = new AssemblyName(className);
        }

        private AssemblyName _asemblyName = null;

        private TypeBuilder CreateClass()
        {
            //AssemblyBuilder assemblyBuilder = AppDomain.CurrentDomain.DefineDynamicAssembly(_asemblyName, AssemblyBuilderAccess.Run);
            AssemblyBuilder assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(_asemblyName, AssemblyBuilderAccess.Run);

            ModuleBuilder moduleBuilder = assemblyBuilder.DefineDynamicModule("MainModule");
            TypeBuilder typeBuilder = moduleBuilder.DefineType(_asemblyName.FullName
                                , TypeAttributes.Public |
                                TypeAttributes.Class |
                                TypeAttributes.AutoClass |
                                TypeAttributes.AnsiClass |
                                TypeAttributes.BeforeFieldInit |
                                TypeAttributes.AutoLayout
                                , null);
            return typeBuilder;
        }

        /// <summary>
        /// Adds the attribute.
        /// </summary>
        /// <param name="props">
        /// <para> ITEM 1 : Member's Name </para>
        /// <para> ITEM 2 : Member's Return Type </para>
        /// <para> ITEM 3 : Member's Class Type</para>
        /// <para>  ITEM 4 : Dictionary of Attributes To Add
        ///     KEY : Attribute To Add
        ///     VALUE: Attribute Params To Construct With
        /// </para>
        /// </param>
        /// <param name="methods">
        /// <para> ITEM 1 : Method's Name </para>
        /// <para> ITEM 2 : Method's Return Type </para>
        /// <para> ITEM 3 : Method's Parameter Types</para>
        /// <para> ITEM 4 : Method Type Attributes /para>
        /// <para>  ITEM 5 : Dictionary of Attributes To Add
        ///     KEY : Attribute To Add
        ///     VALUE: Attribute Params To Construct With
        /// </para>
        /// </param>
        private Type CreateType(
            List<Tuple<string, Type, Dictionary<Type, object[]>>> props,
            List<Tuple<string, Type, MethodAttributes, List<Tuple<Type, ParameterAttributes>>, Dictionary<Type, object[]>>> methods)
        {
            if (props != null && props.Any(a => a.Item1 == null) || props.Any(a => a.Item2 == null))
                throw new Exception("The property names and return types must never be null...");

            if (methods != null && methods.Any(a => a.Item1 == null) || props.Any(a => a.Item2 == null))
                throw new Exception("The method names and return types  must never be null...");

            methods = methods?.Where(a => !props.Any(b => "get_" + b.Item1 == a.Item1))
                              .Where(a => !props.Any(b => "set_" + b.Item1 == a.Item1))
                              .ToList();
            TypeBuilder dynamicClass = CreateClass();
            CreateConstructor(dynamicClass);

            if (props != null)
                foreach (var pair in props)
                    CreateProperty(dynamicClass, pair.Item1, pair.Item2, pair.Item3);

            if (methods != null)
                foreach (var pair in methods)
                    CreateMethod(dynamicClass, pair.Item1, pair.Item2, pair.Item3, pair.Item4, pair.Item5);

            return dynamicClass.CreateType();
        }

        private object CreateObject(
            List<Tuple<string, Type,
            Dictionary<Type, object[]>>> props,
            List<Tuple<string, Type, MethodAttributes, List<Tuple<Type, ParameterAttributes>>, Dictionary<Type, object[]>>> methods)
        {
            if (props != null && props.Any(a => a.Item1 == null) || props.Any(a => a.Item2 == null))
                throw new Exception("The property names and return types must never be null...");

            if (methods != null && methods.Any(a => a.Item1 == null) || props.Any(a => a.Item2 == null))
                throw new Exception("The method names and return types  must never be null...");

            Type type = CreateType(props, methods);
            return Activator.CreateInstance(type);
        }

        private void CreateConstructor(TypeBuilder typeBuilder)
        {
            typeBuilder.DefineDefaultConstructor(MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.RTSpecialName);
        }

        private void CreateProperty(TypeBuilder typeBuilder, string propertyName, Type propertyType, Dictionary<Type, object[]> attributes)
        {
            FieldBuilder fieldBuilder = typeBuilder.DefineField("_" + propertyName, propertyType, FieldAttributes.Private);
            PropertyBuilder propertyBuilder = typeBuilder.DefineProperty(propertyName, PropertyAttributes.HasDefault, propertyType, null);
            MethodBuilder getPropMthdBldr = typeBuilder.DefineMethod("get_" + propertyName, MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig, propertyType, Type.EmptyTypes);
            ILGenerator getIl = getPropMthdBldr.GetILGenerator();

            getIl.Emit(OpCodes.Ldarg_0);
            getIl.Emit(OpCodes.Ldfld, fieldBuilder);
            getIl.Emit(OpCodes.Ret);

            MethodBuilder setPropMthdBldr = typeBuilder.DefineMethod("set_" + propertyName,
                  MethodAttributes.Public |
                  MethodAttributes.SpecialName |
                  MethodAttributes.HideBySig,
                  null, new[] { propertyType });
            ILGenerator setIl = setPropMthdBldr.GetILGenerator();
            Label modifyProperty = setIl.DefineLabel();
            Label exitSet = setIl.DefineLabel();

            setIl.MarkLabel(modifyProperty);
            setIl.Emit(OpCodes.Ldarg_0);
            setIl.Emit(OpCodes.Ldarg_1);
            setIl.Emit(OpCodes.Stfld, fieldBuilder);

            setIl.Emit(OpCodes.Nop);
            setIl.MarkLabel(exitSet);
            setIl.Emit(OpCodes.Ret);

            propertyBuilder.SetGetMethod(getPropMthdBldr);
            propertyBuilder.SetSetMethod(setPropMthdBldr);

            if (attributes != null && attributes.Keys.All(a => a != null))
                foreach (var attribute in attributes)
                {
                    Type[] attrParams = attribute.Value?.Select(a => a.GetType()).ToArray();
                    ConstructorInfo attrConstructor = attribute.Key.GetConstructor(attrParams ?? new Type[] { })
                                        ?? attribute.Key.GetConstructors().Where((a, b) => a.GetParameters().Length == attrParams.Length && attrParams[b] == a.GetParameters()[b].ParameterType).FirstOrDefault()
                                        ?? attribute.Key.GetConstructors()[0];

                    CustomAttributeBuilder attrBuilder = new CustomAttributeBuilder(attrConstructor, attribute.Value ?? new object[] { });
                    propertyBuilder.SetCustomAttribute(attrBuilder);
                }
        }

        private void CreateMethod(TypeBuilder typeBuilder, string methodName, Type returnType, MethodAttributes methodAttributes, List<Tuple<Type, ParameterAttributes>> paramTypes, 
            Dictionary<Type, object[]> attributes)
        {
            paramTypes = paramTypes ?? new List<Tuple<Type, ParameterAttributes>>();
            MethodBuilder methodBuilder = typeBuilder.DefineMethod(methodName, methodAttributes, returnType, paramTypes.Select(a => a.Item1).ToArray());

            ILGenerator il = methodBuilder.GetILGenerator();

            // If method isn't static push target instance on top of stack.
            if (!methodBuilder.IsStatic)
                // Argument 0 of dynamic method is target instance.
                il.Emit(OpCodes.Ldarg_0);

            // Lay out args array onto stack.
            LocalBuilder[] locals = new LocalBuilder[paramTypes.Count];
            List<LocalBuilder> outOrRefLocals = new List<LocalBuilder>();
            for (int i = 0; i < paramTypes.Count; i++)
            {
                //Push args array reference onto the stack, followed
                //by the current argument index (i). The Ldelem_Ref opcode
                //will resolve them to args[i].
                if (paramTypes[i].Item2 == ParameterAttributes.Out)
                {
                    // Argument 1 of dynamic method is argument array.
                    il.Emit(OpCodes.Ldarg_1);
                    il.Emit(OpCodes.Ldc_I4, i);
                    il.Emit(OpCodes.Ldelem_Ref);
                }

                // If parameter [i] is a value type perform an unboxing.
                Type parameterType = paramTypes[i].Item1;
                if (parameterType.IsValueType)
                    il.Emit(OpCodes.Unbox_Any, parameterType);
            }

            //Create locals for out parameters
            for (int i = 0; i < paramTypes.Count; i++)
            {
                if (paramTypes[i].Item2 == ParameterAttributes.Out)
                {
                    locals[i] = il.DeclareLocal(paramTypes[i].Item1.GetElementType());
                    il.Emit(OpCodes.Ldloca, locals[locals.Length - 1]);
                }
            }

            if (methodBuilder.IsFinal || !methodBuilder.IsVirtual)
            {
                il.Emit(OpCodes.Call, methodBuilder);
            }
            else
            {
                il.Emit(OpCodes.Callvirt, methodBuilder);
            }

            for (int idx = 0; idx < paramTypes.Count; ++idx)
            {
                if (paramTypes[idx].Item2 == ParameterAttributes.Out || paramTypes[idx].Item1.IsByRef)
                {
                    il.Emit(OpCodes.Ldarg_1);
                    il.Emit(OpCodes.Ldc_I4, idx);
                    il.Emit(OpCodes.Ldloc, locals[idx].LocalIndex);

                    if (paramTypes[idx].Item1.GetElementType().IsValueType)
                        il.Emit(OpCodes.Box, paramTypes[idx].Item1.GetElementType());

                    il.Emit(OpCodes.Stelem_Ref);
                }
            }

            if (methodBuilder.ReturnType != typeof(void))
            {
                // If result is of value type it needs to be boxed
                if (methodBuilder.ReturnType.IsValueType)
                    il.Emit(OpCodes.Box, methodBuilder.ReturnType);
            }
            else
            {
                il.Emit(OpCodes.Ldnull);
            }

            il.Emit(OpCodes.Ret);

            if (attributes != null && attributes.Keys.All(a => a != null))
                foreach (var attribute in attributes)
                {
                    Type[] attrParams = attribute.Value?.Select(a => a.GetType()).ToArray();
                    ConstructorInfo attrConstructor = attribute.Key.GetConstructor(attrParams ?? new Type[] { })
                                        ?? attribute.Key.GetConstructors().Where((a, b) => a.GetParameters().Length == attrParams.Length && attrParams[b] == a.GetParameters()[b].ParameterType).FirstOrDefault()
                                        ?? attribute.Key.GetConstructors()[0];

                    CustomAttributeBuilder attrBuilder = new CustomAttributeBuilder(attrConstructor, attribute.Value ?? new object[] { });
                    methodBuilder.SetCustomAttribute(attrBuilder);
                }
        }

        public static Type CreateType(
            string className,
            List<Tuple<string, Type, Dictionary<Type, object[]>>> props,
            List<Tuple<string, Type, MethodAttributes, List<Tuple<Type, ParameterAttributes>>, Dictionary<Type, object[]>>> methods)
        {
            ClassBuilder builder = new ClassBuilder(className);
            return builder.CreateType(props, methods);
        }

        public static object CreateObject(
            string className,
            List<Tuple<string, Type, Dictionary<Type, object[]>>> props,
            List<Tuple<string, Type, MethodAttributes, List<Tuple<Type, ParameterAttributes>>, Dictionary<Type, object[]>>> methods)
        {
            ClassBuilder builder = new ClassBuilder(className);
            return builder.CreateObject(props, methods);
        }
    }
}