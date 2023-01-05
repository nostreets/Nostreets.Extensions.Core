using System;
using System.Data;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;
using Nostreets.Extensions.Extend.Basic;
using Nostreets.Extensions.Extend.Data;

namespace Nostreets.Extensions.Helpers.Data
{
    public class DataMapper
    {

        public static object MapToObject(IDataReader reader, Type type)
        {
            PropertyInfo[] props = type.GetProperties();
            string[] colNames = reader.GetColumnNames();
            var obj = type.Instantiate(); //Activator.CreateInstance(classType);

            if (props.Length > 1 && colNames.Length > 1)
            {
                foreach (PropertyInfo prop in props)
                {
                    if (colNames.Any(a => a.ToLower() == prop.Name.ToLower()))
                    {
                        Type propType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;

                        if (!propType.IsEnum && propType.IsClass && 
                            propType == typeof(string) && propType == typeof(char))
                        {
                            object property = JsonConvert.DeserializeObject(reader.GetString(reader.GetOrdinal(prop.Name)) ?? "", propType);
                            if (property == null) { property = Activator.CreateInstance(propType); }
                            if (property.GetType() != propType) { throw new Exception("Property " + propType.Name + " Does Not Equal Type in DB"); }
                            prop.SetValue(obj, property, null);
                        }
                        else if (prop.PropertyType.IsEnum)
                        {
                            if (reader[prop.Name + "Id"] != DBNull.Value)
                                prop.SetValue(obj, (reader.GetValue(reader.GetOrdinal(prop.Name + "Id")) ?? null), null);
                        }
                        else if (reader[prop.Name] != DBNull.Value)
                        {
                            if (reader[prop.Name].GetType() == typeof(decimal))
                                prop.SetValue(obj, (reader.GetDouble(prop.Name)), null);

                            else
                            {
                                object cell = reader.GetValue(reader.GetOrdinal(prop.Name));
                                Type cellType = cell.GetType();
                                prop.SetValue(obj, cellType == propType ? cell : null);
                            }
                        }
                    }

                }
            }
            else
            {
                var item = reader.GetValue(reader.GetSchemaTable().Columns[0].Ordinal);
                if (item != null) { obj = item; }
            }

            if (type != typeof(object) && obj.GetType() != type)
                throw new Exception("DataMapper did not successfully map to provided type...");

            return obj;
        }

        public static T MapToObject<T>(IDataReader reader)
        {
            Type type = typeof(T);
            T obj = (T)MapToObject(reader, type);
            return obj;
        }

    }
}