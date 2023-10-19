

using System.ComponentModel.DataAnnotations;

namespace Nostreets.Extensions.Core.DataControl.Enums
{
    public class EnumDBObject
    {
        private EnumDBObject() { }

        [Key]
        public int Value { get; set; }
        public string Name { get; set; }

        public static List<EnumDBObject> GetEnumDBObjects(Type type)
        {
            if (!type.IsEnum)
                throw new Exception("type has to be a Enum...");

            var result = new List<EnumDBObject>();
            var values = Enum.GetValues(type);
            foreach (var value in values)
                result.Add(new EnumDBObject { Name = value.ToString(), Value = (int)value });

            return result;
        }
    }
}
