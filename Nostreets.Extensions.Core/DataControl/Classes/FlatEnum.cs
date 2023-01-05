using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Nostreets.Extensions.DataControl.Classes
{
    public class FlatEnum<T> where T : struct, IConvertible
    {
        private FlatEnum(T @enum)
        {
            if (!typeof(T).IsEnum)
                throw new ArgumentNullException("Type must be an enum");

            Value = (int)(object)@enum;
            Name = @enum.ToString();
        }


        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        [Required]
        public long Value { get; set; }

        [Required, MaxLength(100)]
        public string Name { get; set; }

    }
}
