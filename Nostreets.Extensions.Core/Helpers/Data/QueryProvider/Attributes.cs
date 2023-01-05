using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nostreets.Extensions.Helpers.Data.QueryProvider
{
    public abstract class MappingAttribute : Attribute
    {
    }

    public abstract class TableBaseAttribute : MappingAttribute
    {
        public string Name { get; set; }
        public string Alias { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
    public class TableAttribute : TableBaseAttribute
    {
        public Type EntityType { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
    public class ExtensionTableAttribute : TableBaseAttribute
    {
        public string KeyColumns { get; set; }
        public string RelatedAlias { get; set; }
        public string RelatedKeyColumns { get; set; }
    }

    public abstract class MemberAttribute : MappingAttribute
    {
        public string Member { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
    public class ColumnAttribute : MemberAttribute
    {
        public string Name { get; set; }
        public string Alias { get; set; }
        public string DbType { get; set; }
        public bool IsComputed { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsGenerated { get; set; }
        public bool IsReadOnly { get; set; }
    }

    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true)]
    public class AssociationAttribute : MemberAttribute
    {
        public string Name { get; set; }
        public string KeyMembers { get; set; }
        public string RelatedEntityID { get; set; }
        public Type RelatedEntityType { get; set; }
        public string RelatedKeyMembers { get; set; }
        public bool IsForeignKey { get; set; }
    }

}
