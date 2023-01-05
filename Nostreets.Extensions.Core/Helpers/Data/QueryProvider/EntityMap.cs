using System;
using System.Reflection;
using System.Xml.Serialization;

namespace Nostreets.Extensions.Helpers.Data.QueryProvider
{
    [Serializable(), XmlRoot(Namespace = "", IsNullable = false, ElementName = "Entity")]
    public class EntityMap
    {
        public EntityMap() { }
        public EntityMap(Type type, EntityTable table, EntityColumn[] column, EntityAssociation[] association, string id)
        {
            Table = table ?? throw new ArgumentNullException(nameof(table));
            Columns = column ?? throw new ArgumentNullException(nameof(column));
            Association = association ?? throw new ArgumentNullException(nameof(association));
            Id = id ?? throw new ArgumentNullException(nameof(id));
            Type = type ?? throw new ArgumentNullException(nameof(type));
        }

        public Type Type { get; set; }

        public EntityTable Table { get; set; }


        [XmlElement("Column")]
        public EntityColumn[] Columns { get; set; }


        [XmlElement("Association")]
        public EntityAssociation[] Association { get; set; }


        [XmlAttribute()]
        public string Id { get; set; }
    }

    [Serializable(), XmlType(AnonymousType = true)]
    public class EntityTable
    {
        public EntityTable() { }
        public EntityTable(string name)
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
        }

        [XmlAttribute()]
        public string Name { get; set; }
    }

    [Serializable(), XmlType(AnonymousType = true)]
    public class EntityColumn
    {
        public EntityColumn() { }
        public EntityColumn(string propName, string columnName, bool isPrimaryKey, bool isGenerated, string dbType, PropertyInfo property = null)
        {
            Member = propName ?? throw new ArgumentNullException(nameof(propName));
            IsPrimaryKey = isPrimaryKey;
            IsGenerated = isGenerated;
            DbType = dbType;
            Name = columnName;
            Property = property;
        }

        [XmlAttribute()]
        public string Member { get; set; }


        [XmlAttribute()]
        public bool IsPrimaryKey { get; set; }


        [XmlAttribute()]
        public bool IsGenerated { get; set; }

        [XmlAttribute()]
        public string DbType { get; set; }

        [XmlAttribute()]
        public string Name { get; set; }


        //+Custom
        [XmlAttribute()]
        public string IsNull { get; set; }

        public PropertyInfo Property { get; set; }

    }

    [Serializable(), XmlType(AnonymousType = true)]
    public class EntityAssociation
    {
        public EntityAssociation() { }
        public EntityAssociation(Type type, string member, string keyMember, string relatedEntityID, string relatedKeyMembers)
        {
            Member = member ?? throw new ArgumentNullException(nameof(member));
            KeyMembers = keyMember ?? throw new ArgumentNullException(nameof(keyMember));
            RelatedEntityID = relatedEntityID ?? throw new ArgumentNullException(nameof(relatedEntityID));
            RelatedKeyMembers = relatedKeyMembers ?? throw new ArgumentNullException(nameof(relatedKeyMembers));
            Type = type ?? throw new ArgumentNullException(nameof(type));
        }

        public Type Type { get; set; }

        [XmlAttribute()]
        public string Member { get; set; }


        [XmlAttribute()]
        public string KeyMembers { get; set; }


        [XmlAttribute()]
        public string RelatedEntityID { get; set; }


        [XmlAttribute()]
        public string RelatedKeyMembers { get; set; }
    }
}
