using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Xml.Linq;

namespace Nostreets.Extensions.Helpers.Data.QueryProvider
{
    public class AttributeMapping : AdvancedMapping
    {
        Type contextType;
        Dictionary<string, MappingEntity> entities = new Dictionary<string, MappingEntity>();
        ReaderWriterLock rwLock = new ReaderWriterLock();

        public AttributeMapping(Type contextType)
        {
            this.contextType = contextType;
        }

        public override MappingEntity GetEntity(MemberInfo contextMember)
        {
            Type elementType = TypeHelper.GetElementType(TypeHelper.GetMemberType(contextMember));
            return this.GetEntity(elementType, contextMember.Name);
        }

        public override MappingEntity GetEntity(Type type, string tableId)
        {
            return this.GetEntity(type, tableId, type);
        }

        private MappingEntity GetEntity(Type elementType, string tableId, Type entityType)
        {
            MappingEntity entity;
            rwLock.AcquireReaderLock(Timeout.Infinite);
            if (!entities.TryGetValue(tableId, out entity))
            {
                rwLock.ReleaseReaderLock();
                rwLock.AcquireWriterLock(Timeout.Infinite);
                if (!entities.TryGetValue(tableId, out entity))
                {
                    entity = this.CreateEntity(elementType, tableId, entityType);
                    this.entities.Add(tableId, entity);
                }
                rwLock.ReleaseWriterLock();
            }
            else
            {
                rwLock.ReleaseReaderLock();
            }
            return entity;
        }

        protected virtual IEnumerable<MappingAttribute> GetMappingAttributes(string rootEntityId)
        {
            var contextMember = this.FindMember(this.contextType, rootEntityId);
            return (MappingAttribute[])Attribute.GetCustomAttributes(contextMember, typeof(MappingAttribute));
        }

        public override string GetTableId(Type entityType)
        {
            if (contextType != null)
            {
                foreach (var mi in contextType.GetMembers(BindingFlags.Instance | BindingFlags.Public))
                {
                    FieldInfo fi = mi as FieldInfo;
                    if (fi != null && TypeHelper.GetElementType(fi.FieldType) == entityType)
                        return fi.Name;
                    PropertyInfo pi = mi as PropertyInfo;
                    if (pi != null && TypeHelper.GetElementType(pi.PropertyType) == entityType)
                        return pi.Name;
                }
            }
            return entityType.Name;
        }

        private MappingEntity CreateEntity(Type elementType, string tableId, Type entityType)
        {
            if (tableId == null)
                tableId = this.GetTableId(elementType);
            var members = new HashSet<string>();
            var mappingMembers = new List<AttributeMappingMember>();
            int dot = tableId.IndexOf('.');
            var rootTableId = dot > 0 ? tableId.Substring(0, dot) : tableId;
            var path = dot > 0 ? tableId.Substring(dot + 1) : "";
            var mappingAttributes = this.GetMappingAttributes(rootTableId);
            var tableAttributes = mappingAttributes.OfType<TableBaseAttribute>()
                .OrderBy(ta => ta.Name);
            var tableAttr = tableAttributes.OfType<TableAttribute>().FirstOrDefault();
            if (tableAttr != null && tableAttr.EntityType != null && entityType == elementType)
            {
                entityType = tableAttr.EntityType;
            }
            var memberAttributes = mappingAttributes.OfType<MemberAttribute>()
                .Where(ma => ma.Member.StartsWith(path))
                .OrderBy(ma => ma.Member);

            foreach (var attr in memberAttributes)
            {
                if (string.IsNullOrEmpty(attr.Member))
                    continue;
                string memberName = (path.Length == 0) ? attr.Member : attr.Member.Substring(path.Length + 1);
                MemberInfo member = null;
                MemberAttribute attribute = null;
                AttributeMappingEntity nested = null;
                if (memberName.Contains('.')) // additional nested mappings
                {
                    string nestedMember = memberName.Substring(0, memberName.IndexOf('.'));
                    if (nestedMember.Contains('.'))
                        continue; // don't consider deeply nested members yet
                    if (members.Contains(nestedMember))
                        continue; // already seen it (ignore additional)
                    members.Add(nestedMember);
                    member = this.FindMember(entityType, nestedMember);
                    string newTableId = tableId + "." + nestedMember;
                    nested = (AttributeMappingEntity)this.GetEntity(TypeHelper.GetMemberType(member), newTableId);
                }
                else
                {
                    if (members.Contains(memberName))
                    {
                        throw new InvalidOperationException(string.Format("AttributeMapping: more than one mapping attribute specified for member '{0}' on type '{1}'", memberName, entityType.Name));
                    }
                    member = this.FindMember(entityType, memberName);
                    attribute = attr;
                }
                mappingMembers.Add(new AttributeMappingMember(member, attribute, nested));
            }
            return new AttributeMappingEntity(elementType, tableId, entityType, tableAttributes, mappingMembers);
        }

        private static readonly char[] dotSeparator = new char[] { '.' };

        private MemberInfo FindMember(Type type, string path)
        {
            MemberInfo member = null;
            string[] names = path.Split(dotSeparator);
            foreach (string name in names)
            {
                member = type.GetMember(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.IgnoreCase).FirstOrDefault();
                if (member == null)
                {
                    throw new InvalidOperationException(string.Format("AttributMapping: the member '{0}' does not exist on type '{1}'", name, type.Name));
                }
                type = TypeHelper.GetElementType(TypeHelper.GetMemberType(member));
            }
            return member;
        }

        public override string GetTableName(MappingEntity entity)
        {
            AttributeMappingEntity en = (AttributeMappingEntity)entity;
            var table = en.Tables.FirstOrDefault();
            return this.GetTableName(table);
        }

        private string GetTableName(MappingEntity entity, TableBaseAttribute attr)
        {
            string name = (attr != null && !string.IsNullOrEmpty(attr.Name))
                ? attr.Name
                : entity.TableId;
            return name;
        }

        public override IEnumerable<MemberInfo> GetMappedMembers(MappingEntity entity)
        {
            return ((AttributeMappingEntity)entity).MappedMembers;
        }

        public override bool IsMapped(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingMember mm = ((AttributeMappingEntity)entity).GetMappingMember(member.Name);
            return mm != null;
        }

        public override bool IsColumn(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingMember mm = ((AttributeMappingEntity)entity).GetMappingMember(member.Name);
            return mm != null && mm.Column != null;
        }

        public override bool IsComputed(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingMember mm = ((AttributeMappingEntity)entity).GetMappingMember(member.Name);
            return mm != null && mm.Column != null && mm.Column.IsComputed;
        }

        public override bool IsGenerated(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingMember mm = ((AttributeMappingEntity)entity).GetMappingMember(member.Name);
            return mm != null && mm.Column != null && mm.Column.IsGenerated;
        }

        public override bool IsReadOnly(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingMember mm = ((AttributeMappingEntity)entity).GetMappingMember(member.Name);
            return mm != null && mm.Column != null && mm.Column.IsReadOnly;
        }

        public override bool IsPrimaryKey(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingMember mm = ((AttributeMappingEntity)entity).GetMappingMember(member.Name);
            return mm != null && mm.Column != null && mm.Column.IsPrimaryKey;
        }

        public override string GetColumnName(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingMember mm = ((AttributeMappingEntity)entity).GetMappingMember(member.Name);
            if (mm != null && mm.Column != null && !string.IsNullOrEmpty(mm.Column.Name))
                return mm.Column.Name;
            return base.GetColumnName(entity, member);
        }

        public override string GetColumnDbType(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingMember mm = ((AttributeMappingEntity)entity).GetMappingMember(member.Name);
            if (mm != null && mm.Column != null && !string.IsNullOrEmpty(mm.Column.DbType))
                return mm.Column.DbType;
            return null;
        }

        public override bool IsAssociationRelationship(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingMember mm = ((AttributeMappingEntity)entity).GetMappingMember(member.Name);
            return mm != null && mm.Association != null;
        }

        public override bool IsRelationshipSource(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingMember mm = ((AttributeMappingEntity)entity).GetMappingMember(member.Name);
            if (mm != null && mm.Association != null)
            {
                if (mm.Association.IsForeignKey && !typeof(IEnumerable).IsAssignableFrom(TypeHelper.GetMemberType(member)))
                    return true;
            }
            return false;
        }

        public override bool IsRelationshipTarget(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingMember mm = ((AttributeMappingEntity)entity).GetMappingMember(member.Name);
            if (mm != null && mm.Association != null)
            {
                if (!mm.Association.IsForeignKey || typeof(IEnumerable).IsAssignableFrom(TypeHelper.GetMemberType(member)))
                    return true;
            }
            return false;
        }

        public override bool IsNestedEntity(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingMember mm = ((AttributeMappingEntity)entity).GetMappingMember(member.Name);
            return mm != null && mm.NestedEntity != null;
        }

        public override MappingEntity GetRelatedEntity(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingEntity thisEntity = (AttributeMappingEntity)entity;
            AttributeMappingMember mm = thisEntity.GetMappingMember(member.Name);
            if (mm != null)
            {
                if (mm.Association != null)
                {
                    Type elementType = TypeHelper.GetElementType(TypeHelper.GetMemberType(member));
                    Type entityType = (mm.Association.RelatedEntityType != null) ? mm.Association.RelatedEntityType : elementType;
                    return this.GetReferencedEntity(elementType, mm.Association.RelatedEntityID, entityType, "Association.RelatedEntityID");
                }
                else if (mm.NestedEntity != null)
                {
                    return mm.NestedEntity;
                }
            }
            return base.GetRelatedEntity(entity, member);
        }

        private static readonly char[] separators = new char[] { ' ', ',', '|' };

        public override IEnumerable<MemberInfo> GetAssociationKeyMembers(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingEntity thisEntity = (AttributeMappingEntity)entity;
            AttributeMappingMember mm = thisEntity.GetMappingMember(member.Name);
            if (mm != null && mm.Association != null)
            {
                return this.GetReferencedMembers(thisEntity, mm.Association.KeyMembers, "Association.KeyMembers", thisEntity.EntityType);
            }
            return base.GetAssociationKeyMembers(entity, member);
        }

        public override IEnumerable<MemberInfo> GetAssociationRelatedKeyMembers(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingEntity thisEntity = (AttributeMappingEntity)entity;
            AttributeMappingEntity relatedEntity = (AttributeMappingEntity)this.GetRelatedEntity(entity, member);
            AttributeMappingMember mm = thisEntity.GetMappingMember(member.Name);
            if (mm != null && mm.Association != null)
            {
                return this.GetReferencedMembers(relatedEntity, mm.Association.RelatedKeyMembers, "Association.RelatedKeyMembers", thisEntity.EntityType);
            }
            return base.GetAssociationRelatedKeyMembers(entity, member);
        }

        private IEnumerable<MemberInfo> GetReferencedMembers(AttributeMappingEntity entity, string names, string source, Type sourceType)
        {
            return names.Split(separators).Select(n => this.GetReferencedMember(entity, n, source, sourceType));
        }

        private MemberInfo GetReferencedMember(AttributeMappingEntity entity, string name, string source, Type sourceType)
        {
            var mm = entity.GetMappingMember(name);
            if (mm == null)
            {
                throw new InvalidOperationException(string.Format("AttributeMapping: The member '{0}.{1}' referenced in {2} for '{3}' is not mapped or does not exist", entity.EntityType.Name, name, source, sourceType.Name));
            }
            return mm.Member;
        }

        private MappingEntity GetReferencedEntity(Type elementType, string name, Type entityType, string source)
        {
            var entity = this.GetEntity(elementType, name, entityType);
            if (entity == null)
            {
                throw new InvalidOperationException(string.Format("The entity '{0}' referenced in {1} of '{2}' does not exist", name, source, entityType.Name));
            }
            return entity;
        }

        public override IList<MappingTable> GetTables(MappingEntity entity)
        {
            return ((AttributeMappingEntity)entity).Tables;
        }

        public override string GetAlias(MappingTable table)
        {
            return ((AttributeMappingTable)table).Attribute.Alias;
        }

        public override string GetAlias(MappingEntity entity, MemberInfo member)
        {
            AttributeMappingMember mm = ((AttributeMappingEntity)entity).GetMappingMember(member.Name);
            return (mm != null && mm.Column != null) ? mm.Column.Alias : null;
        }

        public override string GetTableName(MappingTable table)
        {
            var amt = (AttributeMappingTable)table;
            return this.GetTableName(amt.Entity, amt.Attribute);
        }

        public override bool IsExtensionTable(MappingTable table)
        {
            return ((AttributeMappingTable)table).Attribute is ExtensionTableAttribute;
        }

        public override string GetExtensionRelatedAlias(MappingTable table)
        {
            var attr = ((AttributeMappingTable)table).Attribute as ExtensionTableAttribute;
            return (attr != null) ? attr.RelatedAlias : null;
        }

        public override IEnumerable<string> GetExtensionKeyColumnNames(MappingTable table)
        {
            var attr = ((AttributeMappingTable)table).Attribute as ExtensionTableAttribute;
            if (attr == null) return new string[] { };
            return attr.KeyColumns.Split(separators);
        }

        public override IEnumerable<MemberInfo> GetExtensionRelatedMembers(MappingTable table)
        {
            var amt = (AttributeMappingTable)table;
            var attr = amt.Attribute as ExtensionTableAttribute;
            if (attr == null) return new MemberInfo[] { };
            return attr.RelatedKeyColumns.Split(separators).Select(n => this.GetMemberForColumn(amt.Entity, n));
        }

        private MemberInfo GetMemberForColumn(MappingEntity entity, string columnName)
        {
            foreach (var m in this.GetMappedMembers(entity))
            {
                if (this.IsNestedEntity(entity, m))
                {
                    var m2 = this.GetMemberForColumn(this.GetRelatedEntity(entity, m), columnName);
                    if (m2 != null)
                        return m2;
                }
                else if (this.IsColumn(entity, m) && string.Compare(this.GetColumnName(entity, m), columnName, true) == 0)
                {
                    return m;
                }
            }
            return null;
        }

        public override QueryMapper CreateMapper(QueryTranslator translator)
        {
            return new AttributeMapper(this, translator);
        }

        class AttributeMapper : AdvancedMapper
        {
            AttributeMapping mapping;

            public AttributeMapper(AttributeMapping mapping, QueryTranslator translator)
                : base(mapping, translator)
            {
                this.mapping = mapping;
            }
        }

        class AttributeMappingMember
        {
            MemberInfo member;
            MemberAttribute attribute;
            AttributeMappingEntity nested;

            internal AttributeMappingMember(MemberInfo member, MemberAttribute attribute, AttributeMappingEntity nested)
            {
                this.member = member;
                this.attribute = attribute;
                this.nested = nested;
            }

            internal MemberInfo Member
            {
                get { return this.member; }
            }

            internal ColumnAttribute Column
            {
                get { return this.attribute as ColumnAttribute; }
            }

            internal AssociationAttribute Association
            {
                get { return this.attribute as AssociationAttribute; }
            }

            internal AttributeMappingEntity NestedEntity
            {
                get { return this.nested; }
            }
        }

        class AttributeMappingTable : MappingTable
        {
            AttributeMappingEntity entity;
            TableBaseAttribute attribute;

            internal AttributeMappingTable(AttributeMappingEntity entity, TableBaseAttribute attribute)
            {
                this.entity = entity;
                this.attribute = attribute;
            }

            public AttributeMappingEntity Entity
            {
                get { return this.entity; }
            }

            public TableBaseAttribute Attribute
            {
                get { return this.attribute; }
            }
        }

        class AttributeMappingEntity : MappingEntity
        {
            string tableId;
            Type elementType;
            Type entityType;
            ReadOnlyCollection<MappingTable> tables;
            Dictionary<string, AttributeMappingMember> mappingMembers;

            internal AttributeMappingEntity(Type elementType, string tableId, Type entityType, IEnumerable<TableBaseAttribute> attrs, IEnumerable<AttributeMappingMember> mappingMembers)
            {
                this.tableId = tableId;
                this.elementType = elementType;
                this.entityType = entityType;
                this.tables = attrs.Select(a => (MappingTable)new AttributeMappingTable(this, a)).ToReadOnly();
                this.mappingMembers = mappingMembers.ToDictionary(mm => mm.Member.Name);
            }

            public override string TableId
            {
                get { return this.tableId; }
            }

            public override Type ElementType
            {
                get { return this.elementType; }
            }

            public override Type EntityType
            {
                get { return this.entityType; }
            }

            internal ReadOnlyCollection<MappingTable> Tables
            {
                get { return this.tables; }
            }

            internal AttributeMappingMember GetMappingMember(string name)
            {
                AttributeMappingMember mm = null;
                this.mappingMembers.TryGetValue(name, out mm);
                return mm;
            }

            internal IEnumerable<MemberInfo> MappedMembers
            {
                get { return this.mappingMembers.Values.Select(mm => mm.Member); }
            }
        }
    }


    public abstract class MappingTable
    {
    }

    public abstract class AdvancedMapping : BasicMapping
    {
        public abstract bool IsNestedEntity(MappingEntity entity, MemberInfo member);
        public abstract IList<MappingTable> GetTables(MappingEntity entity);
        public abstract string GetAlias(MappingTable table);
        public abstract string GetAlias(MappingEntity entity, MemberInfo member);
        public abstract string GetTableName(MappingTable table);
        public abstract bool IsExtensionTable(MappingTable table);
        public abstract string GetExtensionRelatedAlias(MappingTable table);
        public abstract IEnumerable<string> GetExtensionKeyColumnNames(MappingTable table);
        public abstract IEnumerable<MemberInfo> GetExtensionRelatedMembers(MappingTable table);

        protected AdvancedMapping()
        {
        }

        public override bool IsRelationship(MappingEntity entity, MemberInfo member)
        {
            return base.IsRelationship(entity, member)
                || this.IsNestedEntity(entity, member);
        }

        public override object CloneEntity(MappingEntity entity, object instance)
        {
            object clone = base.CloneEntity(entity, instance);

            // need to clone nested entities too
            foreach (var mi in this.GetMappedMembers(entity))
            {
                if (this.IsNestedEntity(entity, mi))
                {
                    MappingEntity nested = this.GetRelatedEntity(entity, mi);
                    var nestedValue = mi.GetValue(instance);
                    if (nestedValue != null)
                    {
                        var nestedClone = this.CloneEntity(nested, mi.GetValue(instance));
                        mi.SetValue(clone, nestedClone);
                    }
                }
            }

            return clone;
        }

        public override bool IsModified(MappingEntity entity, object instance, object original)
        {
            if (base.IsModified(entity, instance, original))
                return true;

            // need to check nested entities too
            foreach (var mi in this.GetMappedMembers(entity))
            {
                if (this.IsNestedEntity(entity, mi))
                {
                    MappingEntity nested = this.GetRelatedEntity(entity, mi);
                    if (this.IsModified(nested, mi.GetValue(instance), mi.GetValue(original)))
                        return true;
                }
            }

            return false;
        }

        public override QueryMapper CreateMapper(QueryTranslator translator)
        {
            return new AdvancedMapper(this, translator);
        }
    }

    public class AdvancedMapper : BasicMapper
    {
        AdvancedMapping mapping;

        public AdvancedMapper(AdvancedMapping mapping, QueryTranslator translator)
            : base(mapping, translator)
        {
            this.mapping = mapping;
        }

        public virtual IEnumerable<MappingTable> GetDependencyOrderedTables(MappingEntity entity)
        {
            var lookup = this.mapping.GetTables(entity).ToLookup(t => this.mapping.GetAlias(t));
            return this.mapping.GetTables(entity).Sort(t => this.mapping.IsExtensionTable(t) ? lookup[this.mapping.GetExtensionRelatedAlias(t)] : null);
        }

        public override EntityExpression GetEntityExpression(Expression root, MappingEntity entity)
        {
            // must be some complex type constructed from multiple columns
            var assignments = new List<EntityAssignment>();
            foreach (MemberInfo mi in this.mapping.GetMappedMembers(entity))
            {
                if (!this.mapping.IsAssociationRelationship(entity, mi))
                {
                    Expression me;
                    if (this.mapping.IsNestedEntity(entity, mi))
                    {
                        me = this.GetEntityExpression(root, this.mapping.GetRelatedEntity(entity, mi));
                    }
                    else
                    {
                        me = this.GetMemberExpression(root, entity, mi);
                    }
                    if (me != null)
                    {
                        assignments.Add(new EntityAssignment(mi, me));
                    }
                }
            }

            return new EntityExpression(entity, this.BuildEntityExpression(entity, assignments));
        }

        public override Expression GetMemberExpression(Expression root, MappingEntity entity, MemberInfo member)
        {
            if (this.mapping.IsNestedEntity(entity, member))
            {
                MappingEntity subEntity = this.mapping.GetRelatedEntity(entity, member);
                return this.GetEntityExpression(root, subEntity);
            }
            else
            {
                return base.GetMemberExpression(root, entity, member);
            }
        }

        public override ProjectionExpression GetQueryExpression(MappingEntity entity)
        {
            var tables = this.mapping.GetTables(entity);
            if (tables.Count <= 1)
            {
                return base.GetQueryExpression(entity);
            }

            var aliases = new Dictionary<string, TableAlias>();
            MappingTable rootTable = tables.Single(ta => !this.mapping.IsExtensionTable(ta));
            var tex = new TableExpression(new TableAlias(), entity, this.mapping.GetTableName(rootTable));
            aliases.Add(this.mapping.GetAlias(rootTable), tex.Alias);
            Expression source = tex;

            foreach (MappingTable table in tables.Where(t => this.mapping.IsExtensionTable(t)))
            {
                TableAlias joinedTableAlias = new TableAlias();
                string extensionAlias = this.mapping.GetAlias(table);
                aliases.Add(extensionAlias, joinedTableAlias);

                List<string> keyColumns = this.mapping.GetExtensionKeyColumnNames(table).ToList();
                List<MemberInfo> relatedMembers = this.mapping.GetExtensionRelatedMembers(table).ToList();
                string relatedAlias = this.mapping.GetExtensionRelatedAlias(table);

                TableAlias relatedTableAlias;
                aliases.TryGetValue(relatedAlias, out relatedTableAlias);

                TableExpression joinedTex = new TableExpression(joinedTableAlias, entity, this.mapping.GetTableName(table));

                Expression cond = null;
                for (int i = 0, n = keyColumns.Count; i < n; i++)
                {
                    var memberType = TypeHelper.GetMemberType(relatedMembers[i]);
                    var colType = this.GetColumnType(entity, relatedMembers[i]);
                    var relatedColumn = new ColumnExpression(memberType, colType, relatedTableAlias, this.mapping.GetColumnName(entity, relatedMembers[i]));
                    var joinedColumn = new ColumnExpression(memberType, colType, joinedTableAlias, keyColumns[i]);
                    var eq = joinedColumn.Equal(relatedColumn);
                    cond = (cond != null) ? cond.And(eq) : eq;
                }

                source = new JoinExpression(JoinType.SingletonLeftOuter, source, joinedTex, cond);
            }

            var columns = new List<ColumnDeclaration>();
            this.GetColumns(entity, aliases, columns);
            SelectExpression root = new SelectExpression(new TableAlias(), columns, source, null);
            var existingAliases = aliases.Values.ToArray();

            Expression projector = this.GetEntityExpression(root, entity);
            var selectAlias = new TableAlias();
            var pc = ColumnProjector.ProjectColumns(this.Translator.Linguist.Language, projector, null, selectAlias, root.Alias);
            var proj = new ProjectionExpression(
                new SelectExpression(selectAlias, pc.Columns, root, null),
                pc.Projector
                );

            return (ProjectionExpression)this.Translator.Police.ApplyPolicy(proj, entity.ElementType);
        }

        private void GetColumns(MappingEntity entity, Dictionary<string, TableAlias> aliases, List<ColumnDeclaration> columns)
        {
            foreach (MemberInfo mi in this.mapping.GetMappedMembers(entity))
            {
                if (!this.mapping.IsAssociationRelationship(entity, mi))
                {
                    if (this.mapping.IsNestedEntity(entity, mi))
                    {
                        this.GetColumns(this.mapping.GetRelatedEntity(entity, mi), aliases, columns);
                    }
                    else if (this.mapping.IsColumn(entity, mi))
                    {
                        string name = this.mapping.GetColumnName(entity, mi);
                        string aliasName = this.mapping.GetAlias(entity, mi);
                        TableAlias alias;
                        aliases.TryGetValue(aliasName, out alias);
                        var colType = this.GetColumnType(entity, mi);
                        ColumnExpression ce = new ColumnExpression(TypeHelper.GetMemberType(mi), colType, alias, name);
                        ColumnDeclaration cd = new ColumnDeclaration(name, ce, colType);
                        columns.Add(cd);
                    }
                }
            }
        }

        public override Expression GetInsertExpression(MappingEntity entity, Expression instance, LambdaExpression selector)
        {
            var tables = this.mapping.GetTables(entity);
            if (tables.Count < 2)
            {
                return base.GetInsertExpression(entity, instance, selector);
            }

            var commands = new List<Expression>();

            var map = this.GetDependentGeneratedColumns(entity);
            var vexMap = new Dictionary<MemberInfo, Expression>();

            foreach (var table in this.GetDependencyOrderedTables(entity))
            {
                var tableAlias = new TableAlias();
                var tex = new TableExpression(tableAlias, entity, this.mapping.GetTableName(table));
                var assignments = this.GetColumnAssignments(tex, instance, entity,
                    (e, m) => this.mapping.GetAlias(e, m) == this.mapping.GetAlias(table) && !this.mapping.IsGenerated(e, m),
                    vexMap
                    );
                var totalAssignments = assignments.Concat(
                    this.GetRelatedColumnAssignments(tex, entity, table, vexMap)
                    );
                commands.Add(new InsertCommand(tex, totalAssignments));

                List<MemberInfo> members;
                if (map.TryGetValue(this.mapping.GetAlias(table), out members))
                {
                    var d = this.GetDependentGeneratedVariableDeclaration(entity, table, members, instance, vexMap);
                    commands.Add(d);
                }
            }

            if (selector != null)
            {
                commands.Add(this.GetInsertResult(entity, instance, selector, vexMap));
            }

            return new BlockCommand(commands);
        }

        private Dictionary<string, List<MemberInfo>> GetDependentGeneratedColumns(MappingEntity entity)
        {
            return
                (from xt in this.mapping.GetTables(entity).Where(t => this.mapping.IsExtensionTable(t))
                 group xt by this.mapping.GetExtensionRelatedAlias(xt))
                .ToDictionary(
                    g => g.Key,
                    g => g.SelectMany(xt => this.mapping.GetExtensionRelatedMembers(xt)).Distinct().ToList()
                );
        }

        // make a variable declaration / initialization for dependent generated values
        private CommandExpression GetDependentGeneratedVariableDeclaration(MappingEntity entity, MappingTable table, List<MemberInfo> members, Expression instance, Dictionary<MemberInfo, Expression> map)
        {
            // first make command that retrieves the generated ids if any
            DeclarationCommand genIdCommand = null;
            var generatedIds = this.mapping.GetMappedMembers(entity).Where(m => this.mapping.IsPrimaryKey(entity, m) && this.mapping.IsGenerated(entity, m)).ToList();
            if (generatedIds.Count > 0)
            {
                genIdCommand = this.GetGeneratedIdCommand(entity, members, map);

                // if that's all there is then just return the generated ids
                if (members.Count == generatedIds.Count)
                {
                    return genIdCommand;
                }
            }

            // next make command that retrieves the generated members
            // only consider members that were not generated ids
            members = members.Except(generatedIds).ToList();

            var tableAlias = new TableAlias();
            var tex = new TableExpression(tableAlias, entity, this.mapping.GetTableName(table));

            Expression where = null;
            if (generatedIds.Count > 0)
            {
                where = generatedIds.Select((m, i) =>
                    this.GetMemberExpression(tex, entity, m).Equal(map[m])
                    ).Aggregate((x, y) => x.And(y));
            }
            else
            {
                where = this.GetIdentityCheck(tex, entity, instance);
            }

            TableAlias selectAlias = new TableAlias();
            var columns = new List<ColumnDeclaration>();
            var variables = new List<VariableDeclaration>();
            foreach (var mi in members)
            {
                ColumnExpression col = (ColumnExpression)this.GetMemberExpression(tex, entity, mi);
                columns.Add(new ColumnDeclaration(this.mapping.GetColumnName(entity, mi), col, col.QueryType));
                ColumnExpression vcol = new ColumnExpression(col.Type, col.QueryType, selectAlias, col.Name);
                variables.Add(new VariableDeclaration(mi.Name, col.QueryType, vcol));
                map.Add(mi, new VariableExpression(mi.Name, col.Type, col.QueryType));
            }

            var genMembersCommand = new DeclarationCommand(variables, new SelectExpression(selectAlias, columns, tex, where));

            if (genIdCommand != null)
            {
                return new BlockCommand(genIdCommand, genMembersCommand);
            }

            return genMembersCommand;
        }

        private IEnumerable<ColumnAssignment> GetColumnAssignments(
            Expression table, Expression instance, MappingEntity entity,
            Func<MappingEntity, MemberInfo, bool> fnIncludeColumn,
            Dictionary<MemberInfo, Expression> map)
        {
            foreach (var m in this.mapping.GetMappedMembers(entity))
            {
                if (this.mapping.IsColumn(entity, m) && fnIncludeColumn(entity, m))
                {
                    yield return new ColumnAssignment(
                        (ColumnExpression)this.GetMemberExpression(table, entity, m),
                        this.GetMemberAccess(instance, m, map)
                        );
                }
                else if (this.mapping.IsNestedEntity(entity, m))
                {
                    var assignments = this.GetColumnAssignments(
                        table,
                        Expression.MakeMemberAccess(instance, m),
                        this.mapping.GetRelatedEntity(entity, m),
                        fnIncludeColumn,
                        map
                        );
                    foreach (var ca in assignments)
                    {
                        yield return ca;
                    }
                }
            }
        }

        private IEnumerable<ColumnAssignment> GetRelatedColumnAssignments(Expression expr, MappingEntity entity, MappingTable table, Dictionary<MemberInfo, Expression> map)
        {
            if (this.mapping.IsExtensionTable(table))
            {
                var keyColumns = this.mapping.GetExtensionKeyColumnNames(table).ToArray();
                var relatedMembers = this.mapping.GetExtensionRelatedMembers(table).ToArray();
                for (int i = 0, n = keyColumns.Length; i < n; i++)
                {
                    MemberInfo member = relatedMembers[i];
                    Expression exp = map[member];
                    yield return new ColumnAssignment((ColumnExpression)this.GetMemberExpression(expr, entity, member), exp);
                }
            }
        }

        private Expression GetMemberAccess(Expression instance, MemberInfo member, Dictionary<MemberInfo, Expression> map)
        {
            Expression exp;
            if (map == null || !map.TryGetValue(member, out exp))
            {
                exp = Expression.MakeMemberAccess(instance, member);
            }
            return exp;
        }

        public override Expression GetUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression updateCheck, LambdaExpression selector, Expression @else)
        {
            var tables = this.mapping.GetTables(entity);
            if (tables.Count < 2)
            {
                return base.GetUpdateExpression(entity, instance, updateCheck, selector, @else);
            }

            var commands = new List<Expression>();
            foreach (var table in this.GetDependencyOrderedTables(entity))
            {
                TableExpression tex = new TableExpression(new TableAlias(), entity, this.mapping.GetTableName(table));
                var assignments = this.GetColumnAssignments(tex, instance, entity, (e, m) => this.mapping.GetAlias(e, m) == this.mapping.GetAlias(table) && this.mapping.IsUpdatable(e, m), null);
                var where = this.GetIdentityCheck(tex, entity, instance);
                commands.Add(new UpdateCommand(tex, where, assignments));
            }

            if (selector != null)
            {
                commands.Add(
                    new IFCommand(
                        this.Translator.Linguist.Language.GetRowsAffectedExpression(commands[commands.Count - 1]).GreaterThan(Expression.Constant(0)),
                        this.GetUpdateResult(entity, instance, selector),
                        @else
                        )
                    );
            }
            else if (@else != null)
            {
                commands.Add(
                    new IFCommand(
                        this.Translator.Linguist.Language.GetRowsAffectedExpression(commands[commands.Count - 1]).LessThanOrEqual(Expression.Constant(0)),
                        @else,
                        null
                        )
                    );
            }

            Expression block = new BlockCommand(commands);

            if (updateCheck != null)
            {
                var test = this.GetEntityStateTest(entity, instance, updateCheck);
                return new IFCommand(test, block, null);
            }

            return block;
        }

        private Expression GetIdentityCheck(TableExpression root, MappingEntity entity, Expression instance, MappingTable table)
        {
            if (this.mapping.IsExtensionTable(table))
            {
                var keyColNames = this.mapping.GetExtensionKeyColumnNames(table).ToArray();
                var relatedMembers = this.mapping.GetExtensionRelatedMembers(table).ToArray();

                Expression where = null;
                for (int i = 0, n = keyColNames.Length; i < n; i++)
                {
                    var relatedMember = relatedMembers[i];
                    var cex = new ColumnExpression(TypeHelper.GetMemberType(relatedMember), this.GetColumnType(entity, relatedMember), root.Alias, keyColNames[n]);
                    var nex = this.GetMemberExpression(instance, entity, relatedMember);
                    var eq = cex.Equal(nex);
                    where = (where != null) ? where.And(eq) : where;
                }
                return where;
            }
            else
            {
                return base.GetIdentityCheck(root, entity, instance);
            }
        }

        public override Expression GetDeleteExpression(MappingEntity entity, Expression instance, LambdaExpression deleteCheck)
        {
            var tables = this.mapping.GetTables(entity);
            if (tables.Count < 2)
            {
                return base.GetDeleteExpression(entity, instance, deleteCheck);
            }

            var commands = new List<Expression>();
            foreach (var table in this.GetDependencyOrderedTables(entity).Reverse())
            {
                TableExpression tex = new TableExpression(new TableAlias(), entity, this.mapping.GetTableName(table));
                var where = this.GetIdentityCheck(tex, entity, instance);
                commands.Add(new DeleteCommand(tex, where));
            }

            Expression block = new BlockCommand(commands);

            if (deleteCheck != null)
            {
                var test = this.GetEntityStateTest(entity, instance, deleteCheck);
                return new IFCommand(test, block, null);
            }

            return block;
        }
    }

    public abstract class BasicMapping : QueryMapping
    {
        protected BasicMapping()
        {
        }

        public override MappingEntity GetEntity(Type elementType, string tableId)
        {
            if (tableId == null)
                tableId = this.GetTableId(elementType);
            return new BasicMappingEntity(elementType, tableId);
        }

        public override MappingEntity GetEntity(MemberInfo contextMember)
        {
            Type elementType = TypeHelper.GetElementType(TypeHelper.GetMemberType(contextMember));
            return this.GetEntity(elementType);
        }

        class BasicMappingEntity : MappingEntity
        {
            string entityID;
            Type type;

            public BasicMappingEntity(Type type, string entityID)
            {
                this.entityID = entityID;
                this.type = type;
            }

            public override string TableId
            {
                get { return this.entityID; }
            }

            public override Type ElementType
            {
                get { return this.type; }
            }

            public override Type EntityType
            {
                get { return this.type; }
            }
        }

        public override bool IsRelationship(MappingEntity entity, MemberInfo member)
        {
            return this.IsAssociationRelationship(entity, member);
        }

        /// <summary>
        /// Deterimines is a property is mapped onto a column or relationship
        /// </summary>
        public virtual bool IsMapped(MappingEntity entity, MemberInfo member)
        {
            return true;
        }

        /// <summary>
        /// Determines if a property is mapped onto a column
        /// </summary>
        public virtual bool IsColumn(MappingEntity entity, MemberInfo member)
        {
            //return this.mapping.IsMapped(entity, member) && this.translator.Linguist.Language.IsScalar(TypeHelper.GetMemberType(member));
            return this.IsMapped(entity, member);
        }

        /// <summary>
        /// The type declaration for the column in the provider's syntax
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="member"></param>
        /// <returns>a string representing the type declaration or null</returns>
        public virtual string GetColumnDbType(MappingEntity entity, MemberInfo member)
        {
            return null;
        }

        /// <summary>
        /// Determines if a property represents or is part of the entities unique identity (often primary key)
        /// </summary>
        public override bool IsPrimaryKey(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        /// <summary>
        /// Determines if a property is computed after insert or update
        /// </summary>
        public virtual bool IsComputed(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        /// <summary>
        /// Determines if a property is generated on the server during insert
        /// </summary>
        public virtual bool IsGenerated(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        /// <summary>
        /// Determines if a property should not be written back to database
        /// </summary>
        public virtual bool IsReadOnly(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        /// <summary>
        /// Determines if a property can be part of an update operation
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual bool IsUpdatable(MappingEntity entity, MemberInfo member)
        {
            return !this.IsPrimaryKey(entity, member) && !this.IsReadOnly(entity, member);
        }

        /// <summary>
        /// The type of the entity on the other side of the relationship
        /// </summary>
        public virtual MappingEntity GetRelatedEntity(MappingEntity entity, MemberInfo member)
        {
            Type relatedType = TypeHelper.GetElementType(TypeHelper.GetMemberType(member));
            return this.GetEntity(relatedType);
        }

        /// <summary>
        /// Determines if the property is an assocation relationship.
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual bool IsAssociationRelationship(MappingEntity entity, MemberInfo member)
        {
            return false;
        }

        /// <summary>
        /// Returns the key members on this side of the association
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual IEnumerable<MemberInfo> GetAssociationKeyMembers(MappingEntity entity, MemberInfo member)
        {
            return new MemberInfo[] { };
        }

        /// <summary>
        /// Returns the key members on the other side (related side) of the association
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="member"></param>
        /// <returns></returns>
        public virtual IEnumerable<MemberInfo> GetAssociationRelatedKeyMembers(MappingEntity entity, MemberInfo member)
        {
            return new MemberInfo[] { };
        }

        public abstract bool IsRelationshipSource(MappingEntity entity, MemberInfo member);

        public abstract bool IsRelationshipTarget(MappingEntity entity, MemberInfo member);

        /// <summary>
        /// The name of the corresponding database table
        /// </summary>
        public virtual string GetTableName(MappingEntity entity)
        {
            return entity.EntityType.Name;
        }

        /// <summary>
        /// The name of the corresponding table column
        /// </summary>
        public virtual string GetColumnName(MappingEntity entity, MemberInfo member)
        {
            return member.Name;
        }

        /// <summary>
        /// A sequence of all the mapped members
        /// </summary>
        public override IEnumerable<MemberInfo> GetMappedMembers(MappingEntity entity)
        {
            //Type type = entity.ElementType.IsInterface ? entity.EntityType : entity.ElementType;
            Type type = entity.EntityType;
            HashSet<MemberInfo> members = new HashSet<MemberInfo>(type.GetFields().Cast<MemberInfo>().Where(m => this.IsMapped(entity, m)));
            members.UnionWith(type.GetProperties().Cast<MemberInfo>().Where(m => this.IsMapped(entity, m)));
            return members.OrderBy(m => m.Name);
        }

        public override object CloneEntity(MappingEntity entity, object instance)
        {
            var clone = System.Runtime.Serialization.FormatterServices.GetUninitializedObject(entity.EntityType);
            foreach (var mi in this.GetMappedMembers(entity))
            {
                if (this.IsColumn(entity, mi))
                {
                    mi.SetValue(clone, mi.GetValue(instance));
                }
            }
            return clone;
        }

        public override bool IsModified(MappingEntity entity, object instance, object original)
        {
            foreach (var mi in this.GetMappedMembers(entity))
            {
                if (this.IsColumn(entity, mi))
                {
                    if (!object.Equals(mi.GetValue(instance), mi.GetValue(original)))
                        return true;
                }
            }
            return false;
        }

        public override object GetPrimaryKey(MappingEntity entity, object instance)
        {
            object firstKey = null;
            List<object> keys = null;
            foreach (var mi in this.GetPrimaryKeyMembers(entity))
            {
                if (firstKey == null)
                {
                    firstKey = mi.GetValue(instance);
                }
                else
                {
                    if (keys == null)
                    {
                        keys = new List<object>();
                        keys.Add(firstKey);
                    }
                    keys.Add(mi.GetValue(instance));
                }
            }
            if (keys != null)
            {
                return new CompoundKey(keys.ToArray());
            }
            return firstKey;
        }

        public override Expression GetPrimaryKeyQuery(MappingEntity entity, Expression source, Expression[] keys)
        {
            // make predicate
            ParameterExpression p = Expression.Parameter(entity.ElementType, "p");
            Expression pred = null;
            var idMembers = this.GetPrimaryKeyMembers(entity).ToList();
            if (idMembers.Count != keys.Length)
            {
                throw new InvalidOperationException("Incorrect number of primary key values");
            }
            for (int i = 0, n = keys.Length; i < n; i++)
            {
                MemberInfo mem = idMembers[i];
                Type memberType = TypeHelper.GetMemberType(mem);
                if (keys[i] != null && TypeHelper.GetNonNullableType(keys[i].Type) != TypeHelper.GetNonNullableType(memberType))
                {
                    throw new InvalidOperationException("Primary key value is wrong type");
                }
                Expression eq = Expression.MakeMemberAccess(p, mem).Equal(keys[i]);
                pred = (pred == null) ? eq : pred.And(eq);
            }
            var predLambda = Expression.Lambda(pred, p);

            return Expression.Call(typeof(Queryable), "SingleOrDefault", new Type[] { entity.ElementType }, source, predLambda);
        }

        public override IEnumerable<EntityInfo> GetDependentEntities(MappingEntity entity, object instance)
        {
            foreach (var mi in this.GetMappedMembers(entity))
            {
                if (this.IsRelationship(entity, mi) && this.IsRelationshipSource(entity, mi))
                {
                    MappingEntity relatedEntity = this.GetRelatedEntity(entity, mi);
                    var value = mi.GetValue(instance);
                    if (value != null)
                    {
                        var list = value as IList;
                        if (list != null)
                        {
                            foreach (var item in list)
                            {
                                if (item != null)
                                {
                                    yield return new EntityInfo(item, relatedEntity);
                                }
                            }
                        }
                        else
                        {
                            yield return new EntityInfo(value, relatedEntity);
                        }
                    }
                }
            }
        }

        public override IEnumerable<EntityInfo> GetDependingEntities(MappingEntity entity, object instance)
        {
            foreach (var mi in this.GetMappedMembers(entity))
            {
                if (this.IsRelationship(entity, mi) && this.IsRelationshipTarget(entity, mi))
                {
                    MappingEntity relatedEntity = this.GetRelatedEntity(entity, mi);
                    var value = mi.GetValue(instance);
                    if (value != null)
                    {
                        var list = value as IList;
                        if (list != null)
                        {
                            foreach (var item in list)
                            {
                                if (item != null)
                                {
                                    yield return new EntityInfo(item, relatedEntity);
                                }
                            }
                        }
                        else
                        {
                            yield return new EntityInfo(value, relatedEntity);
                        }
                    }
                }
            }
        }

        public override QueryMapper CreateMapper(QueryTranslator translator)
        {
            return new BasicMapper(this, translator);
        }
    }

    public class BasicMapper : QueryMapper
    {
        BasicMapping mapping;
        QueryTranslator translator;

        public BasicMapper(BasicMapping mapping, QueryTranslator translator)
        {
            this.mapping = mapping;
            this.translator = translator;
        }

        public override QueryMapping Mapping
        {
            get { return this.mapping; }
        }

        public override QueryTranslator Translator
        {
            get { return this.translator; }
        }

        /// <summary>
        /// The query language specific type for the column
        /// </summary>
        public virtual QueryType GetColumnType(MappingEntity entity, MemberInfo member)
        {
            string dbType = this.mapping.GetColumnDbType(entity, member);
            if (dbType != null)
            {
                return this.translator.Linguist.Language.TypeSystem.Parse(dbType);
            }
            return this.translator.Linguist.Language.TypeSystem.GetColumnType(TypeHelper.GetMemberType(member));
        }

        public override ProjectionExpression GetQueryExpression(MappingEntity entity)
        {
            var tableAlias = new TableAlias();
            var selectAlias = new TableAlias();
            var table = new TableExpression(tableAlias, entity, this.mapping.GetTableName(entity));

            Expression projector = this.GetEntityExpression(table, entity);
            var pc = ColumnProjector.ProjectColumns(this.translator.Linguist.Language, projector, null, selectAlias, tableAlias);

            var proj = new ProjectionExpression(
                new SelectExpression(selectAlias, pc.Columns, table, null),
                pc.Projector
                );

            return (ProjectionExpression)this.Translator.Police.ApplyPolicy(proj, entity.ElementType);
        }

        public override EntityExpression GetEntityExpression(Expression root, MappingEntity entity)
        {
            // must be some complex type constructed from multiple columns
            var assignments = new List<EntityAssignment>();
            foreach (MemberInfo mi in this.mapping.GetMappedMembers(entity))
            {
                if (!this.mapping.IsAssociationRelationship(entity, mi))
                {
                    Expression me = this.GetMemberExpression(root, entity, mi);
                    if (me != null)
                    {
                        assignments.Add(new EntityAssignment(mi, me));
                    }
                }
            }

            return new EntityExpression(entity, BuildEntityExpression(entity, assignments));
        }

        public class EntityAssignment
        {
            public MemberInfo Member { get; private set; }
            public Expression Expression { get; private set; }
            public EntityAssignment(MemberInfo member, Expression expression)
            {
                this.Member = member;
                System.Diagnostics.Debug.Assert(expression != null);
                this.Expression = expression;
            }
        }

        protected virtual Expression BuildEntityExpression(MappingEntity entity, IList<EntityAssignment> assignments)
        {
            NewExpression newExpression;

            // handle cases where members are not directly assignable
            EntityAssignment[] readonlyMembers = assignments.Where(b => TypeHelper.IsReadOnly(b.Member)).ToArray();
            ConstructorInfo[] cons = entity.EntityType.GetConstructors(BindingFlags.Public | BindingFlags.Instance);
            bool hasNoArgConstructor = cons.Any(c => c.GetParameters().Length == 0);

            if (readonlyMembers.Length > 0 || !hasNoArgConstructor)
            {
                // find all the constructors that bind all the read-only members
                var consThatApply = cons.Select(c => this.BindConstructor(c, readonlyMembers))
                                        .Where(cbr => cbr != null && cbr.Remaining.Count == 0).ToList();
                if (consThatApply.Count == 0)
                {
                    throw new InvalidOperationException(string.Format("Cannot construct type '{0}' with all mapped includedMembers.", entity.ElementType));
                }
                // just use the first one... (Note: need better algorithm. :-)
                if (readonlyMembers.Length == assignments.Count)
                {
                    return consThatApply[0].Expression;
                }
                var r = this.BindConstructor(consThatApply[0].Expression.Constructor, assignments);

                newExpression = r.Expression;
                assignments = r.Remaining;
            }
            else
            {
                newExpression = Expression.New(entity.EntityType);
            }

            Expression result;
            if (assignments.Count > 0)
            {
                if (entity.ElementType.IsInterface)
                {
                    assignments = this.MapAssignments(assignments, entity.EntityType).ToList();
                }
                result = Expression.MemberInit(newExpression, (MemberBinding[])assignments.Select(a => Expression.Bind(a.Member, a.Expression)).ToArray());
            }
            else
            {
                result = newExpression;
            }

            if (entity.ElementType != entity.EntityType)
            {
                result = Expression.Convert(result, entity.ElementType);
            }

            return result;
        }

        private IEnumerable<EntityAssignment> MapAssignments(IEnumerable<EntityAssignment> assignments, Type entityType)
        {
            foreach (var assign in assignments)
            {
                MemberInfo[] members = entityType.GetMember(assign.Member.Name, BindingFlags.Instance | BindingFlags.Public);
                if (members != null && members.Length > 0)
                {
                    yield return new EntityAssignment(members[0], assign.Expression);
                }
                else
                {
                    yield return assign;
                }
            }
        }

        protected virtual ConstructorBindResult BindConstructor(ConstructorInfo cons, IList<EntityAssignment> assignments)
        {
            var ps = cons.GetParameters();
            var args = new Expression[ps.Length];
            var mis = new MemberInfo[ps.Length];
            HashSet<EntityAssignment> members = new HashSet<EntityAssignment>(assignments);
            HashSet<EntityAssignment> used = new HashSet<EntityAssignment>();

            for (int i = 0, n = ps.Length; i < n; i++)
            {
                ParameterInfo p = ps[i];
                var assignment = members.FirstOrDefault(a =>
                    p.Name == a.Member.Name
                    && p.ParameterType.IsAssignableFrom(a.Expression.Type));
                if (assignment == null)
                {
                    assignment = members.FirstOrDefault(a =>
                        string.Compare(p.Name, a.Member.Name, true) == 0
                        && p.ParameterType.IsAssignableFrom(a.Expression.Type));
                }
                if (assignment != null)
                {
                    args[i] = assignment.Expression;
                    if (mis != null)
                        mis[i] = assignment.Member;
                    used.Add(assignment);
                }
                else
                {
                    MemberInfo[] mems = cons.DeclaringType.GetMember(p.Name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
                    if (mems != null && mems.Length > 0)
                    {
                        args[i] = Expression.Constant(TypeHelper.GetDefault(p.ParameterType), p.ParameterType);
                        mis[i] = mems[0];
                    }
                    else
                    {
                        // unknown parameter, does not match any member
                        return null;
                    }
                }
            }

            members.ExceptWith(used);

            return new ConstructorBindResult(Expression.New(cons, args, mis), members);
        }

        protected class ConstructorBindResult
        {
            public NewExpression Expression { get; private set; }
            public ReadOnlyCollection<EntityAssignment> Remaining { get; private set; }
            public ConstructorBindResult(NewExpression expression, IEnumerable<EntityAssignment> remaining)
            {
                this.Expression = expression;
                this.Remaining = remaining.ToReadOnly();
            }
        }

        public override bool HasIncludedMembers(EntityExpression entity)
        {
            var policy = this.translator.Police.Policy;
            foreach (var mi in this.mapping.GetMappedMembers(entity.Entity))
            {
                if (policy.IsIncluded(mi))
                    return true;
            }
            return false;
        }

        public override EntityExpression IncludeMembers(EntityExpression entity, Func<MemberInfo, bool> fnIsIncluded)
        {
            var assignments = this.GetAssignments(entity.Expression).ToDictionary(ma => ma.Member.Name);
            bool anyAdded = false;
            foreach (var mi in this.mapping.GetMappedMembers(entity.Entity))
            {
                EntityAssignment ea;
                bool okayToInclude = !assignments.TryGetValue(mi.Name, out ea) || IsNullRelationshipAssignment(entity.Entity, ea);
                if (okayToInclude && fnIsIncluded(mi))
                {
                    ea = new EntityAssignment(mi, this.GetMemberExpression(entity.Expression, entity.Entity, mi));
                    assignments[mi.Name] = ea;
                    anyAdded = true;
                }
            }
            if (anyAdded)
            {
                return new EntityExpression(entity.Entity, this.BuildEntityExpression(entity.Entity, assignments.Values.ToList()));
            }
            return entity;
        }

        private bool IsNullRelationshipAssignment(MappingEntity entity, EntityAssignment assignment)
        {
            if (this.mapping.IsRelationship(entity, assignment.Member))
            {
                var cex = assignment.Expression as ConstantExpression;
                if (cex != null && cex.Value == null)
                    return true;
            }
            return false;
        }


        private IEnumerable<EntityAssignment> GetAssignments(Expression newOrMemberInit)
        {
            var assignments = new List<EntityAssignment>();
            var minit = newOrMemberInit as MemberInitExpression;
            if (minit != null)
            {
                assignments.AddRange(minit.Bindings.OfType<MemberAssignment>().Select(a => new EntityAssignment(a.Member, a.Expression)));
                newOrMemberInit = minit.NewExpression;
            }
            var nex = newOrMemberInit as NewExpression;
            if (nex != null && nex.Members != null)
            {
                assignments.AddRange(
                    Enumerable.Range(0, nex.Arguments.Count)
                              .Where(i => nex.Members[i] != null)
                              .Select(i => new EntityAssignment(nex.Members[i], nex.Arguments[i]))
                              );
            }
            return assignments;
        }


        public override Expression GetMemberExpression(Expression root, MappingEntity entity, MemberInfo member)
        {
            if (this.mapping.IsAssociationRelationship(entity, member))
            {
                MappingEntity relatedEntity = this.mapping.GetRelatedEntity(entity, member);
                ProjectionExpression projection = this.GetQueryExpression(relatedEntity);

                // make where clause for joining back to 'root'
                var declaredTypeMembers = this.mapping.GetAssociationKeyMembers(entity, member).ToList();
                var associatedMembers = this.mapping.GetAssociationRelatedKeyMembers(entity, member).ToList();

                Expression where = null;
                for (int i = 0, n = associatedMembers.Count; i < n; i++)
                {
                    Expression equal =
                        this.GetMemberExpression(projection.Projector, relatedEntity, associatedMembers[i]).Equal(
                            this.GetMemberExpression(root, entity, declaredTypeMembers[i])
                        );
                    where = (where != null) ? where.And(equal) : equal;
                }

                TableAlias newAlias = new TableAlias();
                var pc = ColumnProjector.ProjectColumns(this.translator.Linguist.Language, projection.Projector, null, newAlias, projection.Select.Alias);

                LambdaExpression aggregator = Aggregator.GetAggregator(TypeHelper.GetMemberType(member), typeof(IEnumerable<>).MakeGenericType(pc.Projector.Type));
                var result = new ProjectionExpression(
                    new SelectExpression(newAlias, pc.Columns, projection.Select, where),
                    pc.Projector, aggregator
                    );

                return this.translator.Police.ApplyPolicy(result, member);
            }
            else
            {
                AliasedExpression aliasedRoot = root as AliasedExpression;
                if (aliasedRoot != null && this.mapping.IsColumn(entity, member))
                {
                    return new ColumnExpression(TypeHelper.GetMemberType(member), this.GetColumnType(entity, member), aliasedRoot.Alias, this.mapping.GetColumnName(entity, member));
                }
                return QueryBinder.BindMember(root, member);
            }
        }

        public override Expression GetInsertExpression(MappingEntity entity, Expression instance, LambdaExpression selector)
        {
            var tableAlias = new TableAlias();
            var table = new TableExpression(tableAlias, entity, this.mapping.GetTableName(entity));
            var assignments = this.GetColumnAssignments(table, instance, entity, (e, m) => !(mapping.IsGenerated(e, m) || mapping.IsReadOnly(e, m)));   // #MLCHANGE

            if (selector != null)
            {
                return new BlockCommand(
                    new InsertCommand(table, assignments),
                    this.GetInsertResult(entity, instance, selector, null)
                    );
            }

            return new InsertCommand(table, assignments);
        }

        private IEnumerable<ColumnAssignment> GetColumnAssignments(Expression table, Expression instance, MappingEntity entity, Func<MappingEntity, MemberInfo, bool> fnIncludeColumn)
        {
            foreach (var m in this.mapping.GetMappedMembers(entity))
            {
                if (this.mapping.IsColumn(entity, m) && fnIncludeColumn(entity, m))
                {
                    yield return new ColumnAssignment(
                        (ColumnExpression)this.GetMemberExpression(table, entity, m),
                        Expression.MakeMemberAccess(instance, m)
                        );
                }
            }
        }

        protected virtual Expression GetInsertResult(MappingEntity entity, Expression instance, LambdaExpression selector, Dictionary<MemberInfo, Expression> map)
        {
            var tableAlias = new TableAlias();
            var tex = new TableExpression(tableAlias, entity, this.mapping.GetTableName(entity));
            var aggregator = Aggregator.GetAggregator(selector.Body.Type, typeof(IEnumerable<>).MakeGenericType(selector.Body.Type));

            Expression where;
            DeclarationCommand genIdCommand = null;
            var generatedIds = this.mapping.GetMappedMembers(entity).Where(m => this.mapping.IsPrimaryKey(entity, m) && this.mapping.IsGenerated(entity, m)).ToList();
            if (generatedIds.Count > 0)
            {
                if (map == null || !generatedIds.Any(m => map.ContainsKey(m)))
                {
                    var localMap = new Dictionary<MemberInfo, Expression>();
                    genIdCommand = this.GetGeneratedIdCommand(entity, generatedIds.ToList(), localMap);
                    map = localMap;
                }

                // is this just a retrieval of one generated id member?
                var mex = selector.Body as MemberExpression;
                if (mex != null && this.mapping.IsPrimaryKey(entity, mex.Member) && this.mapping.IsGenerated(entity, mex.Member))
                {
                    if (genIdCommand != null)
                    {
                        // just use the select from the genIdCommand
                        return new ProjectionExpression(
                            genIdCommand.Source,
                            new ColumnExpression(mex.Type, genIdCommand.Variables[0].QueryType, genIdCommand.Source.Alias, genIdCommand.Source.Columns[0].Name),
                            aggregator
                            );
                    }
                    else
                    {
                        TableAlias alias = new TableAlias();
                        var colType = this.GetColumnType(entity, mex.Member);
                        return new ProjectionExpression(
                            new SelectExpression(alias, new[] { new ColumnDeclaration("", map[mex.Member], colType) }, null, null),
                            new ColumnExpression(TypeHelper.GetMemberType(mex.Member), colType, alias, ""),
                            aggregator
                            );
                    }
                }

                where = generatedIds.Select((m, i) =>
                    this.GetMemberExpression(tex, entity, m).Equal(map[m])
                    ).Aggregate((x, y) => x.And(y));
            }
            else
            {
                where = this.GetIdentityCheck(tex, entity, instance);
            }

            Expression typeProjector = this.GetEntityExpression(tex, entity);
            Expression selection = DbExpressionReplacer.Replace(selector.Body, selector.Parameters[0], typeProjector);
            TableAlias newAlias = new TableAlias();
            var pc = ColumnProjector.ProjectColumns(this.translator.Linguist.Language, selection, null, newAlias, tableAlias);
            var pe = new ProjectionExpression(
                new SelectExpression(newAlias, pc.Columns, tex, where),
                pc.Projector,
                aggregator
                );

            if (genIdCommand != null)
            {
                return new BlockCommand(genIdCommand, pe);
            }
            return pe;
        }

        protected virtual DeclarationCommand GetGeneratedIdCommand(MappingEntity entity, List<MemberInfo> members, Dictionary<MemberInfo, Expression> map)
        {
            var columns = new List<ColumnDeclaration>();
            var decls = new List<VariableDeclaration>();
            var alias = new TableAlias();
            foreach (var member in members)
            {
                Expression genId = this.translator.Linguist.Language.GetGeneratedIdExpression(member);
                var name = member.Name;
                var colType = this.GetColumnType(entity, member);
                columns.Add(new ColumnDeclaration(member.Name, genId, colType));
                decls.Add(new VariableDeclaration(member.Name, colType, new ColumnExpression(genId.Type, colType, alias, member.Name)));
                if (map != null)
                {
                    var vex = new VariableExpression(member.Name, TypeHelper.GetMemberType(member), colType);
                    map.Add(member, vex);
                }
            }
            var select = new SelectExpression(alias, columns, null, null);
            return new DeclarationCommand(decls, select);
        }

        protected virtual Expression GetIdentityCheck(Expression root, MappingEntity entity, Expression instance)
        {
            return this.mapping.GetMappedMembers(entity)
            .Where(m => this.mapping.IsPrimaryKey(entity, m))
            .Select(m => this.GetMemberExpression(root, entity, m).Equal(Expression.MakeMemberAccess(instance, m)))
            .Aggregate((x, y) => x.And(y));
        }

        protected virtual Expression GetEntityExistsTest(MappingEntity entity, Expression instance)
        {
            ProjectionExpression tq = this.GetQueryExpression(entity);
            Expression where = this.GetIdentityCheck(tq.Select, entity, instance);
            return new ExistsExpression(new SelectExpression(new TableAlias(), null, tq.Select, where));
        }

        protected virtual Expression GetEntityStateTest(MappingEntity entity, Expression instance, LambdaExpression updateCheck)
        {
            ProjectionExpression tq = this.GetQueryExpression(entity);
            Expression where = this.GetIdentityCheck(tq.Select, entity, instance);
            Expression check = DbExpressionReplacer.Replace(updateCheck.Body, updateCheck.Parameters[0], tq.Projector);
            where = where.And(check);
            return new ExistsExpression(new SelectExpression(new TableAlias(), null, tq.Select, where));
        }

        public override Expression GetUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression updateCheck, LambdaExpression selector, Expression @else)
        {
            var tableAlias = new TableAlias();
            var table = new TableExpression(tableAlias, entity, this.mapping.GetTableName(entity));

            var where = this.GetIdentityCheck(table, entity, instance);
            if (updateCheck != null)
            {
                Expression typeProjector = this.GetEntityExpression(table, entity);
                Expression pred = DbExpressionReplacer.Replace(updateCheck.Body, updateCheck.Parameters[0], typeProjector);
                where = where.And(pred);
            }

            var assignments = this.GetColumnAssignments(table, instance, entity, (e, m) => this.mapping.IsUpdatable(e, m));

            Expression update = new UpdateCommand(table, where, assignments);

            if (selector != null)
            {
                return new BlockCommand(
                    update,
                    new IFCommand(
                        this.translator.Linguist.Language.GetRowsAffectedExpression(update).GreaterThan(Expression.Constant(0)),
                        this.GetUpdateResult(entity, instance, selector),
                        @else
                        )
                    );
            }
            else if (@else != null)
            {
                return new BlockCommand(
                    update,
                    new IFCommand(
                        this.translator.Linguist.Language.GetRowsAffectedExpression(update).LessThanOrEqual(Expression.Constant(0)),
                        @else,
                        null
                        )
                    );
            }
            else
            {
                return update;
            }
        }

        protected virtual Expression GetUpdateResult(MappingEntity entity, Expression instance, LambdaExpression selector)
        {
            var tq = this.GetQueryExpression(entity);
            Expression where = this.GetIdentityCheck(tq.Select, entity, instance);
            Expression selection = DbExpressionReplacer.Replace(selector.Body, selector.Parameters[0], tq.Projector);
            TableAlias newAlias = new TableAlias();
            var pc = ColumnProjector.ProjectColumns(this.translator.Linguist.Language, selection, null, newAlias, tq.Select.Alias);
            return new ProjectionExpression(
                new SelectExpression(newAlias, pc.Columns, tq.Select, where),
                pc.Projector,
                Aggregator.GetAggregator(selector.Body.Type, typeof(IEnumerable<>).MakeGenericType(selector.Body.Type))
                );
        }

        public override Expression GetInsertOrUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression updateCheck, LambdaExpression resultSelector)
        {
            if (updateCheck != null)
            {
                Expression insert = this.GetInsertExpression(entity, instance, resultSelector);
                Expression update = this.GetUpdateExpression(entity, instance, updateCheck, resultSelector, null);
                var check = this.GetEntityExistsTest(entity, instance);
                return new IFCommand(check, update, insert);
            }
            else
            {
                Expression insert = this.GetInsertExpression(entity, instance, resultSelector);
                Expression update = this.GetUpdateExpression(entity, instance, updateCheck, resultSelector, insert);
                return update;
            }
        }

        public override Expression GetDeleteExpression(MappingEntity entity, Expression instance, LambdaExpression deleteCheck)
        {
            TableExpression table = new TableExpression(new TableAlias(), entity, this.mapping.GetTableName(entity));
            Expression where = null;

            if (instance != null)
            {
                where = this.GetIdentityCheck(table, entity, instance);
            }

            if (deleteCheck != null)
            {
                Expression row = this.GetEntityExpression(table, entity);
                Expression pred = DbExpressionReplacer.Replace(deleteCheck.Body, deleteCheck.Parameters[0], row);
                where = (where != null) ? where.And(pred) : pred;
            }

            return new DeleteCommand(table, where);
        }
    }

    public class ImplicitMapping : BasicMapping
    {
        public ImplicitMapping()
        {
        }

        public override string GetTableId(Type type)
        {
            return this.InferTableName(type);
        }

        public override bool IsPrimaryKey(MappingEntity entity, MemberInfo member)
        {
            // Customers has CustomerID, Orders has OrderID, etc
            if (this.IsColumn(entity, member))
            {
                string name = NameWithoutTrailingDigits(member.Name);
                return member.Name.EndsWith("ID") && member.DeclaringType.Name.StartsWith(member.Name.Substring(0, member.Name.Length - 2));
            }
            return false;
        }

        private string NameWithoutTrailingDigits(string name)
        {
            int n = name.Length - 1;
            while (n >= 0 && char.IsDigit(name[n]))
            {
                n--;
            }
            if (n < name.Length - 1)
            {
                return name.Substring(0, n);
            }
            return name;
        }

        public override bool IsColumn(MappingEntity entity, MemberInfo member)
        {
            return IsScalar(TypeHelper.GetMemberType(member));
        }

        private bool IsScalar(Type type)
        {
            type = TypeHelper.GetNonNullableType(type);
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Empty:
                case TypeCode.DBNull:
                    return false;
                case TypeCode.Object:
                    return
                        type == typeof(DateTimeOffset) ||
                        type == typeof(TimeSpan) ||
                        type == typeof(Guid) ||
                        type == typeof(byte[]);
                default:
                    return true;
            }
        }

        public override bool IsAssociationRelationship(MappingEntity entity, MemberInfo member)
        {
            if (IsMapped(entity, member) && !IsColumn(entity, member))
            {
                Type otherType = TypeHelper.GetElementType(TypeHelper.GetMemberType(member));
                return !this.IsScalar(otherType);
            }
            return false;
        }

        public override bool IsRelationshipSource(MappingEntity entity, MemberInfo member)
        {
            if (IsAssociationRelationship(entity, member))
            {
                if (typeof(IEnumerable).IsAssignableFrom(TypeHelper.GetMemberType(member)))
                    return false;

                // is source of relationship if relatedKeyMembers are the related entity's primary keys
                MappingEntity entity2 = GetRelatedEntity(entity, member);
                var relatedPKs = new HashSet<string>(this.GetPrimaryKeyMembers(entity2).Select(m => m.Name));
                var relatedKeyMembers = new HashSet<string>(this.GetAssociationRelatedKeyMembers(entity, member).Select(m => m.Name));
                return relatedPKs.IsSubsetOf(relatedKeyMembers) && relatedKeyMembers.IsSubsetOf(relatedPKs);
            }
            return false;
        }

        public override bool IsRelationshipTarget(MappingEntity entity, MemberInfo member)
        {
            if (IsAssociationRelationship(entity, member))
            {
                if (typeof(IEnumerable).IsAssignableFrom(TypeHelper.GetMemberType(member)))
                    return true;

                // is target of relationship if the assoctions keys are the same as this entities primary key
                var pks = new HashSet<string>(this.GetPrimaryKeyMembers(entity).Select(m => m.Name));
                var keys = new HashSet<string>(this.GetAssociationKeyMembers(entity, member).Select(m => m.Name));
                return keys.IsSubsetOf(pks) && pks.IsSubsetOf(keys);
            }
            return false;
        }

        public override IEnumerable<MemberInfo> GetAssociationKeyMembers(MappingEntity entity, MemberInfo member)
        {
            List<MemberInfo> keyMembers;
            List<MemberInfo> relatedKeyMembers;
            this.GetAssociationKeys(entity, member, out keyMembers, out relatedKeyMembers);
            return keyMembers;
        }

        public override IEnumerable<MemberInfo> GetAssociationRelatedKeyMembers(MappingEntity entity, MemberInfo member)
        {
            List<MemberInfo> keyMembers;
            List<MemberInfo> relatedKeyMembers;
            this.GetAssociationKeys(entity, member, out keyMembers, out relatedKeyMembers);
            return relatedKeyMembers;
        }

        private void GetAssociationKeys(MappingEntity entity, MemberInfo member, out List<MemberInfo> keyMembers, out List<MemberInfo> relatedKeyMembers)
        {
            MappingEntity entity2 = GetRelatedEntity(entity, member);

            // find all members in common (same name)
            var map1 = this.GetMappedMembers(entity).Where(m => this.IsColumn(entity, m)).ToDictionary(m => m.Name);
            var map2 = this.GetMappedMembers(entity2).Where(m => this.IsColumn(entity2, m)).ToDictionary(m => m.Name);
            var commonNames = map1.Keys.Intersect(map2.Keys).OrderBy(k => k);
            keyMembers = new List<MemberInfo>();
            relatedKeyMembers = new List<MemberInfo>();
            foreach (string name in commonNames)
            {
                keyMembers.Add(map1[name]);
                relatedKeyMembers.Add(map2[name]);
            }
        }

        public override string GetTableName(MappingEntity entity)
        {
            return !string.IsNullOrEmpty(entity.TableId) ? entity.TableId : this.InferTableName(entity.EntityType);
        }

        private string InferTableName(Type rowType)
        {
            return SplitWords(Plural(rowType.Name));
        }

        public static string SplitWords(string name)
        {
            StringBuilder sb = null;
            bool lastIsLower = char.IsLower(name[0]);
            for (int i = 0, n = name.Length; i < n; i++)
            {
                bool thisIsLower = char.IsLower(name[i]);
                if (lastIsLower && !thisIsLower)
                {
                    if (sb == null)
                    {
                        sb = new StringBuilder();
                        sb.Append(name, 0, i);
                    }
                    sb.Append(" ");
                }
                if (sb != null)
                {
                    sb.Append(name[i]);
                }
                lastIsLower = thisIsLower;
            }
            if (sb != null)
            {
                return sb.ToString();
            }
            return name;
        }

        public static string Plural(string name)
        {
            if (name.EndsWith("x", StringComparison.InvariantCultureIgnoreCase)
                || name.EndsWith("ch", StringComparison.InvariantCultureIgnoreCase)
                || name.EndsWith("ss", StringComparison.InvariantCultureIgnoreCase))
            {
                return name + "es";
            }
            else if (name.EndsWith("y", StringComparison.InvariantCultureIgnoreCase))
            {
                return name.Substring(0, name.Length - 1) + "ies";
            }
            else if (!name.EndsWith("s"))
            {
                return name + "s";
            }
            return name;
        }

        public static string Singular(string name)
        {
            if (name.EndsWith("es", StringComparison.InvariantCultureIgnoreCase))
            {
                string rest = name.Substring(0, name.Length - 2);
                if (rest.EndsWith("x", StringComparison.InvariantCultureIgnoreCase)
                    || name.EndsWith("ch", StringComparison.InvariantCultureIgnoreCase)
                    || name.EndsWith("ss", StringComparison.InvariantCultureIgnoreCase))
                {
                    return rest;
                }
            }
            if (name.EndsWith("ies", StringComparison.InvariantCultureIgnoreCase))
            {
                return name.Substring(0, name.Length - 3) + "y";
            }
            else if (name.EndsWith("s", StringComparison.InvariantCultureIgnoreCase)
                && !name.EndsWith("ss", StringComparison.InvariantCultureIgnoreCase))
            {
                return name.Substring(0, name.Length - 1);
            }
            return name;
        }
    }
    public class XmlMapping : AttributeMapping
    {
        Dictionary<string, XElement> entities;
        private static readonly XName Entity = XName.Get("Entity");
        private static readonly XName Id = XName.Get("Id");

        public XmlMapping(XElement root)
            : base(null)
        {
            this.entities = root.Elements().Where(e => e.Name == Entity).ToDictionary(e => (string)e.Attribute(Id));
        }

        public static XmlMapping FromXml(string xml)
        {
            return new XmlMapping(XElement.Parse(xml));
        }

        protected override IEnumerable<MappingAttribute> GetMappingAttributes(string rootEntityId)
        {
            XElement root;
            if (this.entities.TryGetValue(rootEntityId, out root))
            {
                foreach (var elem in root.Elements())
                {
                    if (elem != null)
                    {
                        yield return this.GetMappingAttribute(elem);
                    }
                }
            }
        }

        private MappingAttribute GetMappingAttribute(XElement element)
        {
            switch (element.Name.LocalName)
            {
                case "Table":
                    return this.GetMappingAttribute(typeof(TableAttribute), element);
                case "ExtensionTable":
                    return this.GetMappingAttribute(typeof(ExtensionTableAttribute), element);
                case "Column":
                    return this.GetMappingAttribute(typeof(ColumnAttribute), element);
                case "Association":
                    return this.GetMappingAttribute(typeof(AssociationAttribute), element);
                default:
                    return null;
            }
        }

        private MappingAttribute GetMappingAttribute(Type attrType, XElement element)
        {
            var ma = (MappingAttribute)Activator.CreateInstance(attrType);
            foreach (var prop in attrType.GetProperties())
            {
                var xa = element.Attribute(prop.Name);
                if (xa != null)
                {
                    if (prop.PropertyType == typeof(Type))
                    {
                        prop.SetValue(ma, this.FindType(xa.Value), null);
                    }
                    else
                    {
                        prop.SetValue(ma, Convert.ChangeType(xa.Value, prop.PropertyType), null);
                    }
                }
            }
            return ma;
        }

        private Type FindType(string name)
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                Type type = assembly.GetType(name);
                if (type != null)
                    return type;
            }
            return null;
        }
    }


    public abstract class MappingEntity
    {
        public abstract string TableId { get; }
        public abstract Type ElementType { get; }
        public abstract Type EntityType { get; }
    }

    public struct EntityInfo
    {
        object instance;
        MappingEntity mapping;

        public EntityInfo(object instance, MappingEntity mapping)
        {
            this.instance = instance;
            this.mapping = mapping;
        }

        public object Instance
        {
            get { return this.instance; }
        }

        public MappingEntity Mapping
        {
            get { return this.mapping; }
        }
    }

    public interface IHaveMappingEntity
    {
        MappingEntity Entity { get; }
    }

    /// <summary>
    /// Defines mapping information and rules for the query provider
    /// </summary>
    public abstract class QueryMapping
    {
        /// <summary>
        /// Determines the entity Id based on the type of the entity alone
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public virtual string GetTableId(Type type)
        {
            return type.Name;
        }

        /// <summary>
        /// Get the meta entity directly corresponding to the CLR type
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public virtual MappingEntity GetEntity(Type type)
        {
            return this.GetEntity(type, this.GetTableId(type));
        }

        /// <summary>
        /// Get the meta entity that maps between the CLR type 'entityType' and the database table, yet
        /// is represented publicly as 'elementType'.
        /// </summary>
        /// <param name="elementType"></param>
        /// <param name="entityID"></param>
        /// <returns></returns>
        public abstract MappingEntity GetEntity(Type elementType, string entityID);

        /// <summary>
        /// Get the meta entity represented by the IQueryable context member
        /// </summary>
        /// <param name="contextMember"></param>
        /// <returns></returns>
        public abstract MappingEntity GetEntity(MemberInfo contextMember);

        public abstract IEnumerable<MemberInfo> GetMappedMembers(MappingEntity entity);

        public abstract bool IsPrimaryKey(MappingEntity entity, MemberInfo member);

        public virtual IEnumerable<MemberInfo> GetPrimaryKeyMembers(MappingEntity entity)
        {
            return this.GetMappedMembers(entity).Where(m => this.IsPrimaryKey(entity, m));
        }

        /// <summary>
        /// Determines if a property is mapped as a relationship
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="member"></param>
        /// <returns></returns>
        public abstract bool IsRelationship(MappingEntity entity, MemberInfo member);

        /// <summary>
        /// Determines if a relationship property refers to a single entity (as opposed to a collection.)
        /// </summary>
        public virtual bool IsSingletonRelationship(MappingEntity entity, MemberInfo member)
        {
            if (!this.IsRelationship(entity, member))
                return false;
            Type ieType = TypeHelper.FindIEnumerable(TypeHelper.GetMemberType(member));
            return ieType == null;
        }

        /// <summary>
        /// Determines whether a given expression can be executed locally. 
        /// (It contains no parts that should be translated to the target environment.)
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public virtual bool CanBeEvaluatedLocally(Expression expression)
        {
            // any operation on a query can't be done locally
            ConstantExpression cex = expression as ConstantExpression;
            if (cex != null)
            {
                IQueryable query = cex.Value as IQueryable;
                if (query != null && query.Provider == this)
                    return false;
            }
            MethodCallExpression mc = expression as MethodCallExpression;
            if (mc != null &&
                (mc.Method.DeclaringType == typeof(Enumerable) ||
                 mc.Method.DeclaringType == typeof(Queryable) ||
                 mc.Method.DeclaringType == typeof(Updatable))
                 )
            {
                return false;
            }
            if (expression.NodeType == ExpressionType.Convert &&
                expression.Type == typeof(object))
                return true;
            return expression.NodeType != ExpressionType.Parameter &&
                   expression.NodeType != ExpressionType.Lambda;
        }

        public abstract object GetPrimaryKey(MappingEntity entity, object instance);
        public abstract Expression GetPrimaryKeyQuery(MappingEntity entity, Expression source, Expression[] keys);
        public abstract IEnumerable<EntityInfo> GetDependentEntities(MappingEntity entity, object instance);
        public abstract IEnumerable<EntityInfo> GetDependingEntities(MappingEntity entity, object instance);
        public abstract object CloneEntity(MappingEntity entity, object instance);
        public abstract bool IsModified(MappingEntity entity, object instance, object original);

        public abstract QueryMapper CreateMapper(QueryTranslator translator);
    }

    public abstract class QueryMapper
    {
        public abstract QueryMapping Mapping { get; }
        public abstract QueryTranslator Translator { get; }

        /// <summary>
        /// Get a query expression that selects all entities from a table
        /// </summary>
        public abstract ProjectionExpression GetQueryExpression(MappingEntity entity);

        /// <summary>
        /// Gets an expression that constructs an entity instance relative to a root.
        /// The root is most often a TableExpression, but may be any other experssion such as
        /// a ConstantExpression.
        /// </summary>
        /// <param name="root"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        public abstract EntityExpression GetEntityExpression(Expression root, MappingEntity entity);

        /// <summary>
        /// Get an expression for a mapped property relative to a root expression. 
        /// The root is either a TableExpression or an expression defining an entity instance.
        /// </summary>
        /// <param name="root"></param>
        /// <param name="entity"></param>
        /// <param name="member"></param>
        /// <returns></returns>
        public abstract Expression GetMemberExpression(Expression root, MappingEntity entity, MemberInfo member);

        /// <summary>
        /// Get an expression that represents the insert operation for the specified instance.
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="instance">The instance to insert.</param>
        /// <param name="selector">A lambda expression that computes a return value from the operation.</param>
        /// <returns></returns>
        public abstract Expression GetInsertExpression(MappingEntity entity, Expression instance, LambdaExpression selector);

        /// <summary>
        /// Get an expression that represents the update operation for the specified instance.
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="instance"></param>
        /// <param name="updateCheck"></param>
        /// <param name="selector"></param>
        /// <param name="else"></param>
        /// <returns></returns>
        public abstract Expression GetUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression updateCheck, LambdaExpression selector, Expression @else);

        /// <summary>
        /// Get an expression that represents the insert-or-update operation for the specified instance.
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="instance"></param>
        /// <param name="updateCheck"></param>
        /// <param name="resultSelector"></param>
        /// <returns></returns>
        public abstract Expression GetInsertOrUpdateExpression(MappingEntity entity, Expression instance, LambdaExpression updateCheck, LambdaExpression resultSelector);

        /// <summary>
        /// Get an expression that represents the delete operation for the specified instance.
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="instance"></param>
        /// <param name="deleteCheck"></param>
        /// <returns></returns>
        public abstract Expression GetDeleteExpression(MappingEntity entity, Expression instance, LambdaExpression deleteCheck);

        /// <summary>
        /// Recreate the type projection with the additional members included
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="fnIsIncluded"></param>
        /// <returns></returns>
        public abstract EntityExpression IncludeMembers(EntityExpression entity, Func<MemberInfo, bool> fnIsIncluded);

        /// <summary>
        /// </summary>
        /// <returns></returns>
        public abstract bool HasIncludedMembers(EntityExpression entity);

        /// <summary>
        /// Apply mapping to a sub query expression
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public virtual Expression ApplyMapping(Expression expression)
        {
            return QueryBinder.Bind(this, expression);
        }

        /// <summary>
        /// Apply mapping translations to this expression
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public virtual Expression Translate(Expression expression)
        {
            // convert references to LINQ operators into query specific nodes
            expression = QueryBinder.Bind(this, expression);

            // move aggregate computations so they occur in same select as group-by
            expression = AggregateRewriter.Rewrite(this.Translator.Linguist.Language, expression);

            // do reduction so duplicate association's are likely to be clumped together
            expression = UnusedColumnRemover.Remove(expression);
            expression = RedundantColumnRemover.Remove(expression);
            expression = RedundantSubqueryRemover.Remove(expression);
            expression = RedundantJoinRemover.Remove(expression);

            // convert references to association properties into correlated queries
            var bound = RelationshipBinder.Bind(this, expression);
            if (bound != expression)
            {
                expression = bound;
                // clean up after ourselves! (multiple references to same association property)
                expression = RedundantColumnRemover.Remove(expression);
                expression = RedundantJoinRemover.Remove(expression);
            }

            // rewrite comparision checks between entities and multi-valued constructs
            expression = ComparisonRewriter.Rewrite(this.Mapping, expression);

            return expression;
        }
    }
}
