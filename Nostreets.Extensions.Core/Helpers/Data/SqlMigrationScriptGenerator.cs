using Nostreets.Extensions.Extend.Data;

using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Reflection;

namespace Nostreets.Extensions.Core.Helpers.Data
{
    public class SqlMigrationScriptGenerator
    {
        private static List<Tuple<string, string>> TempTableColumns { get; set; } = new List<Tuple<string, string>>();
        //Tuple<string, string>
        //Item 1 = Temp Table Column Name
        //Item 2 = Temp Table Column Data Type

        private static List<Tuple<string, int, string, Action<DbDataReader>, Action>> SegmentedScripts { get; set; } = new List<Tuple<string, int, string, Action<DbDataReader>, Action>>();
        //Tuple<string, int, string, Action>
        //Item 1 = Script Key
        //Item 2 = Order Number
        //Item 3 = SQL Script
        //Item 4 = Success Callback
        //Item 4 = Rollback Callback

        public static void Migrate(DbConnection connection, string tableName, Type type)
        {
            SegmentedScripts.Clear();

            // Get Old Table Columns for refernce
            StoreColumns(connection, tableName);

            // Generate temp table name
            string tempTableName = $"temp_{tableName}_{Guid.NewGuid().ToString().Replace("-", "")}";

            // Generate CREATE TABLE script for temp table
            string createTempTableScript = $"SELECT * INTO {tempTableName} FROM {tableName};";
            SegmentedScripts.Add(new Tuple<string, int, string, Action<DbDataReader>, Action>("CREATE TEMP TABLE", 1, createTempTableScript, null, null));

            // Migrate indexes
            MigrateIndexes(connection, tableName);

            // Migrate foreign keys
            MigrateForeignKeys(connection, tableName);

            // Migrate column defaults
            MigrateColumnDefaults(connection, tableName);

            // Generate DROP TABLE script for old table
            string dropOldTableScript = $"DROP TABLE {tableName};";
            SegmentedScripts.Add(new Tuple<string, int, string, Action<DbDataReader>, Action>("DROP OLD TABLE", 3, dropOldTableScript, null, () =>
            {
                // Rollback action for dropping old table
                string createOldTableScript = $"SELECT * INTO {tableName} FROM {tempTableName};";
                RunQuery(connection, createOldTableScript);
            }));

            // Generate CREATE TABLE script for new table
            string createNewTableScript = GenerateCreateTableScript(tableName, type);
            SegmentedScripts.Add(new Tuple<string, int, string, Action<DbDataReader>, Action>("CREATE NEW TABLE", 4, createNewTableScript, null, () =>
            {
                // Rollback action for creating new table
                RunQuery(connection, dropOldTableScript);
            }));

            // Generate INSERT INTO script to re-insert data from temp table into new table
            string insertDataScript = GenerateInsertDataScript(tableName, tempTableName, type);
            SegmentedScripts.Add(new Tuple<string, int, string, Action<DbDataReader>, Action>("INSERT INTO NEW TABLE", 8, insertDataScript, null, () =>
            {
                // Rollback action for inserting data
                string deleteDataScript = $"DELETE FROM {tableName};";
                RunQuery(connection, deleteDataScript);
            }));

            // Generate DROP TABLE script for temp table
            string dropTempTableScript = $"DROP TABLE {tempTableName};";
            SegmentedScripts.Add(new Tuple<string, int, string, Action<DbDataReader>, Action>("DROP TEMP TABLE", 9, dropTempTableScript, null, () =>
            {
                // Rollback action for dropping temp table
                RunQuery(connection, createTempTableScript);
            }));

            var orderedScripts = SegmentedScripts.OrderBy(a => a.Item2);
            foreach (var script in orderedScripts)
            {
                try
                {
                    if (script.Item3 != null)
                        RunQuery(connection, script.Item3, script.Item4);
                }
                catch (Exception ex)
                {
                    List<Exception> innerExs = new List<Exception>();
                    int[] rollbackOrder = orderedScripts.Where(a => a.Item2 <= script.Item2).Select(a => a.Item2).OrderDescending().ToArray();

                    // Error occurred, execute rollback action
                    foreach (var orderNumber in rollbackOrder)
                    {
                        var rollbackScript = orderedScripts.ElementAt(orderNumber);

                        try
                        {
                            if (rollbackScript.Item5 != null)
                                rollbackScript.Item5.Invoke();
                        }
                        catch (Exception innerEx)
                        {
                            innerExs.Add(innerEx);
                        }
                    }

                    Exception masterEx = null;
                    for (var i = innerExs.Count - 1; i > 0; i--)
                    {
                        var outsideIndexRange = innerExs.Count < i + 1;
                        var currentEx = innerExs[i];
                        var previousEx = innerExs[i + 1];
                        masterEx = new Exception(currentEx.Message, outsideIndexRange ? null : previousEx);
                    }

                    throw new Exception(ex.Message, masterEx); // Re-throw the exception to propagate it to the caller
                }
            }
        }

        private static string GenerateCreateTableScript(string tableName, Type type)
        {
            string createTableScript = $"CREATE TABLE {tableName} (";
            PropertyInfo[] properties = type.GetProperties();

            foreach (PropertyInfo property in properties)
            {
                // Check if the property has the [NotMapped] attribute
                if (Attribute.IsDefined(property, typeof(NotMappedAttribute)))
                    continue;

                bool isKey = false;
                bool isNullable = Nullable.GetUnderlyingType(property.PropertyType) != null;
                string columnName = property.Name;
                Type columnType = property.PropertyType;

                // Check if the property has the [Key] attribute
                if (Attribute.IsDefined(property, typeof(KeyAttribute)))
                    isKey = true;

                string sqlType = columnType.GetSqlType(isKey);

                if (isNullable)
                {
                    // Add NULL if the column is nullable
                    sqlType += " NULL";
                }

                createTableScript += $"{columnName} {sqlType}, ";
            }

            // Add primary key constraint
            var primaryKeyColumns = properties.Where(p => Attribute.IsDefined(p, typeof(KeyAttribute))).Select(p => p.Name);
            if (primaryKeyColumns.Any())
            {
                createTableScript += $"CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ({string.Join(", ", primaryKeyColumns.Select(c => $"[{c}] ASC"))})";
            }

            createTableScript = createTableScript.TrimEnd(',', ' ');
            createTableScript += ") ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];";

            return createTableScript;
        }

        private static string GenerateInsertDataScript(string tableName, string tempTableName, Type type)
        {
            bool columnExistsInTempTable(string name, Type type) => TempTableColumns.Any(a => a.Item1 == name && type.MatchDotNetToSqlType(a.Item2));
            string insertDataScript = $"INSERT INTO {tableName} (";
            var properties = type.GetProperties();

            foreach (PropertyInfo property in properties)
            {
                // Check if the property has the [NotMapped] attribute
                if (Attribute.IsDefined(property, typeof(NotMappedAttribute)))
                    continue;

                // Check if the property is not present in the temp table with the same type
                if (!columnExistsInTempTable(property.Name, property.PropertyType))
                    continue;

                string columnName = property.Name;
                insertDataScript += $"{columnName}, ";
            }

            insertDataScript = insertDataScript.TrimEnd(',', ' ');
            insertDataScript += $") SELECT ";

            foreach (PropertyInfo property in properties)
            {
                // Check if the property has the [NotMapped] attribute or if it is not present in the temp table
                if (Attribute.IsDefined(property, typeof(NotMappedAttribute)))
                    continue;

                // Check if the property is not present in the temp table with the same type
                if (!columnExistsInTempTable(property.Name, property.PropertyType))
                    continue;

                string columnName = property.Name;
                insertDataScript += $"{columnName}, ";
            }

            insertDataScript = insertDataScript.TrimEnd(',', ' ');
            insertDataScript += $" FROM {tempTableName};";

            return insertDataScript;
        }

        private static void MigrateIndexes(DbConnection connection, string tableName)
        {
            string script = $"SELECT i.name AS IndexName, c.name AS ColumnName " +
                            $"FROM sys.indexes i " +
                            $"JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id " +
                            $"JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id " +
                            $"WHERE i.object_id = OBJECT_ID(N'{tableName}') AND i.is_primary_key = 0";

            RunQuery(connection, script, reader =>
            {
                // Extract the index name and column name
                string indexName = reader["IndexName"].ToString();
                string columnName = reader["ColumnName"].ToString();

                // Generate the CREATE INDEX statement for the new table
                string createIndexScript = $"CREATE INDEX {indexName} ON {tableName} ({columnName})";

                SegmentedScripts.Add(new Tuple<string, int, string, Action<DbDataReader>, Action>("ALTER TABLE - INDEXES", 7, createIndexScript, null, () =>
                {
                    // Generate the rollback script to drop the index from the new table
                    string rollbackScript = $"DROP INDEX {tableName}.{indexName}";
                    RunQuery(connection, rollbackScript);
                }));
            });
        }

        private static void MigrateForeignKeys(DbConnection connection, string tableName)
        {
            string script = @"
            SELECT
                f.name AS ConstraintName,
                c1.name AS ColumnName,
                t2.name AS ReferencedTableName,
                c2.name AS ReferencedColumnName
            FROM
                sys.foreign_keys AS f
                INNER JOIN sys.foreign_key_columns AS fc ON f.object_id = fc.constraint_object_id
                INNER JOIN sys.tables AS t1 ON f.parent_object_id = t1.object_id
                INNER JOIN sys.tables AS t2 ON f.referenced_object_id = t2.object_id
                INNER JOIN sys.columns AS c1 ON f.parent_object_id = c1.object_id AND fc.parent_column_id = c1.column_id
                INNER JOIN sys.columns AS c2 ON f.referenced_object_id = c2.object_id AND fc.referenced_column_id = c2.column_id
            WHERE
                t1.name = '{tableName}'";

            RunQuery(connection, script, reader =>
            {
                string constraintName = reader["ConstraintName"].ToString();
                string columnName = reader["ColumnName"].ToString();
                string referencedTableName = reader["ReferencedTableName"].ToString();
                string referencedColumnName = reader["ReferencedColumnName"].ToString();

                string alterTableScript = $"ALTER TABLE {tableName} ADD CONSTRAINT {constraintName} " +
                                          $"FOREIGN KEY ({columnName}) REFERENCES {referencedTableName}({referencedColumnName})";

                SegmentedScripts.Add(new Tuple<string, int, string, Action<DbDataReader>, Action>("ALTER TABLE - FOREIGN KEY", 5, alterTableScript, null, () =>
                {
                    string rollbackScript = $"ALTER TABLE {tableName} DROP CONSTRAINT {constraintName}";
                    RunQuery(connection, rollbackScript);
                }));
            });
        }

        private static void MigrateColumnDefaults(DbConnection connection, string tableName)
        {
            string script = $"SELECT * FROM sys.default_constraints WHERE parent_object_id = OBJECT_ID('{tableName}')";
            RunQuery(connection, script, reader =>
            {
                // Extract the column name and default constraint name
                string columnName = reader["parent_column_id"].ToString();
                string constraintName = reader["name"].ToString();

                // Generate the CREATE DEFAULT statement for the new table
                string createDefaultScript = $"CREATE DEFAULT {constraintName} AS {tableName}.{columnName}";

                SegmentedScripts.Add(new Tuple<string, int, string, Action<DbDataReader>, Action>("CREATE DEFAULT", 6, createDefaultScript, null, () =>
                {
                    // Generate the rollback script to drop the default constraint from the new table
                    string rollbackScript = $"DROP DEFAULT {constraintName}";
                    RunQuery(connection, rollbackScript);
                }));
            });
        }

        private static void StoreColumns(DbConnection connection, string tableName)
        {
            TempTableColumns.Clear();
            string tmpTblColumnsScript = $"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}'";

            RunQuery(connection, tmpTblColumnsScript,
            (reader) =>
            {
                string columnName = reader["COLUMN_NAME"].ToString();
                string dataType = reader["DATA_TYPE"].ToString();
                TempTableColumns.Add(new Tuple<string, string>(columnName, dataType));
            });
        }

        private static void RunQuery(DbConnection connection, string script, Action<DbDataReader> action = null)
        {
            if (connection.State != ConnectionState.Open)
                connection.Open();

            using (var command = connection.CreateCommand())
            {
                // Get the primary key information from the database for the original table
                command.CommandText = script;

                if (action == null)
                    command.ExecuteNonQuery();
                else
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            action(reader);
                        }
                    }
            }

            if (connection.State != ConnectionState.Closed)
                connection.Close();
        }
    }
}
