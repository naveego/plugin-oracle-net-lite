using Naveego.Sdk.Plugins;
using PluginOracleNetLite.API.Utility;
using PluginOracleNetLite.DataContracts;

namespace PluginOracleNetLite.API.Replication
{
    public static partial class Replication
    {
        public static ReplicationTable GetGoldenReplicationTable(Schema schema, string safeSchemaName, string safeGoldenTableName)
        {
            var goldenTable = ConvertSchemaToReplicationTable(schema, safeSchemaName, safeGoldenTableName);
            goldenTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationRecordId,
                DataType = "VARCHAR(255)",
                PrimaryKey = true
            });
            goldenTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationVersionIds,
                DataType = "CLOB",
                PrimaryKey = false,
                Serialize = true
            });

            return goldenTable;
        }
    }
}
