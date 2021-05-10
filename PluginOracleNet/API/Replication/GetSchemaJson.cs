using System.Collections.Generic;

namespace PluginOracleNet.API.Replication
{
    public static partial class Replication
    {
        public static string GetSchemaJson()
        {
            Dictionary<string, object> schemaJsonObj = new Dictionary<string, object>
            {
                { "type", "object"},
                { "properties", new Dictionary<string, object>
                    {
                        {
                            "GoldenTableName", new Dictionary<string, string>
                            {
                                {"type", "string"},
                                {"title", "Golden Record Table Name" },
                                {"description", "Name for gold record table in Oracle.Net"}
                            }
                        },
                        {
                            "Version", new Dictionary<string, string>
                            {
                                {"type","string" },
                                {"title", "Version Record Bucket Name" },
                                {"description", "Name for the version record table in Oracle.Net" }
                            }
                        }
                    }
                }
            };
            return null; 
        }
    }
}
