using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginOracleNet.API.Factory;
using PluginOracleNet.API.Utility;
using PluginOracleNet.DataContracts;
using PluginOracleNet.Helper;

namespace PluginOracleNet.API.Replication
{
    public static partial class Replication
    {
        private static readonly SemaphoreSlim WriteSemaphoreSlim = new SemaphoreSlim(10, 10);

        /// <summary>
        /// Adds and removes records to replication db
        /// Adds and updates available shapes
        /// </summary>
        /// <param name="connFactory"></param>
        /// <param name="schema"></param>
        /// <param name="record"></param>
        /// <param name="config"></param>
        /// <param name="responseStream"></param>
        /// <returns>Error message string</returns>
        public static async Task<string> WriteRecordAsync(IConnectionFactory connectionFactory, Schema schema, Record record, ConfigureReplicationFormData config, IServerStreamWriter<RecordAck> responseStream)
        {
            Logger.Debug($"Starting {record.RecordId}");
            Stopwatch timer = Stopwatch.StartNew();

            try
            {
                Logger.Debug(JsonConvert.SerializeObject(record, Formatting.Indented));

                await WriteSemaphoreSlim.WaitAsync();

                string safeSchemaName = config.SchemaName;
                string safeGoldenTableName = config.GoldenTableName;
                string safeVersionTableName = config.VersionTableName;

                ReplicationTable goldenTable = GetGoldenReplicationTable(schema, safeSchemaName, safeGoldenTableName);
                ReplicationTable versionTable = GetVersionReplicationTable(schema, safeSchemaName, safeVersionTableName);

                List<string> recordVersionIds = record.Versions.Select(r => r.RecordId).ToList();
                // GetNamed Record Data
                // TODO: Finish

                return null;

            }
            catch ( Exception ex)
            {
                Logger.Error(ex, $"Error writing record: {ex.Message}");

                RecordAck ack = new RecordAck
                {
                    CorrelationId = record.CorrelationId,
                    Error = ex.Message
                };

                await responseStream.WriteAsync(ack);

                timer.Stop();

                return ex.Message;
            }
            finally
            {
                Logger.Debug($"Stopped {record.RecordId}. Time: {timer.ElapsedMilliseconds}");
                WriteSemaphoreSlim.Release();
            }
        }
    }
}
