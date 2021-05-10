using System;
using System.Collections.Generic;
using System.Text;
using PluginOracleNet.Helper;
using Newtonsoft.Json;
using Naveego.Sdk.Plugins;
using Xunit;
using System.Threading.Tasks;
using Grpc.Core;
using System.Linq;

namespace PluginOracleNetTest.Plugin
{
    public class PluginIntegrationTest
    {
        private Settings GetSettings()
        {
            var settingsFile = System.IO.File.ReadAllText("C:\\Temp\\OracleServerSettings.json");
            Settings settings = JsonConvert.DeserializeObject<Settings>(settingsFile);
            return settings;
        }

        private ConnectRequest GetConnectSettings()
        {
            var settings = GetSettings();

            return new ConnectRequest
            {
                SettingsJson = JsonConvert.SerializeObject(settings),
                OauthConfiguration = new OAuthConfiguration(),
                OauthStateJson = ""
            };
        }

        private Schema GetTestSchema(string id = "test", string name = "test", string query = "")
        {
            return new Schema
            {
                Id = id,
                Name = name,
                Query = query
            };
        }

        // Test Variables

        private static string TestSchemaID = "\"C##DEMO\".\"ACCOUNTARCHIVE\"";
        private static string TestSchemaName = "C##DEMO.ACCOUNTARCHIVE";
        private static int TestSampleCount = 10;
        private static int TestPropertyCount = 11;

        private static string TestPropertyID = "\"ID\"";
        private static string TestPropertyName = "ID";

        [Fact]
        public async Task ConnectSessionTest()
        {
            // setup
            Server server = new Server
            {
                Services = { Publisher.BindService(new PluginOracleNet.Plugin.Plugin()) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();
            var disconnectRequest = new DisconnectRequest();

            // act
            var response = client.ConnectSession(request);
            var responseStream = response.ResponseStream;
            var records = new List<ConnectResponse>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
                client.Disconnect(disconnectRequest);
            }

            // assert
            Assert.Single(records);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ConnectTest()
        {
            // setup
            Server server = new Server
            {
                Services = { Publisher.BindService(new PluginOracleNet.Plugin.Plugin()) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();

            // act
            var response = client.Connect(request);

            // assert
            Assert.IsType<ConnectResponse>(response);
            Assert.Equal("", response.SettingsError);
            Assert.Equal("", response.ConnectionError);
            Assert.Equal("", response.OauthError);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        // TODO: Fix Oracle Query
        [Fact]
        public async Task DiscoverSchemasAllTest()
        {
            // setup
            Server server = new Server
            {
                Services = { Publisher.BindService(new PluginOracleNet.Plugin.Plugin()) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.All,
                SampleSize = 10
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Equal(4, response.Schemas.Count);

            var schema = response.Schemas[0];

            Assert.Equal(TestSchemaID, schema.Id);
            Assert.Equal(TestSchemaName, schema.Name);
            Assert.Equal($"", schema.Query);
            Assert.Equal(TestSampleCount, schema.Sample.Count);
            Assert.Equal(TestPropertyCount, schema.Properties.Count);

            var property = schema.Properties[0];
            Assert.Equal(TestPropertyID, property.Id);
            Assert.Equal(TestPropertyName, property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.String, property.Type);
            Assert.False(property.IsKey);
            Assert.True(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        // TODO: Fix Oracle Query
        [Fact]
        public async Task DiscoverSchemasRefreshTableTest()
        {
            // setup
            Server server = new Server
            {
                Services = { Publisher.BindService(new PluginOracleNet.Plugin.Plugin()) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            server.Start();

            int port = server.Ports.First().BoundPort;

            Channel channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            Publisher.PublisherClient client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                SampleSize = 10,
                ToRefresh = { GetTestSchema(TestSchemaID, TestSchemaName) }
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Single(response.Schemas);

            var schema = response.Schemas[0];
            Assert.Equal(TestSchemaID, schema.Id);
            Assert.Equal(TestSchemaName, schema.Name);
            Assert.Equal($"", schema.Query);
            Assert.Equal(TestSampleCount, schema.Sample.Count);
            Assert.Equal(TestPropertyCount, schema.Properties.Count);

            var property = schema.Properties[4];
            Assert.Equal(TestPropertyID, property.Id);
            Assert.Equal(TestPropertyName, property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.String, property.Type);
            Assert.False(property.IsKey);
            Assert.True(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        // TODO: Fix Oracle Query
        [Fact]
        public async Task DiscoverSchemasRefreshQueryTest()
        {
            // setup
            Server server = new Server
            {
                Services = { Publisher.BindService(new PluginOracleNet.Plugin.Plugin()) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                SampleSize = 10,
                ToRefresh = { GetTestSchema("test", "test", $"SELECT * FROM {TestSchemaID}") }
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Single(response.Schemas);

            var schema = response.Schemas[0];
            Assert.Equal($"test", schema.Id);
            Assert.Equal("test", schema.Name);
            Assert.Equal($"SELECT * FROM {TestSchemaID}", schema.Query);
            Assert.Equal(TestSampleCount, schema.Sample.Count);
            Assert.Equal(TestPropertyCount, schema.Properties.Count);

            var property = schema.Properties[0];
            Assert.Equal(TestPropertyID, property.Id);
            Assert.Equal(TestPropertyName, property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.String, property.Type);
            Assert.False(property.IsKey);
            Assert.True(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        // TODO: Fix Oracle Query
        [Fact]
        public async Task DiscoverSchemasRefreshQueryBadSyntaxTest()
        {
            // setup
            Server server = new Server
            {
                Services = { Publisher.BindService(new PluginOracleNet.Plugin.Plugin()) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                SampleSize = 10,
                ToRefresh = { GetTestSchema("bad syntax") }
            };

            // act
            client.Connect(connectRequest);

            try
            {
                var response = client.DiscoverSchemas(request);
            }
            catch (Exception e)
            {
                // assert
                Assert.IsType<RpcException>(e);
                Assert.Contains("ORA-", e.Message);
            }

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        // TODO: Fix Oracle Query
        [Fact]
        public async Task ReadStreamTableSchemaTest()
        {
            // setup
            Server server = new Server
            {
                Services = { Publisher.BindService(new PluginOracleNet.Plugin.Plugin()) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var schema = GetTestSchema(TestSchemaID, TestSchemaName);

            var connectRequest = GetConnectSettings();

            var schemaRequest = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = { schema }
            };

            var request = new ReadRequest()
            {
                DataVersions = new DataVersions
                {
                    JobId = "test"
                },
                JobId = "test",
            };

            // act
            client.Connect(connectRequest);
            var schemasResponse = client.DiscoverSchemas(schemaRequest);
            request.Schema = schemasResponse.Schemas[0];

            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Naveego.Sdk.Plugins.Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(5, records.Count);

            var record = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[0].DataJson);
            Assert.Equal("3", record["\"CHANNEL_ID\""]);
            Assert.Equal("Direct Sales", record["\"CHANNEL_DESC\""]);
            Assert.Equal("Direct", record["\"CHANNEL_CLASS\""]);
            Assert.Equal("12", record["\"CHANNEL_CLASS_ID\""]);
            Assert.Equal("Channel total", record["\"CHANNEL_TOTAL\""]);
            Assert.Equal("1", record["\"CHANNEL_TOTAL_ID\""]);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        // TODO: Fix Oracle Query
        [Fact]
        public async Task ReadStreamQuerySchemaTest()
        {
            // setup
            Server server = new Server
            {
                Services = { Publisher.BindService(new PluginOracleNet.Plugin.Plugin()) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var schema = GetTestSchema("test", "test", $"SELECT * FROM \"C##DEMO\".\"ACCOUNTARCHIVE\"");

            var connectRequest = GetConnectSettings();

            var schemaRequest = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = { schema }
            };

            var request = new ReadRequest()
            {
                DataVersions = new DataVersions
                {
                    JobId = "test"
                },
                JobId = "test",
            };

            // act
            client.Connect(connectRequest);
            var schemasResponse = client.DiscoverSchemas(schemaRequest);
            request.Schema = schemasResponse.Schemas[0];

            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Naveego.Sdk.Plugins.Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(5, records.Count);

            var record = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[0].DataJson);
            Assert.Equal("3", record["\"CHANNEL_ID\""]);
            Assert.Equal("Direct Sales", record["\"CHANNEL_DESC\""]);
            Assert.Equal("Direct", record["\"CHANNEL_CLASS\""]);
            Assert.Equal("12", record["\"CHANNEL_CLASS_ID\""]);
            Assert.Equal("Channel total", record["\"CHANNEL_TOTAL\""]);
            Assert.Equal("1", record["\"CHANNEL_TOTAL_ID\""]);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        // TODO: Fix Oracle Query
        [Fact]
        public async Task ReadStreamLimitTest()
        {
            // setup
            Server server = new Server
            {
                Services = { Publisher.BindService(new PluginOracleNet.Plugin.Plugin()) },
                Ports = { new ServerPort("localhost", 0, ServerCredentials.Insecure) }
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var schema = GetTestSchema("test", "test", $"SELECT * FROM \"SH\".\"CHANNELS\"");

            var connectRequest = GetConnectSettings();

            var schemaRequest = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = { schema }
            };

            var request = new ReadRequest()
            {
                DataVersions = new DataVersions
                {
                    JobId = "test"
                },
                JobId = "test",
                Limit = 10
            };

            // act
            client.Connect(connectRequest);
            var schemasResponse = client.DiscoverSchemas(schemaRequest);
            request.Schema = schemasResponse.Schemas[0];

            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Naveego.Sdk.Plugins.Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(5, records.Count);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }
    }
}
