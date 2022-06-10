using System.Data;
using PluginOracleNetLite.Helper;

namespace PluginOracleNetLite.API.Factory
{
    public interface IConnectionFactory
    {
        void Initialize(Settings settings);
        IConnection GetConnection();
        ICommand GetCommand(string commandText, IConnection conn);
    }
}