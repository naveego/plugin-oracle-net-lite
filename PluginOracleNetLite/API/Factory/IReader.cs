using System.Data;
using System.Threading.Tasks;

namespace PluginOracleNetLite.API.Factory
{
    public interface IReader
    {
        Task<bool> ReadAsync();
        Task CloseAsync();
        DataTable GetSchemaTable();
        object GetValueById(string id, char trimChar = '"');
        bool HasRows();
    }
}