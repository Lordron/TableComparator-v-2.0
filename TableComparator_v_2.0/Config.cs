using System;

namespace TableComparator_v_2._0
{
    [Serializable]
    public class Config
    {
        public string Host;
        public string Port;
        public string Username;
        public string Password;
        public string Database;
        public string ConnectionTimeOut;

        public Config()
        {
            Host = "127.0.0.1";
            Port = "3306";
            Username = "root";
            Password = "root";
            Database = "mangos";
            ConnectionTimeOut = "120000";
        }
    }
}