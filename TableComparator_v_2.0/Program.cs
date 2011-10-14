using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.Serialization;
using MySql.Data.MySqlClient;

namespace TableComparator_v_2._0
{
    class Program
    {
        MySqlConnection _connection;
        static void Main()
        {
            Console.Title = "Table Comparator V 2.0";
            new Program().Initial();
        }

        void Initial()
        {
            _connection = InitConnection();
            if (!IsConnected)
                return;

            Console.WriteLine("-================================= Welcome =================================-");
            Console.WriteLine("==========||====================================================||=========||");
            Console.WriteLine("Statistics|| Templates: || Bad fields:  || Table:               || %       ||");

            string path = Path.Combine(Directory.GetCurrentDirectory(), "tables");
            if (!Directory.Exists(path))
                throw new Exception("Dirrectory 'tables' is exist!");

            FileInfo[] structures = new DirectoryInfo(path).GetFiles();
            if (structures.Length == 0)
                throw new Exception("Dirrectory 'tables' is empty!");

            foreach (FileInfo document in structures)
            {
                XmlDocument xmlDocument = new XmlDocument();

                xmlDocument.Load(document.FullName);

                XmlNodeList fields = xmlDocument.GetElementsByTagName("field");
                if (fields.Count < 1)
                {
                    Console.WriteLine("File '{0}' has no field 'fields'", document.Name);
                    continue;
                }

                List<string> fieldsName = new List<string>();
                for (int i = 0; i < fields.Count; ++i)
                    fieldsName.Add(fields[i].Attributes["name"].Value);

                Work(fieldsName, document.Name.Replace(".xml", string.Empty));
            }
            Console.WriteLine("==========||====================================================||=========||");
            Console.WriteLine("-=================================== Done ==================================-");
            Console.Read();
        }

        void Work(List<string> fieldsName, string tableName)
        {
            StringBuilder content = new StringBuilder();
            content.Append("SELECT ");
            foreach (string field in fieldsName)
                content.AppendFormat("{0}.{1}, ", tableName, field);

            foreach (string field in fieldsName)
                content.AppendFormat("{0}_sniff.{1}, ", tableName, field);

            string entry = fieldsName[0];
            content.AppendFormat("FROM {0} INNER JOIN {0}_sniff ON {0}.{1} = {0}_sniff.{1} ORDER BY {0}.{1};", tableName, entry).AppendLine().Replace(", FROM", " FROM");

            using (MySqlCommand command = new MySqlCommand(content.ToString(), _connection))
            {
                List<List<object>> dbNormalData = new List<List<object>>();
                List<List<object>> dbSniffData = new List<List<object>>();
                using (MySqlDataReader db = command.ExecuteReader())
                {
                    while (db.Read())
                    {
                        List<object> normalData = new List<object>();
                        List<object> sniffData = new List<object>();
                        int count = db.FieldCount / 2;
                        for (int i = 0; i < count; ++i)
                            normalData.Add(db[i]);

                        for (int i = count; i < db.FieldCount; ++i)
                            sniffData.Add(db[i]);

                        dbNormalData.Add(normalData);
                        dbSniffData.Add(sniffData);
                    }
                }

                using (StreamWriter writer = new StreamWriter(string.Format("{0}.sql", tableName)))
                {
                    int badFieldCount = 0;
                    int count = dbNormalData.Count;
                    for (int i = 0; i < count; ++i)
                    {
                        List<object> normalData = dbNormalData[i];
                        List<object> sniffData = dbSniffData[i];
                        for (int y = 0; y < normalData.Count; ++y)
                        {
                            if (!Equals(normalData[y], sniffData[y]))
                            {
                                writer.WriteLine(string.Format(NumberFormatInfo.InvariantInfo, "UPDATE `{0}` SET `{1}` = '{2}' WHERE `{3}` = {4};", tableName, fieldsName[y], sniffData[y], entry, sniffData[0]));
                                ++badFieldCount;
                            }
                        }
                        if ((count - 1) == i)
                        {
                            Console.WriteLine("==========|| {0,-11}|| {1,-13}|| {2,-21}|| {3, -8}||", count, badFieldCount,
                                              tableName, (((float)badFieldCount / count) * 100));
                        }
                    }
                }
            }
        }

        MySqlConnection InitConnection()
        {
            if (File.Exists("config.xml"))
            {
                using (StreamReader reader = new StreamReader("config.xml"))
                {
                    XmlSerializer serializer = new XmlSerializer(typeof(Config));
                    Config config = (Config)serializer.Deserialize(reader);

                    string connectionInfo =
                        string.Format(
                            "host={0};port='{1}';database='{2}';UserName='{3}';Password='{4}';Connection Timeout='{5}'",
                            config.Host, config.Port, config.Database, config.Username, config.Password,
                            config.ConnectionTimeOut);

                    return new MySqlConnection(connectionInfo);
                }
            }

            return null;
        }

        public bool IsConnected
        {
            get
            {
                try
                {
                    _connection.Open();
                    _connection.Close();
                    _connection.Open();
                    return true;
                }
                catch
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Check your MySQL server");
                    return false;
                }
            }
        }
    }
}
