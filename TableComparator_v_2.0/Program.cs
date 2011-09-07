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
        MySqlConnection connection;
        static void Main()
        {
            new Program().Initial();
        }

        void Initial()
        {
            Console.Title = "Table Comparator V 2.0";

            connection = InitConnection();
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

            foreach (FileInfo file in structures)
            {
                XmlDocument def = new XmlDocument();

                def.Load(file.FullName);

                XmlNodeList fields = def.GetElementsByTagName("field");
                if (fields == null)
                {
                    Console.WriteLine("{0} has no field 'fields'", file.Name);
                    continue;
                }

                List<string> dbFields = new List<string>();
                for (int i = 0; i < fields.Count; ++i)
                {
                    XmlAttributeCollection attributes = fields[i].Attributes;
                    dbFields.Add(attributes["name"].Value);
                }
                Work(dbFields, file.Name.Replace(".xml", string.Empty));
            }
            Console.WriteLine("==========||====================================================||=========||");
            Console.WriteLine("-=================================== Done ==================================-");
            Console.Read();
        }

        void Work(List<string> dbFields, string tableName)
        {
            StringBuilder content = new StringBuilder();
            content.Append("SELECT ");
            foreach (string field in dbFields)
                content.AppendFormat("{0}.{1}, ", tableName, field);

            foreach (string field in dbFields)
                content.AppendFormat("{0}_sniff.{1}, ", tableName, field);


            string entry = dbFields[0];
            content.AppendFormat("FROM {0} INNER JOIN {0}_sniff ON {0}.{1} = {0}_sniff.{1} ORDER BY {0}.{1};", tableName, entry).AppendLine().Replace(", FROM", " FROM");

            using (MySqlCommand command = new MySqlCommand(content.ToString(), connection))
            {
                List<List<object>> normalData = new List<List<object>>();
                List<List<object>> sniffData = new List<List<object>>();
                using (MySqlDataReader db = command.ExecuteReader())
                {
                    while (db.Read())
                    {
                        List<object> normal = new List<object>();
                        List<object> sniff = new List<object>();
                        int count = db.FieldCount / 2;
                        for (int i = 0; i < count; ++i)
                            normal.Add(db[i]);

                        for (int i = count; i < db.FieldCount; ++i)
                            sniff.Add(db[i]);

                        normalData.Add(normal);
                        sniffData.Add(sniff);
                    }
                }

                using (StreamWriter writer = new StreamWriter(string.Format("{0}.sql", tableName)))
                {
                    int count = normalData.Count;
                    int badField = 0;
                    for (int i = 0; i < count; ++i)
                    {
                        List<object> normal = normalData[i];
                        List<object> sniff = sniffData[i];
                        for (int y = 0; y < normal.Count; ++y)
                        {
                            if (!Equals(normal[y], sniff[y]))
                            {
                                writer.WriteLine(string.Format(NumberFormatInfo.InvariantInfo, "UPDATE `{0}` SET `{1}` = '{2}' WHERE `{3}` = {4};", tableName, dbFields[y], sniff[y], entry, sniff[0]));
                                ++badField;
                            }
                        }
                        if (count - 1 == i)
                        {
                            Console.WriteLine("==========|| {0,-11}|| {1,-13}|| {2,-21}|| {3, -8}||", count, badField,
                                              tableName, (((float)badField / count) * 100));
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
                    connection.Open();
                    connection.Close();
                    connection.Open();
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
