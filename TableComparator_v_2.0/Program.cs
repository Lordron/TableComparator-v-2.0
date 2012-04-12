using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Xml;
using MySql.Data.MySqlClient;
using TableComparator_v_2._0.Properties;

namespace TableComparator_v_2._0
{
    internal class Program
    {
        private MySqlConnection _connection;

        public static void Main()
        {
            Console.Title = "Table Comparator V 2.0";
            new Program().Initial();
        }

        public void Initial()
        {
            _connection = InitConnection();
            if (!IsConnected)
                return;

            Console.WriteLine("-================================= Welcome =================================-");
            Console.WriteLine("==========||====================================================||=========||");
            Console.WriteLine("Statistics|| Templates: || Bad fields:  || Table:               || %       ||");

            DirectoryInfo info = new DirectoryInfo("tables");
            FileInfo[] structures = info.GetFiles("*.xml", SearchOption.TopDirectoryOnly);
            foreach (FileInfo document in structures)
            {
                XmlDocument xmlDocument = new XmlDocument();
                xmlDocument.Load(document.OpenRead());

                XmlNodeList fields = xmlDocument.GetElementsByTagName("field");
                if (fields.Count == 0)
                    continue;

                List<string> fieldsName = new List<string>(fields.Count);
                for (int i = 0; i < fields.Count; ++i)
                {
                    XmlAttributeCollection attributes = fields[i].Attributes;
                    if (attributes != null)
                        fieldsName.Add(attributes["name"].Value);
                }

                string tableName = document.Name.Replace(".xml", string.Empty);

                StringBuilder content = new StringBuilder();

                content.Append("SELECT ");

                foreach (string field in fieldsName)
                    content.AppendFormat("{0}.{1}, ", tableName, field);

                foreach (string field in fieldsName)
                    content.AppendFormat("{0}_sniff.{1}, ", tableName, field);

                string entry = fieldsName[0];
                content.AppendFormat("FROM {0} INNER JOIN {0}_sniff ON {0}.{1} = {0}_sniff.{1} ORDER BY {0}.{1};", tableName, entry).AppendLine().Replace(", FROM", " FROM");

                List<List<object>> dbNormalData = new List<List<object>>();
                List<List<object>> dbSniffData = new List<List<object>>();

                using (MySqlCommand command = new MySqlCommand(content.ToString(), _connection))
                using (MySqlDataReader db = command.ExecuteReader())
                {
                    int count = db.FieldCount/2;
                    while (db.Read())
                    {
                        List<object> normalData = new List<object>(count);
                        List<object> sniffData = new List<object>(count);

                        for (int i = 0; i < count; ++i)
                            normalData.Add(db[i]);

                        for (int i = count; i < (count*2); ++i)
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
                        for (int j = 0; j < normalData.Count; ++j)
                        {
                            if (!Equals(normalData[0], sniffData[0]))
                                continue;

                            if (Equals(normalData[j], sniffData[j]))
                                continue;

                            writer.WriteLine(string.Format(NumberFormatInfo.InvariantInfo, "UPDATE `{0}` SET `{1}` = '{2}' WHERE `{3}` = {4};", tableName, fieldsName[j], sniffData[j], entry, sniffData[0]));
                            ++badFieldCount;
                        }
                    }
                    
                    Console.WriteLine("==========|| {0,-11}|| {1,-13}|| {2,-21}|| {3:P, -8}||", count, badFieldCount,
                                      tableName, (((float) badFieldCount/count)*100));
                }
            }

            Console.WriteLine("==========||====================================================||=========||");
            Console.WriteLine("-=================================== Done ==================================-");
            Console.Read();
        }

        public MySqlConnection InitConnection()
        {
            string connectionInfo =
                string.Format(
                    "host={0};port='{1}';database='{2}';UserName='{3}';Password='{4}';Connection Timeout='{5}'",
                    Settings.Default.Host, Settings.Default.Port, Settings.Default.Database, Settings.Default.Login, Settings.Default.Password,
                    Settings.Default.ConnectionTimeOut);

            return new MySqlConnection(connectionInfo);
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