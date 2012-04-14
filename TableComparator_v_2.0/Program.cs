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
    public static class Program
    {
        public static void Main()
        {
            Console.Title = "Table Comparator V 2.0";

            MySqlConnection connection = new MySqlConnection(Settings.Default.ConnectionString);
            if (!connection.Connected())
                return;

            Console.WriteLine("-================================== Welcome ==================================-");
            Console.WriteLine("==========||========================================================||=======||");
            Console.WriteLine("Statistics|| Total:  || Bad fields:  || Table:                      || %     ||");
            Console.WriteLine("==========||========================================================||=======||");

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

                string key = fieldsName[0];
                content.AppendFormat("FROM {0} INNER JOIN {0}_sniff ON {0}.{1} = {0}_sniff.{1} ORDER BY {0}.{1};", tableName, key).AppendLine().Replace(", FROM", " FROM");

                List<List<object>> dbNormalData = new List<List<object>>();
                List<List<object>> dbSniffData = new List<List<object>>();

                using (MySqlCommand command = new MySqlCommand(content.ToString(), connection))
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
                    for (int i = 0; i < dbNormalData.Count; ++i)
                    {
                        bool error = false;
                        List<object> normalData = dbNormalData[i];
                        List<object> sniffData = dbSniffData[i];

                        object entry = normalData[0];
                        if (!Equals(entry, sniffData[0]))
                            continue;

                        for (int j = 0; j < normalData.Count; ++j)
                        {
                            if (Equals(normalData[j], sniffData[j]))
                                continue;

                            writer.WriteLine(string.Format(NumberFormatInfo.InvariantInfo, "UPDATE `{0}` SET `{1}` = '{2}' WHERE `{3}` = {4};", tableName, fieldsName[j], sniffData[j], key, entry));
                            error = true;
                        }
                        if (error)
                            ++badFieldCount;
                    }

                    Console.WriteLine("==========|| {0,-8}|| {1,-13}|| {2,-28}|| {3,-6}||", dbNormalData.Count, badFieldCount,
                                      tableName, Math.Round(((float) badFieldCount/dbNormalData.Count)*100, 3));
                }
            }

            Console.WriteLine("==========||========================================================||=======||");
            Console.WriteLine("-=================================== Done ====================================-");
            Console.Read();
        }

        public static bool Connected(this MySqlConnection connection)
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
                Console.WriteLine("Error! Cannot open connection. Check your connection info and MySQL server");
                return false;
            }
        }
    }
}