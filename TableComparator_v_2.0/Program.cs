using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Xml.Serialization;
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
                XmlSerializer serializer = new XmlSerializer(typeof(Table));
                Table table = (Table)serializer.Deserialize(document.OpenRead());

                string key = table.Fields[0];
                string prefix = table.Prefix;
                string tableName = table.TableName;

                StringBuilder cmdText = new StringBuilder(1024);
                {
                    cmdText.Append("SELECT ");

                    foreach (string field in table.Fields)
                        cmdText.AppendFormat("{0}.{1}, ", tableName, field);

                    foreach (string field in table.Fields)
                        cmdText.AppendFormat("{0}_{1}.{2}, ", tableName, prefix, field);

                    cmdText.AppendFormat("FROM {0} INNER JOIN {0}_sniff ON {0}.{1} = {0}_sniff.{1} ORDER BY {0}.{1};", tableName, key).AppendLine().Replace(", FROM", " FROM");
                }
                List<List<object>> normalDataTemplates = new List<List<object>>(UInt16.MaxValue);
                List<List<object>> sniffedDataTemplates = new List<List<object>>(UInt16.MaxValue);

                try
                {
                    using (MySqlCommand command = new MySqlCommand(cmdText.ToString(), connection))
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        int count = reader.FieldCount / 2;
                        while (reader.Read())
                        {
                            List<object> normalTemplate = new List<object>(count);
                            List<object> sniffTemplate = new List<object>(count);

                            for (int i = 0; i < count; ++i)
                                normalTemplate.Add(reader[i]);

                            for (int i = count; i < (count * 2); ++i)
                                sniffTemplate.Add(reader[i]);

                            normalDataTemplates.Add(normalTemplate);
                            sniffedDataTemplates.Add(sniffTemplate);
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("ERROR: {0}", e.Message);
                    continue;
                }

                int badFieldCount = 0;
                StringBuilder content = new StringBuilder(UInt16.MaxValue);
                for (int i = 0; i < normalDataTemplates.Count; ++i)
                {
                    bool error = false;
                    StringBuilder contentInternal = new StringBuilder();
                    List<object> normalTemplate = normalDataTemplates[i];
                    List<object> sniffTemplate = sniffedDataTemplates[i];

                    object entry = normalTemplate[0];
                    if (!entry.Equals(sniffTemplate[0]))
                        continue;

                    contentInternal.AppendFormat("UPDATE `{0}` SET ", tableName);
                    for (int j = 1; j < normalTemplate.Count; ++j)
                    {
                        if (normalTemplate[j].Equals(sniffTemplate[j]))
                            continue;

                        contentInternal.AppendFormat(NumberFormatInfo.InvariantInfo, "`{0}` = '{1}', ", table.Fields[j], sniffTemplate[j]);
                        error = true;
                    }
                    contentInternal.Remove(contentInternal.Length - 2, 2);
                    contentInternal.AppendFormat(" WHERE `{0}` = {1};", key, entry).AppendLine();

                    if (!error)
                        continue;

                    content.Append(contentInternal);
                    ++badFieldCount;
                }

                if (badFieldCount > 0)
                {
                    using (StreamWriter writer = new StreamWriter(string.Format("{0}.sql", tableName)))
                    {
                        writer.Write(content);
                    }
                }

                Console.WriteLine("==========|| {0,-8}|| {1,-13}|| {2,-28}|| {3,-6}||", normalDataTemplates.Count, badFieldCount,
                    tableName, badFieldCount > 0 ? Math.Round(((float)badFieldCount / normalDataTemplates.Count) * 100 , 3) : 0.0f);
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