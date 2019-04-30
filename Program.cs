using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.SqlClient;

namespace DWHloganalyzer
{
    class Data
    {
        public static DataTable table;
        public static DataRow row;
        private static int count = 1;
        public static int Count { get { return count; } }

        public static void Initialize()
        {
            table = new DataTable();
            table.Columns.Add("Filedate", typeof(DateTime));
            table.Columns.Add("Filename", typeof(string));
            table.Columns.Add("Worktype", typeof(string));
            table.Columns.Add("Workstep", typeof(string));
            table.Columns.Add("Duration", typeof(int));
            table.Columns.Add("RowNum", typeof(int));
            row = table.NewRow();
        }

        public static void NewRow(bool new_filedate, bool new_filename, bool new_worktype, bool new_workstep, bool new_duration)
        {
            DataRow old = row;
            row = table.NewRow();
            if (!new_filedate) row["Filedate"] = old["Filedate"];
            if (!new_filename) row["Filename"] = old["Filename"];
            if (!new_worktype) row["Worktype"] = old["Worktype"];
            if (!new_workstep) row["Workstep"] = old["Workstep"];
            if (!new_duration) row["Duration"] = old["Duration"];
        }

        public static void AddRow(bool new_filedate, bool new_filename, bool new_worktype, bool new_workstep, bool new_duration)
        {
            row["RowNum"] = count++;
            table.Rows.Add(row);
            //Console.WriteLine(row["RowNum"] + " | " + row["Filedate"].ToString().Substring(0, 10) + " | " + row["Filename"] + " | " + row["Worktype"] + " | " + row["Workstep"] + " | " + row["Duration"].ToString());
            NewRow(new_filedate, new_filename, new_worktype, new_workstep, new_duration);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            DateTime starttime = DateTime.Now;
            string dir = @"E:\BATCH\LOG";
            string pattern = "20??-??-??_SAP_import.log";
            //string pattern = "20??-??-??_DWH_Nachtlauf.log";
            if (args.GetLength(0) > 0)
            {
                pattern = System.IO.Path.GetFileName(args[0]);
                dir = args[0].Replace(@"\" + pattern, "");
            }

            if (!Directory.Exists(dir)) throw (new Exception("Directory not existing!"));

            Data.Initialize();

            foreach (string file in Directory.GetFiles(dir, pattern, SearchOption.TopDirectoryOnly))
            {
                Data.NewRow(true, true, true, true, true);
                Data.row["Filename"] = Path.GetFileName(file);
                Data.row["Filedate"] = Convert.ToDateTime(Path.GetFileName(file).Substring(0, 10));

                foreach (string line in File.ReadAllLines(file))
                {
                    if (Path.GetFileName(file).EndsWith("_DWH_Nachtlauf.log"))
                    {
                        if (line.Contains(":  Finished") && Data.row["Workstep"].ToString().Length > 0)
                        {
                            Data.row["Duration"] = Convert.ToInt32((Convert.ToDateTime(line.Substring(0,"YYYY-MM-DD HH:MI:SS".Length)) - starttime).TotalSeconds/60);
                            Data.AddRow(false, false, true, true, true);
                        }
                        else if (line.Contains(":  Executing"))
                        {
                            Data.row["Worktype"] = CutString(line, "Executing ", " [");
                            Data.row["Workstep"] = CutString(line, "[", "]");
                            starttime = Convert.ToDateTime(line.Substring(0, "YYYY-MM-DD HH:MI:SS".Length));
                        }
                        else if (line.Contains(" WITH ERROR:"))
                        {
                            Data.NewRow(false, false, true, true, true);
                        }
                    }
                    else
                    {
                        if (line.Contains("FINISHED - Profile:["))
                        {
                            Data.row["Worktype"] = "SAPExtractV2";
                            Data.row["Workstep"] = CutString(line, "Profile:[", "]");
                            Data.row["Duration"] = ConvertTime(CutString(line, "Duration:[", "]"));
                            Data.AddRow(false, false, true, true, true);
                        }
                        else if (line.Contains("Extracting") && line.Contains("... received datapackge with rows:"))
                        {
                            Data.row["Worktype"] = "SAPExtractV1";
                            Data.row["Workstep"] = CutString(line.Replace(" ", "."), "Extracting.", ".");
                        }
                        else if (Data.row["Worktype"] == "SAPExtractV1" && line.Contains("Execution took :"))
                        {
                            Data.row["Duration"] = ConvertTime(CutString(line, "Execution took :", ":"));
                            Data.AddRow(false, false, true, true, true);
                        }
                        else if (line.Contains(@"Try to read E:\q4bis\Projects\") && line.Contains(".iqp"))
                        {
                            Data.row["Worktype"] = CutString(line, @"E:\q4bis\Projects\", ".iqp") + ".iqp";
                        }
                        else if (Data.row["Worktype"].ToString().EndsWith(".iqp"))
                        {
                            if (line.Contains("ExitCode:"))
                            {
                                Data.NewRow(false, false, true, true, true);
                            }
                            else if (line.Contains("Step <") && line.Contains("> started"))
                            {
                                Data.row["Workstep"] = CutString(line, "Step <", ">");
                            }
                            else if (line.Contains("Completed:") && line.Contains("Duration:"))
                            {
                                Data.row["Duration"] = ConvertTime(CutString(line, "Duration:", "."));
                                Data.AddRow(false, false, false, true, true);
                            }
                        }
                    }
                }
            }

            SqlBulkCopy sql = new SqlBulkCopy("server=saaplegl001;user id=export_db;password=export_dwh_2008;database=Export_DB;connection timeout=30;trusted_connection=no;");
            sql.DestinationTableName = "[Export_DB].[dbo].[DWHloganalyzer]";
            sql.WriteToServer(Data.table);

            //End();
        }

        static private string CutString(string text, string start, string end)
        {
            if (!text.Contains(start)) return null;
            int startpos = text.IndexOf(start) + start.Length;
            int length = (text + end).IndexOf(end, startpos) - startpos;
            return text.Substring(startpos, length).Trim();
        }

        static private int ConvertTime(string text)
        {
            string[] split = text.Replace("h", ":").Replace(".", ":").Split(':');
            return Convert.ToInt32(split[0].Trim()) * 60 + Convert.ToInt32(split[1].Trim());
        }


        static public void End()
        {
#if DEBUG
            Console.WriteLine("");
            Console.WriteLine("Press any key to close ...");
            Console.ReadKey(true);
#endif
        }
    }
}
