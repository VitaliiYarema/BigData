using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigData.DAL
{
    public class BigFileRepository
    {
        public static FileInfo StartFileInfo { get; set; }
        public static string StartFileName { get; set; }
        public static string StartDirName { get; set; }
        public static List<string> filesToDivide = new List<string>();
        public static List<string> filesToCombine = new List<string>();

        public static async Task DivideFile(string fileName, long longSize)
        {
            await Task.Run(() =>
            {
                int indexChar = 0;
                List<string> listFiles = new List<string>();
                while (filesToDivide.Count > 0)
                {
                    listFiles = new List<string>();

                    Parallel.For(0, filesToDivide.Count, (i) =>
                    {
                        if (filesToDivide[i] == fileName)
                        {
                            DivideIntoFiles(filesToDivide[i], listFiles, indexChar, false);
                        }
                        else
                        {
                            DivideIntoFiles(filesToDivide[i], listFiles, indexChar);
                        }
                    });

                    filesToDivide.Clear();
                    for (int i = 0; i < listFiles.Count; i++)
                    {
                        if (listFiles[i].Contains("  "))
                        {
                            continue;
                        }
                        var sizeD = GetFileSize(listFiles[i]);
                        if (sizeD > longSize)
                        {
                            filesToDivide.Add(listFiles[i]);
                        }
                    }
                    indexChar++;
                }
            });
        }

        private static long GetFileSize(string fileName)
        {
            try
            {
                long size = new FileInfo(fileName).Length;
                return size;
            }
            catch
            {
                return 0;
            }
        }

        private static Record GetRecord(string line)
        {
            var strNumber = GetStringNumber(line);
            if (string.IsNullOrEmpty(strNumber))
            {
                return null;
            }
            var number = int.Parse(strNumber);

            var strLine = GetStringLine(line);
            var record = new Record() { Number = number, Line = strLine };

            return record;
        }

        private static string GetStringLine(string line)
        {
            var str = line.Substring(line.IndexOf(". ") + 2);
            return str;
        }

        private static string GetStringNumber(string line)
        {
            var str = line.Substring(0, line.IndexOf("."));
            return str;
        }

        private static void DivideIntoFiles(string fileName, List<string> listFiles, int charNum, bool deleteFile = true, string parentDirectory = "")
        {
            try
            {
                var info = new FileInfo(fileName);
                using (StreamReader sr = new StreamReader(fileName))
                {
                    while (sr.Peek() >= 0)
                    {
                        var textLine = sr.ReadLine();
                        var str = GetStringLine(textLine);
                        if (string.IsNullOrEmpty(str))
                        {
                            continue;
                        }
                        if (str.Length > charNum)
                        {
                            var fileNameW = Path.GetFileNameWithoutExtension(info.Name);
                            var fileWrite = Path.Combine(info.DirectoryName, fileNameW + " " + str[charNum] + ".txt");

                            File.OpenWrite(fileWrite).Close();
                            if (!listFiles.Contains(fileWrite))
                            {
                                listFiles.Add(fileWrite);
                            }

                            using (StreamWriter sw = new StreamWriter(fileWrite, true, Encoding.Unicode))
                            {
                                sw.WriteLine(textLine);
                            }
                        }
                        else
                        {
                            var fileNameW = Path.GetFileNameWithoutExtension(info.Name);
                            var fileWrite = Path.Combine(info.DirectoryName, fileNameW + "  " + ".txt");

                            File.OpenWrite(fileWrite).Close();
                            if (!listFiles.Contains(fileWrite))
                            {
                                listFiles.Add(fileWrite);
                            }

                            using (StreamWriter sw = new StreamWriter(fileWrite, true, Encoding.Unicode))
                            {
                                sw.WriteLine(textLine);
                            }
                        }
                    }
                }
                if (deleteFile)
                {
                    File.Delete(fileName);
                }
            }
            catch
            {

            }
        }
    }
}
