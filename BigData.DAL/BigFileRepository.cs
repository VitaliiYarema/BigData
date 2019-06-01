using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigData.DAL
{
    public static class BigFileRepository
    {
        public static string msg { get; set; }
        public static FileInfo StartFileInfo { get; set; }
        /// <summary>
        /// File to divide
        /// </summary>
        public static string StartFileName { get; set; }
        public static string StartDirName { get; set; }
        /// <summary>
        /// List of files to divede now
        /// </summary>
        public static List<string> filesToStartDivide = new List<string>();
        private static List<string> filesToDivide = new List<string>();
        public static long MaximumSize { get; set; }
        /// <summary>
        /// Maximum size of file to load into the memory
        /// </summary>
        public static long LongSizeStart { get; set; }
        public static Dictionary<string, int> names = new Dictionary<string, int>();
        private static int indexName = 0;
        private static readonly object sync = new object();

        public static string PrepareFilesToDivide()
        {
            try
            {
                filesToStartDivide.Clear();
                var info = new FileInfo(StartFileName);
                var number = info.Length / LongSizeStart;

                //using (FileStream fileR = new FileStream(StartFileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    foreach (var line in File.ReadLines(StartFileName))
                    {
                        Parallel.For(0, number, (i) =>
                        {
                            var fileNameW = Path.GetFileNameWithoutExtension(info.Name);
                            var fileWrite = Path.Combine(info.DirectoryName, fileNameW + "!" + i + ".txt");
                            if (!filesToStartDivide.Contains(fileWrite))
                            {
                                filesToStartDivide.Add(fileWrite);
                            }

                            using (FileStream fileW = new FileStream(fileWrite, FileMode.Append, FileAccess.Write, FileShare.Write))
                            {
                                AddText(fileW, line);
                            }

                            //using (StreamWriter sw = new StreamWriter(fileWrite, true))
                            //{
                            //    try
                            //    {
                            //        var textLine = sr.ReadLine();
                            //        sw.WriteLine(textLine);
                            //        sw.Flush();
                            //    }
                            //    catch (Exception exc)
                            //    {
                            //    }
                            //}
                        });
                    }

                    //using (StreamReader sr = new StreamReader(fileR))
                    //{
                    //    while (sr.Peek() >= 0)
                    //    {
                    //        //var tasks = new List<Task>();
                    //        for (int i = 0; i < number; i++)
                    //        {
                    //            if (sr.Peek() >= 0)
                    //            {
                    //                var fileNameW = Path.GetFileNameWithoutExtension(info.Name);
                    //                var fileWrite = Path.Combine(info.DirectoryName, fileNameW + "!" + i + ".txt");
                    //                if (!filesToStartDivide.Contains(fileWrite))
                    //                {
                    //                    filesToStartDivide.Add(fileWrite);
                    //                }

                    //                using (StreamWriter sw = new StreamWriter(fileWrite, true))
                    //                {
                    //                    try
                    //                    {
                    //                        var textLine = sr.ReadLine();
                    //                        sw.WriteLine(textLine);
                    //                        sw.Flush();
                    //                    }
                    //                    catch (Exception exc)
                    //                    {
                    //                        return exc.Message;
                    //                    }
                    //                }
                    //            }
                    //        }
                    //        //Task.WaitAll(tasks.ToArray());
                    //    }
                    //}
                }
                return "OK";
            }
            catch (Exception exc)
            {
                return exc.Message;
            }
        }

        public static async Task<string> DivideFile()
        {
            List<string> messages = new List<string>();
            try
            {
                await Task.Run(() =>
                {
                    filesToStartDivide = new List<string>();
                    filesToStartDivide.Add(StartFileName);

                    filesToDivide = new List<string>();
                    filesToDivide.Add(StartFileName);

                    int indexChar = 1;
                    List<string> listFiles = new List<string>();
                    
                    while (filesToDivide.Count > 0)
                    {
                        msg = filesToDivide.Count.ToString();
                        listFiles = new List<string>();

                        Parallel.For(0, filesToDivide.Count, (i) =>
                        {
                            messages.Add(DivideIntoFiles(filesToDivide[i], listFiles, indexChar));
                        });

                        filesToDivide.Clear();
                        Parallel.For(0, listFiles.Count, (i) =>
                        {
                            if (names.Keys.Contains("AAAAAAAAAA") && listFiles[i].Contains("_" + names["AAAAAAAAAA"]))
                            {
                                return;
                            }
                            var sizeD = GetFileSize(listFiles[i]);
                            if (sizeD > MaximumSize)
                            {
                                filesToDivide.Add(listFiles[i]);
                            }
                        });

                        indexChar++;
                    }
                });
                return string.Join(" ", messages);
            }
            catch (Exception exc)
            {
                messages.Add(exc.Message);
                return string.Join(" ", messages);
            }
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

        private static string CreateFileName(string addName)
        {
            var info = new FileInfo(StartFileName);
            var fileNameW = Path.GetFileNameWithoutExtension(StartFileName);
            return Path.Combine(info.DirectoryName, fileNameW + "_" + addName + ".txt");
        }

        private static string DivideStartFile(string fileName, List<string> listFiles, int charNum, bool deleteFile = true)
        {
            try
            {
                var info = new FileInfo(fileName);
                lock (sync)
                {
                    using (FileStream fileR = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                    using (StreamReader sr = new StreamReader(fileR))
                    {
                        while (sr.Peek() >= 0)
                        {
                            var textLine = sr.ReadLine();
                            var str = GetStringLine(textLine);
                            if (string.IsNullOrEmpty(str))
                            {
                                continue;
                            }

                            var addName = str.Substring(0, charNum);
                            if (!names.Keys.Contains(addName))
                            {
                                names.Add(addName, indexName++);
                            }

                            var fileWrite = CreateFileName(names[addName].ToString());

                            // Create file if it is not exists
                            File.OpenWrite(fileWrite).Close();
                            if (!listFiles.Contains(fileWrite))
                            {
                                listFiles.Add(fileWrite);
                            }

                            //lock (sync)
                            {
                                using (FileStream fileW = new FileStream(fileWrite, FileMode.Append, FileAccess.Write, FileShare.Write))
                                {
                                    AddText(fileW, textLine);
                                    //using (StreamWriter sw = new StreamWriter(fileW))
                                    //{
                                    //    try
                                    //    {
                                    //        sw.WriteLine(textLine);
                                    //        sw.Flush();
                                    //    }
                                    //    catch (Exception exc)
                                    //    {
                                    //        return exc.Message;
                                    //    }
                                    //}
                                }
                            }
                        }
                    }
                    if (deleteFile)
                    {
                        File.Delete(fileName);
                    }
                }
                return "OK";
            }
            catch(Exception exc)
            {
                return exc.Message;
            }
        }

        private static string ExtractAddName(string fileName)
        {
            var info = new FileInfo(fileName);
            var fileNameW = Path.GetFileNameWithoutExtension(info.Name);
            var addName = fileNameW.Substring(info.Name.IndexOf('_') + 1);
            return names.Where(r => r.Value.ToString() == addName).SingleOrDefault().Key;
        }

        /// <summary>
        /// Creates new files from one file with adding letter to new file names
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="listFiles"></param>
        /// <param name="charNum">Number of letters from the begining of string</param>
        /// <param name="deleteFile"></param>
        /// <returns></returns>
        private static string DivideIntoFiles(string fileName, List<string> listFiles, int charNum, bool deleteFile = true)
        {
            try
            {
                var info = new FileInfo(fileName);

                using (FileStream fileR = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read))
                using (StreamReader sr = new StreamReader(fileR))
                {
                    while (sr.Peek() >= 0)
                    {
                        var textLine = sr.ReadLine();
                        var str = GetStringLine(textLine);
                        if (str.Length > charNum)
                        {
                            var addName = str.Substring(0, charNum);
                            if (!names.Keys.Contains(addName))
                            {
                                names.Add(addName, indexName++);
                            }

                            var fileWrite = CreateFileName(names[addName].ToString());

                            if (!File.Exists(fileWrite))
                            {
                                File.OpenWrite(fileWrite).Close();
                            }
                            if (!listFiles.Contains(fileWrite))
                            {
                                listFiles.Add(fileWrite);
                            }

                            using (FileStream fileW = new FileStream(fileWrite, FileMode.Append, FileAccess.Write, FileShare.Write))
                            {
                                AddText(fileW, textLine + Environment.NewLine);
                            }
                        }
                        else
                        {
                            var addName = "AAAAAAAAAA";
                            if (!names.Keys.Contains(addName))
                            {
                                names.Add(addName, indexName++);
                            }

                            var fileWrite = CreateFileName(names[addName].ToString());

                            if (!File.Exists(fileWrite))
                            {
                                File.OpenWrite(fileWrite).Close();
                            }
                            if (!listFiles.Contains(fileWrite))
                            {
                                listFiles.Add(fileWrite);
                            }

                            using (FileStream fileW = new FileStream(fileWrite, FileMode.Append, FileAccess.Write, FileShare.Write))
                            {
                                AddText(fileW, textLine + Environment.NewLine);
                            }
                        }
                    }
                }

                //Parallel.ForEach(File.ReadLines(fileName), (textLine, _, i) =>
                //{
                //    var str = GetStringLine(textLine + Environment.NewLine);
                //    if (str.Length > charNum)
                //    {
                //        var addName = str.Substring(0, charNum);
                //        if (!names.Keys.Contains(addName))
                //        {
                //            Monitor.Enter(names);
                //            names.Add(addName, indexName++);
                //            Monitor.Exit(names);
                //        }

                //        var fileWrite = CreateFileName(names[addName].ToString());

                //        if (!File.Exists(fileWrite))
                //        {
                //            File.OpenWrite(fileWrite).Close();
                //        }
                //        if (!listFiles.Contains(fileWrite))
                //        {
                //            Monitor.Enter(listFiles);
                //            listFiles.Add(fileWrite);
                //            Monitor.Exit(listFiles);
                //        }

                //        lock (sync)
                //        {
                //            using (FileStream fileW = new FileStream(fileWrite, FileMode.Append, FileAccess.Write, FileShare.Write))
                //            {
                //                AddText(fileW, textLine);
                //            }
                //        }
                //    }
                //    else
                //    {
                //        var addName = "AAAAAAAAAA";
                //        if (!names.Keys.Contains(addName))
                //        {
                //            Monitor.Enter(names);
                //            names.Add(addName, indexName++);
                //            Monitor.Exit(names);
                //        }

                //        var fileWrite = CreateFileName(names[addName].ToString());

                //        if (!File.Exists(fileWrite))
                //        {
                //            File.OpenWrite(fileWrite).Close();
                //        }
                //        if (!listFiles.Contains(fileWrite))
                //        {
                //            Monitor.Enter(listFiles);
                //            listFiles.Add(fileWrite);
                //            Monitor.Exit(listFiles);
                //        }

                //        lock (sync)
                //        {
                //            using (FileStream fileW = new FileStream(fileWrite, FileMode.Append, FileAccess.Write, FileShare.Write))
                //            {
                //                AddText(fileW, textLine);
                //            }
                //        }
                //    }
                //});

                //lock (sync)
                #region old code
                //{
                //    using (FileStream fileR = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.None))
                //    using (StreamReader sr = new StreamReader(fileR))
                //    {
                //        while (sr.Peek() >= 0)
                //        {
                //            var textLine = sr.ReadLine();
                //            var str = GetStringLine(textLine);
                //            if (str.Length > charNum)
                //            {
                //                var addName = str.Substring(0, charNum);
                //                if (!names.Keys.Contains(addName))
                //                {
                //                    names.Add(addName, indexName++);
                //                }

                //                var fileWrite = CreateFileName(names[addName].ToString());

                //                File.OpenWrite(fileWrite).Close();
                //                if (!listFiles.Contains(fileWrite))
                //                {
                //                    listFiles.Add(fileWrite);
                //                }

                //                //lock (sync)
                //                {
                //                    using (FileStream fileW = new FileStream(fileWrite, FileMode.Append, FileAccess.Write, FileShare.Write))
                //                    {
                //                        AddText(fileW, textLine + Environment.NewLine);
                //                    }
                //                }
                //            }
                //            else
                //            {
                //                var addName = "AAAAAAAAAA";
                //                if (!names.Keys.Contains(addName))
                //                {
                //                    names.Add(addName, indexName++);
                //                }

                //                var fileWrite = CreateFileName(names[addName].ToString());

                //                File.OpenWrite(fileWrite).Close();
                //                if (!listFiles.Contains(fileWrite))
                //                {
                //                    listFiles.Add(fileWrite);
                //                }

                //                //lock (sync)
                //                {
                //                    using (FileStream fileW = new FileStream(fileWrite, FileMode.Append, FileAccess.Write, FileShare.Write))
                //                    {
                //                        AddText(fileW, textLine + Environment.NewLine);
                //                    }
                //                }
                //            }
                //        }
                //    }
                //    if (deleteFile)
                //    {
                //        File.Delete(fileName);
                //        names.Remove(ExtractAddName(fileName));
                //    }
                //}
                #endregion

                if (deleteFile)
                {
                    File.Delete(fileName);
                    names.Remove(ExtractAddName(fileName));
                }

                return "OK";
            }
            catch (Exception exc)
            {
                return exc.Message;
            }
        }

        /// <summary>
        /// Add text to file
        /// </summary>
        /// <param name="fs"></param>
        /// <param name="value"></param>
        private static void AddText(FileStream fs, string value)
        {
            byte[] info = new UTF8Encoding(true).GetBytes(value);
            fs.Write(info, 0, info.Length);
        }

        public static bool SortAllFiles()
        {
            var filesToCombine = GetFilesToSort();
            ParallelLoopResult loopResult = Parallel.For(0, filesToCombine.Count, (i) =>
            {
                SortFile(filesToCombine[i]);
            });

            if (loopResult.IsCompleted)
            {
               return true;
            }
            return false;
        }
        
        public static List<string> GetFilesToSort()
        {
            var filesToCombine = new List<string>();
            string[] fileEntries = Directory.GetFiles(StartDirName);
            foreach (string fileName in fileEntries)
            {
                var info = new FileInfo(fileName);
                var fileNameW = Path.GetFileNameWithoutExtension(StartFileName);
                if (info.Name.Contains(fileNameW + "_"))
                {
                    filesToCombine.Add(fileName);
                }
            }
            return filesToCombine;
        }

        public static void SortFile(string fileName)
        {
            var records = new List<Record>();

            Parallel.ForEach(File.ReadLines(fileName), (textLine, _, i) => {
                var str = GetStringLine(textLine);
                if (!string.IsNullOrEmpty(str))
                {
                    var record = GetRecord(textLine);
                    if (record != null)
                    {
                        records.Add(record);
                    }
                }
            });

            //using (StreamReader sr = new StreamReader(fileName))
            //{
            //    while (sr.Peek() >= 0)
            //    {
            //        var textLine = sr.ReadLine();
            //        var str = GetStringLine(textLine);
            //        if (string.IsNullOrEmpty(str))
            //        {
            //            continue;
            //        }
            //        var record = GetRecord(textLine);
            //        records.Add(record);
            //    }
            //}

            var sortedRecords = SortRecords(records);
            File.Delete(fileName);
            using (StreamWriter sw = new StreamWriter(fileName))
            {
                foreach (var record in sortedRecords)
                {
                    sw.WriteLine(record.GetString);
                    sw.Flush();
                }
            }
        }

        private static List<Record> SortRecords(List<Record> records)
        {
            return records.OrderBy(r => r.Line).ThenBy(n => n.Number).ToList();
        }

        public static List<string> MakeCombineList()
        {
            var startName = Path.GetFileNameWithoutExtension(StartFileName);
            var allNames = names.OrderBy(r => r.Key).ToList();
            return allNames.Select(r => string.Format(startName + "_" + r.Value + ".txt")).ToList();
        }

        private static void MergeFile(string fileRead, string fileWrite)
        {
            if (File.Exists(fileRead))
            {
                using (FileStream fileW = new FileStream(fileWrite, FileMode.Append, FileAccess.Write, FileShare.None))
                using (StreamWriter sw = new StreamWriter(fileW))
                {
                    using (StreamReader sr = new StreamReader(fileRead))
                    {
                        while (sr.Peek() >= 0)
                        {
                            var textLine = sr.ReadLine();
                            sw.WriteLine(textLine);
                            sw.Flush();
                        }
                    }
                }
            }
        }

        public static async Task CombineFile(string fileWrite)
        {            
            await Task.Run(() =>
            {
                var filesToCombine = MakeCombineList();
                File.Create(fileWrite).Close();
                for (int i = 0; i < filesToCombine.Count; i++)
                {
                    var fileRead = Path.Combine(StartDirName, filesToCombine[i]);
                    MergeFile(fileRead, fileWrite);
                    File.Delete(fileRead);
                }
            });
        }
    }
}
