using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigData.DAL
{
    public static class BigFileRepository
    {
        public static string Marker { get; set; }
        public static FileInfo StartFileInfo { get; set; }
        public static string StartFileName { get; set; }
        public static string StartDirName { get; set; }
        public static List<string> filesToStartDivide = new List<string>();
        private static List<string> filesToDivide = new List<string>();
        public static List<string> filesToCombine = new List<string>();
        public static long MaximumSize { get; set; }
        public static long LongSizeStart { get; set; }
        private static List<string> filesInUse = new List<string>();
        private static Dictionary<string, int> names = new Dictionary<string, int>();
        private static int indexName = 0;

        static BigFileRepository()
        {
            //names.Add("AAAAAAAAAA", 0);
        }

        public static void PrepareFilesToDivide()
        {
            filesToDivide.Clear();
            filesToDivide.Add(StartFileName);

            //filesToStartDivide.Clear();
            //var info = new FileInfo(StartFileName);
            //var number = info.Length / LongSizeStart;
            //using (StreamReader sr = new StreamReader(StartFileName))
            //{
            //    while (sr.Peek() >= 0)
            //    {
            //        var tasks = new List<Task>();
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
            //                //tasks.Add(Task.Run(() =>
            //                //{
            //                //    lock (locker)
            //                //    {
            //                //        using (StreamWriter sw = new StreamWriter(fileWrite, true))
            //                //        {
            //                //            try
            //                //            {
            //                //                var textLine = sr.ReadLine();
            //                //                sr.DiscardBufferedData();
            //                //                sw.WriteLine(textLine);
            //                //                sw.Flush();
            //                //            }
            //                //            catch { }
            //                //        }
            //                //    }
            //                //}));

            //                using (StreamWriter sw = new StreamWriter(fileWrite, true))
            //                {
            //                    try
            //                    {
            //                        var textLine = sr.ReadLine();
            //                        sw.WriteLine(textLine);
            //                        sw.Flush();
            //                    }
            //                    catch { }
            //                }
            //            }
            //        }
            //        //Task.WaitAll(tasks.ToArray());
            //    }
            //}
        }

        public static async Task DivideFile()
        {
            await Task.Run(() =>
            {
                PrepareFilesToDivide();

                int indexChar = 0;
                List<string> listFiles = new List<string>();

                //if (filesToStartDivide.Count > 0)
                //{
                //    listFiles = new List<string>();

                //    //Parallel.For(0, filesToStartDivide.Count, (i) =>
                //    //{
                //    //    DivideStartFile(filesToStartDivide[i], listFiles, indexChar);
                //    //});

                //    int num = filesToStartDivide.Count;
                //    for (int i = 0; i < num; i++)
                //    {
                //        DivideStartFile(filesToStartDivide[i], listFiles, indexChar);
                //    }

                //    filesToDivide.Clear();
                //    for (int i = 0; i < listFiles.Count; i++)
                //    {
                //        var sizeD = GetFileSize(listFiles[i]);
                //        if (sizeD > MaximumSize)
                //        {
                //            filesToDivide.Add(listFiles[i]);
                //        }
                //    }
                //    indexChar++;
                //}

                while (filesToDivide.Count > 0)
                {
                    listFiles = new List<string>();

                    Parallel.For(0, filesToDivide.Count, (i) =>
                    {
                        if (filesToDivide[i] == StartFileName)
                        {
                            DivideIntoFiles(filesToDivide[i], listFiles, indexChar, false);
                        }
                        else
                        {
                            DivideIntoFiles(filesToDivide[i], listFiles, indexChar);
                        }
                    });

                    //int num = filesToDivide.Count;
                    //for (int i = 0; i < num; i++)
                    //{
                    //    DivideIntoFiles(filesToDivide[i], listFiles, indexChar);
                    //}

                    filesToDivide.Clear();
                    for (int i = 0; i < listFiles.Count; i++)
                    {
                        if (names.Keys.Contains("AAAAAAAAAA") && listFiles[i].Contains("_" + names["AAAAAAAAAA"]))
                        {
                            continue;
                        }
                        var sizeD = GetFileSize(listFiles[i]);
                        if (sizeD > MaximumSize)
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

        private static string CreateFileName(string addName)
        {
            var info = new FileInfo(StartFileName);
            var fileNameW = Path.GetFileNameWithoutExtension(StartFileName);
            return Path.Combine(info.DirectoryName, fileNameW + "_" + addName + ".txt");
        }

        private static void DivideStartFile(string fileName, List<string> listFiles, int charNum, bool deleteFile = true)
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

                        var addName = str.Substring(0, charNum);
                        if (!names.Keys.Contains(addName))
                        {
                            names.Add(addName, indexName++);
                        }
                        
                        var fileWrite = CreateFileName(names[addName].ToString()); 

                        File.OpenWrite(fileWrite).Close();
                        if (!listFiles.Contains(fileWrite))
                        {
                            listFiles.Add(fileWrite);
                        }

                        using (StreamWriter sw = new StreamWriter(fileWrite, true))
                        {
                            try
                            {
                                sw.WriteLine(textLine);
                                sw.Flush();
                            }
                            catch
                            {

                            }
                        }
                    }
                }
                if (deleteFile)
                {
                    File.Delete(fileName);
                    names.Remove(ExtractAddName(fileName));
                }
            }
            catch
            {

            }
        }

        private static string ExtractAddName(string fileName)
        {
            var info = new FileInfo(fileName);
            var addName = info.Name.Substring(info.Name.IndexOf('_') + 1);
            return names.Where(r => r.Value.ToString() == addName).SingleOrDefault().Key;
        }

        private static void DivideIntoFiles(string fileName, List<string> listFiles, int charNum, bool deleteFile = true)
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
                        if (str.Length > charNum)
                        {
                            var addName = str.Substring(0, charNum);
                            if (!names.Keys.Contains(addName))
                            {
                                names.Add(addName, indexName++);
                            }

                            var fileWrite = CreateFileName(names[addName].ToString());

                            File.OpenWrite(fileWrite).Close();
                            if (!listFiles.Contains(fileWrite))
                            {
                                listFiles.Add(fileWrite);
                            }

                            using (StreamWriter sw = new StreamWriter(fileWrite, true))
                            {
                                try
                                {
                                    sw.WriteLine(textLine);
                                    sw.Flush();
                                }
                                catch
                                {

                                }
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

                            File.OpenWrite(fileWrite).Close();
                            if (!listFiles.Contains(fileWrite))
                            {
                                listFiles.Add(fileWrite);
                            }

                            using (StreamWriter sw = new StreamWriter(fileWrite, true))
                            {
                                try
                                {
                                    sw.WriteLine(textLine);
                                    sw.Flush();
                                }
                                catch
                                {

                                }
                            }
                        }
                    }
                }
                if (deleteFile)
                {
                    File.Delete(fileName);
                    names.Remove(ExtractAddName(fileName));
                }
            }
            catch
            {

            }
        }
        
        public static void GetFilesToSort()
        {
            filesToCombine.Clear();
            string[] fileEntries = Directory.GetFiles(StartDirName);
            foreach (string fileName in fileEntries)
            {
                var info = new FileInfo(fileName);
                var fileNameW = Path.GetFileNameWithoutExtension(StartFileInfo.Name);
                if (info.Name.Contains(fileNameW + "_"))
                {
                    filesToCombine.Add(fileName);
                }
            }
        }

        public static void SortFile(string fileName)
        {
            List<Record> records = new List<Record>();
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
                    var record = GetRecord(textLine);
                    records.Add(record);
                }
            }
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

        public static void MakeCombineList()
        {
            //GetFilesToSort();
            var startName = Path.GetFileNameWithoutExtension(StartFileName);
            //var files = filesToCombine.Select(r => Path.GetFileNameWithoutExtension(r)).ToList();
            var allNames = names.OrderBy(r => r.Key).ToList();
            
            //files = files.OrderBy(r => r).ToList();
            filesToCombine = allNames.Select(r => string.Format(StartFileInfo.DirectoryName + startName + "_" + r.Value + ".txt")).ToList();
        }

        private static void MergeFile(string fileRead, string fileWrite)
        {
            using (StreamWriter sw = new StreamWriter(fileWrite, true))
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

        public static async Task CombineFile(string fileWrite)
        {            
            await Task.Run(() =>
            {
                if (File.Exists(fileWrite))
                {
                    for (int i = 0; i < filesToCombine.Count; i++)
                    {
                        var fileRead = Path.Combine(StartDirName, filesToCombine[i]);
                        MergeFile(fileRead, fileWrite);
                    }
                }
            });
        }
    }
}
