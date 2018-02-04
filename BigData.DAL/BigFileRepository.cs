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
        private static Object locker = new Object();

        public static void PrepareFilesToDivide()
        {
            filesToStartDivide.Clear();
            var info = new FileInfo(StartFileName);
            var number = info.Length / LongSizeStart;
            using (StreamReader sr = new StreamReader(StartFileName))
            {
                while (sr.Peek() >= 0)
                {
                    var tasks = new List<Task>();
                    for (int i = 0; i < number; i++)
                    {
                        if (sr.Peek() >= 0)
                        {
                            var fileNameW = Path.GetFileNameWithoutExtension(info.Name);
                            var fileWrite = Path.Combine(info.DirectoryName, fileNameW + "!" + i + ".txt");
                            if (!filesToStartDivide.Contains(fileWrite))
                            {
                                filesToStartDivide.Add(fileWrite);
                            }
                            //tasks.Add(Task.Run(() =>
                            //{
                            //    lock (locker)
                            //    {
                            //        using (StreamWriter sw = new StreamWriter(fileWrite, true))
                            //        {
                            //            try
                            //            {
                            //                var textLine = sr.ReadLine();
                            //                sr.DiscardBufferedData();
                            //                sw.WriteLine(textLine);
                            //                sw.Flush();
                            //            }
                            //            catch { }
                            //        }
                            //    }
                            //}));

                            using (StreamWriter sw = new StreamWriter(fileWrite, true))
                            {
                                try
                                {
                                    var textLine = sr.ReadLine();
                                    sw.WriteLine(textLine);
                                    sw.Flush();
                                }
                                catch { }
                            }
                        }
                    }
                    //Task.WaitAll(tasks.ToArray());
                }
            }
        }

        public static async Task DivideFile()
        {
            await Task.Run(() =>
            {
                PrepareFilesToDivide();

                int indexChar = 0;
                List<string> listFiles = new List<string>();

                if (filesToStartDivide.Count > 0)
                {
                    listFiles = new List<string>();

                    //Parallel.For(0, filesToStartDivide.Count, (i) =>
                    //{
                    //    DivideStartFile(filesToStartDivide[i], listFiles, indexChar);
                    //});

                    int num = filesToStartDivide.Count;
                    for (int i = 0; i < num; i++)
                    {
                        DivideStartFile(filesToStartDivide[i], listFiles, indexChar);
                    }

                    filesToDivide.Clear();
                    for (int i = 0; i < listFiles.Count; i++)
                    {
                        var sizeD = GetFileSize(listFiles[i]);
                        if (sizeD > MaximumSize)
                        {
                            filesToDivide.Add(listFiles[i]);
                        }
                    }
                    indexChar++;
                }

                while (filesToDivide.Count > 0)
                {
                    listFiles = new List<string>();

                    //Parallel.For(0, filesToDivide.Count, (i) =>
                    //{
                    //    if (filesToDivide[i] == StartFileName)
                    //    {
                    //        DivideIntoFiles(filesToDivide[i], listFiles, indexChar, false);
                    //    }
                    //    else
                    //    {
                    //        DivideIntoFiles(filesToDivide[i], listFiles, indexChar);
                    //    }
                    //});

                    int num = filesToDivide.Count;
                    for (int i = 0; i < num; i++)
                    {
                        DivideIntoFiles(filesToDivide[i], listFiles, indexChar);
                    }

                    filesToDivide.Clear();
                    for (int i = 0; i < listFiles.Count; i++)
                    {
                        if (listFiles[i].Contains("_" + "  "))
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
                        var str = GetStringLine(textLine).ToUpper();
                        if (string.IsNullOrEmpty(str))
                        {
                            continue;
                        }
                        
                        char[] invalidPathChars = Path.GetInvalidPathChars();
                        var charFile = str[charNum];
                        while (invalidPathChars.Contains(charFile))
                        {
                            charFile++;
                        }

                        var fileNameW = Path.GetFileNameWithoutExtension(StartFileName);
                        var fileWrite = Path.Combine(info.DirectoryName, fileNameW + "_" + charFile + ".txt");
                        File.OpenWrite(fileWrite).Close();
                        if (!listFiles.Contains(fileWrite))
                        {
                            listFiles.Add(fileWrite);
                        }

                        lock (locker)
                        {
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
                }
            }
            catch
            {

            }
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
                        var str = GetStringLine(textLine).ToUpper();
                        if (string.IsNullOrEmpty(str))
                        {
                            continue;
                        }
                        if (str.Length > charNum)
                        {
                            char[] invalidPathChars = Path.GetInvalidPathChars();
                            var charFile = str[charNum];
                            while (invalidPathChars.Contains(charFile))
                            {
                                charFile++;
                            }

                            var fileNameW = Path.GetFileNameWithoutExtension(info.Name);
                            var fileWrite = Path.Combine(info.DirectoryName, fileNameW + "_" + charFile + ".txt");

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
                            var fileNameW = Path.GetFileNameWithoutExtension(info.Name);
                            var fileWrite = Path.Combine(info.DirectoryName, fileNameW + "_" + "  " + ".txt");

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
            GetFilesToSort();
            var files = filesToCombine.Select(r => System.IO.Path.GetFileNameWithoutExtension(r)).ToList();
            files = files.OrderBy(r => r).ToList();
            filesToCombine = files.Select(r => string.Format(r + ".txt")).ToList();
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
                for (int i = 0; i < filesToCombine.Count; i++)
                {
                    var fileRead = Path.Combine(StartDirName, filesToCombine[i]);
                    MergeFile(fileRead, fileWrite);
                }
            });
        }
    }
}
