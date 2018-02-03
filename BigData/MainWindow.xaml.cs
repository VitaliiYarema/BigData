using BigData.DAL;
using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace BigData
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public FileInfo StartFileInfo { get; set; }
        public string StartFileName { get; set; }
        public string StartDirName { get; set; }
        public List<string> filesToDivide = new List<string>();
        public List<string> filesToCombine = new List<string>();

        public MainWindow()
        {
            InitializeComponent();
            var threads = System.Diagnostics.Process.GetCurrentProcess().Threads;
            Dispatcher.Invoke(() => { TextBoxThreads.Text = threads.Count.ToString(); });

            List<char> allChars = new List<char>();
            char value = char.MinValue;
            while(value <= char.MinValue + 100)
            {
                allChars.Add(value++);
            }

            allChars.OrderBy(r => r);

            ListBoxChars.ItemsSource = allChars;
        }

        private static Random random = new Random();
        public static string RandomString(int length)
        {
            int number = random.Next(1, length);
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ ";
            return new string(Enumerable.Repeat(chars, number)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        private async void ButtonReadFile_Click(object sender, RoutedEventArgs e)
        {
            ///// Start
            TextBoxDone.Text = "Start";

            FileInfo info = null;
            long size = 0;
            string dirName = "";
            string fileName = "";
            OpenFileDialog openFileDialog = new OpenFileDialog
            {
                Filter = "Text files (*.txt)|*.txt"
            };
            if (openFileDialog.ShowDialog() == true)
            {
                StartFileInfo = info = new FileInfo(openFileDialog.FileName);
                StartDirName = dirName = info.DirectoryName;
                StartFileName = fileName = openFileDialog.FileName;
                size = info.Length;
                filesToDivide.Add(fileName);
                BigFileRepository.filesToDivide.Clear();
                BigFileRepository.filesToDivide.Add(fileName);
            }

            ///// Dividing
            var strSize = TextBoxSize.Text.Trim();
            if(string.IsNullOrEmpty(strSize))
            {
                return;
            }
            var longSize = long.Parse(strSize);
            if (size > longSize)
            {
                TextBoxInfo.Text = "Dividing started";
                await BigFileRepository.DivideFile(fileName, longSize);
                TextBoxInfo.Text = "Dividing finished";

                //await Task.Run(() =>
                //{
                //    //TextBoxDone.Text = "Working";
                //    int indexChar = 0;
                //    List<string> listFiles = new List<string>();
                //    while (filesToDivide.Count > 0)
                //    {
                //        listFiles = new List<string>();

                //        Parallel.For(0, filesToDivide.Count, (i) =>
                //        {
                //            var threads = System.Diagnostics.Process.GetCurrentProcess().Threads;
                //            Dispatcher.Invoke(() => { TextBoxThreads.Text = threads.Count.ToString(); });

                //            if (filesToDivide[i] == fileName)
                //            {
                //                DivideIntoFiles(filesToDivide[i], listFiles, indexChar, false);
                //            }
                //            else
                //            {
                //                DivideIntoFiles(filesToDivide[i], listFiles, indexChar);
                //            }
                //        });

                //        Dispatcher.Invoke(() => { TextBoxDone.Text = "Dividing " + filesToDivide.Count; });

                //        filesToDivide.Clear();
                //        for (int i = 0; i < listFiles.Count; i++)
                //        {
                //            if (listFiles[i].Contains("  "))
                //            {
                //                continue;
                //            }
                //            var sizeD = GetFileSize(listFiles[i]);
                //            if (sizeD > longSize)
                //            {
                //                filesToDivide.Add(listFiles[i]);
                //            }
                //        }
                //        indexChar++;
                //    }
                //});
            }

            ///// Sorting
            //GetFilesToSort(StartDirName);

            ////foreach(var file in filesToCombine)
            ////{
            ////    SortFile(file);
            ////}

            //Parallel.For(0, filesToCombine.Count, (i) =>
            //{
            //    SortFile(filesToCombine[i]);
            //});

            ///// Combining back

            ///// Done
            TextBoxDone.Text = "Done";
        }

        private long GetFileSize(string fileName)
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

        private void DivideIntoFiles(string fileName, List<string> listFiles, int charNum, bool deleteFile = true, string parentDirectory = "")
        {
            try
            {
                //Dictionary<string, StreamWriter> streamWrites = new Dictionary<string, StreamWriter>();
                var info = new FileInfo(fileName);
                using (StreamReader sr = new StreamReader(fileName))
                {
                    while (sr.Peek() >= 0)
                    {
                        var textLine = sr.ReadLine();
                        var str = GetStringLine(textLine);
                        if(string.IsNullOrEmpty(str))
                        {
                            continue;
                        }
                        if (str.Length > charNum)
                        {
                            var fileNameW = System.IO.Path.GetFileNameWithoutExtension(info.Name);
                            var fileWrite = System.IO.Path.Combine(info.DirectoryName, fileNameW + " " + str[charNum] + ".txt");

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
                            var fileNameW = System.IO.Path.GetFileNameWithoutExtension(info.Name);
                            var fileWrite = System.IO.Path.Combine(info.DirectoryName, fileNameW + "  " + ".txt");

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
                if(deleteFile)
                {
                    File.Delete(fileName);
                }
            }
            catch
            {

            }
        }

        //private async Task DivideIntoFilesAsync(string fileName, List<string> listFiles, int charNum, bool deleteFile = true, string parentDirectory = "")
        //{
        //    try
        //    {
        //        await Task.Yield();
        //        var info = new FileInfo(fileName);
        //        using (StreamReader sr = new StreamReader(fileName))
        //        {
        //            while (sr.Peek() >= 0)
        //            {
        //                var textLine = sr.ReadLine();
        //                var str = GetStringLine(textLine);
        //                // if file exists write to it or
        //                // create new file + A - letter from abc
        //                if (string.IsNullOrEmpty(str))
        //                {
        //                    continue;
        //                }
        //                if (str.Length > charNum)
        //                {
        //                    var fileWrite = System.IO.Path.Combine(info.DirectoryName, info.Name + "_" + str[charNum] + ".txt");
        //                    File.OpenWrite(fileWrite).Close();
        //                    if (!listFiles.Contains(fileWrite))
        //                    {
        //                        listFiles.Add(fileWrite);
        //                    }
        //                    using (StreamWriter sw = new StreamWriter(fileWrite, true, Encoding.Unicode))
        //                    {
        //                        sw.WriteLine(textLine);
        //                    }
        //                }
        //                else
        //                {
        //                    var fileWrite = System.IO.Path.Combine(info.DirectoryName, info.Name + "__.txt");
        //                    File.OpenWrite(fileWrite).Close();
        //                    if (!listFiles.Contains(fileWrite))
        //                    {
        //                        listFiles.Add(fileWrite);
        //                    }
        //                    using (StreamWriter sw = new StreamWriter(fileWrite, true, Encoding.Unicode))
        //                    {
        //                        sw.WriteLine(textLine);
        //                    }
        //                }
        //            }
        //        }
        //        if (deleteFile)
        //        {
        //            File.Delete(fileName);
        //        }
        //    }
        //    catch
        //    {

        //    }
        //}

        private Record GetRecord(string line)
        {
            var strNumber = GetStringNumber(line);
            if(string.IsNullOrEmpty(strNumber))
            {
                return null;
            }
            var number = int.Parse(strNumber);

            var strLine = GetStringLine(line);
            var record = new Record() { Number = number, Line = strLine};

            return record;
        }

        private string GetStringLine(string line)
        {
            var str = line.Substring(line.IndexOf(". ") + 2);
            return str;
        }

        private string GetStringNumber(string line)
        {
            var str = line.Substring(0, line.IndexOf("."));
            return str;
        }

        //private int? GetStringNumber(string record)
        //{
        //    var str = record.Substring(0, record.IndexOf("."));
        //    return string.IsNullOrEmpty(str) == false ? (int?)int.Parse(str) : null;
        //}

        private void GetFilesToSort(string dirName)
        {
            filesToCombine.Clear();
            string[] fileEntries = Directory.GetFiles(dirName);
            foreach (string fileName in fileEntries)
            {
                var info = new FileInfo(fileName);
                var fileNameW = System.IO.Path.GetFileNameWithoutExtension(StartFileInfo.Name);
                if (info.Name.Contains(fileNameW + "_"))
                {
                    filesToCombine.Add(fileName);
                }
            }
        }

        private void SortFile(string fileName)
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
            using (StreamWriter sw = new StreamWriter(fileName, true, Encoding.Unicode))
            {
                foreach(var record in sortedRecords)
                {
                    sw.WriteLine(record.GetLine);
                }
            }
        }

        private List<Record> SortRecords(List<Record> records)
        {
            return records.OrderBy(r => r.Line).ThenBy(n => n.Number).ToList();
        }

        private void ButtonGenerate_Click(object sender, RoutedEventArgs e)
        {
            var sn = TextBoxStringsNumber.Text.Trim();
            var stringsNumbers = string.IsNullOrEmpty(sn) ? 1000 : int.Parse(sn);

            var ln = TextBoxLettersNumber.Text.Trim();
            var lettersNumbers = string.IsNullOrEmpty(ln)? 10 : int.Parse(ln);

            CreateGeneratedFile(stringsNumbers, lettersNumbers);            
        }

        private void CreateGeneratedFile(int stringsNumbers, int lettersNumbers)
        {
            try
            {
                SaveFileDialog saveFileDialog = new SaveFileDialog
                {
                    Filter = "Text files (*.txt)|*.txt",
                    DefaultExt = "txt",
                    AddExtension = true,
                    FileName = "Yarema"
                };
                if (saveFileDialog.ShowDialog() == true)
                {
                    using (StreamWriter sw = new StreamWriter(saveFileDialog.FileName, false, Encoding.Unicode))
                    {
                        //sw.WriteLine(textLine);
                        string randomString = "";
                        string stringLine = "";
                        for (int i = 1; i < stringsNumbers; i++)
                        {
                            //var record = new Record();
                            if (i % 5 == 0)
                            {
                                stringLine = i + ". " + randomString;
                                sw.WriteLine(stringLine);
                            }
                            else
                            if (i % 10 == 0)
                            {
                                stringLine = i + ". " + randomString;
                                sw.WriteLine(stringLine);
                            }
                            else
                            if (i % 15 == 0)
                            {
                                stringLine = i + ". " + randomString;
                                sw.WriteLine(stringLine);
                            }
                            else
                            {
                                randomString = RandomString(lettersNumbers).Trim();
                                stringLine = i + ". " + randomString;
                                sw.WriteLine(stringLine);
                            }
                        }
                    }
                }
            }
            catch
            {

            }
        }
        
        private void ButtonSort_Click(object sender, RoutedEventArgs e)
        {
            TextBoxDone.Text = "Start sort";
            GetFilesToSort(StartDirName);
            ParallelLoopResult loopResult = Parallel.For(0, filesToCombine.Count, (i) =>
            {
                SortFile(filesToCombine[i]);
            });

            if (loopResult.IsCompleted)
            {
                TextBoxDone.Text = "Sorted";
            }
        }

        private void MakeCombineList()
        {
            GetFilesToSort(StartDirName);
            var files = filesToCombine.Select(r => System.IO.Path.GetFileNameWithoutExtension(r)).ToList();
            files = files.OrderBy(r => r).ToList();
            ListBoxFiles.ItemsSource = files;
            filesToCombine = files.Select(r => string.Format(r + ".txt")).ToList();
        }

        private void ButtonFiles_Click(object sender, RoutedEventArgs e)
        {
            MakeCombineList();
        }

        private void ButtonCombine_Click(object sender, RoutedEventArgs e)
        {
            string fileWrite = "";
            SaveFileDialog saveFileDialog = new SaveFileDialog
            {
                Filter = "Text files (*.txt)|*.txt",
                DefaultExt = "txt",
                AddExtension = true,
                FileName = "Result"
            };
            if (saveFileDialog.ShowDialog() == true)
            {
                fileWrite = saveFileDialog.FileName;
                for (int i = 0; i < filesToCombine.Count; i++)
                {
                    var fileRead = System.IO.Path.Combine(StartDirName, filesToCombine[i]);
                    MergeFile(fileRead, fileWrite);
                }
            }
        }

        private void MergeFile(string fileRead, string fileWrite)
        {
            using (StreamWriter sw = new StreamWriter(fileWrite, true, Encoding.Unicode))
            {
                using (StreamReader sr = new StreamReader(fileRead))
                {
                    while (sr.Peek() >= 0)
                    {
                        var textLine = sr.ReadLine();
                        sw.WriteLine(textLine);
                    }
                }
            }
        }

        private async void ButtonCreateBigFile_Click(object sender, RoutedEventArgs e)
        {
            SaveFileDialog saveFileDialog = new SaveFileDialog
            {
                Filter = "Text files (*.txt)|*.txt",
                DefaultExt = "txt",
                AddExtension = true,
                FileName = "BigFile"
            };
            if (saveFileDialog.ShowDialog() == true)
            {
                var number = int.Parse(TextBoxLettersNumber.Text.Trim());
                var start = int.Parse(TextBoxStart.Text.Trim());
                var end = int.Parse(TextBoxEnd.Text.Trim());
                var recordsNumber = int.Parse(TextBoxStringsNumber.Text.Trim());

                TextBoxInfo.Text = "BigFile start creating";
                await Task.Run(() =>
                {
                    using (StreamWriter sw = new StreamWriter(saveFileDialog.FileName, false, Encoding.Unicode))
                    {
                        for (int i = 0; i < recordsNumber; i++)
                        {
                            var record = CreateFile.CreateRecord(number, start, end);
                            sw.WriteLine(record.GetString);
                        }
                    }
                });
                TextBoxInfo.Text = "BigFile created";
            }
        }

        private async Task<int> ReadDictionary(string fileName)
        {
            var number = await Task.Run(() =>
            {
                //var info = new FileInfo(fileName);
                //BigFileRepository.StartFileInfo = info;
                //BigFileRepository.StartFileName = info.Name;
                //BigFileRepository.StartDirName = info.DirectoryName;
                return CreateFile.ReadDictionary(fileName);
            });
            return number;
        }

        private async void ButtonDictionary_Click(object sender, RoutedEventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog
            {
                Filter = "Text files (*.txt)|*.txt"
            };
            if (openFileDialog.ShowDialog() == true)
            {
                TextBoxInfo.Text = "Loading dictionary";
                var number = await ReadDictionary(openFileDialog.FileName);
                TextBoxInfo.Text = "Dictionary loaded";
                TextBoxWordsInDictionary.Text = number.ToString();
            }
        }
    }
}
