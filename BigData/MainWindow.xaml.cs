using BigData.DAL;
using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace BigData
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
            var threads = System.Diagnostics.Process.GetCurrentProcess().Threads;
            Dispatcher.Invoke(() => { TextBoxThreads.Text = threads.Count.ToString(); });

            //List<char> allChars = new List<char>();
            //char value = char.MinValue;
            //while(value <= char.MinValue + 100)
            //{
            //    allChars.Add(value++);
            //}

            //allChars.OrderBy(r => r);

            //ListBoxChars.ItemsSource = allChars;
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
            OpenFileDialog openFileDialog = new OpenFileDialog
            {
                Filter = "Text files (*.txt)|*.txt"
            };
            if (openFileDialog.ShowDialog() == true)
            {
                BigFileRepository.StartFileInfo = new FileInfo(openFileDialog.FileName);
                BigFileRepository.StartDirName = BigFileRepository.StartFileInfo.DirectoryName;
                BigFileRepository.StartFileName = openFileDialog.FileName;
            }

            ///// Dividing
            var strSize = TextBoxSize.Text.Trim();
            if(string.IsNullOrEmpty(strSize))
            {
                return;
            }
            BigFileRepository.MaximumSize = long.Parse(strSize);

            var longSize = TextBoxLongSizeStart.Text.Trim();
            if (string.IsNullOrEmpty(longSize))
            {
                return;
            }
            BigFileRepository.LongSizeStart = long.Parse(longSize);
            if (BigFileRepository.LongSizeStart > 0)
            {
                if ((BigFileRepository.StartFileInfo?.Length ?? 0) > BigFileRepository.MaximumSize)
                {
                    TextBoxInfo.Text = "Dividing started";
                    await BigFileRepository.DivideFile();
                    TextBoxInfo.Text = "Dividing finished";
                }
            }
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
                                sw.Flush();
                            }
                            else
                            if (i % 10 == 0)
                            {
                                stringLine = i + ". " + randomString;
                                sw.WriteLine(stringLine);
                                sw.Flush();
                            }
                            else
                            if (i % 15 == 0)
                            {
                                stringLine = i + ". " + randomString;
                                sw.WriteLine(stringLine);
                                sw.Flush();
                            }
                            else
                            {
                                randomString = RandomString(lettersNumbers).Trim();
                                stringLine = i + ". " + randomString;
                                sw.WriteLine(stringLine);
                                sw.Flush();
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
            TextBoxInfo.Text = "Sort started";
            BigFileRepository.GetFilesToSort();
            ParallelLoopResult loopResult = Parallel.For(0, BigFileRepository.filesToCombine.Count, (i) =>
            {
                BigFileRepository.SortFile(BigFileRepository.filesToCombine[i]);
            });

            if (loopResult.IsCompleted)
            {
                TextBoxInfo.Text = "Sort finished";
            }
        }

        private void ButtonFiles_Click(object sender, RoutedEventArgs e)
        {
            TextBoxInfo.Text = "Combine list started";
            BigFileRepository.MakeCombineList();
            TextBoxInfo.Text = "Combine list finished";
        }

        private async void ButtonCombine_Click(object sender, RoutedEventArgs e)
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
                TextBoxInfo.Text = "Combine started";
                await BigFileRepository.CombineFile(fileWrite);
                TextBoxInfo.Text = "Combine finished";
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
                            sw.Flush();
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
