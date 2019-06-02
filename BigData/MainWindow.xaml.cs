using BigData.DAL;
using Microsoft.Win32;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;
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
            BigFileRepository.StartDirName = Properties.Settings.Default.StartDirName;
            BigFileRepository.StartFileName = Properties.Settings.Default.StartFileName;
        }
        
        private async void ButtonReadFile_Click(object sender, RoutedEventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog
            {
                Filter = "Text files (*.txt)|*.txt"
            };
            if (openFileDialog.ShowDialog() == true)
            {
                BigFileRepository.names = new Dictionary<string, int>();
                BigFileRepository.StartFileInfo = new FileInfo(openFileDialog.FileName);
                BigFileRepository.StartDirName = BigFileRepository.StartFileInfo.DirectoryName;
                Properties.Settings.Default.StartDirName = BigFileRepository.StartDirName;
                BigFileRepository.StartFileName = openFileDialog.FileName;
                Properties.Settings.Default.StartFileName = BigFileRepository.StartFileName;
                Properties.Settings.Default.Save();


                ///// Dividing
                var strSize = TextBoxSize.Text.Trim();
                if (string.IsNullOrEmpty(strSize))
                {
                    return;
                }
                BigFileRepository.MaximumSize = long.Parse(strSize);

                //var longSize = TextBoxLongSizeStart.Text.Trim();
                //if (string.IsNullOrEmpty(longSize))
                //{
                //    return;
                //}
                //BigFileRepository.LongSizeStart = long.Parse(longSize);
                //if (BigFileRepository.LongSizeStart > 0)
                //{
                //    if ((BigFileRepository.StartFileInfo?.Length ?? 0) > BigFileRepository.MaximumSize)
                //    {
                //        TextBoxInfo.Text = "Dividing started";
                //        var message = await BigFileRepository.DivideFile();
                //        TextBoxInfo.Text = "Dividing finished";
                //    }
                //}

                if ((BigFileRepository.StartFileInfo?.Length ?? 0) > BigFileRepository.MaximumSize)
                {
                    TextBoxInfo.Text = "Dividing started";
                    var message = await BigFileRepository.DivideFile();
                    TextBoxInfo.Text = "Dividing finished";
                }
            }
        }

        private void ButtonSort_Click(object sender, RoutedEventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog
            {
                Filter = "Text files (*.txt)|*.txt"
            };
            if (openFileDialog.ShowDialog() == true)
            {
                BigFileRepository.StartFileInfo = new FileInfo(openFileDialog.FileName);
                BigFileRepository.StartDirName = BigFileRepository.StartFileInfo.DirectoryName;
                //BigFileRepository.StartFileName = openFileDialog.FileName;
            }

            TextBoxInfo.Text = "Sort started";
            var result = BigFileRepository.SortAllFiles();
            if (result)
            {
                TextBoxInfo.Text = "Sort finished";
                return;
            }

            TextBoxInfo.Text = "Sorted with error";
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
                TextBoxInfo.Text = "Merge started";
                await BigFileRepository.CombineFile(fileWrite);
                TextBoxInfo.Text = "Merge finished";
            }
        }

        private async void ButtonCreateBigFile_Click(object sender, RoutedEventArgs e)
        {
            var start = int.Parse(TextBoxStart.Text.Trim()); //TextBoxEnd
            var end = int.Parse(TextBoxEnd.Text.Trim());
            var dic = int.Parse(TextBoxWordsInDictionary.Text.Trim());
            if (dic < start || dic < end)
            {
                TextBoxInfo.Text = "Read dictionary first";
            }
            else
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
                    var number = int.Parse(TextBoxWordsNumber.Text.Trim());
                    var recordsNumber = int.Parse(TextBoxStringsNumber.Text.Trim());

                    TextBoxInfo.Text = "BigFile creating started";
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
                TextBoxInfo.Text = "Dictionary loading";
                var number = await ReadDictionary(openFileDialog.FileName);
                TextBoxInfo.Text = "Dictionary loaded";
                TextBoxWordsInDictionary.Text = number.ToString();
            }
        }

        private void Window_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            var fileName = Path.Combine(System.AppDomain.CurrentDomain.BaseDirectory, "Dictionary/names.txt");
            string json = JsonConvert.SerializeObject(BigFileRepository.names);
            File.WriteAllText(fileName, json);
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            var fileName = Path.Combine(System.AppDomain.CurrentDomain.BaseDirectory, "Dictionary/names.txt");
            if (File.Exists(fileName))
            {
                BigFileRepository.names = JsonConvert.DeserializeObject<Dictionary<string, int>>(File.ReadAllText(fileName));
            }
            TextBoxInfo.Text = @"Тестовое задание от Altium Limited.

На входе есть большой текстовый файл, где каждая строка имеет вид Number. String 
Например:
415.Apple
30432.Something something something
1.Apple
32.Cherry is the best
2.Banana is yellow

Обе части могут в пределах файла повторяться. Необходимо получить на выходе другой файл, где все строки отсортированы.Критерий сортировки: сначала сравнивается часть String, если она совпадает, тогда Number.

Т.е.в примере выше должно получиться
1.Apple
415.Apple
2.Banana is yellow
32.Cherry is the best
30432.Something something something

Требуется написать две программы:
1.Утилита для создания тестового файла заданного размера. Результатом работы должен быть текстовый файл описанного выше вида.Должно быть какое - то количество строк с одинаковой частью String.

2.Собственно сортировщик.Важный момент, файл может быть очень большой. Для тестирования будет использоваться размер ~100Gb.

Выполнение в данной программе:
1) Загружаем словарь, содержащий слова, которые будем использовать для создания большого файла.
Для этого жмем на кнопку Read dictionary, находим файл Adictionary.txt в папке Dictionary программы и выбираем его.
Видим сколько загрузилось слов. Можно, конечно и свой файл словаря создать.
2) Теперь можно нажать кнопку Create big file, предварительно задав параметры:
какое количество слов из словаря использовать Start and End words index;
какое количество слов будет в каждой строчке файла Words in record;
сколько строк будет в файле Records in file.
3) Создав файл, можем его теперь сортировать. Но сначала выставляем максимальный размер файла, который позволит сортировать ОЗУ Maximum size of file to sort in bytes.
А затем нажимаем кнопку Divide file и выбираем файл. Файл для сортировки надо поместить в пустую папку, так как программа автоматически будет создавать и удалять файлы в этой папке.
4) Как только процесс разбиение файла на мелкие файлы завершиться, переходим ко второму этапу - сортировке записей, нажав на кнопку Sort.
5) После завершения сортировки собираем все в один файл, нажав на кнопку Merge to file и указав название и папку, можно в этой же папке.
Сортировка выполнена.";
        }
    }
}
