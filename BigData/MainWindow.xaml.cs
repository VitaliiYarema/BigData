using BigData.DAL;
using Microsoft.Win32;
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


                ///// Dividing
                var strSize = TextBoxSize.Text.Trim();
                if (string.IsNullOrEmpty(strSize))
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
                        var message = await BigFileRepository.DivideFile();
                        TextBoxInfo.Text = "Dividing finished";
                    }
                }
            }
        }
        
        private void ButtonSort_Click(object sender, RoutedEventArgs e)
        {
            TextBoxInfo.Text = "Sort started";
            var result = BigFileRepository.SortAllFiles();
            if(result)
            {
                TextBoxInfo.Text = "Sort finished";
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
                var start = int.Parse(TextBoxStart.Text.Trim());
                var end = int.Parse(TextBoxEnd.Text.Trim());
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
    }
}
