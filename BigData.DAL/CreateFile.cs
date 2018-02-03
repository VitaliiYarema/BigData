using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigData.DAL
{
    public class CreateFile
    {
        private static List<string> _dictionary = new List<string>();
        private static Random random = new Random();
        public static int Index { get; set; } = 1;

        public static int ReadDictionary(string fileName)
        {
            _dictionary.Clear();
            using (StreamReader sr = new StreamReader(fileName))
            {
                while (sr.Peek() >= 0)
                {
                    _dictionary.Add(sr.ReadLine());
                }
            }
            return _dictionary.Count;
        }

        private static string CreateRandomSentence(int numberWords, int start, int end)
        {
            try
            {
                int number = random.Next(1, numberWords);
                List<string> words = new List<string>();
                for (int i = 0; i < number; i++)
                {
                    string s = GetRandomWord(start, end);
                    if(s == null)
                    {
                        break;
                    }
                    words.Add(GetRandomWord(start, end));
                }
                return words.Count > 0 ? string.Join(" ", words) : null;
            }
            catch
            {
                return null;
            }
        }

        private static string GetRandomWord(int start, int end)
        {
            try
            {
                int number = random.Next(start, end);
                return _dictionary[number];
            }
            catch
            {
                return null;
            }            
        }

        public static Record CreateRecord(int numberWords, int start, int end)
        {
            var line = CreateRandomSentence(numberWords, start, end);
            if(line == null)
            {
                return null;
            }
            return new Record() { Number = Index++, Line = line };
        }
    }
}
