using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigData.DAL
{
    public class Record
    {
        public int Number { get; set; }
        public string Line { get; set; }
        public string GetString { get { return Number + ". " + Line; } }
    }
}
