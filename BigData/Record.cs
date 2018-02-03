using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigData
{
    public class Record
    {
        public int Number { get; set; }
        public string Line { get; set; }
        public string GetLine { get { return Number + ". " + Line; } }        
    }
}
