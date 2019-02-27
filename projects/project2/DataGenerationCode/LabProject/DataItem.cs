using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace LabProject.DataDedup.DataGenerator
{
    /// <summary>
    /// Class for holding a tuple/record
    /// </summary>
    [Serializable]
    public class DataItem
    {
        public Person[] Authors { get; set; }
        
        public String Title { get; set; }
        
        public String Journal { get; set; }
        
        public int Year { get; set; }
        
        public int Month { get; set; }
        
        public String Issn { get; set; }
        
        public int Volume { get; set; }
        
        public int Pages { get; set; }

        public int NumPages { get; set; }

        public String Url { get; set; }
        
        public String DOI { get; set; }
        
        public int Acmid { get; set; }
        
        public String Publisher { get; set; }

        public override string ToString()
        {
            String str = Year+",";
            str += Month + ",";
            str += Title + ",";

            foreach (var author in Authors)
            {
                str += author+",";
            }

            str += Journal + ",";
            str += Issn + ",";
            str += Volume + ",";
            str += Pages + ",";
            str += NumPages + ",";
            str += Url + ",";
            str += DOI + ",";
            str += Acmid + ",";
            str += Publisher;

            return str;
        }
    }
}
