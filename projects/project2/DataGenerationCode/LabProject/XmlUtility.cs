using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace LabProject.DataDedup.DataGenerator
{
    /// <summary>
    /// Class for handling XML
    /// </summary>
    public static class XmlUtility
    {
        /// <summary>
        /// Writes the data to xml file
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="data"></param>
        public static void WriteAsXMLFile<T>(T data)
        {
            if (data == null)
                return;


            XmlSerializer serializer = new XmlSerializer(typeof(T));

            using (System.IO.StreamWriter file =
               new System.IO.StreamWriter(@"E:\tmp\Spark_Workspace\data_file1.xml"))
            {
                serializer.Serialize(file, data);
            }
        }
    }
}
