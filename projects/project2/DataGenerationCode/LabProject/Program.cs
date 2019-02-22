using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LabProject.DataDedup.DataGenerator
{
    class Program
    {
        static int totalRecordsRequired = 400000;

        static void Main(string[] args)
        {
            try
            {
                //Program.Run();
                Program.GenerateCommaSeperatedFileWithSkewedData(320000);
            }
            catch(Exception e)
            {
                Console.WriteLine("The program has stopped unexpectedly due to : " + e);
            }
        }

        static void Run(bool addDuplicates, int aproxDuplicatePercentage)
        { 
            Console.WriteLine("The program has started processing...");
            DataItem[] records = new DataItem[totalRecordsRequired];
            RandomRecordGenerator randomRecordGenerator = new RandomRecordGenerator();

            Random random = new Random(Guid.NewGuid().GetHashCode());

            for (int i = 0; i < totalRecordsRequired; i++)
            {
                if(addDuplicates)
                {
                    int temp = random.Next(0, 100);

                    if(temp / 2 > aproxDuplicatePercentage) //Generate new record
                    {
                        records[i] = randomRecordGenerator.GenerateRandomItem();
                    }
                    else  // duplicate a previous record
                    {
                        int randomlySelectedPreviousRecordIndex = random.Next(0, i - 1);
                        records[i] = records[randomlySelectedPreviousRecordIndex];
                    }
                } 
            }

            Console.WriteLine("Writing to XML file...");
            XmlUtility.WriteAsXMLFile<DataItem[]>(records);

            Console.WriteLine("The program has finished processing!");
        }

        static void GenerateCommaSeperatedFile()
        {
            Console.WriteLine("Started to generate comma seperated data...");
            RandomRecordGenerator randomRecordGenerator = new RandomRecordGenerator();

            using (System.IO.StreamWriter file =
            new System.IO.StreamWriter(@"E:\tmp\Spark_Workspace\CommaSeperatedData.txt"))
            {
                for (int i = 0; i < totalRecordsRequired; i++)
                {
                    file.WriteLine(randomRecordGenerator.GenerateRandomItem().ToString());
                }
            }

            Console.WriteLine("Finished creating comma seperated data!");
        }
        
        static void GenerateCommaSeperatedFileWithDuplicates(bool addDuplicates, int aproxDuplicatePercentage)
        {
            Console.WriteLine("The program has started processing...");
            DataItem[] records = new DataItem[totalRecordsRequired];
            RandomRecordGenerator randomRecordGenerator = new RandomRecordGenerator();

            Random random = new Random(Guid.NewGuid().GetHashCode());

            for (int i = 0; i < totalRecordsRequired; i++)
            {
                if (addDuplicates)
                {
                    int temp = random.Next(0, 100);

                    if (temp > aproxDuplicatePercentage) //Generate new record
                    {
                        records[i] = randomRecordGenerator.GenerateRandomItem();
                    }
                    else  // duplicate a previous record
                    {
                        int randomlySelectedPreviousRecordIndex = random.Next(0, i - 1);
                        records[i] = records[randomlySelectedPreviousRecordIndex];
                    }
                }
            }

            Console.WriteLine("Writing to file...");
           
            string text = @"E:\tmp\Spark_Workspace\CommaSeperatedData.txt";
            using (TextWriter writer = File.CreateText(text))
            {
                foreach (var record in records)
                {
                    writer.WriteLine(record);
                }
            }
        }

        static void GenerateCommaSeperatedFileWithSkewedData(int repeatTimes)
        {
            Console.WriteLine("The program has started processing...");
            DataItem[] records = new DataItem[totalRecordsRequired];
            RandomRecordGenerator randomRecordGenerator = new RandomRecordGenerator();

            Random random = new Random(Guid.NewGuid().GetHashCode());

            for (int i = 0; i < totalRecordsRequired; i++)
            {
                if (totalRecordsRequired - repeatTimes > i)
                {
                    records[i] = randomRecordGenerator.GenerateRandomItem();
                }
                else  // duplicate a previous record
                {
                    records[i] = records[i - 1];
                }
            }

            Console.WriteLine("Writing to file...");

            string text = @"E:\tmp\Spark_Workspace\CommaSeperatedData.txt";
            using (TextWriter writer = File.CreateText(text))
            {
                foreach (var record in records)
                {
                    writer.WriteLine(record);
                }
            }
        }
    }
}
