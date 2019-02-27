using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LabProject.DataDedup.DataGenerator
{
    /// <summary>
    /// Generates a random DataItem which can be called a tuple.
    /// </summary>
    public class RandomRecordGenerator
    {
        private Random random = new Random(Guid.NewGuid().GetHashCode());

        /// <summary>
        /// Generates a randomly filled record
        /// </summary>
        /// <returns></returns>
        public DataItem GenerateRandomItem()
        {
            return new DataItem
            {
                Title = GenerateRandomString(13, 0),
                Authors = GenerateMultipleAuthors(random.Next(1, 5), 7),
                DOI = GenerateRandomString(14, 5),
                Issn = GenerateRandomString(12, 6),
                Journal = GenerateRandomString(15, 2),
                Month = random.Next(1, 12),
                Year = random.Next(1857, 2019),
                Pages = random.Next(5, 10000),
                NumPages = random.Next(2, 35),
                Url = GenerateRandomString(20, 1),
                Publisher = GenerateRandomString(13, 0),
                Volume = random.Next(1, 35),
                Acmid = random.Next(100000, 999999)
            };
        }

        /// <summary>
        /// Generate multiple names
        /// </summary>
        /// <param name="totalNames"></param>
        /// <param name="lengthOfName"></param>
        /// <returns></returns>
        private Person[] GenerateMultipleAuthors(int totalNames, int lengthOfName)
        {
            Person[] randomNames = new Person[totalNames];

            for (int i = 0; i < totalNames; i++)
            {
                randomNames[i] = new Person { FirstName = GenerateRandomString(lengthOfName, 2), LastName = GenerateRandomString(lengthOfName, 2) };
            }

            return randomNames;
        }

        /// <summary>
        /// Generate Random String with given length
        /// </summary>
        /// <param name="length"></param>
        /// <returns></returns>
        private String GenerateRandomString(int length, int stringType)
        {
            string compositionElements;

            switch(stringType)
            {
                case 0: //For random string with alphabets, numbers, whitespaces and symbols
                    compositionElements = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-/ ";
                    break;
                case 1: // For random string
                    compositionElements = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
                    break;
                case 2: //For names mostly
                    compositionElements = "ABCDEFGHIJKLMNOPQRSTUVWXYZ.-  ";
                    break;
                case 3: //For random string with alphabets, numbers and symbols
                    compositionElements = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-/";
                    break;
                case 4: // For alphabets and whitespaces only
                    compositionElements = "ABCDEFGHIJKLMNOPQRSTUVWXYZ ";
                    break;
                case 5: // For doi
                    compositionElements = "0123456789./";
                    break;
                case 6: // For ISSN
                    compositionElements = "0123456789-";
                    break;
                default:
                    throw new Exception("While generating random string, type of string specified is incorrect.");
            }

            //return new string(Enumerable.Repeat(compositionElements, length)
              //  .Select(str => str[random.Next(str.Length)]).ToArray());
            return new string(Enumerable.Range(1, length).Select(_ => compositionElements[random.Next(compositionElements.Length)]).ToArray());
        }
    }
}
