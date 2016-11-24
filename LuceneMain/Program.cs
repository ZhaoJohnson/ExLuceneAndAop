using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LuceneAopTest;
using LuceneDal;

namespace LuceneMain
{
    class Program
    {
        static void Main(string[] args)
        {
            List<string> PathSuffixList = new List<string>();
            List<Type> typeList = new List<Type>()
            {
                typeof (JD_Commodity_001)
            };
            int count = 0;
            foreach (Type type in typeList)
            {
                count++;
                PathSuffixList.Add(type.Name);
                LuceneEngine engine = new LuceneEngine(type);
                engine.Star();
                if (count==typeList.Count)
                {
                    engine.MergeIndex(PathSuffixList.ToArray());
                }
            }

        }

    }
}
