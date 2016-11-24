using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.PanGu;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using LuceneDal;
using LuceneModel;
using Directory = Lucene.Net.Store.Directory;
using Version = Lucene.Net.Util.Version;

namespace LuceneAopTest
{
    public class LuceneEngine
    {
        private CancellationTokenSource CTS = null;
        private bool newFolder = true;
        public LuceneEngine(Type tableType)
        {
            newFolder = true;
            this.DataType =tableType;
            Count= GetCount();
            PageCount = GetPage();
        }


        protected Type DataType { get; set; }
        protected int PageSize => 1000;
        protected int PageCount { get; set; }
        protected int Count { get; set; }
        protected EfService Service=>new EfService();



        public bool Star()
        {
            List<Task> taskList = new List<Task>();
            TaskFactory taskFactory = new TaskFactory();
            try
            {
                for (int i = 1; i < PageCount; i++)
                {
                    var dataList = MeteDatainital(i).ToList();
                    taskList.Add(taskFactory.StartNew(()=>LuceneInital(i, dataList)));

                    newFolder = false;

                }
            }
            catch (Exception)
            {
                
                throw;
            }
           



            return true;
        }


        private IEnumerable<JdModel> MeteDatainital(int index)
        {
            return Service.SkipData<JdModel>(DataType, PageSize, index);
        }

        private int GetCount()
        {
           return Service.GetCount(DataType);
        }

        private int GetPage()
        {
            double result = Math.Ceiling(Count / 1000.0);
            return Convert.ToInt32(result);
        }



        private void LuceneInital(int i,List<JdModel> dataList)
        {
            string pathSuffix = DataType.Name;
            string rootIndexPath = AppDomain.CurrentDomain.BaseDirectory;
            string indexPath = string.IsNullOrWhiteSpace(pathSuffix) ? rootIndexPath : string.Format("{0}\\{1}", rootIndexPath, pathSuffix);
          
            //using (Directory ramDir = new RAMDirectory())
            GeneratLuceneIndex(indexPath, dataList);
        }

        private void GeneratLuceneIndex(string indexPath, List<JdModel> list)
        {
            using (Directory ramDir = FSDirectory.Open(new System.IO.DirectoryInfo(indexPath)))
            {
                using (IndexWriter iw = new IndexWriter(ramDir, new PanGuAnalyzer(), newFolder, IndexWriter.MaxFieldLength.LIMITED))
                {
                    try
                    {
                        iw.SetMaxBufferedDocs(100); //控制写入一个新的segent前内存中保存的doc的数量 默认10  
                        iw.MergeFactor = 100; //控制多个segment合并的频率，默认10
                        iw.UseCompoundFile = true; //创建符合文件 减少索引文件数量
                        //iw.TestPoint("abc");
                        foreach (var ci in list)
                        {
                            Document doc = new Document();

                            doc.Add(new Field("id", ci.Id.ToString(), Field.Store.NO, Field.Index.NOT_ANALYZED));
                            doc.Add(new Field("title", ci.Title, Field.Store.YES, Field.Index.ANALYZED)); //盘古分词
                            doc.Add(new Field("productid", ci.ProductId.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
                            doc.Add(new Field("categoryid", ci.CategoryId.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
                            doc.Add(new Field("imageurl", ci.ImageUrl, Field.Store.YES, Field.Index.NOT_ANALYZED));
                            doc.Add(new Field("url", ci.Url, Field.Store.YES, Field.Index.NOT_ANALYZED));
                            doc.Add(new NumericField("price", Field.Store.YES, true).SetFloatValue((float) ci.Price));
                            iw.AddDocument(doc);
                        }
                        list.ForEach(c => CreateCIIndex(iw, c));
                        //iw.Commit();
                    }
                    catch (Exception ex)
                    {
                        throw;
                    }
                    finally
                    {
                        iw.Close();
                    }
                }
            }
        }

        /// <summary>
        /// 创建索引
        /// </summary>
        /// <param name="analyzer"></param>
        /// <param name="title"></param>
        /// <param name="content"></param>
        private void CreateCIIndex(IndexWriter writer, JdModel ci)
        {
            try
            {
                writer.AddDocument(ParseCItoDoc(ci));
            }
            catch (Exception ex)
            {
                // logger.Error("CreateCIIndex异常", ex);
                throw ex;
            }
        }

        /// <summary>
        /// 将Commodity转换成doc
        /// </summary>
        /// <param name="ci"></param>
        /// <returns></returns>
        private Document ParseCItoDoc(JdModel ci)
        {
            Document doc = new Document();

            doc.Add(new Field("id", ci.Id.ToString(), Field.Store.NO, Field.Index.NOT_ANALYZED));
            doc.Add(new Field("title", ci.Title, Field.Store.YES, Field.Index.ANALYZED));//盘古分词
            doc.Add(new Field("productid", ci.ProductId.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
            doc.Add(new Field("categoryid", ci.CategoryId.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
            doc.Add(new Field("imageurl", ci.ImageUrl, Field.Store.YES, Field.Index.NOT_ANALYZED));
            doc.Add(new Field("url", ci.Url, Field.Store.YES, Field.Index.NOT_ANALYZED));
            doc.Add(new NumericField("price", Field.Store.YES, true).SetFloatValue((float)ci.Price));
            return doc;
        }

        /// <summary>
        /// 将索引合并到上级目录
        /// </summary>
        /// <param name="sourceDir">子文件夹名</param>
        public void MergeIndex(string[] childDirs)
        {
            Console.WriteLine("MergeIndex Start");
            IndexWriter writer = null;
            try
            {
                if (childDirs == null || childDirs.Length == 0) return;
                Analyzer analyzer = new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30);
                string rootPath = AppDomain.CurrentDomain.BaseDirectory;
                //DirectoryInfo dirInfo = Directory.CreateDirectory(rootPath);
                Directory directory = FSDirectory.Open(new System.IO.DirectoryInfo(rootPath));
                writer = new IndexWriter(directory, analyzer, true, IndexWriter.MaxFieldLength.LIMITED);//删除原有的
                Directory[] dirNo = childDirs.Select(dir => FSDirectory.Open(System.IO.Directory.CreateDirectory(string.Format("{0}\\{1}", rootPath, dir)))).ToArray();
                writer.MergeFactor = 100;//控制多个segment合并的频率，默认10
                writer.UseCompoundFile = true;//创建符合文件 减少索引文件数量
                writer.AddIndexesNoOptimize(dirNo);
            }
            finally
            {
                if (writer != null)
                {
                    writer.Optimize();
                    writer.Close();
                }
                Console.WriteLine("MergeIndex End");
            }
        }
    }
}
