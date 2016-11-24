using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LuceneDal
{
    public class EfService : StockEntityFramework<JDWarmEntities>
    {
        //public virtual Tclass Add(Tclass t)
        //{
        //    Console.WriteLine(Thread.CurrentThread.ManagedThreadId + "当前ID:" + typeof(Tclass).Name);
        //    return ExecEntityJdData(ef => ef.Set<Tclass>().Add(t), true);
        //}

        //public virtual Tclass QuerySingle(object objectKey)
        //{
        //    return ExecEntityJdData(ef => ef.Set<Tclass>().Find(objectKey));
        //}

        //public virtual Tclass AddorUpdate(Tclass T)
        //{
        //    return ExecEntityJdData(ef =>
        //    {
        //        ef.Set<Tclass>().AddOrUpdate(T);
        //        return ef.Set<Tclass>().Find(T);
        //    }, true);
        //}

        //public virtual IList<Tclass> SkipTable(int pageSize, int pageIndex)
        //{
        //    return ExecEntityJdData(ef =>
        //   {
        //       return ef.Set<Tclass>().Skip((pageIndex - 1) * pageSize).Take(pageSize).ToList();
        //   });
        //}

        //public virtual IList<Tclass> Test()
        //{
        //    return ExecEntityJdData(ef =>
        //    {
        //        return ef.Set<Tclass>().SqlQuery(string.Format("select top 10 * from {0}", typeof(Tclass).Name), null).ToList();
        //    });
        //}

        public IEnumerable<T> readTest<T>(Type t)
        {
            return ExecEntityJdData(ef =>
            {
                //bject createdObj= Activator.CreateInstance(T);
                string strsql = string.Format("select top 10 * from {0}", t.Name);
                return ef.Database.SqlQuery<T>(strsql, new object[] { null }).ToList();
            });
        }

        public int GetCount(Type t)
        {
            return ExecEntityJdData(ef =>
            {
                string strsql = string.Format("select count(*) from {0}", t.Name);
                return ef.Database.SqlQuery<int>(strsql, new object[] { null }).FirstOrDefault();
            });
        }

        public IEnumerable<T> SkipData<T>(Type t, int pageSize, int pageIndex)
        {
            return ExecEntityJdData(ef =>
            {
                string strsql = string.Format("SELECT top {2} * FROM {0} WHERE id>{1};", t.Name, pageSize * Math.Max(0, pageIndex - 1), pageSize);
                return ef.Database.SqlQuery<T>(strsql, new object[] { null }).ToList();
            });
        }
        /// <summary>
        /// Need sql 2012 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="t"></param>
        /// <param name="pageSize"></param>
        /// <param name="pageIndex"></param>
        /// <returns></returns>
        public IEnumerable<T> OffSetData<T>(Type t, int pageSize, int pageIndex)
        {
            return ExecEntityJdData(ef =>
            {
                string strsql = string.Format("SELECT * FROM {0} ORDER BY Id OFFSET ({1} -1) * {2} ROWS FETCH NEXT {2} ROWS ONLY; ;", t.Name, pageIndex, pageSize);
                return ef.Database.SqlQuery<T>(strsql, new object[] { null }).ToList();
            });
        }
    }
}
