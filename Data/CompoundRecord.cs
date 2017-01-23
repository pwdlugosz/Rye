using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data
{

    /// <summary>
    /// !!!! This class is not inteded to be used outside of the aggregator framework
    /// Represents an array of records
    /// </summary>
    public sealed class CompoundRecord
    {

        private Record[] _cache;

        // Constructor //
        /// <summary>
        /// Initializes a class with a predefined size
        /// </summary>
        /// <param name="PageSize">The size of compound record</param>
        public CompoundRecord(int Size)
        {
            this._cache = new Record[Size];
        }

        // Properties //
        /// <summary>
        /// Gets or sets a record at a specificed index
        /// </summary>
        /// <param name="Index">The index of the compound record</param>
        /// <returns>A normal horse record</returns>
        public Record this[int Index]
        {
            get
            {
                return this._cache[Index];
            }
            set
            {
                this._cache[Index] = value;
            }
        }

        /// <summary>
        /// The size of the internal record array
        /// </summary>
        public int Count
        {
            get
            {
                return this._cache.Length;
            }
        }

        // Methods //
        /// <summary>
        /// Converts the compound record to a record. The method stiches together each record in the cache into a fat record
        /// </summary>
        /// <returns></returns>
        public Record ToRecord()
        {

            if (this.Count == 0) return null;
            if (this.Count == 1) return this._cache[0];
            Record r = this[0];

            for (int i = 1; i < this.Count; i++)
            {
                r = Record.Join(r, this[i]);
            }

            return r;

        }

        // Statics //
        /// <summary>
        /// Creates a compound record from a fat record; this is essentially the inverse of ToRecord
        /// </summary>
        /// <param name="R">A fat record to be transformed</param>
        /// <param name="Sig">An array of index values that define the size of a sub-element of the compound record</param>
        /// <returns>A compound record</returns>
        public static CompoundRecord FromRecord(Record R, int[] Sig)
        {

            CompoundRecord cr = new CompoundRecord(Sig.Length);
            int k = 0;

            for (int i = 0; i < Sig.Length; i++)
            {

                List<Cell> c = new List<Cell>();
                for (int j = 0; j < Sig[i]; j++)
                {
                    c.Add(R[k]);
                    k++;
                }
                cr[i] = new Record(c.ToArray());

            }

            return cr;

        }

    }

}
