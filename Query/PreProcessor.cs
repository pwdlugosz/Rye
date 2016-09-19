using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Query
{

    public abstract class PreProcessor
    {

        protected long _Clicks;

        public PreProcessor()
        {
        }

        public long Clicks
        {
            get { return this._Clicks; }
        }

        public abstract void Invoke();

    }

    public sealed class NullPreProcessor : PreProcessor
    {

        public NullPreProcessor()
            : base()
        {
        }

        public override void Invoke()
        {
            // do nothing
        }

    }

    public sealed class SortPreProcessor : PreProcessor
    {

        private ExpressionCollection _cols;
        private Register _memory;
        private TabularData _data;
        private Key _key;

        public SortPreProcessor(TabularData Data, ExpressionCollection Key, Register Memory)
            : base()
        {
            this._cols = Key;
            this._memory = Memory;
            this._data = Data;
        }

        public SortPreProcessor(TabularData Data, Key K)
            : base()
        {
            this._memory = new Register(Data.Header.Name, Data.Columns);
            this._cols = ExpressionCollection.Render(Data.Columns, Data.Header.Name, this._memory, K);
            this._data = Data;
        }

        public override void Invoke()
        {


            // Try to decompile to a key because it's faster //
            this._key = ExpressionCollection.DecompileToKey(this._cols);
            if (this._key.Count != this._cols.Count)
            {
                this._key = null;
            }

            // See if we're already using a key rather than an expression //
            if (this._key != null)
            {

                this.State = 2;
                
                // If already sorted, then do nothing //
                if (KeyComparer.IsWeakSubset(this._data.SortBy ?? new Key(), this._key))
                    return;
                
                if (this._data.Header.Affinity == HeaderType.Extent)
                {
                    this._Clicks += SortMaster.Sort(this._data as Extent, this._key);
                }
                else
                {
                    this._Clicks += SortMaster.Sort(this._data as Table, this._key);
                }

                return;

            }

            // Create a record comparer //
            RecordComparer rc = new ExpressionSortComparer(this._cols, this._memory);
            this.State = 1;

            // Sort //
            if (this._data.Header.Affinity == HeaderType.Extent)
            {
                this._Clicks += SortMaster.Sort(this._data as Extent, rc);
            }
            else
            {
                this._Clicks += SortMaster.Sort(this._data as Table, rc);
            }

        }

        /// <summary>
        /// 1 == sort by expression, 2 == sort by key
        /// </summary>
        public int State
        {
            get;
            private set;
        }

    }


}
