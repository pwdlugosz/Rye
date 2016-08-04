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

        private ExpressionCollection _keys;
        private Register _memory;
        private DataSet _data;

        public SortPreProcessor(DataSet Data, ExpressionCollection Key, Register Memory)
            : base()
        {
            this._keys = Key;
            this._memory = Memory;
            this._data = Data;
        }

        public SortPreProcessor(DataSet Data, Key K)
            : base()
        {
            this._memory = new Register(Data.Name, Data.Columns);
            this._keys = ExpressionCollection.Render(Data.Columns, Data.Name, this._memory, K);
            this._data = Data;
        }

        public override void Invoke()
        {
            
            // Create a record comparer //
            RecordComparer rc = new ExpressionSortComparer(this._keys, this._memory);
            this.State = 1;

            // Try to decompile to a key because it's faster //
            Key k = ExpressionCollection.DecompileToKey(this._keys);
            if (k.Count == this._keys.Count)
            {
                rc = new KeyedRecordComparer(k);
                this.State = 2;
            }

            // Sort //
            if (this._data.Header.Affinity == HeaderType.Extent)
            {
                SortMaster.Sort(this._data as Extent, rc);
            }
            else
            {
                SortMaster.Sort(this._data as Table, rc);
            }

            this._Clicks += rc.Clicks;

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
