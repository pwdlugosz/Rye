using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Methods
{


    // Note: cannot be called from RYE script //
    public sealed class MethodSort : Method
    {

        private TabularData _data;
        private ExpressionCollection _values;
        private Key _key;
        private Register _reg;
        private int _State = 0; // 0 == dummy (do nothing), 1 == keyed, 2 == expression based
        private long _Clicks = 0;

        public MethodSort(Method Parent, TabularData Data, ExpressionCollection Values, Register Memory, Key SortState)
            : base(Parent)
        {

            // Hand the empty case //
            if (Data == null || Values == null || Memory == null || SortState == null)
                return;

            // Set the data //
            this._data = Data;
            
            // Try to optimize the sort //
            Key k = ExpressionCollection.DecompileToKey(Values);
            if (k.Count == Values.Count)
            {

                for (int i = 0; i < k.Count; i++)
                {
                    k.SetAffinity(i, SortState.Affinity(i));
                }

                this._key = k;
                this._State = 1;

            }
            else
            {

                // Set teh values //
                this._values = Values;
                this._key = SortState;
                this._reg = Memory;
                this._State = 2;

            }

        }

        public MethodSort(Method Parent, TabularData Data, Key Values)
            :base(Parent)
        {
            this._data = Data;
            this._key = Values;
            this._State = 1;
        }

        public int State
        {
            get { return this._State; }
        }

        public long Clicks
        {
            get { return this._Clicks; }
        }

        public override void Invoke()
        {

            // State == 0, do nothing //
            if (this._State == 0)
                return;

            // State == 1, sort by keys //
            if (this._State == 1)
            {

                if (this._data is Table)
                {
                    this._Clicks = SortMaster.Sort(this._data as Table, this._key);
                }
                else
                {
                    this._Clicks = SortMaster.Sort(this._data as Extent, this._key);
                }

                return;

            }

            // State == 2, sort by expressions //
            if (this._data is Table)
            {
                this._Clicks = SortMaster.Sort(this._data as Table, this._values, this._reg, this._key);
            }
            else
            {
                this._Clicks = SortMaster.Sort(this._data as Extent, this._values, this._reg, this._key);
            }

        }

        public override Method CloneOfMe()
        {
            return new MethodSort(this.Parent, this._data, this._values, this._reg, this._key);
        }

        public static MethodSort Empty
        {
            get
            {
                MethodSort x = new MethodSort(null, null, null, null, null);
                x._State = 0;
                return x;
            }
        }

    }


}
