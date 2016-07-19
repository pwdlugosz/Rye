using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.MatrixExpressions;
using Rye.Methods;
using Rye.Structures;

namespace Rye.Query
{

    public sealed class SelectProcessNode : QueryNode
    {

        public const string KEY_CHANGE = "KEY_CHANGE";
        public const string ROW_ID = "ROW_ID";
        public const string EXTENT_ID = "EXTENT_ID";
        public const string IS_FIRST = "IS_FIRST";
        public const string IS_LAST = "IS_LAST";

        private Volume _data;

        private Register _main;
        private Register _peek;
        private MemoryStructure _local;
        private MethodCollection _map;
        private MethodCollection _reduce;

        private Filter _where;
        private ExpressionCollection _key1;
        private ExpressionCollection _key2;

        private int _rowIDptr = 0;
        private int _extentIDptr = 0;
        private int _kcptr = 0;
        private int _isFirstptr = 0;
        private int _isLastptr = 0;
        private bool _hasKC = false;

        public SelectProcessNode(int ThreadID, Volume Data, Register Main, MemoryStructure LocalHeap, MethodCollection MapActions, MethodCollection ReduceActions, 
            Filter Where, ExpressionCollection KeyChange)
            : base(ThreadID)
        {


            this._data = Data;

            this._main = Main;
            this._local = LocalHeap;
            this._map = MapActions;
            this._reduce = ReduceActions;
            this._where = Where;

            // Handle the key change //
            if (KeyChange == null)
            {
                this._hasKC = false;
            }
            else
            {
                this._key1 = KeyChange;
                this._peek = new Register("PEEK", this._data.Columns);
                this._key2 = this._key1.CloneOfMe();
                this._key2.AssignMemoryRegister(this._key1.GetMemoryRegisters()[0].UID, this._peek);
                this._hasKC = true;
            }

        }

        public override void BeginInvoke()
        {
            
            // Get the pointers //
            this._rowIDptr = this._local.Scalars.GetPointer(ROW_ID);
            this._extentIDptr = this._local.Scalars.GetPointer(EXTENT_ID);
            this._kcptr = this._local.Scalars.GetPointer(KEY_CHANGE);
            this._isFirstptr = this._local.Scalars.GetPointer(IS_FIRST);
            this._isLastptr = this._local.Scalars.GetPointer(IS_LAST);
            this._map.BeginInvoke();

        }

        public override void EndInvoke()
        {
            this._map.EndInvoke();
        }

        public MethodCollection Reducer
        {
            get
            {
                return this._reduce;
            }
        }

        public override void Invoke()
        {

            Console.WriteLine("{0} thread ID", System.Threading.Thread.CurrentThread.ManagedThreadId);

            // Open a stream to read data //
            RecordReader stream = this._data.OpenReader(this._main, this._where);

            // Open a record comparer for the key change //
            RecordComparer rc = new RecordComparer();

            // Create a bool trip flag for the first record //
            bool first = true;

            // Traverse the stream //
            while (!stream.EndOfData)
            {

                // Set up the main values //
                this._main.Value = stream.ReadNext();

                // Conditionally set the lag value //
                if (this._hasKC)
                {
                    if (!stream.EndOfData)
                        this._peek.Value = stream.Read();
                    else
                        this._peek.Value = this._peek.Columns.NullRecord;
                }

                // Set the key variables //
                this._local.Scalars[this._extentIDptr] = new Cell(stream.SetID);
                this._local.Scalars[this._rowIDptr]++;
                this._local.Scalars[this._isFirstptr] = (first ? Cell.TRUE : Cell.FALSE);
                this._local.Scalars[this._isLastptr] = (stream.EndOfData ? Cell.TRUE : Cell.FALSE);
                if (first) 
                    first = false;
                if (this._hasKC)
                {
                    bool b = !rc.Equals(this._key1.Evaluate(), this._key2.Evaluate());
                    this._local.Scalars[this._kcptr] = new Cell(b);
                }

                // Perform our actions //
                this._map.Invoke();

                // Check if we have a read break //
                if (this._map.CheckBreak)
                {
                    return;
                }

            }

        }

    }

    public sealed class SelectProcessConsolidation : QueryConsolidation<SelectProcessNode>
    {

        public override void Consolidate(List<SelectProcessNode> Nodes)
        {
            foreach (SelectProcessNode n in Nodes)
            {
                n.Reducer.BeginInvoke();
                n.Reducer.Invoke();
                n.Reducer.EndInvoke();
            }
        }

    }

}
