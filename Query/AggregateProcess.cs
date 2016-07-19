using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Aggregates;

namespace Rye.Query
{
    
    public sealed class AggregateProcessNode : QueryNode
    {

        private Volume _data;
        private List<Header> _FlushedExtents;
        private KeyValueSet _CurrentGrouper;
        private ExpressionCollection _By;
        private AggregateCollection _Over;
        private Filter _Where;
        private Register _MemoryLocation;
        private long _clicks = 0;

        public AggregateProcessNode(int ThreadID, Volume Data, ExpressionCollection Keys, AggregateCollection Values, Filter Where, Register Memory)
            : base(ThreadID)
        {

            this._data = Data;
            this._FlushedExtents = new List<Header>();
            this._CurrentGrouper = new KeyValueSet(Keys, Values);
            this._By = Keys;
            this._Over = Values;
            this._Where = Where;
            this._MemoryLocation = Memory;
        }

        public List<Header> Headers
        {
            get
            {
                return this._FlushedExtents;
            }
        }

        public override void Invoke()
        {

            // go through each extent //
            foreach (Extent e in this._data.Extents)
            {

                // go through each record //
                for (int i = 0; i < e.Count; i++)
                {

                    // assign register //
                    this._MemoryLocation.Value = e[i];

                    // check our condition //
                    if (this._Where.Render())
                    {

                        // The current record is a candidate for grouping; before the KeyValueSet accepts a new record, we need to check if it's full //
                        if (this._CurrentGrouper.IsFull)
                        {

                            // It's full:
                            // (1) convert to a record set
                            // (2) dump it, but save the header
                            // (3) build a new KeyValueSet
                            Header TempHeader = KeyValueSet.Save(Kernel.TempDirectory, this._CurrentGrouper);
                            this._FlushedExtents.Add(TempHeader);
                            this._clicks += this._CurrentGrouper.BaseComparer.Clicks;
                            this._CurrentGrouper = new KeyValueSet(this._By, this._Over);

                        }

                        // Append the KeyValueSet //
                        this._CurrentGrouper.Accumulate();

                    }
                    

                }

            }

            this._clicks += this._CurrentGrouper.BaseComparer.Clicks;
                       
        }

        public override void EndInvoke()
        {
            
            // Here, we need to take our current grouper and flush it, then add to the collection of flushed groupers //
            Header TempHeader = KeyValueSet.Save(Kernel.TempDirectory, this._CurrentGrouper);
            this._FlushedExtents.Add(TempHeader);
            this._CurrentGrouper = null;

        }

        public long Clicks
        {
            get { return this._clicks; }
        }

    }

    public sealed class AggregateConsolidationProcess : QueryConsolidation<AggregateProcessNode>
    {

        /* This is the most complicated consolidator, but it could be even more complicated (and may be in the future if we need to squeeze more performance out)
         * 
         * The last step of the query node process is to 'flush' the current grouper and append it to the extent list. Realistically, we don't need this Kernel call if the
         * process could fit everything in one grouper per a node. Even if it didn't, we could still go through and try to consolidate all in memory groupers. To simplify the
         * algorithm, the model just flushes everything so we can treat all groupers the same. Realistically, the Kernel should be caching most of these so we don't have to 
         * worry about excessive hard reads/writes.
         * 
         * Step one is to combine all headers in one list, step two is to go through and compare each header, then step three is write everything to the output stream
         * 
         * */

        private List<Header> _RawHeaders;
        private RecordWriter _output;
        private ExpressionCollection _By;
        private AggregateCollection _Over;
        private Register _MemoryLocation;
        private ExpressionCollection _Select;
        private long _Clicks = 0;

        public AggregateConsolidationProcess(ExpressionCollection Keys, AggregateCollection Values, RecordWriter Output, ExpressionCollection Select, Register Memory)
            : base()
        {

            this._RawHeaders = new List<Header>();
            this._output = Output;
            this._By = Keys;
            this._Over = Values;
            this._Select = Select;
            this._MemoryLocation = Memory;

        }

        public override void Consolidate(List<AggregateProcessNode> Nodes)
        {
            
            // Combine all headers //
            this._RawHeaders.Clear();
            foreach (AggregateProcessNode q in Nodes)
            {
                this._RawHeaders.AddRange(q.Headers);
                this._Clicks += q.Clicks;
            }

            // Go through an merge groupers accross all headers //
            for (int i = 0; i < this._RawHeaders.Count; i++)
            {

                // Open the left table //
                KeyValueSet x = KeyValueSet.Open(this._RawHeaders[i], this._By, this._Over);

                // Cross match each grouper //
                for (int j = i + 1; j < this._RawHeaders.Count; j++)
                {

                    // Get the right side of the comparison //
                    KeyValueSet y = KeyValueSet.Open(this._RawHeaders[j], this._By, this._Over);

                    // Update x; entry R is in x and y, R in x will be updated while R in y will be removed; if R is not in both, nothing happens
                    if (y.Count != 0)
                        KeyValueSet.Union(x, y);

                    // Dump the header //
                    KeyValueSet.Save(this._RawHeaders[j], y);

                }

                // Output 'x' //
                x.WriteToFinal(this._output, this._Select, this._MemoryLocation);

            }

            // Close the stream //
            this._output.Close();

        }

        public long Clicks
        {
            get { return this._Clicks; }
        }

    }

    internal sealed class KeyValueSet
    {

        private ExpressionCollection _Maps;
        private AggregateCollection _Reducers;
        private Dictionary<Record, CompoundRecord> _cache = new Dictionary<Record, CompoundRecord>();
        private long _Capacity = Extent.DEFAULT_MAX_RECORD_COUNT;
        private RecordComparer _BaseComparer;

        // Constructor //
        public KeyValueSet(ExpressionCollection Fields, AggregateCollection Aggregates)
        {
            this._Maps = Fields;
            this._Reducers = Aggregates;
            this._BaseComparer = new RecordComparer();
            this._cache = new Dictionary<Record, CompoundRecord>(this._BaseComparer);
        }

        // Properties //
        public int Count
        {
            get { return this._cache.Count; }
        }

        public bool IsEmpty
        {
            get { return this._cache.Count == 0; }
        }

        public bool IsFull
        {
            get { return this.Count >= this._Capacity; }
        }

        public long Capacity
        {
            get { return this._Capacity; }
            set { this._Capacity = value; }
        }

        public ExpressionCollection BaseMappers
        {
            get { return this._Maps; }
        }

        public AggregateCollection BaseReducers
        {
            get { return this._Reducers; }
        }

        public Schema OutputSchema
        {
            get { return Schema.Join(BaseMappers.Columns, BaseReducers.Columns); }
        }

        public RecordComparer BaseComparer
        {
            get { return this._BaseComparer; }
        }

        // Methods //
        public void Accumulate()
        {

            // Check to see if it exists //
            Record r = this._Maps.Evaluate();
            CompoundRecord cr;
            bool b = this._cache.TryGetValue(r, out cr);

            // If exists, then accumulate //
            if (b == true)
            {
                this._Reducers.Accumulate(cr);
            }
            else
            {
                cr = this._Reducers.Initialize();
                this._Reducers.Accumulate(cr);
                this._cache.Add(r, cr);
            }

        }

        /// <summary>
        /// Returns true if it added a new reocrd, false if updated a current record
        /// </summary>
        /// <param name="R"></param>
        /// <param name="CM"></param>
        /// <returns></returns>
        public bool Merge(Record R, CompoundRecord CM)
        {

            // Check to see if it exists //
            CompoundRecord cr;
            bool b = this._cache.TryGetValue(R, out cr);

            // If exists, then merge //
            if (b == true)
            {
                this._Reducers.Merge(CM, cr);
            }
            else
            {
                this._cache.Add(R, CM);
            }
            return !b;

        }

        public void Remove(Record R)
        {
            this._cache.Remove(R);
        }

        // To Methods //
        public void WriteToFinal(RecordWriter Writter, ExpressionCollection Fields, Register MemoryLocation)
        {

            if (Writter.Columns != Fields.Columns)
                throw new ArgumentException("Base stream and output schema are different");

            // Load //
            foreach (KeyValuePair<Record, CompoundRecord> t in this._cache)
            {

                // Assign the value to the register //
                MemoryLocation.Value = Record.Join(t.Key, this._Reducers.Evaluate(t.Value));

                // Evaluate the record //
                Record r = Fields.Evaluate();

                // Write //
                Writter.Insert(r);

            }

        }

        public void WriteToFinal(RecordWriter Writter)
        {
            Schema s = Schema.Join(this._Maps.Columns, this._Reducers.Columns);
            ExpressionCollection leafs = ExpressionCollection.Render(s, "OUT");
            Register mem = leafs.GetMemoryRegister("OUT");
            this.WriteToFinal(Writter, leafs, mem);
        }

        public Extent ToInterim()
        {

            // Get schema //
            Schema s = Schema.Join(this._Maps.Columns, this._Reducers.GetInterimSchema);

            // Build the table //
            Extent e = new Extent(s); ;

            // Load //
            foreach (KeyValuePair<Record, CompoundRecord> t in this._cache)
            {
                Record r = Record.Join(t.Key, t.Value.ToRecord());
                e.Add(r);
            }

            return e;

        }

        public void FromInterim(Extent InterimData)
        {

            int MapperCount = this.BaseMappers.Count;
            int[] Signiture = this.BaseReducers.Signiture;
            int TotalCellCount = MapperCount + Signiture.Sum();

            // Check that this is the correct size //
            if (InterimData.Columns.Count != TotalCellCount)
                throw new ArgumentException(string.Format("RecordSet passed [{0}] has few columns than required by deserializer [{1}]", InterimData.Columns.Count, TotalCellCount));

            // Import the data //
            for (int i = 0; i < InterimData.Count; i++)
            {

                // Build map key //
                RecordBuilder KeyBuilder = new RecordBuilder();
                for (int j = 0; j < MapperCount; j++)
                {
                    KeyBuilder.Add(InterimData[i][j]);
                }

                // Build compound record //
                RecordBuilder ValueBuilder = new RecordBuilder();
                for (int j = MapperCount; j < TotalCellCount; j++)
                {
                    ValueBuilder.Add(InterimData[i][j]);
                }

                // Accumulate to dictionary //
                this._cache.Add(KeyBuilder.ToRecord(), CompoundRecord.FromRecord(ValueBuilder.ToRecord(), Signiture));

            }

        }

        // Statics //
        /// <summary>
        /// Takes all the records from T2 and merges into T1; if the record exists in T1, then:
        /// -- T1's record is updated
        /// -- T2's record is deleted
        /// Otherwise, if the record does not exist in T1, nothing happens
        /// </summary>
        /// <param name="T1"></param>
        /// <param name="T2"></param>
        public static void Union(KeyValueSet T1, KeyValueSet T2)
        {

            List<Record> Deletes = new List<Record>();

            // Merge and tag deletes //
            foreach (KeyValuePair<Record, CompoundRecord> t in T2._cache)
            {
                if (T1._cache.ContainsKey(t.Key))
                {
                    T1.Merge(t.Key, t.Value);
                    Deletes.Add(t.Key);
                }
            }

            // Clear deletes //
            foreach (Record r in Deletes)
                T2._cache.Remove(r);

        }

        public static Header Save(string Dir, KeyValueSet Data)
        {
            Extent e = Data.ToInterim();
            Header h = Header.NewExtentHeader(Dir, Header.TempName(), 0, e.Columns.Count, e.MaxRecords);
            e.Header = h;
            Kernel.RequestFlushExtent(e);
            return h;
        }

        public static void Save(Header H, KeyValueSet Data)
        {
            Extent e = Data.ToInterim();
            e.Header = H;
            Kernel.RequestFlushExtent(e);
        }

        public static KeyValueSet Open(Header h, ExpressionCollection Fields, AggregateCollection Aggregates)
        {
            Extent e = Kernel.RequestBufferExtent(h.Path);
            KeyValueSet kvs = new KeyValueSet(Fields, Aggregates);
            kvs.FromInterim(e);
            return kvs;
        }

    }


}
