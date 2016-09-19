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
    
    /// <summary>
    /// Aggregates a table using a hash table
    /// </summary>
    public sealed class AggregateHashTableProcessNode : QueryNode
    {

        private Volume _data;
        private KeyValueSet _CurrentGrouper;
        private ExpressionCollection _By;
        private AggregateCollection _Over;
        private Filter _Where;
        private Register _MemoryLocation;
        private long _clicks = 0;
        private Table _Sink;

        public AggregateHashTableProcessNode(int ThreadID, Session Session, Volume Data, ExpressionCollection Keys, AggregateCollection Values, 
            Filter Where, Register Memory, Table Sink)
            : base(ThreadID, Session)
        {

            this._data = Data;
            this._CurrentGrouper = new KeyValueSet(Keys, Values);
            this._By = Keys;
            this._Over = Values;
            this._Where = Where;
            this._MemoryLocation = Memory;
            this._Sink = Sink;

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
                            KeyValueSet.Save(this._Sink, this._CurrentGrouper);
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
            KeyValueSet.Save(this._Sink, this._CurrentGrouper);
            this._CurrentGrouper = null;

        }

        public long Clicks
        {
            get { return this._clicks; }
        }

        public Table Sink
        {
            get { return this._Sink; }
            set { this._Sink = value; }
        }

    }

    /// <summary>
    /// Consolidates an aggregate hash table node
    /// </summary>
    public sealed class AggregateHashTableConsolidationProcess : QueryConsolidation<AggregateHashTableProcessNode>
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

        private List<long> _IDs;
        private RecordWriter _output;
        private ExpressionCollection _By;
        private AggregateCollection _Over;
        private Register _MemoryLocation;
        private ExpressionCollection _Select;
        private long _Clicks = 0;
        private Table _Sink;

        public AggregateHashTableConsolidationProcess(Session Session, ExpressionCollection Keys, AggregateCollection Values, RecordWriter Output, 
            ExpressionCollection Select, Register Memory, Table Sink)
            : base(Session)
        {

            this._output = Output;
            this._By = Keys;
            this._Over = Values;
            this._Select = Select;
            this._MemoryLocation = Memory;
            this._Sink = Sink;

        }

        public override void Consolidate(List<AggregateHashTableProcessNode> Nodes)
        {
            
            // Go through an merge groupers accross all headers //
            for (int i = 0; i < this._Sink.ExtentCount; i++)
            {

                // Open the left table //
                KeyValueSet x = KeyValueSet.Open(this._Sink, i, this._By, this._Over);

                // Cross match each grouper //
                for (int j = i + 1; j < this._Sink.ExtentCount; j++)
                {

                    // Get the right side of the comparison //
                    KeyValueSet y = KeyValueSet.Open(this._Sink, j, this._By, this._Over);

                    // Update x; entry R is in x and y, R in x will be updated while R in y will be removed; if R is not in both, nothing happens
                    if (y.Count != 0)
                        KeyValueSet.Union(x, y);

                    // Dump the table //
                    KeyValueSet.Resave(this._Sink, y);

                }

                // Output 'x' //
                x.WriteToFinal(this._output, this._Select, this._MemoryLocation);

            }

            // Drop the table //
            this._Session.Kernel.RequestDropTable(this._Sink.Header.Path);

            // Close the stream //
            this._output.Close();

        }

        public long Clicks
        {
            get { return this._Clicks; }
        }

    }

    /// <summary>
    /// Internal hash table class
    /// </summary>
    internal sealed class KeyValueSet
    {

        private ExpressionCollection _Maps;
        private AggregateCollection _Reducers;
        private Dictionary<Record, CompoundRecord> _cache = new Dictionary<Record, CompoundRecord>();
        private long _Capacity = 0;
        private RecordComparer _BaseComparer;
        private long _SinkID;

        // Constructor //
        public KeyValueSet(ExpressionCollection Fields, AggregateCollection Aggregates)
        {
            this._Maps = Fields;
            this._Reducers = Aggregates;
            this._BaseComparer = new RecordComparer();
            this._cache = new Dictionary<Record, CompoundRecord>(this._BaseComparer);
            this._Capacity = Extent.DEFAULT_PAGE_SIZE / Schema.Join(Fields.Columns, Aggregates.GetInterimSchema).RecordDiskCost;
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

        public long SinkID
        {
            get { return this._SinkID; }
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
            Register mem = new Register("OUT", s);
            ExpressionCollection leafs = ExpressionCollection.Render(s, "OUT", mem);
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

        public static void Save(Table Sink, KeyValueSet Data)
        {

            // Create the extent //
            Extent e = Data.ToInterim();

            // Dump the extent //
            Sink.AddExtent(e);

            // Get the id of the saved extent //
            Data._SinkID = e.Header.ID;

        }

        public static void Resave(Table Sink, KeyValueSet Data)
        {

            // Create the extent //
            Extent e = Data.ToInterim();

            // Load the header //
            e.Header = Sink.RenderHeader((int)Data.SinkID);

            // Dump the extent //
            Sink.SetExtent(e);

        }

        public static KeyValueSet Open(Table Sink, long ID, ExpressionCollection Fields, AggregateCollection Aggregates)
        {
            
            // Build the extent //
            Extent e = Sink.GetExtent((int)ID);
            
            // Build the KVS //
            KeyValueSet kvs = new KeyValueSet(Fields, Aggregates);

            // Import the interim data //
            kvs.FromInterim(e);

            // Return //
            return kvs;

        }

        public static Table DataSink(Session Session, ExpressionCollection Fields, AggregateCollection Aggregates)
        {

            // Build the schema //
            Schema s = Schema.Join(Fields.Columns, Aggregates.GetInterimSchema);

            // Create the table //
            Table t = Table.CreateTable(Session.Kernel, Session.Kernel.TempDirectory, Header.TempName(), s);
            return t;

        }

    }

    public sealed class AggregateOrderedProcessNode : QueryNode
    {

        private Volume _data;
        private ExpressionCollection _By;
        private AggregateCollection _Over;
        private Filter _Where;
        private Register _MemoryLocation;
        private RecordWriter _Writer;
        private long _clicks = 0;

        private ExpressionCollection _OutputKey;
        private Register _OutputMemory;

        private Record _BeginEdgeKey;
        private Record _EndEdgeKey;
        private CompoundRecord _BeginEdgeValue;
        private CompoundRecord _EndEdgeValue;
        private bool _LowerEdge = false; // true == lower edge initialized, false = not initialized

        private Record _WorkingKey;
        private CompoundRecord _WorkingValue;

        public AggregateOrderedProcessNode(int ThreadID, Session Session, Volume Data, ExpressionCollection Keys, AggregateCollection Values, Filter Where, Register Memory, 
            ExpressionCollection OutputKeys, Register OutputMemory, RecordWriter Writer)
            : base(ThreadID, Session)
        {

            this._data = Data;
            this._By = Keys;
            this._Over = Values;
            this._Where = Where;
            this._MemoryLocation = Memory;
            this._Writer = Writer;

            this._OutputKey = OutputKeys;
            this._OutputMemory = OutputMemory;

            // Set up the work pieces //
            this._WorkingValue = this._Over.Initialize();

        }

        public override void Invoke()
        {

            // Create a record comparer //
            RecordComparer rc = new RecordComparer();

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

                        // Render the key record //
                        Record r = this._By.Evaluate();

                        // Do a null check //
                        if (this._WorkingKey == null)
                            this._WorkingKey = r;

                        // Check if the keys match //
                        if (!rc.Equals(this._WorkingKey, r))
                        {

                            // Output the current record //
                            if (this._LowerEdge)
                            {
                                this._OutputMemory.Value = Record.Join(this._WorkingKey, this._Over.Evaluate(this._WorkingValue));
                                this._Writer.Insert(this._OutputKey.Evaluate());
                            }
                            else
                            {
                                this._LowerEdge = true;
                                this._BeginEdgeKey = this._WorkingKey;
                                this._BeginEdgeValue = this._WorkingValue;
                            }

                            // Update the key and re-set the value //
                            this._WorkingKey = r;
                            this._WorkingValue = this._Over.Initialize();

                        }

                        // Accumulate the value //
                        this._Over.Accumulate(this._WorkingValue);

                    } // End where 

                } // End Unit Shard Loop 

            } // End Volume Loop

            // Handle the edge cases //
            this._EndEdgeKey = this._WorkingKey;
            this._EndEdgeValue = this._WorkingValue;

            this._clicks += rc.Clicks;

        }

        public long Clicks
        {
            get { return this._clicks; }
        }

        public Record BeginEdgeKey 
        { 
            get { return this._BeginEdgeKey; } 
        }

        public Record EndEdgeKey 
        { 
            get { return this._EndEdgeKey; } 
        }

        public CompoundRecord BeginEdgeValue 
        { 
            get { return this._BeginEdgeValue; } 
        }

        public CompoundRecord EndEdgeValue 
        { 
            get { return this._EndEdgeValue; } 
        }

        public ExpressionCollection HashKey
        {
            get { return this._By; }
        }

        public AggregateCollection HashValue
        {
            get { return this._Over; }
        }

        public ExpressionCollection Select
        {
            get { return this._OutputKey; }
        }

        public Register SelectMemory
        {
            get { return this._OutputMemory; }
        }

        public RecordWriter OutputWriter
        {
            get { return this._Writer; }
        }
    
    }

    public sealed class AggregateOrderedConsolidationProcess : QueryConsolidation<AggregateOrderedProcessNode>
    {

        private RecordWriter _output;
        private ExpressionCollection _By;
        private AggregateCollection _Over;
        private Register _MemoryLocation;
        private ExpressionCollection _Select;
        private long _Clicks = 0;

        public AggregateOrderedConsolidationProcess(Session Session)
            : base(Session)
        {
        }

        public override void Consolidate(List<AggregateOrderedProcessNode> Nodes)
        {

            // Get our working variables //
            this._By = Nodes.First().HashKey;
            this._Over = Nodes.First().HashValue;
            this._MemoryLocation = Nodes.First().SelectMemory;
            this._Select = Nodes.First().Select;
            this._output = Nodes.First().OutputWriter;

            /* 
             * Create a dictionary to load the data into, merging all like edges allong the way 
             * 
             * We do it this way because we're not sure the nodes will finish in order, otherwise we could do simple
             * comparison to A's end to B's begin.
             * 
             */
            RecordComparer rc = new RecordComparer();
            Dictionary<Record, CompoundRecord> HashTable = new Dictionary<Record, CompoundRecord>(rc);
            foreach (AggregateOrderedProcessNode n in Nodes)
            {

                // Check the begin edge //
                if (HashTable.ContainsKey(n.BeginEdgeKey))
                {
                    n.HashValue.Merge(n.BeginEdgeValue, HashTable[n.BeginEdgeKey]);
                }
                else
                {
                    HashTable.Add(n.BeginEdgeKey, n.BeginEdgeValue);
                }

                // Check the end edge //
                if (HashTable.ContainsKey(n.EndEdgeKey))
                {
                    n.HashValue.Merge(n.EndEdgeValue, HashTable[n.EndEdgeKey]);
                }
                else
                {
                    HashTable.Add(n.EndEdgeKey, n.EndEdgeValue);
                }

                // Clicks //
                this._Clicks += n.Clicks;

            }

            // Dump the data to the stream //
            foreach (KeyValuePair<Record, CompoundRecord> kv in HashTable)
            {
                this._MemoryLocation.Value = Record.Join(kv.Key, this._Over.Evaluate(kv.Value));
                this._output.Insert(this._Select.Evaluate());
            }

            // Close the stream //
            foreach (AggregateOrderedProcessNode node in Nodes)
            {
                node.OutputWriter.Close();
            }
            //this._output.Close();

            // Clicks //
            this._Clicks += rc.Clicks;

        }

        public long Clicks
        {
            get { return this._Clicks; }
        }


    }

}
