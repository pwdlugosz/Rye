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

            // Get the clicks //
            foreach (AggregateHashTableProcessNode n in Nodes)
            {
                this._Clicks += n.Clicks;
            }
            
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

                    // Update OriginalNode; entry R is in OriginalNode and NewNode, R in OriginalNode will be updated while R in NewNode will be removed; if R is not in both, nothing happens
                    if (y.Count != 0)
                    {
                        KeyValueSet.Union(x, y);
                    }

                    // Dump the table //
                    KeyValueSet.Resave(this._Sink, y);

                }

                // Output 'OriginalNode' //
                x.WriteToFinal(this._output, this._Select, this._MemoryLocation);

            }

            // FreeAll the table //
            this._Session.Kernel.RequestDropTable(this._Sink.Header.Path);

            // Close the stream //
            this._output.Close();

        }

        public override long Clicks
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

                // Assign the Value to the register //
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
            {
                T2._cache.Remove(r);
            }

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
            kvs._SinkID = ID;

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

    /// <summary>
    /// Aggregates a table using pre-sorted expressions
    /// </summary>
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

                            // Update the key and re-set the Value //
                            this._WorkingKey = r;
                            this._WorkingValue = this._Over.Initialize();

                        }

                        // Accumulate the Value //
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

    /// <summary>
    /// Consolidates pre-sorted aggregated data
    /// </summary>
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
            //this._Fields.Close();

            // Clicks //
            this._Clicks += rc.Clicks;

        }

        public override long Clicks
        {
            get { return this._Clicks; }
        }


    }

    /// <summary>
    /// Describes aggregate algorithms
    /// </summary>
    public enum AggregateAlgorithm
    {
        HashTable,
        Ordered
    }

    /// <summary>
    /// Provides support for building data aggregations
    /// </summary>
    public class AggregateModel : QueryModel
    {

        // Aggregate Phase Variables //
        private TabularData _Source;
        private string _SourceAlias;
        private ExpressionCollection _Keys;
        private AggregateCollection _Values;
        private Filter _Where;

        // Output Phase Variables //
        private TabularData _OutTable;
        private string _OutAlias;
        private ExpressionCollection _OutFields;

        // Supplemental nodes //
        private Methods.MethodDump _PostDumpNode;
        private Methods.MethodSort _PostSortNode;

        // Hash only variables //
        Table _Sink = null;

        // Other variables //
        private string _Hint = null;

        public AggregateModel(Session Session)
            :base(Session)
        {

            this._Keys = new ExpressionCollection();
            this._Values = new AggregateCollection();
            this._OutFields = new ExpressionCollection();
            this._Where = Filter.TrueForAll;
        }

        // Properties //
        public Schema Columns
        {
            get
            {
                return Schema.Join(this._Keys.Columns, this._Values.Columns);
            }
        }

        // Aggregate Pieces //
        public void SetFROM(TabularData Value, string Alias)
        {
            this._Source = Value;
            this._SourceAlias = Alias;
        }

        public void AddKEY(Expression Value, string Alias)
        {
            this._Keys.Add(Value, Alias);
        }

        public void AddKEY(ExpressionCollection Value)
        {
            for (int i = 0; i < Value.Count; i++)
            {
                this._Keys.Add(Value[i], Value.Alias(i));
            }
        }

        public void AddAGGREGATE(Aggregate Value, string Alias)
        {
            this._Values.Add(Value, Alias);
        }

        public void AddAGGREGATE(AggregateCollection Value)
        {
            for (int i = 0; i < Value.Count; i++)
            {
                this._Values.Add(Value[i], Value.GetAlias(i));
            }
        }

        public void SetWHERE(Filter Value)
        {
            this._Where = Value;
        }

        // Consolidate Pieces //
        public void SetOUTPUT(TabularData Value, string Alias)
        {
            this._OutTable = Value;
            this._OutAlias = Alias;
        }

        public void AddFIELD(Expression Value, string Alias)
        {
            this._OutFields.Add(Value, Alias);
        }

        public void AddFIELD(ExpressionCollection Value)
        {
            for (int i = 0; i < Value.Count; i++)
            {
                this._OutFields.Add(Value[i], Value.Alias(i));
            }
        }

        // Other pieces //
        public void SetHINT(string Value)
        {
            this._Hint = Value;
        }

        public void SetDUMP(Methods.MethodDump Value)
        {
            this._PostDumpNode = Value;
        }

        public void SetSORT(Methods.MethodSort Value)
        {
            this._PostSortNode = Value;
        }

        // Nodes - Hash table //
        public AggregateHashTableProcessNode RenderNodeHT(int ThreadID, int ThreadCount)
        {

            // Potentially build a sink //
            if (this._Sink == null)
            {
                this._Sink = KeyValueSet.DataSink(this._Session, this._Keys, this._Values);
            }

            // Create our volume //
            Volume source = this._Source.CreateVolume(ThreadID, ThreadCount);

            // Create the registers //
            Register mem = new Register(this._SourceAlias, source.Columns);
            
            // Create the memory envrioment //
            CloneFactory spiderweb = new CloneFactory();
            spiderweb.Append(mem);
            spiderweb.Append(this._Session.Scalars); // Add in the global scalars
            spiderweb.Append(this._Session.Matrixes); // Add in the global matrixes

            // Clone our expressions //
            ExpressionCollection keys = spiderweb.Clone(this._Keys);
            AggregateCollection vals = spiderweb.Clone(this._Values);
            Filter where = spiderweb.Clone(this._Where);

            // Render the node //
            return new AggregateHashTableProcessNode(ThreadID, this._Session, source, keys, vals, where, mem, this._Sink);

        }

        public List<AggregateHashTableProcessNode> RenderNodesHT(int ThreadCount)
        {

            List<AggregateHashTableProcessNode> nodes = new List<AggregateHashTableProcessNode>();

            for (int i = 0; i < ThreadCount; i++)
            {
                nodes.Add(this.RenderNodeHT(i, ThreadCount));
            }

            return nodes;

        }

        public AggregateHashTableConsolidationProcess RenderReducerHT()
        {

            // Potentially build a sink //
            if (this._Sink == null)
            {
                this._Sink = KeyValueSet.DataSink(this._Session, this._Keys, this._Values);
            }

            // Create memory //
            Register mem = new Register(this._OutAlias, this._Sink.Columns);

            // Create the memory envrioment //
            CloneFactory spiderweb = new CloneFactory();
            spiderweb.Append(mem);
            spiderweb.Append(this._Session.Scalars); // Add in the global scalars
            spiderweb.Append(this._Session.Matrixes); // Add in the global matrixes
            
            // Clone the output expressions //
            ExpressionCollection output = spiderweb.Clone(this._OutFields);

            // Create the output writer //
            RecordWriter outstream = this._OutTable.OpenWriter();

            // Create the reducer - note that it doesnt matter that the keys/values are not assinged to a register //
            return new AggregateHashTableConsolidationProcess(this._Session, this._Keys, this._Values, outstream, output, mem, this._Sink);

        }

        public QueryProcess<AggregateHashTableProcessNode> RenderProcessHT(int ThreadCount)
        {

            // Build the process //
            List<AggregateHashTableProcessNode> nodes = this.RenderNodesHT(ThreadCount);
            AggregateHashTableConsolidationProcess reducer = this.RenderReducerHT();
            QueryProcess<AggregateHashTableProcessNode> process = new QueryProcess<AggregateHashTableProcessNode>(nodes, reducer);

            // Add the post processing nodes //
            if (this._PostSortNode != null) process.PostProcessor.AddChild(this._PostSortNode);
            if (this._PostDumpNode != null) process.PostProcessor.AddChild(this._PostDumpNode);

            return process;

        }

        // Nods - Sort merge //
        public AggregateOrderedProcessNode RenderNodeO(int ThreadID, int ThreadCount)
        {

            // Create our volume //
            Volume source = this._Source.CreateVolume(ThreadID, ThreadCount);

            // Create the registers //
            Register mem = new Register(this._SourceAlias, source.Columns);

            // Create the memory envrioment //
            CloneFactory spiderweb = new CloneFactory();
            spiderweb.Append(mem);
            spiderweb.Append(this._Session.Scalars); // Add in the global scalars
            spiderweb.Append(this._Session.Matrixes); // Add in the global matrixes

            // Clone our expressions //
            ExpressionCollection keys = spiderweb.Clone(this._Keys);
            AggregateCollection vals = spiderweb.Clone(this._Values);
            Filter where = spiderweb.Clone(this._Where);

            // Create the out memory //
            Register omem = new Register(this._OutAlias, Schema.Join(keys.Columns, vals.Columns));

            // Create the select memory web //
            CloneFactory outweb = new CloneFactory();
            outweb.Append(omem);
            outweb.Append(this._Session.Scalars); // Add in the global scalars
            outweb.Append(this._Session.Matrixes); // Add in the global matrixes

            // Clone the output fields //
            ExpressionCollection select = outweb.Clone(this._OutFields);

            // Open a writer //
            RecordWriter stream = this._OutTable.OpenWriter();

            // Render the node //
            return new AggregateOrderedProcessNode(ThreadID, this._Session, source, keys, vals, where, mem, select, omem, stream);

        }

        public List<AggregateOrderedProcessNode> RenderNodesO(int ThreadCount)
        {

            List<AggregateOrderedProcessNode> nodes = new List<AggregateOrderedProcessNode>();

            for (int i = 0; i < ThreadCount; i++)
            {
                nodes.Add(this.RenderNodeO(i, ThreadCount));
            }

            return nodes;

        }

        public QueryProcess<AggregateOrderedProcessNode> RenderProcessO(int ThreadCount)
        {

            // Build the process //
            List<AggregateOrderedProcessNode> nodes = this.RenderNodesO(ThreadCount);
            AggregateOrderedConsolidationProcess reducer = new AggregateOrderedConsolidationProcess(this._Session);
            QueryProcess<AggregateOrderedProcessNode> process = new QueryProcess<AggregateOrderedProcessNode>(nodes, reducer);

            // Add the pre-processor nodes //
            process.PreProcessor.AddChild(this.RenderPreProcessor());

            // Add the post processing nodes //
            if (this._PostSortNode != null) process.PostProcessor.AddChild(this._PostSortNode);
            if (this._PostDumpNode != null) process.PostProcessor.AddChild(this._PostDumpNode);

            return process;

        }

        // Optimization //
        public AggregateAlgorithm SuggestedAlgorithm()
        {

            // Check if any of the expressions are volatile //
            if (this._Keys.IsVolatile)
            {
                return AggregateAlgorithm.HashTable;
            }

            // Check if the data is already sorted //
            Key k = ExpressionCollection.DecompileToKey(this._Keys);
            if (k.Count != this._Keys.Count)
            {
                return AggregateAlgorithm.HashTable;
            }
            if (KeyComparer.IsWeakSubset(this._Source.SortBy ?? new Key(), k))
            {
                return AggregateAlgorithm.Ordered;
            }

            return AggregateAlgorithm.HashTable;

        }

        public AggregateAlgorithm ParseHint(string Value)
        {

            // Check if any of the expressions are volatile; even though we have a hint, we may not be able to override //
            if (this._Keys.IsVolatile)
            {
                return AggregateAlgorithm.HashTable;
            }

            string[] ht = new string[] { "0", "HT", "HASH", "HASH_TABLE", "HASHTABLE" };
            string[] o = new string[] { "1", "O", "ORDER", "S", "SORT" };

            if (ht.Contains(Value, StringComparer.OrdinalIgnoreCase))
                return AggregateAlgorithm.HashTable;

            if (o.Contains(Value, StringComparer.OrdinalIgnoreCase))
                return AggregateAlgorithm.Ordered;

            return AggregateAlgorithm.HashTable;

        }

        public AggregateAlgorithm GetAlgorithm()
        {

            // Figure out which algorithm to use //
            AggregateAlgorithm algorithm = AggregateAlgorithm.HashTable;
            if (this._Hint == null)
            {
                algorithm = this.SuggestedAlgorithm();
            }
            else
            {
                algorithm = this.ParseHint(this._Hint);
            }
            return algorithm;

        }

        public Methods.MethodSort RenderPreProcessor()
        {

            // Create a memory location //
            Register mem = new Register(this._SourceAlias, this._Source.Columns);

            // Create a web //
            CloneFactory web = new CloneFactory();
            web.Append(mem);

            // Generate the expression collection //
            ExpressionCollection keys = web.Clone(this._Keys);
            
            return new Methods.MethodSort(null, this._Source, keys, mem, Key.Build(keys.Count));

        }

        // Execution //
        private void BuildCompileString()
        {

            //this._Message.Append("--- AGGREGATE ------------------------------------\n");
            this._Message.Append(string.Format("From: {0}\n", this._Source.Header.Name));
            if (!this._Where.Default)
                this._Message.Append(string.Format("Where: {0}\n", this._Where.UnParse(this._Source.Columns)));
            this._Message.Append(string.Format("Grouping by {0} key(s)\n", this._Keys.Count));
            this._Message.Append(string.Format("Aggregating {0} Value(s)\n", this._Values.Count));
            this._Message.Append(string.Format("Using the {0} algorithm\n", (this.GetAlgorithm() == AggregateAlgorithm.HashTable ? "hash table" : "natural order")));
            if (this._PostSortNode != null)
                this._Message.Append(string.Format("Sorting output data with cost: {0}\n", this._PostSortNode.Clicks));
            if (this._PostDumpNode != null)
                this._Message.Append(string.Format("Dumping output data to {0}\n", this._PostDumpNode.Path));

        }

        public override void ExecuteConcurrent(int ThreadCount)
        {

            // Fix the threads //
            this.ThreadCount = ThreadCount;

            // Build response //
            this.BuildCompileString();

            // Figure out which algorithm to use //
            AggregateAlgorithm algorithm = this.GetAlgorithm();

            // Do the hash table process //
            if (algorithm == AggregateAlgorithm.HashTable)
            {

                QueryProcess<AggregateHashTableProcessNode> process = this.RenderProcessHT(this.ThreadCount);

                this._Timer = System.Diagnostics.Stopwatch.StartNew();
                process.ExecuteThreaded();
                this._Timer.Stop();

                // Append the run string //
                this._Message.Append(string.Format("Runtime: {0}, over {1} thread(s), with cost {2}\n\n", this._Timer.Elapsed, this.ThreadCount, process.Reducer.Clicks));

            }
            else
            {

                QueryProcess<AggregateOrderedProcessNode> process = this.RenderProcessO(this.ThreadCount);

                this._Timer = System.Diagnostics.Stopwatch.StartNew();
                process.ExecuteThreaded();
                this._Timer.Stop();

                // Append the run string //
                this._Message.Append(string.Format("Pre-processor sort cost {0}\n", process.PreProcessorClicks));
                this._Message.Append(string.Format("Runtime: {0}, over {1} thread(s), with cost {2}\n\n", this._Timer.Elapsed, this.ThreadCount, process.Reducer.Clicks));


            }


        }

        public override void ExecuteAsynchronous()
        {

            // Fix the threads //
            this.ThreadCount = 1;

            // Build response //
            this.BuildCompileString();

            // Figure out which algorithm to use //
            AggregateAlgorithm algorithm = this.GetAlgorithm();

            // Do the hash table process //
            if (algorithm == AggregateAlgorithm.HashTable)
            {

                QueryProcess<AggregateHashTableProcessNode> process = this.RenderProcessHT(this.ThreadCount);

                this._Timer = System.Diagnostics.Stopwatch.StartNew();
                process.Execute();
                this._Timer.Stop();

                // Append the run string //
                this._Message.Append(string.Format("Runtime: {0}, over {1} thread(s), with cost {2}\n\n", this._Timer.Elapsed, this.ThreadCount, process.Reducer.Clicks));

            }
            else
            {

                QueryProcess<AggregateOrderedProcessNode> process = this.RenderProcessO(this.ThreadCount);

                this._Timer = System.Diagnostics.Stopwatch.StartNew();
                process.Execute();
                this._Timer.Stop();

                // Append the run string //
                this._Message.Append(string.Format("Pre-processor sort cost {0}\n", process.PreProcessorClicks));
                this._Message.Append(string.Format("Runtime: {0}, over {1} thread(s), with cost {2}\n\n", this._Timer.Elapsed, this.ThreadCount, process.Reducer.Clicks));


            }

        }

    }

}
