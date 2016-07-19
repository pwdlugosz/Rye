﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Expressions;

namespace Rye.Data
{
    
    public abstract class DataSet
    {

        // Properties //
        public abstract Schema Columns { get; }

        public abstract Key SortBy { get; }

        public bool IsSorted 
        { 
            get 
            { 
                return this.SortBy.Count != 0; 
            }
        }

        public abstract long MaxRecords { get; set; }

        public abstract string Name { get; }

        public abstract IEnumerable<Record> Records { get; }

        public abstract long ExtentCount { get; }

        public abstract long RecordCount { get; }

        public abstract Header Header { get; set; }

        // Methods //
        public abstract Volume CreateVolume();

        public abstract Volume CreateVolume(int ThreadID, int ThreadCount);
        
        public virtual void Sort(Key K)
        {
            long clicks = this.SortC(K);
        }

        public abstract long SortC(Key K);

        public bool IsSortedBy(Key K)
        {
            if (K == null || this.SortBy == null)
                return false;
            return Key.SubsetStrong(this.SortBy, K);
        }

        public abstract void PreSerialize();

        public abstract RecordWriter OpenWriter();

        // Costs //
        public abstract long CellCount
        {
            get;
        }

        public abstract int MemCost { get; }

        public abstract int DiskCost { get; }

    
    }

    public sealed class Extent : DataSet
    {

        internal const long DEFAULT_MAX_RECORD_COUNT = 10000000;
        internal const long DEFAULT_MEMORY_FOOTPRINT = 65536; // = 64 mb in kb
        internal const long ESTIMATE_META_DATA = 4096; // estimate 4 kb in meta data
        
        private List<Record> _Cache;
        private Schema _Columns;
        private Header _Head;
        private Key _OrderBy;
        
        // Constructor //
        public Extent(Schema NewColumns, Header NewHeader, List<Record> NewCache, Key NewOrderBy)
        {
            this._Columns = NewColumns;
            this._Cache = NewCache;
            this._OrderBy = NewOrderBy;
            this._Head = NewHeader;
            
        }

        public Extent(Schema NewColumns, Header NewHeader)
            : this(NewColumns, NewHeader, new List<Record>(), new Key())
        {

        }

        public Extent(Schema NewColumns)
            : this(NewColumns, Header.NewMemoryOnlyExtentHeader("EXTENT", NewColumns.Count, EstimateMaxRecords(NewColumns)))
        {
        }
        
        public Extent(string Directory, string Name, Schema S, long MaxRecords)
            : this(S)
        {
            this.MaxRecords = MaxRecords;
            Header h = Header.NewExtentHeader(Directory, Name, 0, S.Count, EstimateMaxRecords(S));
        }

        public Extent(string Directory, string Name, Schema S)
            : this(Directory, Name, S, EstimateMaxRecords(S))
        {
        }

        // DataSet Override Properties //
        public override Schema Columns
        {
            get
            {
                return this._Columns;
            }
        }

        public override Key SortBy
        {
            get
            {
                return this._OrderBy;
            }
        }

        public override long MaxRecords
        {
            get
            {
                return this._Head.MaxRecordCount;
            }
            set
            {
                this._Head.MaxRecordCount = value;
            }
        }

        public override string Name
        {
            get 
            {
                return this.Header.Name;
            }
        }

        public bool IsEmpty
        {
            get
            {
                return this.Count == 0;
            }
        }

        public override long ExtentCount
        {
            get { return 1; }
        }

        public override long CellCount
        {
            get { return this._Cache.Count * this._Columns.Count; }
        }

        public override long RecordCount
        {
            get { return (long)this._Cache.Count; }
        }

        public override Header Header
        {
            get
            {
                return this._Head;
            }
            set
            {
                this._Head = value;
            }
        }

        public bool IsFull
        {
            get
            {
                return this.Count == this.MaxRecords;
            }
        }
        
        public int Count
        {
            get
            {
                return this._Cache.Count;
            }
        }

        public bool IsMemoryOnly
        {
            get
            {
                return this._Head.IsMemoryOnly;
            }
        }

        public Record this[int Index]
        {
            get
            {
                return this._Cache[Index];
            }
            set
            {
                this._Cache[Index] = value;
            }
        }

        public long ID
        {
            get
            {
                return this.Header.ID;
            }
        }

        public List<Record> Cache
        {
            get
            {
                return this._Cache;
            }
        }

        public override IEnumerable<Record> Records
        {
            get { return this._Cache; }
        }

        public Extent EmptyClone()
        {
            return new Extent(this._Columns, this._Head, new List<Record>(), this._OrderBy);
        }

        // Methods //
        public override Volume CreateVolume()
        {
            return new SingleExtentVolume(this);
        }

        public override Volume CreateVolume(int ThreadID, int ThreadCount)
        {
            return new SingleExtentVolume(this);
        }

        public void Add(Record Data)
        {
            
            if (this.IsFull) 
                throw new Exception("RecordSet is full");
            if (!this.Columns.Check(Data, true))
                throw new Exception("Record passed does not match schema");
            this._Cache.Add(Data);

        }

        public void Remove(int Index)
        {
            this._Cache.RemoveAt(Index);
        }

        public override void PreSerialize()
        {
            this._Head.RecordCount = this.Count;
            this._Head.KeyCount = this._OrderBy.Count;
            this._Head.BigRecordCount = 0;
            this._Head.Stamp();
        }

        public int Seek(Record R)
        {

            // Toss this in incase we seek over no records //
            if (this.Count == 0) return -1;

            int i = 0, j = this.Count - 1;
            while (j >= i)
            {
                if (Record.Compare(this[i], R) == 0) return i;
                if (Record.Compare(this[j], R) == 0) return j;
                i++; 
                j--;
            }
            return -1;
        }

        public int Seek(Record R, Key K)
        {

            // Toss this in incase we seek over no records //
            if (this.Count == 0) return -1;
            
            int i = 0, j = this.Count - 1;
            //Console.WriteLine("i {0} : j {1}", i, j);

            while (j >= i)
            {
                if (Record.Compare(this[i], R, K) == 0) return i;
                if (Record.Compare(this[j], R, K) == 0) return j;
                i++;
                j--;
            }
            return -1;

        }

        public bool Contains(Record R)
        {
            return this.Seek(R) == -1;
        }

        public bool Contains(Record R, Key K)
        {
            return this.Seek(R, K) == -1;
        }

        public void Swap(int Index1, int Index2)
        {
            Record r = this[Index1];
            this[Index1] = this[Index2];
            this[Index2] = r;
        }

        public override string ToString()
        {
            return this.Header.Name;
        }

        public override int GetHashCode()
        {
            long l = this._Cache.Sum<Record>((r) => { return (long)(r.GetHashCode() & sbyte.MaxValue); });
            return (int)l;
        }

        public override void Sort(Key K)
        {

            this._OrderBy = K;
            this._Cache.Sort(new RecordComparer(K, K));
            if (this.Header.IsMemoryOnly)
            {
                Kernel.RequestFlushExtent(this);
            }

        }

        public override long SortC(Key K)
        {
            RecordComparer rc = new RecordComparer(K, K);
            this._Cache.Sort(rc);
            this._OrderBy = K;
            return rc.Clicks;
        }

        public override int DiskCost
        {
            get 
            {
                int header_cost = this._Head.DiskCost; // Record length is stored in the header //
                int key_cost = this._OrderBy.DiskCost;
                int schema_cost = this._Columns.DiskCost;
                int data_cost = this._Columns.RecordDiskCost * this.Count;
                //Console.WriteLine(header_cost);
                //Console.WriteLine(key_cost);
                //Console.WriteLine(schema_cost);
                //Console.WriteLine(data_cost);

                return header_cost + key_cost + schema_cost + data_cost;
            }
        }

        public override int MemCost
        {
            get
            {
                int header_cost = this._Head.MemCost; // Record length is stored in the header //
                int key_cost = this._OrderBy.MemCost;
                int schema_cost = this._Columns.MemCost;
                int data_cost = this._Columns.RecordMemCost * this.Count;
                return header_cost + key_cost + schema_cost + data_cost;
            }
        }

        public override RecordWriter OpenWriter()
        {
            return new ExtentWriter(this);
        }

        // Statics //
        public static long EstimateMaxRecords(Schema Columns, long TotalMemoryFootPrintKB)
        {
            return (TotalMemoryFootPrintKB * 1024 - ESTIMATE_META_DATA) / Columns.RecordMemCost;
        }

        public static long EstimateMaxRecords(Schema Columns)
        {
            return EstimateMaxRecords(Columns, DEFAULT_MEMORY_FOOTPRINT);
        }

        public static void SetCache(Extent DataToSet, Extent DataToImport)
        {

            if (DataToImport.Columns.GetHashCode() != DataToSet.Columns.GetHashCode())
                throw new ArgumentException("The schema for both extents don't match");
            DataToSet._Cache = DataToImport._Cache;

        }

        // Private classes //
        /*
        private sealed class SingleExtentCollection : ExtentCollection
        {

            public Extent _e;

            public SingleExtentCollection(Extent Data)
                : base()
            {
                this._e = Data;
            }

            public override IEnumerable<Extent> Extents
            {
                get 
                {
                    List<Extent> e = new List<Extent>();
                    e.Add(this._e);
                    return e;
                }
            }

            public override long ExtentCount
            {
                get { return 1; }
            }

            public override long BeginRowID
            {
                get { return 0; }
            }

            public override void SetExtent(Extent Data)
            {
                // do nothing
            }

            public override Extent GetExtent(int Index)
            {
                if (Index != 0)
                    throw new ArgumentOutOfRangeException(string.Format("Invalid index for a collection with only one extent: {0}", Index));
                return this._e;
            }

            public override Schema Columns
            {
                get
                {
                    return this._e.Columns;
                }
            }

        }
        */
        
        private sealed class SingleExtentVolume : Volume
        {

            private Extent _E;

            public SingleExtentVolume(Extent E)
                :base(0)
            {
                this._E = E;
            }

            public override Schema Columns 
            { 
                get 
                { 
                    return this._E._Columns;
                } 
            }

            public override Key SortKey
            {
                get
                {
                    return this._E.SortBy;
                }
            }

            public override void Sort(Key K)
            {
                this._E.Sort(K);
            }

            public override DataSet Parent
            {
                get
                {
                    return this._E;
                }
            }

            public override long ExtentCount 
            {
                get
                {
                    return 1L;
                }
            }

            public override long RecordCount
            {
                get
                {
                    return this._E.RecordCount;
                }
            }

            public override long FirstRowID
            {
                get
                {
                    return 0L;
                }
            }

            public override long LastRowID
            {
                get
                {
                    return (long)(this._E.RecordCount - 1);
                }
            }

            public override IEnumerable<Header> Headers
            {
                get
                {
                    List<Header> headers = new List<Header>();
                    headers.Add(this._E.Header);
                    return headers;
                }
            }

            public override IEnumerable<Extent> Extents
            {
                get
                {
                    List<Extent> Es = new List<Extent>();
                    Es.Add(this._E);
                    return Es;
                }
            }

            public override IEnumerable<Record> Records
            {
                get
                {
                    return this._E._Cache;
                }
            }

            public override Extent GetExtent(int Index)
            {
                if (Index >= (int)this._E.ExtentCount)
                    throw new IndexOutOfRangeException(string.Format("Index {0} is out of range: 0 - {1}", Index, this._E.Count));
                return this._E;
            }

            public override RecordReader OpenReader(Register Memory, Filter Predicate)
            {
                return new RecordReader(this._E, Memory, Predicate);
            }

        }

    }

    public class Table : DataSet
    {

        internal const long DEFAULT_MAX_SIZE = 1000000;
        protected const int OFFSET_ID = 0;
        protected const int OFFSET_COUNT = 1;

        //protected List<Header> _Cache;
        protected Extent _Refs; // The move to virtual headers is designed to reduce the memory footprint and make IO faster
        protected Schema _Columns;
        protected Header _Head;
        protected Key _OrderBy;
        protected object _lock = new object();

        // Constructors //
        public Table(Header H, Schema S, List<Record> R, Key SortedKeySet)
        {

            //this._Cache = HS;
            this._Columns = S;
            this._Head = H;
            this._OrderBy = SortedKeySet;
            this._Refs = new Extent(new Schema("ID INT, COUNT INT"), Header, R, SortedKeySet);

        }

        protected Table(string Directory, string Name, Schema S, long MaxRecords, bool Flush)
        {

            Header h = Data.Header.NewTableHeader(Directory, Name, S.Count, MaxRecords);

            //this._Cache = new List<Header>();
            this._Columns = S;
            this._Head = h;
            this._OrderBy = new Key();
            this._Refs = new Extent(new Schema("ID INT, COUNT INT"), h);

            if (Flush)
            {
                Kernel.RequestDropTable(h.Path);
                Kernel.RequestFlushTable(this);
            }

        }

        /// <summary>
        /// Creates a table, dropping a table of the same name if it exists, and flushes the table to disk
        /// </summary>
        /// <param name="Directory"></param>
        /// <param name="Name"></param>
        /// <param name="S"></param>
        /// <param name="MaxRecords"></param>
        public Table(string Directory, string Name, Schema S, long MaxRecords)
            : this(Directory, Name, S, MaxRecords, true)
        {
        }

        public Table(string Directory, string Name, Schema S)
            : this(Directory, Name, S, DEFAULT_MAX_SIZE)
        {
        }

        // Properties //
        public override long ExtentCount
        {
            get
            {
                return this.Header.RecordCount;
            }
        }

        public override long RecordCount
        {
	        get { return this._Head.BigRecordCount; }
        }

        public override Schema Columns
        {
            get
            {
                return this._Columns;
            }
        }

        public override Header Header
        {
            get
            {
                return this._Head;
            }
            set
            {
                if (value.Affinity != HeaderType.Table)
                    throw new ArgumentException(string.Format("Header must have type {0}", HeaderType.Table));
                this._Head = value;
            }
        }

        public virtual IEnumerable<Header> Headers
        {
            get { return new HeaderEnumerable(this); }
        }

        public override Key SortBy
        {
            get
            {
                return this._OrderBy;
            }
        }

        public override long MaxRecords
        {
            get
            {
                return this._Head.MaxRecordCount;
            }
            set
            {
                if (value < 0) 
                    this.Header.MaxRecordCount = -value;
                this.Header.MaxRecordCount = value;
            }
        }

        public override string Name
        {
            get { return this.Header.Name; }
        }

        public override IEnumerable<Record> Records
        {
            get 
            {
                return new RecordIEnumerable(this); 
            }
        }

        public override long CellCount
        {
            get { return this.RecordCount * this._Columns.Count; }
        }

        public double AvgExtentSize
        {
            get { return (double)this._Head.RecordCount / (double)this._Head.RecordCount; }
        }

        // Volumes //
        public override Volume CreateVolume()
        {
            return new TableVolume(this, 0, 1);
        }

        public override Volume CreateVolume(int ThreadID, int ThreadCount)
        {
            return new TableVolume(this, ThreadID, ThreadCount);
        }

        // IO Methods //
        public virtual Header RenderHeader(int Index)
        {
            long id = this._Refs[Index][OFFSET_ID].INT;
            return new Header(this.Header.Directory, this.Header.Name, id, this.Columns.Count, 0, 0, this.MaxRecords, 0, HeaderType.Extent);
        }

        public virtual Extent ReferenceTable
        {
            get { return this._Refs; }
        }

        /* ## Thread Safe ##*/
        protected void AddRefRecord(long ID, long RecordCount)
        {

            lock (this._lock)
            {
                this._Refs.Add(Record.Stitch(new Cell(ID), new Cell(RecordCount)));
                this._Head.BigRecordCount += RecordCount;
                this._Head.RecordCount = (long)this._Refs.Count;
            }

        }

        /* ## Thread Safe ##*/
        protected void UpdateRefRercord(long ID, long RecordCount)
        {
            lock (this._lock)
            {
                long OldCount = this._Refs[(int)ID][OFFSET_COUNT].INT;
                this.Header.BigRecordCount += (RecordCount - OldCount);
                this._Refs[(int)ID][OFFSET_COUNT] = new Cell(RecordCount);

            }
        }

        /* ## Thread Safe ##*/
        /// <summary>
        /// Gets a given extent form the table
        /// </summary>
        /// <param name="Index"></param>
        /// <returns></returns>
        public virtual Extent GetExtent(int Index)
        {

            lock (this._lock)
            {

                if (this.ExtentCount <= Index)
                    throw new ArgumentException(string.Format("Attempting to pop chunk {0} but this table only has {1} extents", Index, this.ExtentCount));

                // Get the header //
                Header h = this.RenderHeader(Index);

                // Buffer //
                return Kernel.RequestBufferExtent(h.Path);

            }

        }

        /* ## Thread Safe ##*/
        /// <summary>
        /// Note: this method requires the following of the RecordSet:
        ///     -- It must be attached
        ///     -- It must have the same schema (names can be different) as the parent
        ///     -- It must have the same name as the parent
        ///     -- It must have an ID present in the parent
        /// </summary>
        /// <param name="DataSet"></param>
        public virtual void SetExtent(Extent Data)
        {

            lock (this._lock)
            {

                // Check a few things: the table is attached, columns match, name matches, and the ID exists in the current table //
                if (Data.IsMemoryOnly)
                    throw new ArgumentException("The Extent passed is not attached; use the 'AddExtent' method to add a disjoint Extent");
                if (Data.Columns.GetHashCode() != this.Columns.GetHashCode())
                    throw new ArgumentException("The schema passed for the current record set does not match the Table");
                if (Data.Header.Name != this.Header.Name)
                    throw new ArgumentException("The 'name' of this Extent does not match the parent; use the 'AddExtent' method to add a disjoint Extent");
                if (this.Header.ID >= this.ExtentCount)
                    throw new ArgumentException("The ID of this Extent does not match the parent; use the 'AddExtent' method to add a disjoint Extent");
                if (this._Refs[(int)Data.Header.ID][OFFSET_ID].INT != (int)Data.Header.ID)
                    throw new ArgumentException("The ID of this Extent does not match the parent; use the 'AddExtent' method to add a disjoint Extent");

                // Get the current record count //
                this.UpdateRefRercord(Data.ID, Data.RecordCount);

                // Flush //
                Kernel.RequestFlushExtent(Data);

            }

        }

        /* ## Thread Safe ##*/
        /// <summary>
        /// Accumulate non-existant data to the set 
        /// </summary>
        /// <param name="Data"></param>
        public virtual void AddExtent(Extent Data)
        {

            lock (this._lock)
            {

                // Check that the schema match and that the data set is not too big //
                if (Data.Columns.GetHashCode() != this.Columns.GetHashCode())
                    throw new Exception("The schema passed for the current record set does not match the Table");
                if (Data.Count > this.MaxRecords)
                    throw new Exception("The data set passed is too large");

                // Need to create a new header //
                long id = this._Refs.Count;
                Header h = new Header(this.Header.Directory, this.Header.Name, id, (long)Data.Columns.Count, Data.RecordCount, this._OrderBy.Count, this.MaxRecords, 0, HeaderType.Extent);
                Data.Header = h;

                // Accumulate and dump //
                this.AddRefRecord(id, Data.Count);
                Kernel.RequestFlushExtent(Data);

            }

        }

        /* ## Thread Safe ##*/
        /// <summary>
        /// Generates a new extent
        /// </summary>
        /// <returns></returns>
        public virtual Extent Grow()
        {

            lock (this._lock)
            {

                long id = this.ExtentCount;
                Header h = Data.Header.NewExtentHeader(this.Header.Directory, this.Header.Name, id, this.Columns.Count, this.MaxRecords);
                
                Extent e = new Extent(this.Columns, h);

                // Accumulate to our cache //
                this.AddRefRecord(id, 0);

                // Dump //
                Kernel.RequestFlushExtent(e);

                // Return //
                return e;

            }

        }

        public virtual Extent PopFirst()
        {
            return this.GetExtent(0);
        }

        public virtual Extent PopLast()
        {
            return this.GetExtent(this._Refs.Count - 1);
        }

        public virtual Extent PopFirstOrGrow()
        {
            if (this._Refs.Count == 0)
                return this.Grow();
            return this.PopFirst();
        }

        public virtual Extent PopLastOrGrow()
        {
            if (this._Refs.Count == 0) 
                return this.Grow();
            return this.PopLast();
        }

        public override void PreSerialize()
        {
            
        }

        internal virtual void CursorClose(Extent Data)
        {

            // Dont flush if zero records otherwise we may run into trouble when we buffer again //
            if (Data.Count == 0)
                return;
            this.AddExtent(Data);
            Kernel.RequestFlushTable(this);

        }

        // Sorts //
        public override long SortC(Key K)
        {
            return Table.SortBase(this, K);
        }

        // Sort support //
        internal static long SortEach(Table Data, Key OrderBy)
        {

            long Clicks = 0;
            foreach (Header h in Data.Headers)
            {

                // Buffer record set //
                Extent e = Kernel.RequestBufferExtent(h.Path);

                // Check if it is sorted //
                if (!Key.EqualsStrict(e.SortBy, OrderBy))
                {
                    Clicks += e.SortC(OrderBy);
                }

                // FlushRecordUnion //
                Kernel.RequestFlushExtent(e);

            }

            return Clicks;

        }

        internal static long SortEach(Table Data, Key OrderBy, int[] Extents)
        {

            long Clicks = 0;
            foreach (int idx in Extents)
            {

                // Buffer record set //
                Extent e = Data.GetExtent(idx);

                // Check if it is sorted //
                if (!Key.EqualsStrict(e.SortBy, OrderBy))
                {
                    Clicks += e.SortC(OrderBy);
                }

                // FlushRecordUnion //
                Kernel.RequestFlushExtent(e);

            }
            return Clicks;

        }

        internal static long SortMerge(Extent A, Extent B, Key Columns)
        {

            // Variables //
            Extent x = new Extent(A.Columns);
            Extent y = new Extent(B.Columns);
            x.MaxRecords = A.MaxRecords;
            y.MaxRecords = B.MaxRecords;
            int CompareResult = 0;

            RecordComparer rec = new RecordComparer(Columns, Columns);
            
            // Main record loop //
            int ptra = 0, ptrb = 0;
            while (ptra < A.Count && ptrb < B.Count)
            {

                // Compare results //
                CompareResult = rec.Compare(A[ptra], B[ptrb]);
                
                if (CompareResult <= 0 && x.Count < A.Count)
                {
                    x.Add(A[ptra]);
                    ptra++;
                }
                else if (CompareResult > 0 && x.Count < A.Count)
                {
                    x.Add(B[ptrb]);
                    ptrb++;
                }
                else if (CompareResult <= 0 && x.Count >= A.Count)
                {
                    y.Add(A[ptra]);
                    ptra++;
                }
                else if (CompareResult > 0 && x.Count >= A.Count)
                {
                    y.Add(B[ptrb]);
                    ptrb++;
                }

            }

            // Clean up first shard //
            while (ptra < A.Count)
            {

                if (x.Count < A.Count)
                    x.Add(A[ptra]);
                else
                    y.Add(A[ptra]);
                ptra++;

            }

            // Clean up second shard //
            while (ptrb < B.Count)
            {

                if (x.Count < A.Count)
                    x.Add(B[ptrb]);
                else
                    y.Add(B[ptrb]);
                ptrb++;

            }

            // Set the heaps //
            Extent.SetCache(A, x);
            Extent.SetCache(B, y);

            return rec.Clicks;

        }

        internal static long SortBase(Table Data, Key OrderBy)
        {

            // Variables //
            int ptr_FirstShard = 0;
            int ptr_SecondShard = 0;
            int SortCount = 0;
            long Clicks = 0;
            
            // Step one: sort all shards //
            Clicks += Table.SortEach(Data, OrderBy);

            // if only one shard, break //
            if (Data.ExtentCount == 1) 
                return Clicks;

            // Step two: do cartesian sort n x (n - 1) //
            while (ptr_FirstShard < Data.ExtentCount)
            {

                // Secondary loop //
                ptr_SecondShard = ptr_FirstShard + 1;
                while (ptr_SecondShard < Data.ExtentCount)
                {

                    // Open shards //
                    Extent t1 = Data.GetExtent(ptr_FirstShard);
                    Extent t2 = Data.GetExtent(ptr_SecondShard);

                    // Sort merge both shards //
                    Clicks += Table.SortMerge(t1, t2, OrderBy);

                    // Close both shards //
                    Data.SetExtent(t1);
                    Data.SetExtent(t2);

                    // Increment //
                    ptr_SecondShard++;
                    SortCount++;

                }

                // Increment //
                ptr_FirstShard++;

            }

            // Tag the anthology's sort key //
            Data._OrderBy = OrderBy;

            // Flush //
            Kernel.RequestFlushTable(Data);

            // Return cost //
            return Clicks;

        }

        internal static long SortBase(Table Data, Key OrderBy, int[] Extents)
        {

            // Variables //
            int ptr_FirstShard = 0;
            int ptr_SecondShard = 0;
            int SortCount = 0;

            // Step one: sort all shards //
            long Clicks = Table.SortEach(Data, OrderBy);

            // if only one shard, break //
            if (Data.ExtentCount == 1) 
                return Clicks;

            // Step two: do cartesian sort n x (n - 1) //
            while (ptr_FirstShard < Extents.Length)
            {

                // Secondary loop //
                ptr_SecondShard = ptr_FirstShard + 1;
                while (ptr_SecondShard < Extents.Length)
                {

                    // Open shards //
                    Extent t1 = Data.GetExtent(Extents[ptr_FirstShard]);
                    Extent t2 = Data.GetExtent(Extents[ptr_SecondShard]);

                    // Sort merge both shards //
                    Clicks = Table.SortMerge(t1, t2, OrderBy);

                    // Close both shards //
                    Data.SetExtent(t1);
                    Data.SetExtent(t2);

                    // Increment //
                    ptr_SecondShard++;
                    SortCount++;

                }

                // Increment //
                ptr_FirstShard++;

            }

            //Tag the anthology's sort key //
            if (Extents.Length == Data.ExtentCount)
                Data._OrderBy = OrderBy;

            // Flush //
            Kernel.RequestFlushTable(Data);

            return Clicks;

        }

        // Override //
        public override int GetHashCode()
        {
            return Header.GetHashCode() * this._Columns.GetHashCode();
        }

        public override string ToString()
        {
            return this.Header.Path;
        }

        public override int DiskCost
        {
            get
            {
                int header_cost = this._Head.DiskCost; // Record length is stored in the header //
                int key_cost = this._OrderBy.DiskCost;
                int schema_cost = this._Columns.DiskCost;
                int data_cost = this._Refs.DiskCost;
                return header_cost + key_cost + schema_cost + data_cost;
            }
        }

        public override int MemCost
        {
            get
            {
                int header_cost = this._Head.MemCost; // Record length is stored in the header //
                int key_cost = this._OrderBy.MemCost;
                int schema_cost = this._Columns.MemCost;
                int data_cost = this._Refs.MemCost;
                return header_cost + key_cost + schema_cost + data_cost;
            }
        }

        public override RecordWriter OpenWriter()
        {
            return new TableWriter(this);
        }

        // Private classes //
        protected sealed class SetEnumerator : IEnumerator<Extent>, System.Collections.IEnumerator, IDisposable
        {

            private int _idx = -1;
            private Table _t;

            public SetEnumerator(Table Data)
            {
                this._t = Data;
            }

            public bool MoveNext()
            {
                this._idx++;
                return this._idx < this._t.ExtentCount;
            }

            public void Reset()
            {
                this._idx = -1;
            }

            Extent IEnumerator<Extent>.Current
            {

                get
                {
                    return Kernel.RequestBufferExtent(this._t.RenderHeader(this._idx).Path);
                }

            }

            object System.Collections.IEnumerator.Current
            {

                get
                {
                    return Kernel.RequestBufferExtent(this._t.RenderHeader(this._idx).Path);
                }

            }

            public void Dispose()
            {
            }

            public long ExtentCount
            {
                get { return this._t.ExtentCount; }
            }

        }

        protected sealed class SetEnumerable : IEnumerable<Extent>, System.Collections.IEnumerable
        {

            private SetEnumerator _e;

            public SetEnumerable(Table Data)
            {
                _e = new SetEnumerator(Data);
            }

            IEnumerator<Extent> IEnumerable<Extent>.GetEnumerator()
            {
                return _e;
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return _e;
            }

            public long ExtentCount
            {
                get { return this._e.ExtentCount; }
            }

        }

        protected sealed class ThreadedSetEnumerator : IEnumerator<Extent>, System.Collections.IEnumerator, IDisposable
        {

            private int _idx = -1;
            private Table _t;
            private int[] _ExtentIDs;
            private long _rowstart;

            public ThreadedSetEnumerator(Table Data, int ThreadID, int ThreadCount)
            {
                
                this._t = Data;
                int TotalExtents = (int)Data.ExtentCount;
                int Remainder = TotalExtents % ThreadCount;
                int BucketCount = TotalExtents / ThreadCount;
                int RemainderCounter = 0;
                int BeginAt = 0;
                int EndAt = 0;
                int CountOf = 0;

                for (int i = 0; i < ThreadID + 1; i++)
                {
                    BeginAt += CountOf;
                    CountOf = BucketCount + (RemainderCounter < Remainder ? 1 : 0);
                    RemainderCounter++;
                    EndAt = BeginAt + CountOf;
                }

                List<int> ids = new List<int>();
                for (int i = BeginAt; i < EndAt; i++)
                {
                    ids.Add(i);
                }
                this._ExtentIDs = ids.ToArray();

                // Get the count of all records up to this point //
                long counter = 0;
                long FirstID = this._ExtentIDs[0]; 
                foreach (Record r in Data._Refs.Records)
                {
                    if (r[0].INT < FirstID)
                        counter += r[1].INT;
                }

            }

            public bool MoveNext()
            {
                this._idx++;
                return this._idx < this._ExtentIDs.Length;
            }

            public void Reset()
            {
                this._idx = -1;
            }

            Extent IEnumerator<Extent>.Current
            {

                get
                {
                    return Kernel.RequestBufferExtent(this._t.RenderHeader(this._ExtentIDs[this._idx]).Path);
                }

            }

            object System.Collections.IEnumerator.Current
            {

                get
                {
                    return Kernel.RequestBufferExtent(this._t.RenderHeader(this._ExtentIDs[this._idx]).Path);
                }

            }

            public void Dispose()
            {
            }

            public long ExtentCount
            {
                get { return (long)this._ExtentIDs.Length; }
            }

            public int[] ExtentIDs
            {
                get { return this._ExtentIDs; }
            }

            public long RowStart
            {
                get { return this._rowstart; }
            }

        }

        protected sealed class ThreadedSetEnumerable : IEnumerable<Extent>, System.Collections.IEnumerable
        {

            private ThreadedSetEnumerator _e;

            public ThreadedSetEnumerable(Table Data, int ThreadID, int ThreadCount)
            {
                _e = new ThreadedSetEnumerator(Data, ThreadCount, ThreadID);
            }

            IEnumerator<Extent> IEnumerable<Extent>.GetEnumerator()
            {
                return _e;
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return _e;
            }

            public long ExtentCount
            {
                get { return this._e.ExtentCount; }
            }

            public int[] ExtentIDs
            {
                get { return this._e.ExtentIDs; }
            }

            public long BeginRowID
            {
                get { return this._e.RowStart; }
            }

        }

        protected sealed class HeaderEnumerator : IEnumerator<Header>, System.Collections.IEnumerator, IDisposable
        {

            private int _idx = -1;
            private Table _ref;

            public HeaderEnumerator(Table Data)
            {
                this._ref = Data;
            }

            public bool MoveNext()
            {
                this._idx++;
                return this._idx < this._ref.ExtentCount;
            }

            public void Reset()
            {
                this._idx = -1;
            }

            Header IEnumerator<Header>.Current
            {
                get
                {
                    return this._ref.RenderHeader(this._idx);
                }
            }

            object System.Collections.IEnumerator.Current
            {
                get
                {
                    return this._ref.RenderHeader(this._idx);
                }
            }

            public void Dispose()
            {
            }

        }

        protected sealed class HeaderEnumerable : IEnumerable<Header>, System.Collections.IEnumerable
        {

            private HeaderEnumerator _e;

            public HeaderEnumerable(Table Data)
            {
                _e = new HeaderEnumerator(Data);
            }

            IEnumerator<Header> IEnumerable<Header>.GetEnumerator()
            {
                return _e;
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return _e;
            }

        }

        protected sealed class RecordEnumerator : IEnumerator<Record>, System.Collections.IEnumerator, IDisposable
        {

            private int _record_ptr = -1;
            private int _extent_ptr = 0;
            private Table _t;
            private Extent _e;

            public RecordEnumerator(Table Data)
            {
                this._t = Data;
                this._e = this._t.PopFirst();
            }

            public bool MoveNext()
            {

                if (this._e.Count == 0)
                    return false;

                this._record_ptr++;
                if (this._record_ptr >= this._e.Count && this._extent_ptr < this._t.ExtentCount - 1)
                {
                    this._record_ptr = 0;
                    this._extent_ptr++;
                    this._e = this._t.GetExtent(this._extent_ptr);
                    return true;
                }
                else if (this._extent_ptr >= this._e.Count)
                {
                    return false;
                }
                else if (this._record_ptr >= this._e.Count)
                {
                    return false;
                }
                else
                {
                    return true;
                }
                
            }

            public void Reset()
            {
                this._extent_ptr = 0;
                this._record_ptr = -1;
            }

            Record IEnumerator<Record>.Current
            {

                get
                {
                    return this._e[this._record_ptr];
                }

            }

            object System.Collections.IEnumerator.Current
            {

                get
                {
                    return this._e[this._record_ptr];
                }

            }

            public void Dispose()
            {
            }

        }

        protected sealed class RecordIEnumerable : IEnumerable<Record>, System.Collections.IEnumerable
        {

            private RecordEnumerator _e;

            public RecordIEnumerable(Table Data)
            {
                _e = new RecordEnumerator(Data);
            }

            IEnumerator<Record> IEnumerable<Record>.GetEnumerator()
            {
                return _e;
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return _e;
            }

        }

        protected sealed class TableRecordReader : RecordReader
        {

            private Table _ParentData;
            private int _ptrData = 0;

            // Constructor //
            public TableRecordReader(Table From, Register MemoryLocation, Filter Where)
                : base()
            {
            
                this._ptrData = DEFAULT_POINTER;
                this._ParentData = From;
                this._Data = From.PopFirstOrGrow();
                this._Where = Where;
                this._Memory = MemoryLocation;
                if (!Where.Default)
                {
                    this._IsFiltered = true;
                    while (!this.CheckFilter && !this.EndOfData)
                        this.Advance();
                }

            }

            public TableRecordReader(Table From, Register MemoryLocation)
                : this(From, MemoryLocation, Filter.TrueForAll)
            {
            }

            // Properties //
            public override bool EndOfData
            {
                get
                {
                    return this.EndOfCache && this.EndOfExtent || this._ParentData.ExtentCount == 0;
                }
            }

            public bool EndOfExtent
            {
                get
                {
                    return this._ptrRecord >= this._Data.Count;
                }
            }

            public bool EndOfCache
            {
                get
                {
                    return this._ptrData >= this._ParentData.ExtentCount;
                }
            }

            public int ExtentPosition
            {
                get { return this._ptrData; }
            }

            public override bool BeginingOfData
            {
                get
                {
                    return base.BeginingOfData && this._ptrData == 0;
                }
            }

            // Methods //
            public override void UnFilteredAdvance()
            {

                //base.UnFilteredAdvance();
                this._ptrRecord += INCREMENTER;
            
                // If we are at the end of the extent, but not the cache, buffer the next extent //
                if (this.EndOfExtent && !this.EndOfCache)
                {

                    // Increment Pointer //
                    this._ptrData += INCREMENTER;

                    // Exit if at the end of the cache //
                    if (!this.EndOfCache)
                    {

                        // Advance and pop //
                        this._Data = this._ParentData.GetExtent(this._ptrData);

                        // Reset the record pointer //
                        this._ptrRecord = 0;

                    }

                }

                // Allocate memory for the where structure //
                if (!this.EndOfData)
                {
                    this._Memory.Value = this.Read();
                }

            }

        }

        protected sealed class ThreadedTableRecordReader : RecordReader
        {

            private Table _ParentData;
            private int[] _ExtentIDs;
            private int _ptrData = 0;

            // Constructor //
            public ThreadedTableRecordReader(Table From, int[] IDs, Register MemoryLocation, Filter Where)
                : base()
            {
            
                this._ptrData = DEFAULT_POINTER;
                this._ParentData = From;
                this._Data = From.PopFirstOrGrow();
                this._Where = Where;
                this._Memory = MemoryLocation;
                this._ExtentIDs = IDs;
                if (!Where.Default)
                {
                    this._IsFiltered = true;
                    while (!this.CheckFilter && !this.EndOfData)
                        this.Advance();
                }

            }

            public ThreadedTableRecordReader(Table From, int[] IDs, Register MemoryLocation)
                : this(From, IDs, MemoryLocation, Filter.TrueForAll)
            {
            }

            // Properties //
            public override bool EndOfData
            {
                get
                {
                    return this.EndOfCache && this.EndOfExtent || this._ParentData.ExtentCount == 0;
                }
            }

            public bool EndOfExtent
            {
                get
                {
                    return this._ptrRecord >= this._Data.Count;
                }
            }

            public bool EndOfCache
            {
                get
                {
                    return this._ptrData >= this._ExtentIDs.Length;
                }
            }

            public int ExtentPosition
            {
                get 
                { 
                    return this._ptrData; 
                }
            }

            public override bool BeginingOfData
            {
                get
                {
                    return base.BeginingOfData && this._ptrData == 0;
                }
            }

            // Methods //
            public override void UnFilteredAdvance()
            {

                //base.UnFilteredAdvance();
                this._ptrRecord += INCREMENTER;
            
                // If we are at the end of the extent, but not the cache, buffer the next extent //
                if (this.EndOfExtent && !this.EndOfCache)
                {

                    // Increment Pointer //
                    this._ptrData++;

                    // Exit if at the end of the cache //
                    if (!this.EndOfCache)
                    {

                        // Advance and pop //
                        int id = this._ExtentIDs[this._ptrData];
                        this._Data = this._ParentData.GetExtent(id);

                        // Reset the record pointer //
                        this._ptrRecord = 0;

                    }

                }

                // Allocate memory for the where structure //
                if (!this.EndOfData)
                {
                    this._Memory.Value = this.Read();
                }

            }


        }

        protected sealed class TableExtentCollection : ExtentCollection
        {

            private SetEnumerable _base;
            private Table _source;

            public TableExtentCollection(Table Data)
                : base()
            {
                this._base = new SetEnumerable(Data);
                this._source = Data;
            }

            public override long ExtentCount
            {
                get { return this._base.ExtentCount; }
            }

            public override IEnumerable<Extent> Extents
            {
                get { return this._base; }
            }

            public override long BeginRowID
            {
                get { return 0; }
            }

            public override void SetExtent(Extent Data)
            {
                this._source.SetExtent(Data);
            }

            public override Extent GetExtent(int Index)
            {
                return this._source.GetExtent(Index);
            }

            public override Schema Columns
            {
                get 
                { 
                    return this._source.Columns; 
                }
            }

            public override RecordReader OpenReader(Register MemoryLocation)
            {
                return new TableRecordReader(this._source, MemoryLocation);
            }

            public override RecordReader OpenReader(Register MemoryLocation, Filter Where)
            {
                return new TableRecordReader(this._source, MemoryLocation, Where);
            }

        }

        protected sealed class ThreadedTableExtentCollection : ExtentCollection
        {

            private ThreadedSetEnumerable _base;
            private Table _source;

            public ThreadedTableExtentCollection(Table Data, int ThreadID, int ThreadCount)
                : base()
            {

                this._base = new ThreadedSetEnumerable(Data, ThreadCount, ThreadID);
                this._source = Data;

                int FirstID = this._base.ExtentIDs.First();
                int LastID = this._base.ExtentIDs.Last();

            }

            public override long ExtentCount
            {
                get { return this._base.ExtentCount; }
            }

            public override Schema Columns
            {
                get
                {
                    return this._source.Columns;
                }
            }

            public override IEnumerable<Extent> Extents
            {
                get { return this._base; }
            }

            public override long BeginRowID
            {
                get { return this._base.BeginRowID; }
            }

            public override void SetExtent(Extent Data)
            {
                this._source.SetExtent(Data);
            }

            public override Extent GetExtent(int Index)
            {

                if (Index >= this.ExtentCount)
                    throw new ArgumentOutOfRangeException("Extent index is out of range");
                int idx = this._base.ExtentIDs[Index];
                return this._source.GetExtent(idx);

            }

            public override RecordReader OpenReader(Register MemoryLocation)
            {
                return new ThreadedTableRecordReader(this._source, this._base.ExtentIDs, MemoryLocation);
            }

            public override RecordReader OpenReader(Register MemoryLocation, Filter Where)
            {
                return new ThreadedTableRecordReader(this._source, this._base.ExtentIDs, MemoryLocation, Where);
            }

        }

        protected sealed class TableVolume : Volume
        {

            private int[] _ExtentIDs;
            private long _FirstRowID;
            private long _LastRowID;
            private long _RecordCount;
            private Table _T;
            private Key _SortBy;

            public TableVolume(Table T, int ThreadID, int ThreadCount)
                :base(ThreadID)
            {
                
                this._T = T;

                int TotalExtents = (int)T.ExtentCount;
                int Remainder = TotalExtents % ThreadCount;
                int BucketCount = TotalExtents / ThreadCount;
                int RemainderCounter = 0;
                int BeginAt = 0;
                int EndAt = 0;
                int CountOf = 0;

                for (int i = 0; i < ThreadID + 1; i++)
                {
                    BeginAt += CountOf;
                    CountOf = BucketCount + (RemainderCounter < Remainder ? 1 : 0);
                    RemainderCounter++;
                    EndAt = BeginAt + CountOf;
                }

                List<int> ids = new List<int>();
                for (int i = BeginAt; i < EndAt; i++)
                {
                    ids.Add(i);
                }
                this._ExtentIDs = ids.ToArray();

                // Get the count of all records up to this point //
                long counter = 0;
                long FirstID = this._ExtentIDs.First();
                long LastID = this._ExtentIDs.Last();
                foreach (Record r in T._Refs.Records)
                {

                    if (r[0].INT < FirstID)
                    {
                        counter += r[1].INT;
                    }

                    if (this._ExtentIDs.Contains((int)r[0].INT))
                    {
                        this._RecordCount += r[1].INT;
                    }

                }

                this._FirstRowID = counter;
                this._LastRowID = this._FirstRowID + this._RecordCount;

                // Set sort key //
                this._SortBy = T._OrderBy;
                
            }

            public override Schema Columns
            {
                get
                {
                    return this._T.Columns;
                }
            }

            public override Key SortKey
            {
                get
                {
                    return this._T.SortBy;
                }
            }

            public override DataSet Parent
            {
                get
                {
                    return this._T;
                }
            }

            public override long ExtentCount
            {
                get
                {
                    return (long)this._ExtentIDs.Length;
                }
            }

            public override long RecordCount
            {
                get
                {
                    return this._RecordCount;
                }
            }

            public override long FirstRowID
            {
                get
                {
                    return this._FirstRowID;
                }
            }

            public override long LastRowID
            {
                get
                {
                    return this._LastRowID;
                }
            }

            public override IEnumerable<Header> Headers
            {
                get
                {

                    List<Header> headers = new List<Header>();
                    foreach (int i in this._ExtentIDs)
                    {
                        headers.Add(this._T.RenderHeader(i));
                    }
                    return headers;

                }
            }

            public override IEnumerable<Extent> Extents
            {
                get
                {
                    return new TableVolumeExtentEnumerator(this._T, this._ExtentIDs);
                }
            }

            public override IEnumerable<Record> Records
            {
                get
                {
                    return new TableVolumeRecordEnumerator(this._T, this._ExtentIDs);
                }
            }

            public override Extent GetExtent(int Index)
            {
                int idx = this._ExtentIDs[Index];
                return this._T.GetExtent(idx);
            }

            public override void Sort(Key K)
            {
                Table.SortBase(this._T, K, this._ExtentIDs);
            }

            public override RecordReader OpenReader(Register Memory, Filter Predicate)
            {
                return new TableVolumeRecordReader(this._T, this._ExtentIDs, Memory, Predicate);
            }

            // -- Enumerators -- //
            private class TableVolumeHeaderEnumerator : IEnumerator<Header>, System.Collections.IEnumerator, IEnumerable<Header>, System.Collections.IEnumerable, IDisposable
            {

                private int _idx = -1;
                private Table _ref;
                private int[] _ids;

                public TableVolumeHeaderEnumerator(Table Data, int[] ExtentIDs)
                {
                    this._ref = Data;
                    this._ids = ExtentIDs;
                }

                public bool MoveNext()
                {
                    this._idx++;
                    return this._idx < this._ids.Length;
                }

                public void Reset()
                {
                    this._idx = -1;
                }

                Header IEnumerator<Header>.Current
                {
                    get
                    {
                        int i = this._ids[this._idx];
                        return this._ref.RenderHeader(i);
                    }
                }

                object System.Collections.IEnumerator.Current
                {
                    get
                    {
                        int i = this._ids[this._idx];
                        return this._ref.RenderHeader(i);
                    }
                }

                IEnumerator<Header> IEnumerable<Header>.GetEnumerator()
                {
                    return this;
                }

                System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
                {
                    return this;
                }

                public void Dispose()
                {
                }

            }

            private class TableVolumeExtentEnumerator : IEnumerator<Extent>, System.Collections.IEnumerator, IEnumerable<Extent>, System.Collections.IEnumerable, IDisposable
            {

                private int _idx = -1;
                private Table _ref;
                private int[] _ExtentIDs;

                public TableVolumeExtentEnumerator(Table Data, int[] ExtentIDs)
                {
                    this._ref = Data;
                    this._ExtentIDs = ExtentIDs;
                }

                public bool MoveNext()
                {
                    this._idx++;
                    return this._idx < this._ExtentIDs.Length;
                }

                public void Reset()
                {
                    this._idx = -1;
                }

                Extent IEnumerator<Extent>.Current
                {
                    get
                    {
                        int i = this._ExtentIDs[this._idx];
                        return this._ref.GetExtent(i);
                    }
                }

                object System.Collections.IEnumerator.Current
                {
                    get
                    {
                        int i = this._ExtentIDs[this._idx];
                        return this._ref.GetExtent(i);
                    }
                }

                IEnumerator<Extent> IEnumerable<Extent>.GetEnumerator()
                {
                    return this;
                }

                System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
                {
                    return this;
                }

                public void Dispose()
                {
                }

            }

            private class TableVolumeRecordEnumerator : IEnumerator<Record>, System.Collections.IEnumerator, IEnumerable<Record>, System.Collections.IEnumerable, IDisposable
            {

                private int _record_ptr = -1;
                private int _extent_ptr = 0;
                private Table _t;
                private Extent _e;
                private int[] _ExtentIDs;

                public TableVolumeRecordEnumerator(Table Data, int[] ExtentIDs)
                {
                    this._t = Data;
                    this._e = this._t.PopFirst();
                    this._ExtentIDs = ExtentIDs;
                }

                public bool MoveNext()
                {

                    if (this._e.Count == 0)
                        return false;

                    this._record_ptr++;
                    if (this._record_ptr >= this._e.Count && this._extent_ptr < this._ExtentIDs.Length - 1)
                    {
                        this._record_ptr = 0;
                        this._extent_ptr++;
                        int id = this._ExtentIDs[this._extent_ptr];
                        this._e = this._t.GetExtent(id);
                        return true;
                    }
                    else if (this._extent_ptr >= this._e.Count)
                    {
                        return false;
                    }
                    else if (this._record_ptr >= this._e.Count)
                    {
                        return false;
                    }
                    else
                    {
                        return true;
                    }

                }

                public void Reset()
                {
                    this._extent_ptr = 0;
                    this._record_ptr = -1;
                }

                Record IEnumerator<Record>.Current
                {

                    get
                    {
                        return this._e[this._record_ptr];
                    }

                }

                object System.Collections.IEnumerator.Current
                {

                    get
                    {
                        return this._e[this._record_ptr];
                    }

                }

                IEnumerator<Record> IEnumerable<Record>.GetEnumerator()
                {
                    return this;
                }

                System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
                {
                    return this;
                }

                public void Dispose()
                {
                }

            }

            private class TableVolumeRecordReader : RecordReader
            {

                private Table _ParentData;
                private int[] _ExtentIDs;
                private int _ptrData = 0;

                // Constructor //
                public TableVolumeRecordReader(Table From, int[] ExtentIDs, Register MemoryLocation, Filter Where)
                    : base()
                {

                    this._ptrData = DEFAULT_POINTER;
                    this._ParentData = From;
                    this._Data = From.PopFirstOrGrow();
                    this._Where = Where;
                    this._Memory = MemoryLocation;
                    this._ExtentIDs = ExtentIDs;

                    // Check if the Extent is empty, if it is then point the current record to 1 so we immediately force the reader to be at the end of the data //
                    if (this._Data.Count == 0)
                    {
                        this._ptrRecord++;
                        this._ptrData++;
                        return;
                    }

                    // Check the filter //
                    if (!Where.Default)
                    {
                        this._IsFiltered = true;
                        while (!this.CheckFilter && !this.EndOfData)
                            this.Advance();
                    }

                }

                // Properties //
                public override bool EndOfData
                {
                    get
                    {
                        return this.EndOfCache && this.EndOfExtent || this._ParentData.ExtentCount == 0;
                    }
                }

                public bool EndOfExtent
                {
                    get
                    {
                        return this._ptrRecord >= this._Data.Count;
                    }
                }

                public bool EndOfCache
                {
                    get
                    {
                        return this._ptrData >= this._ExtentIDs.Length;
                    }
                }

                public int ExtentPosition
                {
                    get
                    {
                        return this._ptrData;
                    }
                }

                public override bool BeginingOfData
                {
                    get
                    {
                        return base.BeginingOfData && this._ptrData == 0;
                    }
                }

                // Methods //
                public override void UnFilteredAdvance()
                {

                    //base.UnFilteredAdvance();
                    this._ptrRecord += INCREMENTER;

                    // If we are at the end of the extent, but not the cache, buffer the next extent //
                    if (this.EndOfExtent && !this.EndOfCache)
                    {

                        // Increment Pointer //
                        this._ptrData++;

                        // Exit if at the end of the cache //
                        if (!this.EndOfCache)
                        {

                            // Advance and pop //
                            int id = this._ExtentIDs[this._ptrData];
                            this._Data = this._ParentData.GetExtent(id);

                            // Reset the record pointer //
                            this._ptrRecord = 0;

                        }

                    }

                    // Allocate memory for the where structure //
                    if (!this.EndOfData)
                    {
                        this._Memory.Value = this.Read();
                    }

                }

            }

        }

    }

    public abstract class ExtentCollection 
    {

        public abstract long ExtentCount { get; }

        public abstract IEnumerable<Extent> Extents { get; }

        public abstract long BeginRowID { get; }

        public abstract void SetExtent(Extent Data);

        public abstract Extent GetExtent(int Index);

        public abstract Schema Columns { get; }

        public abstract RecordReader OpenReader(Register MemoryLocation);

        public abstract RecordReader OpenReader(Register MemoryLocation, Filter Where);

    }

    public abstract class Volume
    {

        protected int _ThreadID;

        public Volume(int ThreadID)
        {
            this._ThreadID = ThreadID;
        }

        public abstract Schema Columns { get; }

        public abstract Key SortKey { get; }

        public abstract DataSet Parent { get; }

        public int ThreadID
        {
            get { return this._ThreadID; }
        }

        public abstract long ExtentCount { get; }

        public abstract long RecordCount { get; }

        public abstract long FirstRowID { get; }

        public abstract long LastRowID { get; }

        public abstract IEnumerable<Header> Headers { get; }

        public abstract IEnumerable<Extent> Extents { get; }

        public abstract IEnumerable<Record> Records { get; }

        public abstract Extent GetExtent(int Index);

        public abstract RecordReader OpenReader(Register Memory, Filter Predicate);

        public virtual RecordReader OpenReader(Register Memory)
        {
            return this.OpenReader(Memory, Filter.TrueForAll);
        }

        public abstract void Sort(Key K);

        public bool IsSortedBy(Key K)
        {
            if (K == null || this.SortKey == null)
                return false;
            return Key.SubsetStrong(this.SortKey, K);
        }

    }

    public class RecordReader
    {

        protected const int DEFAULT_POINTER = 0;
	    protected const int INCREMENTER = 1;

	    protected int _ptrRecord = DEFAULT_POINTER;
	    protected Extent _Data;
	    protected Filter _Where;
        protected Register _Memory;
        protected bool _IsFiltered = false;
        
	    // Constructor //
        protected RecordReader()
        {
        }

	    public RecordReader(Extent From, Register MemoryLocation, Filter Where)
            :this()
	    {
		
		    this._ptrRecord = DEFAULT_POINTER;
		    this._Data = From;
		    this._Where = Where;
            this._Memory = MemoryLocation;
            
            // Check if the Extent is empty, if it is then point the current record to 1 so we immediately force the reader to be at the end of the data //
            if (From.Count == 0)
            {
                this._ptrRecord++;
                return;
            }

            // Initialize the memory register for the advance //
            this._Memory.Value = this.Read();

            // Fix the default //
            if (!Where.Default)
		    {
			    this._IsFiltered = true;
                while (!this.CheckFilter && !this.EndOfData)
                    this.Advance();
		    }

	    }
            
	    public RecordReader(Extent From, Register MemoryLocation)
            :this(From, MemoryLocation, Filter.TrueForAll)
	    {
	    }
	
	    // Properties //
        public virtual bool EndOfData
	    {
            get
            {
                return (this._ptrRecord >= this._Data.Count);
            }
	    }

        public virtual bool BeginingOfData
        {
            get
            {
                return this._ptrRecord == 0;
            }
        }

        public virtual bool CheckFilter
        {
            get
            {
                if (this.EndOfData == true) 
                    return false;
                return this._Where.Render();
            }
        }

        public virtual int Position
        {
            get { return this._ptrRecord; }
        }

        public virtual long SetID
        {
            get 
            {
                return this._Data.Header.ID;
            }
        }

	    // Methods //
        public virtual void UnFilteredAdvance()
	    {
		    
            this._ptrRecord += INCREMENTER;
            if (!this.EndOfData)
            {
                this._Memory.Value = this.Read();
            }

	    }

        public virtual void FilteredAdvance()
	    {
            // Break if end of stream //
            if (this.EndOfData == true)
                return;
            // While the filter is false, advance, but advance at lease once //
            do
                this.UnFilteredAdvance();
            while (this.CheckFilter == false && this.EndOfData == false);
	    }

        public virtual void Advance()
	    {
		
		    if (this._IsFiltered == false)
                this.UnFilteredAdvance();
		    else
                this.FilteredAdvance();
		
	    }

        // Reads //
        public virtual Record Read()
	    {
            return this._Data[this._ptrRecord];
	    }

        public virtual Record ReadNext()
	    {
		    Record cr = this.Read();
		    this.Advance();
		    return cr;
	    }

    }

    public sealed class ModeStep
    {

        private Volume _V;
        private Register _Memory;
        private Extent _E;
        private Record _NullRecord;

        private int _ptr_CurrentExtent;
        private int _ptr_CurrentRecord;
        private int _TotalExtentCount;
        private int _CurrentExtentRecordCount;

        public ModeStep(Volume V, Register Memory)
        {

            if (V.ExtentCount == 0)
                throw new ArgumentException("Cannot create a ModeStep with no extents");

            this._V = V;
            this._Memory = Memory;
            this._TotalExtentCount = (int)V.ExtentCount;

            this._E = V.GetExtent(0);
            this._CurrentExtentRecordCount = (int)this._E.RecordCount;
            this._Memory.Value = this._E[this._ptr_CurrentRecord];
            this._NullRecord = V.Columns.NullRecord;

        }

        public bool AtOrigin
        {

            get
            {
                return this._ptr_CurrentExtent == 0 || this._ptr_CurrentRecord == 0;
            }

        }

        public bool AtEnd
        {
            get
            {
                return this._ptr_CurrentExtent >= this._TotalExtentCount;
            }
        }

        public Register Memory
        {
            get { return this._Memory; }
        }

        public int RecordPosition
        {
            get { return this._ptr_CurrentRecord; }
        }

        public int ExtentPosition
        {
            get { return this._ptr_CurrentExtent; }
        }

        public void Advance()
        {

            this._ptr_CurrentRecord++;
            if (this._ptr_CurrentRecord >= this._CurrentExtentRecordCount)
            {

                this._ptr_CurrentExtent++;
                if (this._ptr_CurrentExtent < this._TotalExtentCount)
                {

                    this._E = this._V.GetExtent(this._ptr_CurrentExtent);
                    this._CurrentExtentRecordCount = this._E.Count;
                    this._ptr_CurrentRecord = 0;

                }
                else
                {
                    this._E = null;
                }

            }

            if (this._ptr_CurrentRecord < this._CurrentExtentRecordCount && this._E != null)
            {
                this._Memory.Value = this._E[this._ptr_CurrentRecord];
            }

        }

        public void Advance(int Count)
        {

            for (int i = 0; i < Count; i++)
            {
                this.Advance();
                if (this.AtEnd)
                    return;
            }

        }

        public void Revert()
        {

            this._ptr_CurrentRecord--;
            if (this._ptr_CurrentRecord < 0)
            {

                this._ptr_CurrentExtent--;
                if (this._ptr_CurrentExtent >= 0)
                {

                    this._E = this._V.GetExtent(this._ptr_CurrentExtent);
                    this._CurrentExtentRecordCount = this._E.Count;
                    this._ptr_CurrentRecord = this._E.Count - 1;

                }
                else
                {
                    this._E = null;
                }

            }

            if (this._ptr_CurrentRecord >= 0 && this._E != null)
            {
                this._Memory.Value = this._E[this._ptr_CurrentRecord];
            }


        }

        public void Revert(int Count)
        {

            for (int i = 0; i < Count; i++)
            {
                this.Revert();
                if (this.AtOrigin)
                    return;
            }

        }

    }

}