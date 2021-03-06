﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Expressions;

namespace Rye.Data
{
    
    public abstract class TabularData
    {

        public const long DEFAULT_PAGE_SIZE = 1024 * 512;

        // Properties //
        public abstract Schema Columns { get; }

        public abstract Key SortBy { get; set; }

        public bool IsSorted 
        { 
            get 
            { 
                return this.SortBy.Count != 0; 
            }
        }

        public abstract long ExtentCount { get; }

        public abstract long RecordCount { get; }

        public abstract Header Header { get; set; }

        public abstract string InfoString
        {
            get;
        }

        public abstract IConcurrentWriteManager ConcurrentWriteManager
        {
            get;
        }

        // Methods //
        public abstract Volume CreateVolume();

        public abstract Volume CreateVolume(int ThreadID, int ThreadCount);
        
        public bool IsSortedBy(Key K)
        {
            if (K == null || this.SortBy == null)
                return false;
            return Key.SubsetStrong(this.SortBy, K);
        }

        public abstract void PreSerialize();

        public abstract RecordWriter OpenWriter();

        public abstract RecordWriter OpenUncheckedWriter(int Key);

        internal virtual void CursorClose()
        {
            // do nothing
        }

        // Costs //
        public abstract long CellCount
        {
            get;
        }

        public abstract int MemCost { get; }

        public abstract int DiskCost { get; }

    
    }

    public sealed class Extent : TabularData
    {

        internal const long ESTIMATE_META_DATA = 4096; // estimate 4 kb in meta data
        
        internal List<Record> _Cache;
        private Schema _Columns;
        private Header _Head;
        private Key _OrderBy;
        private long _MaxRecords = 0;
        private ConcurrentExtentWriteManager _WriteManager;
        
        // Constructor //
        public Extent(Schema NewColumns, Header NewHeader, List<Record> NewCache, Key NewOrderBy)
        {

            this._Columns = NewColumns;
            this._Cache = NewCache;
            this._OrderBy = NewOrderBy;
            this._Head = NewHeader;
            this._MaxRecords = NewHeader.PageSize / NewColumns.RecordDiskCost;
            this._WriteManager = new ConcurrentExtentWriteManager(this);

        }

        public Extent(Schema NewColumns, Header NewHeader)
            : this(NewColumns, NewHeader, new List<Record>(), new Key())
        {

        }

        public Extent(Schema NewColumns)
            : this(NewColumns, Header.NewMemoryOnlyExtentHeader("EXTENT", NewColumns.Count, DEFAULT_PAGE_SIZE))
        {
        }

        public Extent(Schema NewColumns, long PageSize)
            : this(NewColumns, Header.NewMemoryOnlyExtentHeader("EXTENT", NewColumns.Count, PageSize))
        {
        }
        
        public Extent(string Directory, string Name, Schema S, long PageSize, string Extension)
            : this(S, Header.NewPageHeader(Directory, Name, 0, S, PageSize, Extension))
        {
        }

        // TabularData Override Properties //
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
            set
            {
                this._OrderBy = value;
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
                return this.Count == this._MaxRecords;
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

        public List<Record> Cache
        {
            get
            {
                return this._Cache;
            }
        }

        public Extent EmptyClone()
        {
            return new Extent(this._Columns, this._Head, new List<Record>(), this._OrderBy);
        }

        public long MaxRecordEstimate
        {
            get { return this._Head.PageSize / this._Columns.RecordDiskCost; }
        }

        public override string InfoString
        {
            get 
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(string.Format("Name: {0}", this.Header.Name));
                sb.Append(string.Format("DataPageX PageSize: {0}", this.Header.PageSize));
                sb.Append(string.Format("Record Count: {0}", this.Header.RecordCount));
                sb.Append(string.Format("Disk Location: {0}", this.Header.IsMemoryOnly ? "<Memory>" : this.Header.Path));
                sb.Append(string.Format("Directory: {0}", this.Header.IsMemoryOnly ? "<Memory>" : this.Header.Directory));
                sb.Append("Columns:");
                for (int i = 0; i < this.Columns.Count; i++)
                {
                    sb.Append(string.Format("{0} : {1} : {2}", this.Columns.ColumnName(i), this.Columns.ColumnAffinity(i), this.Columns.ColumnSize(i)));
                }
                return sb.ToString();

            }
        }

        public override IConcurrentWriteManager ConcurrentWriteManager
        {
            get 
            { 
                return this._WriteManager; 
            }
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

        /// <summary>
        /// Adds a record to the extent; will throw an exception if the extent is full; will re-cast record elements if they violate the schema
        /// </summary>
        /// <param name="Data">The record to add</param>
        public void Add(Record Data)
        {
            
            if (this.IsFull) 
                throw new Exception("RecordSet is full");
            if (!this.Columns.Check(Data, true))
                throw new Exception("Record passed does not match schema");
            this._Cache.Add(Data);

        }

        /// <summary>
        /// Adds a record to the extent; will throw an exception if the extent is full; does not re-format the records
        /// </summary>
        /// <param name="Data">The record to add</param>
        public void UncheckedAdd(Record Data)
        {
            if (this.IsFull)
                throw new Exception("RecordSet is full");
            if (Data == null)
                throw new ArgumentNullException();
            this._Cache.Add(Data);

        }

        /// <summary>
        /// Adds a record to the extent; does not check if the extent is full or re-format the records
        /// </summary>
        /// <param name="Data">The record to add</param>
        public void UnsafeAdd(Record Data)
        {
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

        public override RecordWriter OpenUncheckedWriter(int Key)
        {
            if (Key == int.MaxValue || Key == this._Columns.GetHashCode())
                return new UncheckedExtentWriter(this);
            return new ExtentWriter(this);
        }
        
        internal override void CursorClose()
        {
            this._WriteManager.Collapse();
        }

        // Statics //
        public static void SetCache(Extent DataToSet, Extent DataToImport)
        {

            if (DataToImport.Columns.GetHashCode() != DataToSet.Columns.GetHashCode())
                throw new ArgumentException("The schema for both extents don't match");
            DataToSet._Cache = DataToImport._Cache;

        }

        // Private classes //
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
                long l = SortMaster.Sort(this._E, K);
            }

            public override TabularData Parent
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
                get { return (long)this._E.Count; }
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

    public class Table : TabularData
    {

        protected const int OFFSET_ID = 0;
        protected const int OFFSET_COUNT = 1;
        public const int RECORD_LEN = 2;

        //protected List<Header> _Data;
        protected Extent _Refs; // The move to virtual headers is designed to reduce the memory footprint and make IO faster
        protected Schema _Columns;
        protected Header _Head;
        protected Key _OrderBy;
        protected object _lock = new object();
        protected Kernel _IO;
        protected IConcurrentWriteManager _WriteManager;

        // Constructors //
        public Table(Kernel K, Header H, Schema S, List<Record> R, Key SortedKeySet)
        {

            Header t = Rye.Data.Header.NewMemoryOnlyExtentHeader(H.Name, S.Count, Extent.DEFAULT_PAGE_SIZE);

            this._Columns = S;
            this._Head = H;
            this._OrderBy = SortedKeySet;
            this._Refs = new Extent(new Schema("ID INT, COUNT INT"), t, R, SortedKeySet);
            this._IO = K;
            this._WriteManager = new ConcurrentTableWriteManager(this);

        }

        protected Table(Kernel IO, Header Location, Schema Columns, bool Flush)
        {

            Header t = Rye.Data.Header.NewMemoryOnlyExtentHeader(Location.Name, 2, TabularData.DEFAULT_PAGE_SIZE);

            this._Columns = Columns;
            this._Head = Location;
            this._OrderBy = new Key();
            this._Refs = new Extent(new Schema("ID INT, COUNT INT"), t);
            this._IO = IO;

            // Must drop the current table first //
            this._IO.RequestDropTable(Location.Path);
                
            if (Flush)
            {
                this._IO.RequestFlushTable(this);
            }
            this._WriteManager = new ConcurrentTableWriteManager(this);

        }

        public Table(Kernel IO, Header Location, Schema Columns)
            : this(IO, Location, Columns, true)
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
            set
            {
                this._OrderBy = value;
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

        public Kernel IO
        {
            get { return this._IO; }
            set { this._IO = value; }
        }

        public override IConcurrentWriteManager ConcurrentWriteManager
        {
            get 
            { 
                return this._WriteManager; 
            }
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
        public virtual void RequestFlushMe()
        {
            this._IO.RequestFlushTable(this);
        }

        public virtual Header RenderHeader(int Index)
        {
            long id = this._Refs[Index][OFFSET_ID].INT;
            Header h = this.Header.CreateChild(id);
            h.RecordCount = this.GetRecordCount((long)Index);
            return h;
        }

        public virtual Extent ReferenceTable
        {
            get { return this._Refs; }
        }

        public virtual long GetRecordCount(long ID)
        {
            return this._Refs[(int)ID][(int)OFFSET_COUNT].valueINT;
        }

        /* ## Thread Safe ##*/
        public virtual void AddRefRecord(long ID, long RecordCount)
        {

            lock (this._lock)
            {
                this._Refs.Add(Record.Stitch(new Cell(ID), new Cell(RecordCount)));
                this._Head.BigRecordCount += RecordCount;
                this._Head.RecordCount = (long)this._Refs.Count;
            }

        }

        /* ## Thread Safe ##*/
        public virtual void UpdateRefRercord(long ID, long RecordCount)
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
                //Header h = this.RenderHeader(Index);

                // Buffer //
                return this._IO.RequestBufferExtent(this, Index);

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
        /// <param name="TabularData"></param>
        public virtual void SetExtent(Extent Data)
        {

            lock (this._lock)
            {

                // Check a few things: the table is attached, columns match, name matches, and the ID exists in the current table //
                if (Data.IsMemoryOnly)
                    throw new ArgumentException("The Shard passed is not attached; use the 'AddExtent' method to add a disjoint Shard");
                if (Data.Columns.GetHashCode() != this.Columns.GetHashCode())
                    throw new ArgumentException("The schema passed for the current record set does not match the ShartTable");
                if (Data.Header.Name != this.Header.Name)
                    throw new ArgumentException("The 'name' of this Shard does not match the parent; use the 'AddExtent' method to add a disjoint Shard");
                if (this.Header.ID >= this.ExtentCount)
                    throw new ArgumentException("The ID of this Shard does not match the parent; use the 'AddExtent' method to add a disjoint Shard");
                if (this._Refs[(int)Data.Header.ID][OFFSET_ID].INT != (int)Data.Header.ID)
                    throw new ArgumentException("The ID of this Shard does not match the parent; use the 'AddExtent' method to add a disjoint Shard");

                // Get the current record count //
                this.UpdateRefRercord(Data.Header.ID, Data.RecordCount);

                // Flush //
                this._IO.RequestFlushExtent(Data);

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
                    throw new Exception("The schema passed for the current record set does not match the ShartTable");
                if (Data.Header.PageSize != this.Header.PageSize)
                    throw new Exception("The data set passed is too large");
                if (Data.Count == 0)
                    throw new Exception("Can't add an empty extent");

                // Call the pre-serializer to ensure the record count is correct //
                Data.PreSerialize();

                // Need to create a new header //
                long id = this._Refs.Count;
                Header h = this.Header.CreateChild(id);
                //Header h = new Header(this.Header.Directory, this.Header.Name, id, (long)Data.Columns.Count, Data.RecordCount, this._OrderBy.Count, this.MaxRecords, 0, HeaderType.Shard);
                Data.Header = h;

                // Accumulate and dump //
                this.AddRefRecord(id, Data.Count);
                this._IO.RequestFlushExtent(Data);

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
                Header h = this.Header.CreateChild(id);
                
                Extent e = new Extent(this.Columns, h);

                // Accumulate to our cache //
                this.AddRefRecord(id, 0);

                // Dump //
                this._IO.RequestFlushExtent(e);

                // Return //
                return e;

            }

        }

        public virtual Extent NewShell()
        {

            Header h = this.Header.CreateChild(0);
            Extent e = new Extent(this.Columns, h);
            e.Header.PageSize = this.Header.PageSize;

            return e;

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

        /* ## Thread Safe ##*/
        public override void PreSerialize()
        {
            lock (this._lock)
            {
                this._Head.KeyCount = this._OrderBy.Count;
                this._Head.Stamp();
            }
        }

        /* ## Thread Safe ##*/
        internal override void CursorClose()
        {

            lock (this._lock)
            {
                this._WriteManager.Collapse();
                this._IO.RequestFlushTable(this);
            }

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
        
        public override RecordWriter OpenUncheckedWriter(int Key)
        {
            if (Key == int.MaxValue || Key == this._Columns.GetHashCode())
                return new UncheckedTableWriter(this);
            return new TableWriter(this);
        }

        public override string InfoString
        {

            get
            {

                StringBuilder sb = new StringBuilder();
                sb.AppendLine(string.Format("Name: {0}", this.Header.Name));
                sb.AppendLine(string.Format("DataPageX PageSize: {0}", this.Header.PageSize));
                sb.AppendLine(string.Format("Path: {0}", this.Header.Path));
                sb.AppendLine(string.Format("Memory Cost: {0}", this.MemCost));
                sb.AppendLine(string.Format("Disk Cost: {0}", this.DiskCost));
                sb.AppendLine(string.Format("Record Count: {0}", this.RecordCount));
                sb.AppendLine(string.Format("Extent Count: {0}", this.ExtentCount));

                sb.AppendLine(string.Format("Columns:"));
                for (int i = 0; i < this.Columns.Count; i++)
                {
                    sb.AppendLine(string.Format("\t{0} : {1} : {2}", this.Columns.ColumnName(i), this.Columns.ColumnAffinity(i), this.Columns.ColumnSize(i)));
                }

                sb.AppendLine(string.Format("Extent Map:"));
                for (int i = 0; i < this._Refs.Count; i++)
                {
                    sb.AppendLine(string.Format("\t{0} : {1} ", this._Refs[i][0], this._Refs[i][1]));
                }

                if (this.IsSorted)
                {
                    sb.AppendLine(string.Format("Sorted By: {0}", this.SortBy.ToString()));
                }
                else
                {
                    sb.AppendLine("Not Sorted");
                }

                return sb.ToString();
            }

        }

        // Private classes //
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

        protected sealed class TableVolume : Volume
        {

            private int[] _ExtentIDs;
            private long _Count;
            private Table _T;
            private Key _SortBy;
            
            public TableVolume(Table T, int ThreadID, int ThreadCount)
                :base(ThreadID)
            {
                
                this._T = T;
                
                this.IsEmpty = false;
                if (T.RecordCount == 0)
                {
                    this.IsEmpty = true;
                    this._ExtentIDs = new int[0];
                    return;
                }

                this._ExtentIDs = TableVolume.GetIDs(ThreadID, ThreadCount, (int)T.ExtentCount);
                foreach(int idx in this._ExtentIDs)
                {
                    this._Count += T.GetRecordCount(idx);
                }
                
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

            public override TabularData Parent
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
                get { return this._Count; }
            }

            public override IEnumerable<Extent> Extents
            {
                get
                {
                    return new TableVolumeExtentEnumerator(this._T, this._ExtentIDs);
                }
            }

            public override Extent GetExtent(int Index)
            {
                int idx = this._ExtentIDs[Index];
                return this._T.GetExtent(idx);
            }

            public override void Sort(Key K)
            {
                long l = SortMaster.Sort(this._T, K, this._ExtentIDs);
            }

            public override RecordReader OpenReader(Register Memory, Filter Predicate)
            {
                return new TableVolumeRecordReader(this._T, this._ExtentIDs, Memory, Predicate);
            }

            // -- Enumerators -- //
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
                    this._Data = From.GetExtent(ExtentIDs.First());
                    this._Where = Where;
                    this._Memory = MemoryLocation;
                    this._ExtentIDs = ExtentIDs;

                    // Check if the Shard is empty, if it is then point the current record to 1 so we immediately force the reader to be at the end of the data //
                    if (this._Data.Count == 0)
                    {
                        this._ptrRecord++;
                        this._ptrData++;
                        return;
                    }

                    // Initialize the memory register for the advance //
                    this._Memory.Value = this.Read();

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

            public static int[] GetIDs(int ThreadID, int TotalThreadCount, int ExtentCount)
            {

                int[] ExtentCounts = new int[TotalThreadCount];
                int[] ExtentStart = new int[TotalThreadCount];
                int idx = 0;

                for (int i = 0; i < ExtentCount; i++)
                {
                    ExtentCounts[i % TotalThreadCount]++;
                }

                for (int i = 0; i < TotalThreadCount; i++)
                {
                    ExtentStart[i] = idx;
                    idx += ExtentCounts[i];
                }

                int[] Indexes = new int[ExtentCounts[ThreadID]];
                for (int i = 0; i < Indexes.Length; i++)
                {
                    Indexes[i] = ExtentStart[ThreadID] + i;
                }

                return Indexes;

            }

        }

        public static Table CreateTable(Kernel IO, string Dir, string Name, Schema Columns, long PageSize)
        {

            Header h = Header.NewTableHeader(Dir, Name, Columns, PageSize, IO.DefaultExtension);
            return new Table(IO, h, Columns, true);

        }

        public static Table CreateTable(Kernel IO, string Dir, string Name, Schema Columns)
        {

            Header h = Header.NewTableHeader(Dir, Name, Columns, IO.DefaultPageSize, IO.DefaultExtension);
            return new Table(IO, h, Columns, true);

        }

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

        public abstract TabularData Parent { get; }

        public int ThreadID
        {
            get { return this._ThreadID; }
        }

        public bool IsEmpty
        {
            get;
            protected set;
        }

        public abstract long ExtentCount { get; }

        public abstract long RecordCount { get; }

        public abstract IEnumerable<Extent> Extents { get; }

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
            
            // Check if the Shard is empty, if it is then point the current record to 1 so we immediately force the reader to be at the end of the data //
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
                if (this.EndOfData == true || this._Memory.Value == null) 
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

            //if (V.ExtentCount == 0)
            //    throw new ArgumentException("Cannot create a ModeStep with no extents");

            this._V = V;
            this._Memory = Memory;
            this._TotalExtentCount = (int)V.ExtentCount;

            this._E = (V.ExtentCount == 0 ? new Extent(V.Columns) : V.GetExtent(0));
            this._CurrentExtentRecordCount = (int)this._E.RecordCount;
            this._Memory.Value = (this._CurrentExtentRecordCount == 0 ? V.Columns.NullRecord : this._E[this._ptr_CurrentRecord]);
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

        public bool IsEmpty
        {
            get { return this._V.ExtentCount == 0 || (this._V.ExtentCount == 1 && this._E.Count == 0); }
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
