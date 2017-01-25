using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace Rye.Data.Spectre
{

    // PageManager //
    /// <summary>
    /// Base class for record storage
    /// </summary>
    public class Page
    {

        public const int BASE_PAGE_TYPE = 0;
        public const int SORTED_PAGE_TYPE = 0;
        public const int BTREE_PAGE_TYPE = 0;

        /* Page Header:
         * 0-4: HashKey (always 1)
         * 4-4: PageID
         * 8-4: LastPageID
         * 12-4: NextPageID
         * 16-4: PageSize
         * 20-4: FieldCount
         * 24-4: RecordCount
         * 28-4: CheckSum
         * 32-4: PageType
         * 36-4: DataDiskCost
         * 40-24: Dead space (NOT IN USE)
         * 
         */
        public const int HASH_KEY = 1;
        public const int OFFSET_HASH_KEY = 0;
        public const int OFFSET_PAGE_ID = 4;
        public const int OFFSET_LAST_ID = 8;
        public const int OFFSET_NEXT_ID = 12;
        public const int OFFSET_SIZE = 16;
        public const int OFFSET_FCOUNT = 20;
        public const int OFFSET_RCOUNT = 24;
        public const int OFFSET_CHECKSUM = 28;
        public const int OFFSET_TYPE = 32;
        public const int OFFSET_DISK_COST = 36;
        public const int OFFSET_X0 = 40;
        public const int OFFSET_X1 = 44;
        public const int OFFSET_X2 = 48;
        public const int OFFSET_X3 = 52;
        public const int OFFSET_RECORD_TABLE = 64;
        public const int SIZE_ELEMENT = 4;

        /// <summary>
        /// The null index is always -1
        /// </summary>
        public const int NULL_INDEX = -1;

        /// <summary>
        /// Default size is 64k
        /// </summary>
        public const int DEFAULT_SIZE = 64 * 1024; // 64k

        /// <summary>
        /// The header size is 64 bytes
        /// </summary>
        public const int HEADER_SIZE = 64; // Currently using 40 bytes, 16 are taken by the 'X' variable and 8 are free for future use

        protected int _PageSize = DEFAULT_SIZE;
        protected int _PageID = 0;
        protected int _LastPageID = 0;
        protected int _NextPageID = 0;
        protected int _FieldCount = 0;
        protected int _Capacity = 0;
        protected int _DataDiskCost = 0;
        protected int _CheckSum = 0;
        protected int _Type = BASE_PAGE_TYPE;
        protected int _X0 = 0;
        protected int _X1 = 0;
        protected int _X2 = 0;
        protected int _X3 = 0;

        protected List<Record> _Elements;

        public Page(int PageSize, int PageID, int LastPageID, int NextPageID, int FieldCount, int DataDiskCost)
        {

            // Check some stuff //
            if (PageSize % 4096 != 0 || PageSize < 4096)
                throw new ArgumentException("PageSize must be a multiple of 4096 bytes and be greater than or equal to 4096 bytes");
            if (PageID < 0)
                throw new ArgumentException("PageID must be greater than 0");

            this._PageSize = PageSize;
            this._PageID = PageID;
            this._LastPageID = LastPageID;
            this._NextPageID = NextPageID;
            this._FieldCount = FieldCount;
            this._DataDiskCost = DataDiskCost;
            this._Capacity = (this.PageSize - this.HeaderSize) / this._DataDiskCost;
            this._Elements = new List<Record>(this._Capacity);

        }

        public Page(int PageSize, int PageID, int LastPageID, int NextPageID, Schema Columns)
            : this(PageSize, PageID, LastPageID, NextPageID, Columns.Count, Columns.RecordDiskCost)
        {
        }

        // Non-Virtuals //
        /// <summary>
        /// True if the page is the first link in a linked list; false otherwise
        /// </summary>
        public bool IsOrigin
        {
            get { return this.LastPageID == NULL_INDEX; }
        }

        /// <summary>
        /// True if the page is the last link in a linked list; false otherwise
        /// </summary>
        public bool IsTerminal
        {
            get { return this.NextPageID == NULL_INDEX; }
        }

        /// <summary>
        /// Gets the first records on the page
        /// </summary>
        public Record OriginRecord
        {

            get
            {
                if (this.IsEmpty)
                    throw new IndexOutOfRangeException("Page is empty");
                return this.Select(0);
            }

        }

        /// <summary>
        /// Gets the last record on the page
        /// </summary>
        public Record TerminalRecord
        {
            get
            {
                if (this.IsEmpty)
                    throw new IndexOutOfRangeException("Page is empty");
                return this.Select(this.Count - 1);
            }
        }

        /// <summary>
        /// Gets a record key for a given row id
        /// </summary>
        /// <param name="RowID"></param>
        /// <returns></returns>
        public RecordKey GetKey(int RowID)
        {
            return new RecordKey(this.PageID, RowID);
        }

        /// <summary>
        /// Finds the first record satisfying a condition
        /// </summary>
        /// <param name="Filter"></param>
        /// <returns></returns>
        public Record SelectFirst(IRecordSeeker Filter)
        {
            return this.Select(this.SeekFirst(Filter));
        }

        /// <summary>
        /// Finds the last record satisfying a condition
        /// </summary>
        /// <param name="Filter"></param>
        /// <returns></returns>
        public Record SelectLast(IRecordSeeker Filter)
        {
            return this.Select(this.SeekLast(Filter));
        }

        /// <summary>
        /// Finds all records satisfying a condition
        /// </summary>
        /// <param name="Filter"></param>
        /// <returns></returns>
        public IEnumerable<Record> SelectAll(IRecordSeeker Filter)
        {

            int[] idx = this.Seek(Filter);
            List<Record> elements = new List<Record>();
            foreach (int i in idx)
            {
                elements.Add(this.Select(i));
            }
            return elements;

        }

        /// <summary>
        /// Deletes the first record satisfying a condition
        /// </summary>
        /// <param name="Filter"></param>
        public void DeleteFirst(IRecordSeeker Filter)
        {
            this.Delete(this.SeekFirst(Filter));
        }

        /// <summary>
        /// Deletes the last record satisfying a condition
        /// </summary>
        /// <param name="Filter"></param>
        public void DeleteLast(IRecordSeeker Filter)
        {
            this.Delete(this.SeekLast(Filter));
        }

        /// <summary>
        /// Deletes all the records satisfying a condition
        /// </summary>
        /// <param name="Filter"></param>
        public void DeleteAll(IRecordSeeker Filter)
        {
            int[] idx = this.Seek(Filter);
            foreach (int i in idx)
            {
                this.Delete(i);
            }
        }

        // Virtuals //
        /// <summary>
        /// True if the page has no records; false otherwise
        /// </summary>
        public virtual bool IsEmpty
        {
            get { return this.Count == 0; }
        }

        /// <summary>
        /// True if the page can't add any more records; false otherwise
        /// </summary>
        public virtual bool IsFull
        {
            get { return this.Count >= this.Capacity; }
        }

        /// <summary>
        /// Represents a unique ID for the given page type
        /// </summary>
        public virtual int PageType
        {
            get { return this._Type; }
        }

        /// <summary>
        /// The maximum number of records a page can contain
        /// </summary>
        public virtual int Capacity
        {
            get { return this._Capacity; }
        }

        /// <summary>
        /// Represents the number of fields each record a page contains
        /// </summary>
        public virtual int FieldCount
        {
            get { return this._FieldCount; }
        }

        /// <summary>
        /// Represents the number of records a page contains
        /// </summary>
        public virtual int Count
        {
            get { return this._Elements.Count; }
        }

        /// <summary>
        /// Represents all records the page contains
        /// </summary>
        public virtual IEnumerable<Record> Elements
        {
            get { return this._Elements; }
        }

        /// <summary>
        /// The size in bytes of the page header
        /// </summary>
        public virtual int HeaderSize
        {
            get { return Page.HEADER_SIZE; }
        }

        /// <summary>
        /// The page ID preceding this page; -1 if the page is the first in a linked list
        /// </summary>
        public virtual int LastPageID
        {
            get { return this._LastPageID; }
            set
            {
                if ((value == this._PageID || value == this._NextPageID) && value != -1)
                    throw new ArgumentException(string.Format("LastPageID passed ({0}) cannot be the same as the PageID ({1}) or the NextPageID ({2})", value, this._PageID, this._NextPageID));
                this._LastPageID = value;
            }
        }

        /// <summary>
        /// The page ID of the next page; -1 if the page is the last in a linked list
        /// </summary>
        public virtual int NextPageID
        {
            get { return this._NextPageID; }
            set
            {
                if ((value == this._PageID || value == this._LastPageID) && value != -1)
                    throw new ArgumentException(string.Format("NextPageID passed ({0}) cannot be the same as the PageID ({1}) or the LastPageID ({2})", value, this._PageID, this._LastPageID));
                this._NextPageID = value;
            }
        }

        /// <summary>
        /// The ID of the current page
        /// </summary>
        public virtual int PageID
        {
            get { return this._PageID; }
        }

        /// <summary>
        /// The size in bytes of this page
        /// </summary>
        public virtual int PageSize
        {
            get { return this._PageSize; }
        }

        /// <summary>
        /// The page checksum; NOT IMPLEMENTED
        /// </summary>
        public virtual int CheckSum
        {
            get { return this._CheckSum; }
        }

        /// <summary>
        /// The cost in bytes of each byte
        /// </summary>
        public virtual int DataDiskCost
        {
            get { return this._DataDiskCost; }
        }

        /// <summary>
        /// Removes a row from the page
        /// </summary>
        /// <param name="RowID"></param>
        public virtual void Delete(int RowID)
        {
            if (!CheckRowID(RowID))
                throw new IndexOutOfRangeException(string.Format("RowID is invalid: {0}", RowID));
            this._Elements.RemoveAt(RowID);
        }

        /// <summary>
        /// Deletes the last record in the page
        /// </summary>
        public virtual void DeleteLast()
        {
            if (this.Count == 0)
                return;
            this._Elements.RemoveAt(this._Elements.Count - 1);
        }

        /// <summary>
        /// Inserts a row into the page at the end
        /// </summary>
        /// <param name="Key"></param>
        public virtual void Insert(Record Element)
        {

            if (this.IsFull)
                throw new Exception("Page is full");
            this._Elements.Add(Element);

        }

        /// <summary>
        /// Inserts a record into the page at the begining
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="RowID"></param>
        public virtual void Insert(Record Element, int RowID)
        {

            if (this.IsFull)
                throw new Exception("Page is full");
            if (RowID < 0 || RowID > this.Count)
                throw new IndexOutOfRangeException(string.Format("RowID is invalid: {0}", RowID));
            this._Elements.Insert(RowID, Element);

        }

        /// <summary>
        /// Updates a record in the page
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="RowID"></param>
        public virtual void Update(Record Element, int RowID)
        {
            if (!CheckRowID(RowID))
                throw new IndexOutOfRangeException(string.Format("RowID is invalid: {0}", RowID));
            this._Elements[RowID] = Element;
        }

        /// <summary>
        /// Finds the first record in the page satisfying a condition
        /// </summary>
        /// <param name="Filter"></param>
        /// <returns></returns>
        public virtual int SeekFirst(IRecordSeeker Filter)
        {

            int idx = 0;
            while (idx < this.Count)
            {
                if (Filter.Equals(this._Elements[idx]))
                    return idx;
                idx++;
            }
            return Page.NULL_INDEX;

        }

        /// <summary>
        /// Finds the last record in the page satisfying a condition
        /// </summary>
        /// <param name="Filter"></param>
        /// <returns></returns>
        public virtual int SeekLast(IRecordSeeker Filter)
        {

            int idx = this.Count - 1;
            while (idx >= 0)
            {
                if (Filter.Equals(this._Elements[idx]))
                    return idx;
                idx--;
            }
            return Page.NULL_INDEX;

        }

        /// <summary>
        /// Finds all records in the page satisfying a condition
        /// </summary>
        /// <param name="Filter"></param>
        /// <returns></returns>
        public virtual int[] Seek(IRecordSeeker Filter)
        {

            List<int> idx = new List<int>();
            for (int i = 0; i < this.Count; i++)
            {
                if (Filter.Equals(this._Elements[i]))
                    idx.Add(i);
            }
            return idx.ToArray();

        }

        /// <summary>
        /// Gets a page from a given position
        /// </summary>
        /// <param name="RowID"></param>
        /// <returns></returns>
        public virtual Record Select(int RowID)
        {

            if (!CheckRowID(RowID))
                throw new IndexOutOfRangeException(string.Format("RowID is invalid: {0}", RowID));
            return this._Elements[RowID];

        }

        /// <summary>
        /// Sorts all records on the page
        /// </summary>
        /// <param name="SortKey"></param>
        public virtual void Sort(IRecordMatcher SortKey)
        {
            this._Elements.Sort(SortKey);
        }

        /// <summary>
        /// Finds the location of a given record on the page
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public virtual int Search(Record Element)
        {

            for (int i = 0; i < this._Elements.Count; i++)
            {
                if (Record.Compare(Element, this._Elements[i]) == 0)
                    return i;
            }
            return -1;

        }

        /// <summary>
        /// Creates a new page with a given ID and last/next ID
        /// </summary>
        /// <param name="PageID"></param>
        /// <param name="LastPageID"></param>
        /// <param name="NextPageID"></param>
        /// <returns></returns>
        public virtual Page Generate(int PageID, int LastPageID, int NextPageID)
        {
            return new Page(this.PageSize, PageID, LastPageID, NextPageID, this.FieldCount, this.DataDiskCost);
        }

        /// <summary>
        /// Splits a page at a given point
        /// </summary>
        /// <param name="Pivot"></param>
        /// <param name="PageID"></param>
        /// <param name="LastPageID"></param>
        /// <param name="NewPageID"></param>
        /// <returns></returns>
        public virtual Page Split(int PageID, int LastPageID, int NextPageID, int Pivot)
        {

            if (this.Count < 2)
                throw new IndexOutOfRangeException("Cannot split a page with fewer than 2 records");
            if (Pivot == 0 || Pivot == this.Count - 1)
                throw new IndexOutOfRangeException("Cannot split on the first or last record");
            if (Pivot < 0)
                throw new IndexOutOfRangeException(string.Format("Pivot ({0}) must be greater than 0", Pivot));
            if (Pivot >= this.Count)
                throw new IndexOutOfRangeException(string.Format("The pivot ({0}) cannot be greater than the element count ({1})", Pivot, this.Count));

            Page p = this.Generate(PageID, LastPageID, NextPageID);
            for (int i = Pivot; i < this.Count; i++)
            {
                p._Elements.Add(this._Elements[i]);
            }
            this._Elements.RemoveRange(Pivot, this.Count - Pivot);
            return p;

        }

        /// <summary>
        /// Splits the page down the middle
        /// </summary>
        /// <param name="PageID"></param>
        /// <param name="LastPageID"></param>
        /// <param name="NextPageID"></param>
        /// <returns></returns>
        public virtual Page Split(int PageID, int LastPageID, int NextPageID)
        {
            return this.Split(PageID, LastPageID, NextPageID, this.Count / 2);
        }

        // Overrides //
        /// <summary>
        /// Returns the page id as the hash code
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return this._PageID;
        }

        // Debug methods //
        /// <summary>
        /// Creates a string with all values from the page listed in order
        /// </summary>
        /// <returns></returns>
        internal virtual string DebugDump()
        {

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < this._Elements.Count; i++)
            {
                sb.AppendLine(string.Format("{0} : {1}", i, this._Elements[i].ToString(',')));
            }
            return sb.ToString();

        }

        /// <summary>
        /// Gets a string element describing where the page is in the page change, and how many records are stored on it
        /// </summary>
        /// <returns></returns>
        internal string MapElement()
        {
            return string.Format("<{0},{1},{2}> : {3} of {4} : {5}", this.PageID, this.LastPageID, this.NextPageID, this.Count, this._Capacity, this.PageSize);
        }

        // Private Methods //
        /// <summary>
        /// True means the RowID is valid for 
        /// </summary>
        /// <param name="RowID"></param>
        /// <returns></returns>
        protected bool CheckRowID(int RowID)
        {
            return !(RowID < 0 || RowID >= this.Count);
        }

        /// <summary>
        /// Gets the X0 integer
        /// </summary>
        internal int X0
        {
            get { return this._X0; }
        }

        /// <summary>
        /// Gets the X1 integer
        /// </summary>
        internal int X1
        {
            get { return this._X1; }
        }

        /// <summary>
        /// Gets the X2 integer
        /// </summary>
        internal int X2
        {
            get { return this._X2; }
        }

        /// <summary>
        /// Gets the X3 integer
        /// </summary>
        internal int X3
        {
            get { return this._X3; }
        }

        /// <summary>
        /// If true, this page is stored in the page cache, false otherwise; this is used by internal processes to make sure any changes made to the page get saved back to disk
        /// </summary>
        internal bool Cached
        {
            get;
            set;
        }

        /// <summary>
        /// Returns the base record list
        /// </summary>
        internal List<Record> Cache
        {
            get { return this._Elements; }
        }

        // Hashing methods //
        /// <summary>
        /// Reads a page form a buffer
        /// </summary>
        /// <param name="Buffer"></param>
        /// <param name="Location"></param>
        /// <returns></returns>
        public static Page Read(byte[] Buffer, long Location)
        {

            // Check the hash key //
            int HashKey = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_HASH_KEY);
            if (HashKey != Page.HASH_KEY)
                throw new Exception("Hash key is invalid, cannot de-serialize");

            // Read Header Data //
            int PageID = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_PAGE_ID);
            int LastPageID = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_LAST_ID);
            int NextPageID = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_NEXT_ID);
            int Size = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_SIZE);
            int FieldCount = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_FCOUNT);
            int Count = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_RCOUNT);
            int CheckSum = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_CHECKSUM);
            int PageType = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_TYPE);
            int DataDiskCost = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_DISK_COST);
            int x0 = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_X0);
            int x1 = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_X1);
            int x2 = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_X2);
            int x3 = BitConverter.ToInt32(Buffer, (int)Location + OFFSET_X3);
            Location += HEADER_SIZE;

            Page element = new Page(Size, PageID, LastPageID, NextPageID, FieldCount, DataDiskCost);
            element._CheckSum = CheckSum;
            element._Type = PageType;
            element._X0 = x0;
            element._X1 = x1;
            element._X2 = x2;
            element._X3 = x3;

            // Read in records //
            for (int k = 0; k < Count; k++)
            {

                // Array //
                Cell[] q = new Cell[FieldCount];

                // Get cells //
                for (int j = 0; j < FieldCount; j++)
                {

                    Cell C;

                    // Read the affinity //
                    CellAffinity a = (CellAffinity)Buffer[Location];
                    Location++;

                    // Read nullness //
                    bool b = (Buffer[Location] == 1);
                    Location++;

                    // If we are null, then exit
                    // for security reasons, we do not want to write any ghost data if the cell is null //
                    if (b == true)
                    {
                        C = new Cell(a);
                    }
                    else
                    {

                        // Cell c //
                        C = new Cell(a);
                        C.NULL = 0;

                        // BOOL //
                        if (a == CellAffinity.BOOL)
                        {
                            C.B0 = Buffer[Location];
                            Location++;
                        }

                        // BLOB //
                        else if (a == CellAffinity.BLOB)
                        {

                            C.B4 = Buffer[Location];
                            C.B5 = Buffer[Location + 1];
                            C.B6 = Buffer[Location + 2];
                            C.B7 = Buffer[Location + 3];
                            Location += 4;
                            byte[] blob = new byte[C.INT_B];
                            for (int i = 0; i < blob.Length; i++)
                            {
                                blob[i] = Buffer[Location];
                                Location++;
                            }
                            C = new Cell(blob);

                        }

                        // STRING //
                        else if (a == CellAffinity.STRING)
                        {

                            C.B4 = Buffer[Location];
                            C.B5 = Buffer[Location + 1];
                            C.B6 = Buffer[Location + 2];
                            C.B7 = Buffer[Location + 3];
                            Location += 4;
                            char[] chars = new char[C.INT_B];
                            for (int i = 0; i < C.INT_B; i++)
                            {
                                byte c1 = Buffer[Location];
                                Location++;
                                byte c2 = Buffer[Location];
                                Location++;
                                chars[i] = (char)(((int)c2) | (int)(c1 << 8));
                            }
                            C = new Cell(new string(chars));

                        }

                        // Double, Ints, Dates //
                        else
                        {
                            C.B0 = Buffer[Location];
                            C.B1 = Buffer[Location + 1];
                            C.B2 = Buffer[Location + 2];
                            C.B3 = Buffer[Location + 3];
                            C.B4 = Buffer[Location + 4];
                            C.B5 = Buffer[Location + 5];
                            C.B6 = Buffer[Location + 6];
                            C.B7 = Buffer[Location + 7];
                            Location += 8;
                        }

                    }

                    q[j] = C;

                }

                element._Elements.Add(new Record(q));

            }

            return element;

        }

        /// <summary>
        /// Writes a page to a buffer
        /// </summary>
        /// <param name="Buffer"></param>
        /// <param name="Location"></param>
        /// <param name="Key"></param>
        public static void Write(byte[] Buffer, long Location, Page Element)
        {

            // Write the header data //
            Array.Copy(BitConverter.GetBytes(HASH_KEY), 0, Buffer, Location + OFFSET_HASH_KEY, SIZE_ELEMENT);
            Array.Copy(BitConverter.GetBytes(Element.PageID), 0, Buffer, Location + OFFSET_PAGE_ID, SIZE_ELEMENT);
            Array.Copy(BitConverter.GetBytes(Element.LastPageID), 0, Buffer, Location + OFFSET_LAST_ID, SIZE_ELEMENT);
            Array.Copy(BitConverter.GetBytes(Element.NextPageID), 0, Buffer, Location + OFFSET_NEXT_ID, SIZE_ELEMENT);
            Array.Copy(BitConverter.GetBytes(Element.PageSize), 0, Buffer, Location + OFFSET_SIZE, SIZE_ELEMENT);
            Array.Copy(BitConverter.GetBytes(Element.FieldCount), 0, Buffer, Location + OFFSET_FCOUNT, SIZE_ELEMENT);
            Array.Copy(BitConverter.GetBytes(Element.Count), 0, Buffer, Location + OFFSET_RCOUNT, SIZE_ELEMENT);
            Array.Copy(BitConverter.GetBytes(Element.CheckSum), 0, Buffer, Location + OFFSET_CHECKSUM, SIZE_ELEMENT);
            Array.Copy(BitConverter.GetBytes(Element.PageType), 0, Buffer, Location + OFFSET_TYPE, SIZE_ELEMENT);
            Array.Copy(BitConverter.GetBytes(Element.DataDiskCost), 0, Buffer, Location + OFFSET_DISK_COST, SIZE_ELEMENT);
            Array.Copy(BitConverter.GetBytes(Element._X0), 0, Buffer, Location + OFFSET_X0, SIZE_ELEMENT);
            Array.Copy(BitConverter.GetBytes(Element._X1), 0, Buffer, Location + OFFSET_X1, SIZE_ELEMENT);
            Array.Copy(BitConverter.GetBytes(Element._X2), 0, Buffer, Location + OFFSET_X2, SIZE_ELEMENT);
            Array.Copy(BitConverter.GetBytes(Element._X3), 0, Buffer, Location + OFFSET_X3, SIZE_ELEMENT);
            Location += HEADER_SIZE;

            // Start writting the record data //
            foreach (Record R in Element._Elements)
            {

                // Write each cell //
                for (int j = 0; j < R.Count; j++)
                {

                    Cell C = R[j];

                    // Write the affinity //
                    Buffer[Location] = ((byte)C.AFFINITY);
                    Location++;

                    // Write nullness //
                    Buffer[Location] = C.NULL;
                    Location++;

                    // If we are null, then exit
                    // for security reasons, we do not want to write any ghost data if the cell is null //
                    if (C.NULL == 0)
                    {

                        // Bool //
                        if (C.AFFINITY == CellAffinity.BOOL)
                        {
                            Buffer[Location] = (C.BOOL == true ? (byte)1 : (byte)0);
                            Location++;
                        }

                        // BLOB //
                        else if (C.AFFINITY == CellAffinity.BLOB)
                        {

                            C.INT_B = C.BLOB.Length;
                            Buffer[Location] = (C.B4);
                            Buffer[Location + 1] = (C.B5);
                            Buffer[Location + 2] = (C.B6);
                            Buffer[Location + 3] = (C.B7);
                            Location += 4;

                            for (int i = 0; i < C.BLOB.Length; i++)
                            {
                                Buffer[Location + i] = C.BLOB[i];
                            }

                            Location += C.BLOB.Length;

                        }

                        // STRING //
                        else if (C.AFFINITY == CellAffinity.STRING)
                        {

                            C.INT_B = C.STRING.Length;
                            Buffer[Location] = (C.B4);
                            Buffer[Location + 1] = (C.B5);
                            Buffer[Location + 2] = (C.B6);
                            Buffer[Location + 3] = (C.B7);
                            Location += 4;

                            for (int i = 0; i < C.STRING.Length; i++)
                            {
                                byte c1 = (byte)(C.STRING[i] >> 8);
                                byte c2 = (byte)(C.STRING[i] & 255);
                                Buffer[Location] = c1;
                                Location++;
                                Buffer[Location] = c2;
                                Location++;
                            }

                        }

                        // Double, int, date //
                        else
                        {

                            Buffer[Location] = C.B0;
                            Buffer[Location + 1] = C.B1;
                            Buffer[Location + 2] = C.B2;
                            Buffer[Location + 3] = C.B3;
                            Buffer[Location + 4] = C.B4;
                            Buffer[Location + 5] = C.B5;
                            Buffer[Location + 6] = C.B6;
                            Buffer[Location + 7] = C.B7;
                            Location += 8;
                        }

                    }

                }

            }


        }

    }

    /// <summary>
    /// Represents a page that stores data in a sequental, sorted, order
    /// </summary>
    public class SortedPage : Page
    {

        public const int ELEMENT_NOT_FOUND = -1;

        private IRecordMatcher _Matcher;

        public SortedPage(int PageSize, int PageID, int LastPageID, int NextPageID, int FieldCount, int DataDiskCost, IRecordMatcher Matcher)
            : base(PageSize, PageID, LastPageID, NextPageID, FieldCount, DataDiskCost)
        {
            this._Matcher = Matcher;
        }

        public SortedPage(int PageSize, int PageID, int LastPageID, int NextPageID, Schema Columns, IRecordMatcher Matcher)
            : base(PageSize, PageID, LastPageID, NextPageID, Columns)
        {
            this._Matcher = Matcher;
        }

        public SortedPage(Page Primitive, IRecordMatcher Matcher)
            : this(Primitive.PageSize, Primitive.PageID, Primitive.LastPageID, Primitive.NextPageID, Primitive.FieldCount, Primitive.DataDiskCost, Matcher)
        {
            this._Elements = Primitive.Cache;
        }

        /// <summary>
        /// 
        /// </summary>
        public override int PageType
        {
            get { return Page.SORTED_PAGE_TYPE; }
        }

        /// <summary>
        /// Inserts a given record at it's correct position in the page
        /// </summary>
        /// <param name="Key"></param>
        public override void Insert(Record Element)
        {

            int IndexOf = this.Search(Element, false);
            base.Insert(Element, IndexOf);

        }

        /// <summary>
        /// Turn off inserting at a given point
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="RowID"></param>
        public override void Insert(Record Element, int RowID)
        {
            throw new InvalidDataException("Cannot insert a record at specific point in a sorted datapage");
        }

        /// <summary>
        /// Turn off sorting
        /// </summary>
        /// <param name="SortKey"></param>
        public override void Sort(IRecordMatcher SortKey)
        {
            throw new ArgumentException("Cannot sort a sorted page; it's already sorted and the key cannot be changed");
        }

        /// <summary>
        /// Searches for an element; will pass back the location of the element closest to but exceeding the desired element if the element is not found
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public override int Search(Record Element)
        {
            return this.Search(Element, false);
        }

        /// <summary>
        /// Searches for a given record on the page
        /// </summary>
        /// <param name="Key">The record we're searching for</param>
        /// <param name="Exact">If true and the record isn't found, return ELEMENT_NOT_FOUND, otherwise it returns the insertion point</param>
        /// <returns>The index of the record on the page</returns>
        public int Search(Record Element, bool Exact)
        {

            // 0 records //
            if (this.Count == 0)
                return (Exact ? ELEMENT_NOT_FOUND : 0);

            // Set the radix //
            int i = 0;
            int Radix = this.Count - 1;

            // Seek!!! //
            while (i <= Radix)
            {

                int Index = i + (Radix - i >> 1);
                int Seek = this._Matcher.Compare(this._Elements[Index], Element);
                if (Seek == 0)
                {
                    return Index;
                }
                else if (Seek < 0)
                {
                    i = Index + 1;
                }
                else
                {
                    Radix = Index - 1;
                }

            }

            return (Exact ? ELEMENT_NOT_FOUND : i);

        }
 
        /// <summary>
        /// Searches for the given element, returning ELEMENT_NOT_FOUND if it doesnt exist
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public int SearchExact(Record Element)
        {
            return this.Search(Element, true);
        }

        /// <summary>
        /// Searches for the exact start of end location
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Lower"></param>
        /// <returns></returns>
        public int SearchPrecise(Record Element, bool Lower)
        {

            // Find the index and return if null or at the edges of the page //
            int idx = this.Search(Element, true);
            if (idx == NULL_INDEX)
                return idx;
            if (idx == 0 && Lower)
                return idx;
            if (idx == this.Count - 1 && !Lower)
                return idx;
            
            int step = (Lower ? -1 : 1);
            idx += step;
            while (idx >= 0 && idx < this.Count)
            {

                if (this._Matcher.Compare(Element, this._Elements[idx]) != 0)
                    return idx - step;
                idx += step;
                
            }

            return idx - step;

        }

        /// <summary>
        /// Checks if this record belongs in this domain
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public bool InDomain(Record Element)
        {

            if (this.Count == 0)
                return false;

            return this._Matcher.Between(Element, this.OriginRecord, this.TerminalRecord) == 0;

        }

        /// <summary>
        /// Generates a sorted data page; overwrites the base method which returns just a vanilla page
        /// </summary>
        /// <param name="PageID"></param>
        /// <param name="LastPageID"></param>
        /// <param name="NextPageID"></param>
        /// <returns></returns>
        public override Page Generate(int PageID, int LastPageID, int NextPageID)
        {
            return new SortedPage(this.PageSize, PageID, LastPageID, NextPageID, this._FieldCount, this._DataDiskCost, this._Matcher);
        }

    }

    /// <summary>
    /// Represents a single page in a b+ tree
    /// </summary>
    public class BPTreePage : Page
    {

        private int DEBUG_MAX_RECORDS = -1; // used only for debugging; set to -1 to revert to the classic logic

        public const int XPAGE_TYPE = 9;

        // This overrides:
        // _X0 = parent page ID
        // _X1 = (1 == is leaf, 0 == is branch)
        // _X2 = is highest
        
        private RecordMatcher _StrongMatcher; // Matches all key columns + page id for the branch nodes
        private RecordMatcher _WeakMatcher; // Only matches key columns;
        private RecordMatcher _PageSearchMatcher;
        private Key _StrongKeyColumns;
        private Key _WeakKeyColumns;
        private Key _OriginalKeyColumns; // Used only for generating
        private int _RefColumn = 0;
        
        public BPTreePage(int PageSize, int PageID, int LastPageID, int NextPageID, int FieldCount, int DataDiskCost, Key KeyColumns, bool IsLeaf)
            : base(PageSize, PageID, LastPageID, NextPageID, FieldCount, DataDiskCost)
        {

            this.IsLeaf = IsLeaf;
            this._OriginalKeyColumns = KeyColumns;
            this._StrongKeyColumns = IsLeaf ? KeyColumns : BranchObjectiveClone(KeyColumns, false);
            if (this.IsLeaf)
            {
                this._StrongMatcher = new RecordMatcher(KeyColumns); // Designed to match keys to keys or elements to elements
                this._WeakMatcher = new RecordMatcher(KeyColumns); // Designed to match keys to keys or elements to elements
                this._PageSearchMatcher = null; // not used
                this._StrongKeyColumns = KeyColumns;
                this._WeakKeyColumns = KeyColumns;
            }
            else
            {
                this._StrongMatcher = new RecordMatcher(BranchObjectiveClone(KeyColumns, false)); // Designed to match keys to keys
                this._WeakMatcher = new RecordMatcher(BranchObjectiveClone(KeyColumns, true)); // Designed to match elements and keys
                this._PageSearchMatcher = new RecordMatcher(BranchObjectiveClone(KeyColumns, true), KeyColumns);
                this._StrongKeyColumns = BranchObjectiveClone(KeyColumns, false);
                this._WeakKeyColumns = BranchObjectiveClone(KeyColumns, true);
            }
            this._RefColumn = KeyColumns.Count;
            
        }

        // Overrides //
        public override bool IsFull
        {
            get
            {
                if (DEBUG_MAX_RECORDS == -1)
                    return base.IsFull;
                else
                    return this.Count >= DEBUG_MAX_RECORDS;
            }
        }
        
        public override void Insert(Record Element)
        {

            int idx = this._Elements.BinarySearch(Element, this._StrongMatcher);
            if (idx < 0) idx = ~idx;

            if (idx == this.Count && !this.IsHighest)
                throw new Exception("Cannot add a higher record to this page");

            this._Elements.Insert(idx, Element);

        }

        public override int Search(Record Element)
        {
            return this._Elements.BinarySearch(Element, this._StrongMatcher);
        }

        public override int PageType
        {
            get
            {
                return XPAGE_TYPE;
            }
        }

        // Join Leaf / Branch Methods //
        public int ParentPageID
        {
            get { return this._X0; }
            set { this._X0 = value; }
        }

        public bool IsLeaf
        {
            get { return this._X1 == 1; }
            set { this._X1 = (value ? 1 : 0); }
        }

        public bool IsHighest
        {
            get { return this._X2 == 1; }
            set { this._X2 = (value ? 1 : 0); }
        }

        public Key StrongKeyColumns
        {
            get { return this._StrongKeyColumns; }
        }

        public Key WeakKeyColumns
        {
            get { return this._WeakKeyColumns; }
        }

        public Key OriginalKeyColumns
        {
            get { return this._OriginalKeyColumns; }
        }

        public int SearchLower(Record Element)
        {
            
            int idx = this._Elements.BinarySearch(Element, this._StrongMatcher);
            if (idx < 0)
                return -1;
            while (this._StrongMatcher.Compare(this._Elements[idx], Element) == 0)
            {
                idx--;
            }

            return idx + 1;

        }

        public int SearchUpper(Record Element)
        {

            int idx = this._Elements.BinarySearch(Element, this._StrongMatcher);
            if (idx < 0)
                return -1;
            while (this._StrongMatcher.Compare(this._Elements[idx], Element) == 0)
            {
                idx++;
            }

            return idx - 1;

        }

        public List<Record> SelectAll(Record Element)
        {

            int Lower = this.SearchLower(Element);
            int Upper = this.SearchUpper(Element);

            List<Record> elements = new List<Record>();
            if (Lower < 0 || Upper < 0)
                return elements;

            elements.AddRange(this._Elements.GetRange(Lower, Upper - Lower));
            return elements;

        }

        public BPTreePage GenerateXPage(int PageID, int LastPageID, int NextPageID)
        {
            BPTreePage x = new BPTreePage(this.PageSize, PageID, LastPageID, NextPageID, this._FieldCount, this._DataDiskCost, this._OriginalKeyColumns, this.IsLeaf);
            x.IsLeaf = this.IsLeaf;
            return x;
        }

        public BPTreePage SplitXPage(int PageID, int LastPageID, int NextPageID, int Pivot)
        {

            if (this.Count < 2)
                throw new IndexOutOfRangeException("Cannot split a page with fewer than 2 records");
            if (Pivot == 0 || Pivot == this.Count - 1)
                throw new IndexOutOfRangeException("Cannot split on the first or last record");
            if (Pivot < 0)
                throw new IndexOutOfRangeException(string.Format("Pivot ({0}) must be greater than 0", Pivot));
            if (Pivot >= this.Count)
                throw new IndexOutOfRangeException(string.Format("The pivot ({0}) cannot be greater than the element count ({1})", Pivot, this.Count));

            BPTreePage p = this.GenerateXPage(PageID, LastPageID, NextPageID);
            for (int i = Pivot; i < this.Count; i++)
            {
                p._Elements.Add(this._Elements[i]);
            }
            this._Elements.RemoveRange(Pivot, this.Count - Pivot);

            // Set the leafness and the parent page id //
            p.IsLeaf = this.IsLeaf;
            p.ParentPageID = this.ParentPageID;

            return p;

        }

        // Branch only methods //
        public void InsertKey(Record Key, int PageID)
        {

            if (this._WeakMatcher.Compare(Key, this._Elements.Last()) > 0 && !this.IsHighest)
                throw new Exception("Can't insert a record greater the max record unless this is the highest page");

            // InsertKey as usual //
            this.InsertKeyUnsafe(Key, PageID);

        }

        public void InsertKeyUnsafe(Record Key, int PageID)
        {

            // Find the insertion point //
            Record k = Composite(Key, PageID);
            int idx = this._Elements.BinarySearch(k, this._StrongMatcher);
            if (idx < 0) idx = ~idx;

            // InsertKey as usual //
            this._Elements.Insert(idx, k);

        }
        
        public int PageSearch(Record Element)
        {

            if (this.IsLeaf)
                throw new Exception("Cannot page search a leaf");

            int idx = this._Elements.BinarySearch(Element, this._PageSearchMatcher);
            if (idx < 0) 
                idx = ~idx;

            if (idx != this._Elements.Count)
            {
                return this._Elements[idx][this._RefColumn].INT_A;
            }
            else
            {
                throw new Exception();
            }


        }

        public int SearchKey(Record Key)
        {

            if (this.IsLeaf)
                throw new Exception("Cannot page search a leaf");

            int idx = this._Elements.BinarySearch(Key, this._WeakMatcher);
            if (idx < 0)
                idx = ~idx;

            if (idx != this._Elements.Count)
            {
                return this._Elements[idx][this._RefColumn].INT_A;
            }
            else
            {
                throw new Exception();
            }

        }

        public int SearchLowerKey(Record Key)
        {

            int idx = this._Elements.BinarySearch(Key, this._WeakMatcher);
            if (idx < 0)
                return -1;
            while (this._WeakMatcher.Compare(this._Elements[idx], Key) == 0)
            {
                idx--;
            }

            return this._Elements[idx + 1][this._RefColumn].INT_A;

        }

        public int SearchUpperKey(Record Key)
        {

            int idx = this._Elements.BinarySearch(Key, this._WeakMatcher);
            if (idx < 0)
                return -1;
            while (this._WeakMatcher.Compare(this._Elements[idx], Key) == 0)
            {
                idx++;
            }

            return this._Elements[idx - 1][this._RefColumn].INT_A;

        }

        public int GetPageID(int Index)
        {
            return this._Elements[Index][this._RefColumn].INT_A;
        }

        public List<int> AllPageIDs()
        {

            List<int> ids = new List<int>();
            foreach (Record r in this._Elements)
            {
                int i = r[this._RefColumn].INT_A;
                ids.Add(i);
            }
            
            return ids;

        }

        public bool KeyExists(Record Key, int PageID)
        {
            return this._Elements.BinarySearch(Composite(Key, PageID), this._StrongMatcher) >= 0;
        }

        public bool KeyExists(Record Key)
        {
            return this._Elements.BinarySearch(Key, this._WeakMatcher) >= 0;
        }

        public void Delete(Record Element)
        {

            // Element must be the entire data record //
            int idx = this._Elements.BinarySearch(Element, this._StrongMatcher);
            if (idx < 0)
            {
                throw new IndexOutOfRangeException("Element is not in this page");
            }

            this._Elements.RemoveAt(idx);

        }

        public bool LessThanTerminal(Record Key)
        {

            // We want this to be strictly less than the last element, not the case where it may be equal to
            return this._WeakMatcher.Compare(Key, this._Elements.Last()) < 0;

        }

        public Record TerminalKeyOnly
        {
            get { return Record.Split(this._Elements.Last(), this._WeakKeyColumns); }
        }

        // Statics //
        public static BPTreePage Mutate(Page Primitive, Key KeyColumns)
        {
            
            if (Primitive is BPTreePage)
                return Primitive as BPTreePage;

            BPTreePage x = new BPTreePage(Primitive.PageSize, Primitive.PageID, Primitive.LastPageID, Primitive.NextPageID, Primitive.FieldCount, Primitive.DataDiskCost, KeyColumns, Primitive.X1 == 1);
            x._X0 = Primitive.X0;
            x._X1 = Primitive.X1;
            x._X2 = Primitive.X2;
            x._X3 = Primitive.X3;

            return x;

        }

        public static Key BranchObjectiveClone(Key KeyColumns, bool Weak)
        {

            Key k = new Key();
            for (int i = 0; i < KeyColumns.Count; i++)
            {
                k.Add(i, KeyColumns.Affinity(i));
            }
            if (!Weak)
                k.Add(k.Count, KeyAffinity.Ascending);
            return k;

        }

        public static Record Composite(Record Key, int PageID)
        {
            Cell[] c = new Cell[Key.Count + 1];
            Array.Copy(Key.BaseArray, 0, c, 0, Key.Count);
            c[c.Length - 1] = new Cell(PageID, 0);
            return new Record(c);
        }

    }

}
