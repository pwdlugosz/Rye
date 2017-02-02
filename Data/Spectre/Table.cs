using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace Rye.Data.Spectre
{

    // Spectre //
    /// <summary>
    /// Describes a table's access permission
    /// </summary>
    public enum TableState
    {
        ReadWrite,
        ReadOnly,
        WriteOnly,
        FullLock
    }

    /// <summary>
    /// Represents information on a table's record storage logic
    /// </summary>
    public enum TableStructure
    {
        Heap,
        Sorted,
        UniquelySorted
    }

    /// <summary>
    /// 
    /// </summary>
    public interface IElementHeader
    {

        string Name { get; }

        int OriginPageID { get; set; }

        int TerminalPageID { get; set; }

        int RootPageID { get; set; }

        long RecordCount { get; set; }

        int PageCount { get; set; }

    }

    /// <summary>
    /// Represents meta data around a table
    /// </summary>
    public sealed class TableHeader : IElementHeader
    {

        public const int HASH_KEY = 0;

        /// <summary>
        /// The disk size of the header; 64k
        /// </summary>
        public const int SIZE = 64 * 1024;
        public const int LEN_SIZE = 4;

        public const int OFFSET_HASH_KEY = 0;
        public const int OFFSET_NAME_LEN = 4;
        public const int OFFSET_NAME = 8;
        public const int OFFSET_DIR_LEN = 72;
        public const int OFFSET_DIR = 76;
        public const int OFFSET_EXT_LEN = 332;
        public const int OFFSET_EXT = 336;
        public const int OFFSET_PAGE_COUNT = 352;
        public const int OFFSET_RECORD_COUNT = 356;
        public const int OFFSET_COLUMN_COUNT = 364;
        public const int OFFSET_FIRST_PAGE_ID = 368;
        public const int OFFSET_LAST_PAGE_ID = 372;
        public const int OFFSET_PAGE_SIZE = 376;
        public const int OFFSET_INDEX_TABLE_PTR = 380;
        public const int OFFSET_SORT_KEY = 384; // 136 bytes max; 4 for key len, 4 for primary flag, 8 * 16 (16 is the max) for column / affinty
        public const int OFFSET_ROOT_PAGE_ID = 520;
        public const int OFFSET_INDEX_TABLE = 1024; // 1024 total bytes

        public const int OFFSET_COLUMNS = 2048;

        public const int COL_NAME_LEN_PTR = 0;
        public const int COL_NAME_PTR = 1;
        public const int COL_AFFINITY = 33;
        public const int COL_SIZE = 34;
        public const int COL_NULL = 35;
        public const int COL_REC_LEN = 36;

        public const string V1_EXTENSION = ".ryev1";

        private string _Name;
        private string _Directory;
        private string _Extension;
        private int _PageSize;

        private TableHeader()
        {
        }

        public TableHeader(string Name, string Directory, string Extension, int PageCount, long RecordCount, int FirstPageID, int LastPageID, int PageSize, Schema Columns)
        {

            this.Name = Name;
            this.Directory = Directory;
            this.Extension = Extension;
            this.PageCount = PageCount;
            this.RecordCount = RecordCount;
            this.OriginPageID = FirstPageID;
            this.TerminalPageID = LastPageID;
            this.Columns = Columns;
            this.PageSize = PageSize;
            this.SortKey = new Key();
            this.RootPageID = -1;
        }

        public TableHeader(string Name, int PageCount, long RecordCount, int PageSize, Schema Columns)
            : this(Name, null, null, PageCount, RecordCount, 0, PageCount - 1, PageSize, Columns)
        {
        }

        public TableHeader(string Name, string Directory, string Extension, Schema Columns)
            : this(Name, Directory, Extension, 0, 0, 0, 0, Page.DEFAULT_SIZE, Columns)
        {
        }

        public TableHeader(string Name, Schema Columns)
            : this(Name, null, null, 0, 0, 0, 0, Page.DEFAULT_SIZE, Columns)
        {
        }

        /// <summary>
        /// The name of the table
        /// </summary>
        public string Name
        {
            get
            {
                return this._Name;
            }
            set
            {
                this._Name = value;
            }
        }

        /// <summary>
        /// If the table is Scribed, then this is the directory the file exists in; null if it's a dream table
        /// </summary>
        public string Directory
        {
            get
            {
                return this._Directory;
            }
            set
            {
                if (value == null)
                {
                    this._Directory = null;
                    return;
                }
                if (value.Last() != '\\')
                    value += '\\';
                this._Directory = value;
            }
        }

        /// <summary>
        /// If the table is Scribed, then this is the file extension; null if it's a dream table
        /// </summary>
        public string Extension
        {
            get
            {
                return this._Extension;
            }
            set
            {
                if (value == null)
                {
                    this._Extension = null;
                    return;
                }
                if (value.First() != '.')
                    value = '.' + value;
                this._Extension = value;
            }
        }

        /// <summary>
        /// The file path
        /// </summary>
        public string Path
        {
            get { return this.Directory + this.Name + this.Extension; }
        }

        /// <summary>
        /// The count of all pages (index and data)
        /// </summary>
        public int PageCount
        {
            get;
            set;
        }

        /// <summary>
        /// The count of all data records
        /// </summary>
        public long RecordCount
        {
            get;
            set;
        }

        /// <summary>
        /// The first data page id
        /// </summary>
        public int OriginPageID
        {
            get;
            set;
        }

        /// <summary>
        /// The last data page id
        /// </summary>
        public int TerminalPageID
        {
            get;
            set;
        }

        /// <summary>
        /// True if the table is a dream table, false if the table is scribed
        /// </summary>
        public bool IsMemoryOnly
        {
            get { return this.Directory == null; }
        }

        /// <summary>
        /// The table columns 
        /// </summary>
        public Schema Columns
        {
            get;
            set;
        }

        /// <summary>
        /// The table page size
        /// </summary>
        public int PageSize
        {
            get { return this._PageSize; }
            set
            {
                if (value % 4096 != 0 || value < 4096)
                    throw new ArgumentException(string.Format("The page size must be a multiple of 4KB (4096 bytes)"));
                this._PageSize = value;
            }
        }

        /// <summary>
        /// The page cache key; the name for dream tables, the path for scribed tables
        /// </summary>
        public string Key
        {
            get { return this.IsMemoryOnly ? this.Name : this.Path; }
        }

        /// <summary>
        /// The sorted key; if the table is not sorted, the key will have a length of zero; this will never be null
        /// </summary>
        public Key SortKey
        {
            get;
            set;
        }

        /// <summary>
        /// True if the SortKey is the primary key
        /// </summary>
        public bool IsPrimaryKey
        {
            get;
            set;
        }

        /// <summary>
        /// The root page only exists
        /// </summary>
        public int RootPageID
        {
            get;
            set;
        }

        /// <summary>
        /// Maximum records per a page
        /// </summary>
        public int MaxRecordsPerPage
        {
            get { return (this.PageSize - Page.HEADER_SIZE) / this.Columns.RecordDiskCost; }
        }

        // Debug Print //
        public string DebugPrint()
        {

            StringBuilder sb = new StringBuilder();
            sb.AppendLine(string.Format("Name: {0}", this.Name));
            sb.AppendLine(string.Format("Directory: {0}", this.Directory ?? "<Memory Only>"));
            sb.AppendLine(string.Format("Extension: {0}", this.Extension ?? "<Memory Only>"));
            sb.AppendLine(string.Format("Path: {0}", this.Path));
            sb.AppendLine(string.Format("Lookup Key: {0}", this.Key));
            sb.AppendLine(string.Format("Page Size: {0}", this.PageSize));
            sb.AppendLine(string.Format("Page Count: {0}", this.PageCount));
            sb.AppendLine(string.Format("Record Count: {0}", this.RecordCount));
            sb.AppendLine(string.Format("Origin Page: {0}", this.OriginPageID));
            sb.AppendLine(string.Format("Terminal Page: {0}", this.TerminalPageID));
            sb.AppendLine(string.Format("Radix Page: {0}", this.RootPageID));
            sb.AppendLine(string.Format("Max Records Per Page: {0}", this.MaxRecordsPerPage));
            sb.AppendLine(string.Format("Avg Page Fullness: {0}%", Math.Round((double)this.RecordCount / ((double)this.PageCount * (double)this.MaxRecordsPerPage), 3) * 100D));
            if (this.SortKey.Count != 0)
            {
                sb.AppendLine(this.IsPrimaryKey ? "Primary Key:" : "Sort Index:");
                for (int i = 0; i < this.SortKey.Count; i++)
                {
                    sb.AppendLine(string.Format("\t{0} : {1}", this.Columns.ColumnName(this.SortKey[i]), this.Columns.ColumnAffinity(i)));
                }
            }
            sb.AppendLine("Columns:");
            for (int i = 0; i < this.Columns.Count; i++)
            {
                sb.AppendLine(string.Format("\t{0} : {1}.{2}", this.Columns.ColumnName(i), this.Columns.ColumnAffinity(i), this.Columns.ColumnSize(i))); 
            }

            return sb.ToString();

        }

        // Static Methods //
        /// <summary>
        /// Converts a byte array to a TableHeader
        /// </summary>
        /// <param name="Buffer"></param>
        /// <param name="Location"></param>
        /// <returns></returns>
        public static TableHeader FromHash(byte[] Buffer, int Location)
        {

            // Check the size //
            if (Buffer.Length - Location < TableHeader.SIZE)
                throw new Exception("Buffer is incorrect size");

            // Check the hash key //
            if (BitConverter.ToInt32(Buffer, Location + OFFSET_HASH_KEY) != HASH_KEY)
                throw new Exception("Invalid hash key");

            // Create //
            TableHeader h = new TableHeader();
            int Len = 0;

            // Name //
            Len = BitConverter.ToInt32(Buffer, Location + OFFSET_NAME_LEN);
            h.Name = ASCIIEncoding.ASCII.GetString(Buffer, Location + OFFSET_NAME, Len);

            // Directory //
            Len = BitConverter.ToInt32(Buffer, Location + OFFSET_DIR_LEN);
            h.Directory = ASCIIEncoding.ASCII.GetString(Buffer, Location + OFFSET_DIR, Len);

            // Extension //
            Len = BitConverter.ToInt32(Buffer, Location + OFFSET_EXT_LEN);
            h.Extension = ASCIIEncoding.ASCII.GetString(Buffer, Location + OFFSET_EXT, Len);

            // Page count //
            h.PageCount = BitConverter.ToInt32(Buffer, Location + OFFSET_PAGE_COUNT);

            // Row count //
            h.RecordCount = BitConverter.ToInt64(Buffer, Location + OFFSET_RECORD_COUNT);

            // Column Count //
            int ColCount = BitConverter.ToInt32(Buffer, Location + OFFSET_COLUMN_COUNT);

            // First Page //
            h.OriginPageID = BitConverter.ToInt32(Buffer, Location + OFFSET_FIRST_PAGE_ID);

            // Last Page //
            h.TerminalPageID = BitConverter.ToInt32(Buffer, Location + OFFSET_LAST_PAGE_ID);
            
            // Page PageSize //
            h.PageSize = BitConverter.ToInt32(Buffer, Location + OFFSET_PAGE_SIZE);

            // Radix Page //
            h.RootPageID = BitConverter.ToInt32(Buffer, Location + OFFSET_ROOT_PAGE_ID);

            // Key //
            h.SortKey = new Key();
            h.IsPrimaryKey = BitConverter.ToInt32(Buffer, Location + OFFSET_SORT_KEY) == 1; // gets the unique
            int KeyCount = BitConverter.ToInt32(Buffer, Location + OFFSET_SORT_KEY + 4); // gets the key size
            for (int i = 0; i < KeyCount; i++)
            {
                int loc = Location + OFFSET_SORT_KEY + 8 + 8 * i;
                int idx = BitConverter.ToInt32(Buffer, loc);
                KeyAffinity affinity = (KeyAffinity)BitConverter.ToInt32(Buffer, loc + 4);
                h.SortKey.Add(idx, affinity);
            }

            // Load the columns //
            h.Columns = new Schema();
            for (int i = 0; i < ColCount; i++)
            {

                int RecordOffset = Location + OFFSET_COLUMNS + i * COL_REC_LEN;
                int NameLen = (int)Buffer[RecordOffset];
                string ColName = ASCIIEncoding.ASCII.GetString(Buffer, RecordOffset + COL_NAME_PTR, NameLen);
                CellAffinity ColType = (CellAffinity)Buffer[RecordOffset + COL_AFFINITY];
                int ColSize = (int)Buffer[RecordOffset + COL_SIZE];
                bool ColNull = (Buffer[RecordOffset + COL_NULL] == 1);
                h.Columns.Add(ColName, ColType, ColNull, ColSize);

            }

            return h;

        }

        /// <summary>
        /// Converts a TableHeader to a byte array
        /// </summary>
        /// <param name="Buffer"></param>
        /// <param name="Location"></param>
        /// <param name="Key"></param>
        public static void ToHash(byte[] Buffer, int Location, TableHeader Element)
        {

            // Check the size //
            if (Buffer.Length - Location < TableHeader.SIZE)
                throw new Exception("Buffer is incorrect size");

            // Write the hash key //
            Array.Copy(BitConverter.GetBytes(HASH_KEY), 0, Buffer, Location + OFFSET_HASH_KEY, LEN_SIZE);

            // Write the name //
            Array.Copy(BitConverter.GetBytes(Element.Name.Length), 0, Buffer, Location + OFFSET_NAME_LEN, LEN_SIZE);
            Array.Copy(ASCIIEncoding.ASCII.GetBytes(Element.Name), 0, Buffer, Location + OFFSET_NAME, Element.Name.Length);

            // Write the directory //
            Array.Copy(BitConverter.GetBytes(Element.Directory.Length), 0, Buffer, Location + OFFSET_DIR_LEN, LEN_SIZE);
            Array.Copy(ASCIIEncoding.ASCII.GetBytes(Element.Directory), 0, Buffer, Location + OFFSET_DIR, Element.Directory.Length);

            // Write the extension //
            Array.Copy(BitConverter.GetBytes(Element.Extension.Length), 0, Buffer, Location + OFFSET_EXT_LEN, LEN_SIZE);
            Array.Copy(ASCIIEncoding.ASCII.GetBytes(Element.Extension), 0, Buffer, Location + OFFSET_EXT, Element.Extension.Length);

            // Write page count //
            Array.Copy(BitConverter.GetBytes(Element.PageCount), 0, Buffer, Location + OFFSET_PAGE_COUNT, LEN_SIZE);

            // Write record count //
            Array.Copy(BitConverter.GetBytes(Element.RecordCount), 0, Buffer, Location + OFFSET_RECORD_COUNT, 8); // Long integer

            // Write column count //
            Array.Copy(BitConverter.GetBytes(Element.Columns.Count), 0, Buffer, Location + OFFSET_COLUMN_COUNT, LEN_SIZE);

            // Write first page ID //
            Array.Copy(BitConverter.GetBytes(Element.OriginPageID), 0, Buffer, Location + OFFSET_FIRST_PAGE_ID, LEN_SIZE);

            // Write last page ID //
            Array.Copy(BitConverter.GetBytes(Element.TerminalPageID), 0, Buffer, Location + OFFSET_LAST_PAGE_ID, LEN_SIZE);
            
            // Write page size //
            Array.Copy(BitConverter.GetBytes(Element.PageSize), 0, Buffer, Location + OFFSET_PAGE_SIZE, LEN_SIZE);
            
            // Write radix page //
            Array.Copy(BitConverter.GetBytes(Element.RootPageID), 0, Buffer, Location + OFFSET_ROOT_PAGE_ID, LEN_SIZE);
            
            // Write key //
            Array.Copy(BitConverter.GetBytes((Element.IsPrimaryKey ? (int)1 : (int)0)), 0, Buffer, Location + OFFSET_SORT_KEY, LEN_SIZE);
            byte[] b = Element.SortKey.Bash();
            Array.Copy(b, 0, Buffer, Location + OFFSET_SORT_KEY + 4, b.Length);
            
            // Write schema //
            for (int i = 0; i < Element.Columns.Count; i++)
            {

                byte NameLen = (byte)Element.Columns.ColumnName(i).Length;
                byte Affinity = (byte)Element.Columns.ColumnAffinity(i);
                byte Size = (byte)Element.Columns.ColumnSize(i);
                byte Nullness = (byte)(Element.Columns.ColumnNull(i) ? 1 : 0);
                byte[] Name = ASCIIEncoding.ASCII.GetBytes(Element.Columns.ColumnName(i));

                int ptr = Location + OFFSET_COLUMNS + i * COL_REC_LEN;
                Buffer[ptr + COL_NAME_LEN_PTR] = NameLen;
                Buffer[ptr + COL_AFFINITY] = Affinity;
                Buffer[ptr + COL_SIZE] = Size;
                Buffer[ptr + COL_NULL] = Nullness;
                Array.Copy(Name, 0, Buffer, ptr + COL_NAME_PTR, Name.Length);

            }
            
        }

        /// <summary>
        /// Creates a header for a dream table
        /// </summary>
        /// <param name="Name"></param>
        /// <param name="Columns"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        public static TableHeader DreamHeader(string Name, Schema Columns, int PageSize)
        {
            return new TableHeader(Name, null, null, 0, 0, -1, -1, PageSize, Columns);
        }

        /// <summary>
        /// Gets the file name of a v1 rye dataset
        /// </summary>
        /// <param name="Dir"></param>
        /// <param name="Name"></param>
        /// <returns></returns>
        public static string DeriveV1Path(string Dir, string Name)
        {
            if (Dir.Last() != '\\') Dir += '\\';
            return Dir + Name + V1_EXTENSION;
        }

    }

    /// <summary>
    /// This is the base class for all tables
    /// </summary>
    public abstract class BaseTable
    {

        protected Host _Host;
        protected TableState _State = TableState.ReadWrite;
        protected TableHeader _Header;
        protected IndexCollection _Indexes;
        protected string _TableType = "BASE_TABLE";

        public BaseTable(Host Host, TableHeader Header)
        {
            this._Host = Host;
            this._Header = Header;
        }

        // Non virtual / abstract //
        /// <summary>
        /// Gets the access state of the table
        /// </summary>
        public TableState State
        {
            get { return this._State; }
            protected set { this._State = value; }
        }

        /// <summary>
        /// Inner table host
        /// </summary>
        public Host Host
        {
            get { return this._Host; }
        }

        // Meta Data //
        /// <summary>
        /// Table's columns
        /// </summary>
        public virtual Schema Columns 
        {
            get { return this._Header.Columns; }
        }

        /// <summary>
        /// The table's names
        /// </summary>
        public virtual string Name 
        {
            get { return this._Header.Name; }
        }

        /// <summary>
        /// The lookup key for the table
        /// </summary>
        public virtual string Key
        {
            get { return this._Header.Key; }
        }

        /// <summary>
        /// The table's header
        /// </summary>
        public virtual TableHeader Header 
        {
            get { return this._Header; }
        }

        // Indexes //
        /// <summary>
        /// Adds an index to the table
        /// </summary>
        /// <param name="Name"></param>
        /// <param name="Value"></param>
        public virtual void AddIndex(string Name, Index Value)
        {

        }

        /// <summary>
        /// Get's an index from the table
        /// </summary>
        /// <param name="Name"></param>
        /// <returns></returns>
        public virtual Index GetIndex(string Name)
        {
            return this._Indexes[Name];
        }

        /// <summary>
        /// Splits a table into many tables
        /// </summary>
        /// <param name="PartitionIndex"></param>
        /// <param name="PartitionCount"></param>
        /// <returns></returns>
        public abstract BaseTable Partition(int PartitionIndex, int PartitionCount);

        // Reading / Writing Info //
        /// <summary>
        /// Inserts a value into the table
        /// </summary>
        /// <param name="Value"></param>
        public abstract void Insert(Record Value);

        /// <summary>
        /// Inserts many values into a table
        /// </summary>
        /// <param name="Records"></param>
        public virtual void Insert(IEnumerable<Record> Records)
        {
            foreach (Record r in Records)
            {
                this.Insert(r);
            }
        }
        
        /// <summary>
        /// Represents the total record count
        /// </summary>
        public virtual long RecordCount 
        {
            get { return this._Header.RecordCount; }
            set { this._Header.RecordCount = value; }
        }

        /// <summary>
        /// Gets a record from a table
        /// </summary>
        /// <param name="Position"></param>
        /// <returns></returns>
        public virtual Record Select(RecordKey Position)
        {
            return this.GetPage(Position.PAGE_ID).Select(Position.ROW_ID);
        }

        /// <summary>
        /// Opens a read stream
        /// </summary>
        /// <returns></returns>
        public virtual ReadStream OpenReader()
        {
            return new VanillaReadStream(this);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public virtual ReadStream OpenReader(Record Key)
        {
            return new VanillaReadStream(this);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="LKey"></param>
        /// <param name="UKey"></param>
        /// <returns></returns>
        public virtual ReadStream OpenReader(Record LKey, Record UKey)
        {
            return new VanillaReadStream(this);
        }

        /// <summary>
        /// Opens a write stream
        /// </summary>
        /// <returns></returns>
        public virtual WriteStream OpenWriter()
        {
            return new VanillaWriteStream(this);
        }

        /// <summary>
        /// This method gets called right before the table is removed from memory; 
        /// </summary>
        public virtual void PreSerialize()
        {
        }

        // Page Info //
        /// <summary>
        /// Gets the table's page size
        /// </summary>
        public virtual int PageSize 
        { 
            get { return this._Header.PageSize; } 
        }

        /// <summary>
        /// Gets the total page count, including non-data pages
        /// </summary>
        public virtual int PageCount 
        {
            get { return this._Header.PageCount; }
            set { this._Header.PageCount = value; }
        }

        /// <summary>
        /// Gets a page from the table
        /// </summary>
        /// <param name="PageID"></param>
        /// <returns></returns>
        public abstract Page GetPage(int PageID);

        /// <summary>
        /// Sets a age into the table; if the page exists, nothing happens; otherwise it gets added
        /// </summary>
        /// <param name="Key"></param>
        public abstract void SetPage(Page Element);

        /// <summary>
        /// Checks if a page exists in the table
        /// </summary>
        /// <param name="PageID"></param>
        /// <returns></returns>
        public abstract bool PageExists(int PageID);

        /// <summary>
        /// Gets the first data page ID
        /// </summary>
        public virtual int OriginPageID
        {
            get { return this._Header.OriginPageID; }
        }

        /// <summary>
        /// Gets the first data page
        /// </summary>
        public virtual Page OriginPage
        {
            get { return (this.PageExists(this.OriginPageID) ? this.GetPage(this.OriginPageID) : null); }
        }

        /// <summary>
        /// Gets the ID of the last data page
        /// </summary>
        public virtual int TerminalPageID
        {
            get { return this.Header.TerminalPageID; }
            set { this.Header.TerminalPageID = value; }
        }

        /// <summary>
        /// Gets the last data page
        /// </summary>
        public virtual Page TerminalPage
        {
            get { return this.GetPage(this.TerminalPageID); }
        }

        /// <summary>
        /// Generates a brand new page ID that does not exist in the table
        /// </summary>
        public virtual int GenerateNewPageID
        {
            get { return this.PageCount; }
        }

        /// <summary>
        /// Creates a new page
        /// </summary>
        /// <returns></returns>
        public abstract Page GenerateNewPage();

        /// <summary>
        /// Given a page ID, returns a page N steps above or below it
        /// </summary>
        /// <param name="PageID"></param>
        /// <param name="Steps"></param>
        /// <param name="SeekDown"></param>
        /// <returns></returns>
        public virtual Page Seek(int PageID, int Steps, bool SeekDown)
        {

            Page p = this.GetPage(PageID);
            for (int i = 0; i < Steps; i++)
            {

                int NewID = (SeekDown ? p.LastPageID : p.NextPageID);
                if (NewID == -1)
                    break;
                p = this.GetPage(NewID);

            }
            return p;

        }

        /// <summary>
        /// A string representing all data pages in order
        /// </summary>
        /// <returns></returns>
        public string PageMap()
        {

            StringBuilder sb = new StringBuilder();
            Page p = this.OriginPage;
            sb.AppendLine(p.MapElement());
            while (p.NextPageID != -1)
            {
                p = this.GetPage(p.NextPageID);
                sb.AppendLine(p.MapElement());
            }

            return sb.ToString();

        }

        /// <summary>
        /// Represents a collection of all the pages in the table
        /// </summary>
        public IEnumerable<Page> Pages
        {
            get { return new PageEnumerator(this); }
        }

        /// <summary>
        /// Splits a page in two, and preserves all other links in the chain
        /// </summary>
        /// <param name="PageID"></param>
        public void ForkPage(int PageID)
        {

            /*
             * Basically, the current page chain looks like this ... p, r, ...
             * But we are going to change it to: ... p', q, r, ...
             * where p' = the lower half of p, and q is the upper half of p on a new page
             * 
             */

            // Get this page //
            Page p = this.GetPage(PageID);

            // Define the new page variables //
            int NewPageID = this.GenerateNewPageID;

            // Create the forked page //
            Page q = p.Split(NewPageID, -1, -1);
            if (this.PageExists(NewPageID))
                throw new Exception(string.Format("Page exists! {0}", NewPageID));

            // Add q after p //
            this.AddPageAfter(p.PageID, q);

            // Add q to the cache //
            this.SetPage(q);

        }

        /// <summary>
        /// Adds a page after a given page id
        /// </summary>
        /// <param name="PageID"></param>
        /// <param name="Key"></param>
        public void AddPageAfter(int PageID, Page Element)
        {

            // we want to add Key between PageID and NextPageID //
            Page A = this.GetPage(PageID);

            // Set the elements' LastPageID = A.PageID, and the Key's NextPageID = A.NextPageID
            Element.LastPageID = A.PageID;
            Element.NextPageID = A.NextPageID;
            A.NextPageID = Element.PageID;

            // Set the page ids for the NextPage //
            if (Element.NextPageID != -1)
            {
                Page C = this.GetPage(Element.NextPageID);
                C.LastPageID = Element.PageID;
                if (A.PageID == this._Header.RootPageID)
                    this._Header.RootPageID = Element.PageID;
            }
            // Otherwise, set Key as the new terminal page //
            else
            {
                this.TerminalPageID = Element.PageID;
            }


        }

        // Protected helper //
        protected int[] Map(int PartitionCount, int ElementCount)
        {

            int[] map = new int[PartitionCount];
            for (int i = 0; i < ElementCount; i++)
            {
                int idx = i % PartitionCount;
                map[idx]++;
            }
            return map;

        }

        protected int StartIndex(int PartitionIndex, int[] Map)
        {

            int idx = 0;
            for (int i = 0; i < PartitionIndex; i++)
            {
                idx += Map[i];
            }
            return idx;

        }

        protected void IncrementTerminus()
        {

            Page p = this.GenerateNewPage();
            int NewPageID = this.GenerateNewPageID;
            p.NextPageID = -1;
            p.LastPageID = this.TerminalPageID;


        }

        // Internal debugging //
        internal void Dump(string Path)
        {

            using (StreamWriter sw = new StreamWriter(Path))
            {

                sw.WriteLine(this.Columns.ToNameString('\t'));
                ReadStream rs = this.OpenReader();
                while (rs.CanAdvance)
                {

                    sw.WriteLine(rs.ReadNext().ToString('\t'));

                }

                sw.Flush();

            }

        }

        internal string MetaData()
        {
            return this._TableType + "\n" + this._Header.DebugPrint() + this.PageMap();
        }

        // Statics //
        public static void Dump(string Path, ReadStream Stream)
        {

            using (StreamWriter sw = new StreamWriter(Path))
            {

                sw.WriteLine(Stream.Columns.ToNameString('\t'));
                while (Stream.CanAdvance)
                {
                    sw.WriteLine(Stream.ReadNext().ToString('\t'));
                }
                sw.Flush();

            }

        }

        // Classes //
        protected class PageEnumerator : IEnumerable<Page>, IEnumerator<Page>, IEnumerable, IEnumerator, IDisposable
        {

            private BaseTable _Table;
            private Page _Page;

            public PageEnumerator(BaseTable Table)
            {
                this._Table = Table;
                this._Page = (Table.PageCount == 0 ? null : Table.OriginPage);
            }

            public bool MoveNext()
            {

                if (this._Page == null)
                    return false;
                if (this._Page.NextPageID != -1)
                {
                    this._Page = this._Table.GetPage(this._Page.NextPageID);
                    return true;
                }
                return false;

            }

            Page IEnumerator<Page>.Current
            {
                get
                {
                    return this._Page;
                }
            }

            object IEnumerator.Current
            {
                get { return this._Page; }
            }

            public void Reset()
            {
                if (this._Table.PageCount == 0)
                    this._Page = null;
                else
                    this._Page = this._Table.OriginPage;
            }

            IEnumerator<Page> IEnumerable<Page>.GetEnumerator()
            {
                return this;
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this;
            }

            void IDisposable.Dispose()
            {
                // Do nothing
            }

        }

    }

    // Dream tables: exist only in memory and are never saved to disk
    // -----------------------------------------------------------------------

    /// <summary>
    /// Represents an in memory only table
    /// </summary>
    public abstract class DreamTable : BaseTable
    {

        /*
         * The user still needs to implement the following:
         * IsFull
         * InsertKey(Record)
         * Partiton(int, int)
         * 
         */

        public const int MAX_MEMORY = 1024 * 1024 * 16; // 16 mb

        protected int _PageCapacity;

        protected DreamTable(Host Host, string Name, Schema Columns, int PageSize, TableState State)
            : base(Host, TableHeader.DreamHeader(Name, Columns, PageSize))
        {

            // Save our working variables //
            this._PageCapacity = MAX_MEMORY / PageSize;
            this._State = State;

            this._Host.PageCache.AddDreamTable(this);

        }

        public abstract bool IsFull { get; }

        public override Record Select(RecordKey Position)
        {

            if (this.State == TableState.WriteOnly || this.State == TableState.FullLock)
                throw new Exception("BaseTable is locked for reading");

            return this.GetPage(Position.PAGE_ID).Select(Position.ROW_ID);

        }

        public override void Insert(IEnumerable<Record> Records)
        {

            if (this.State == TableState.ReadOnly || this.State == TableState.FullLock)
                throw new Exception("BaseTable is locked for writing");

            foreach (Record r in Records)
            {
                this.Insert(r);
            }

        }

        public override Page GenerateNewPage()
        {

            // Create the new page //
            Page p = new Page(this.PageSize, this.GenerateNewPageID, this.TerminalPageID, -1, this.Columns);

            // Get the last page and switch it's next page id //
            Page q = this.TerminalPage;
            q.NextPageID = p.PageID;

            // Add this page //
            this.SetPage(p);

            return p;

        }

        public override Page GetPage(int PageID)
        {
            return this._Host.PageCache.RequestDreamPage(this.Key, PageID);
        }

        public override void SetPage(Page Element)
        {
            this._Host.PageCache.PushDreamPage(this.Key, Element);
        }

        public override bool PageExists(int PageID)
        {
            return this._Host.PageCache.DreamPageExists(new PageUID(this.Key, PageID));
        }

    }

    /// <summary>
    /// An in memory only table where records are stored in an unorder heap
    /// </summary>
    public class HeapDreamTable : DreamTable
    {

        protected Page _Terminis;
        //protected BTreeIndex _index; // DEV ONLY
        //public BTreeDev _idx;
        
        public HeapDreamTable(Host Host, string Name, Schema Columns, int PageSize)
            : base(Host, Name, Columns, PageSize, TableState.ReadWrite)
        {

            // Create the radix page //
            this._Terminis = new Page(this.PageSize, 0, -1, -1, this.Columns);
            this.SetPage(this._Terminis);
            this._Header.OriginPageID = 0;
            this._TableType = "HEAP_DREAM";

            //this._index = new BTreeIndex("IDX", this, new Key(0));
            //this._idx = new BTreeDev(new RecordMatcher(new Key(0)));

        }

        public HeapDreamTable(Host Host, string Name, Schema Columns)
            : this(Host, Name, Columns, Page.DEFAULT_SIZE)
        {
        }

        public override bool IsFull
        {
            get { return this._Terminis.IsFull && this.PageCount == this._PageCapacity; }
        }

        public override void Insert(Record Value)
        {

            Cell ptr = Cell.NULL_INT;
            this.Insert(Value, out ptr);
            //this._index.Append(Value, ptr);
            //this._idx.InsertKey(Value, new Cell(ptr.INT_B, 0));

        }

        public void Insert(Record Value, out Cell Pointer)
        {

            if (this.State == TableState.ReadOnly || this.State == TableState.FullLock)
                throw new Exception("BaseTable is locked for writing");

            // Handle the terminal page being full //
            if (this._Terminis.IsFull)
            {

                Page p = new Page(this.PageSize, this.GenerateNewPageID, this._Terminis.PageID, -1, this.Columns);
                this._Terminis.NextPageID = p.PageID;
                this.SetPage(p);
                this._Terminis = p;
                this._Header.TerminalPageID = p.PageID;

            }

            // Add the actual record //
            this._Terminis.Insert(Value);
            this.RecordCount++;

            // Get the pointer //
            Pointer = new Cell(this._Terminis.PageID, this._Terminis.Count - 1);

        }

        public override BaseTable Partition(int PartitionIndex, int PartitionCount)
        {

            HeapDreamTable t = new HeapDreamTable(this._Host, this.Name, this.Columns, this.PageSize);
            t._State = TableState.ReadOnly;
            int[] counts = this.Map(PartitionCount, (int)this.PageCount);
            int Start = this.StartIndex(PartitionIndex, counts);
            int Count = counts[PartitionIndex];

            for (int i = Start; i < Start + Count; i++)
            {
                Page p = this.GetPage(i);
                t.SetPage(p);
                t._Terminis = p;
            }

            return t;

        }

        public override Page TerminalPage
        {
            get
            {
                return this._Terminis;
            }
        }

        public override int TerminalPageID
        {
            get
            {
                return this._Terminis.PageID;
            }
        }

    }

    /// <summary>
    /// Represents a tables with a given clustered index
    /// </summary>
    public class ClusteredDreamTable : DreamTable
    {

        protected BPlusTree _Cluster;
        protected int _MaxRecords;

        public ClusteredDreamTable(Host Host, string Name, Schema Columns, Key IndexColumns, int PageSize)
            : base(Host, Name, Columns, PageSize, TableState.ReadWrite)
        {
            this._Cluster = BPlusTree.CreateClusteredIndex(this, IndexColumns);
            this._MaxRecords = DreamTable.MAX_MEMORY / Columns.RecordDiskCost;
            this._TableType = "CLUSTER_DREAM";
        }

        public ClusteredDreamTable(Host Host, string Name, Schema Columns, Key IndexColumns)
            : this(Host, Name, Columns, IndexColumns, Page.DEFAULT_SIZE)
        {
        }

        /// <summary>
        /// Inner B+Tree
        /// </summary>
        public BPlusTree BaseTree
        {
            get { return this._Cluster; }
        }

        /// <summary>
        /// True if the table cannot accept any more records
        /// </summary>
        public override bool IsFull
        {
            get { return this._MaxRecords < this._Header.RecordCount; }
        }

        /// <summary>
        /// Appends a record to a table
        /// </summary>
        /// <param name="Value"></param>
        public override void Insert(Record Value)
        {
            this._Cluster.Insert(Value);
        }

        /// <summary>
        /// Opens a record reader that focuses on a single key
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public override ReadStream OpenReader(Record Key)
        {

            if (Key.Count != this._Cluster.IndexColumns.Count)
                return this.OpenReader();
            RecordKey l = this._Cluster.SeekFirst(Key);
            RecordKey u = this._Cluster.SeekLast(Key);
            return new VanillaReadStream(this, l, u);

        }

        /// <summary>
        /// Opens a record reader to focus on records between A and B (inclusive)
        /// </summary>
        /// <param name="LKey"></param>
        /// <param name="UKey"></param>
        /// <returns></returns>
        public override ReadStream OpenReader(Record LKey, Record UKey)
        {

            if (LKey.Count != UKey.Count || LKey.Count != this._Cluster.IndexColumns.Count)
                return this.OpenReader();
            RecordKey lk = this._Cluster.SeekFirst(LKey);
            RecordKey uk = this._Cluster.SeekLast(UKey);
            return new VanillaReadStream(this, lk, uk);

        }

        // Methods not implemented //
        /// <summary>
        /// Splits a table into N sub tables
        /// </summary>
        /// <param name="PartitionIndex"></param>
        /// <param name="PartitionCount"></param>
        /// <returns></returns>
        public override BaseTable Partition(int PartitionIndex, int PartitionCount)
        {
            throw new NotImplementedException();
        }

    }

    // Scribe tables: are saved to disk
    // -----------------------------------------------------------------------

    /// <summary>
    /// Represents the base class for all tables written to disk
    /// </summary>
    public abstract class ScribeTable : BaseTable
    {

        /// <summary>
        /// This method should be used for creating a table object from an existing table on disk
        /// </summary>
        /// <param name="Host"></param>
        /// <param name="Header"></param>
        /// <param name="SortKey"></param>
        public ScribeTable(Host Host, TableHeader Header)
            : base(Host, Header)
        {
            this._Host.PageCache.AddScribeTable(this);
        }

        /// <summary>
        /// This method should be used for creating a brand new table object
        /// </summary>
        /// <param name="Host"></param>
        /// <param name="Name"></param>
        /// <param name="Dir"></param>
        /// <param name="Columns"></param>
        /// <param name="PageSize"></param>
        /// <param name="SortKey"></param>
        public ScribeTable(Host Host, string Name, string Dir, Schema Columns, int PageSize)
            : base(Host, new TableHeader(Name, Dir, TableHeader.V1_EXTENSION, 0, 0, -1, -1, PageSize, Columns))
        {
            this._Host.PageCache.DropTable(this.Key);
            this._Host.PageCache.AddScribeTable(this);
        }

        public override Page GenerateNewPage()
        {

            // Create the new page //
            Page p = new Page(this.PageSize, this.GenerateNewPageID, this.TerminalPageID, -1, this.Columns);

            // Get the last page and switch it's next page id //
            Page q = this.TerminalPage;
            q.NextPageID = p.PageID;

            // Add this page //
            this.SetPage(p);

            return p;

        }

        public override Page GetPage(int PageID)
        {
            return this._Host.PageCache.RequestScribePage(this.Key, PageID);
        }

        public override void SetPage(Page Element)
        {
            
            this._Host.PageCache.PushScribePage(this.Key, Element, true); // want to ensure our pages end up getting saved to disk
            
            // Need to check to see if the page count was correctly incremented
            // If the page count < PageID, then set the page count to the pageID + 1
            //int PageCount = Math.Max(Key.PageID, Math.Max(Key.LastPageID, Key.NextPageID)) + 1;
            //if (PageCount > this._Header.PageCount)
            //    this._Header.PageCount = PageCount;

        }
        
        public override bool PageExists(int PageID)
        {
            return this._Host.PageCache.ScribePageExists(new PageUID(this.Key, PageID));
        }

    }

    /// <summary>
    /// Represents a scribe table that keeps records in an unordered linked list
    /// </summary>
    public class HeapScribeTable : ScribeTable
    {

        protected Page _Terminis;
        
        /// <summary>
        /// This method should be used for creating a table object from an existing table on disk
        /// </summary>
        /// <param name="Host"></param>
        /// <param name="Header"></param>
        /// <param name="SortKey"></param>
        public HeapScribeTable(Host Host, TableHeader Header)
            : base(Host, Header)
        {
            this._Terminis = this.GetPage(this._Header.TerminalPageID);
            this._TableType = "HEAP_SCRIBE";
        }

        /// <summary>
        /// This method should be used for creating a brand new table object
        /// </summary>
        /// <param name="Host"></param>
        /// <param name="Name"></param>
        /// <param name="Dir"></param>
        /// <param name="Columns"></param>
        /// <param name="PageSize"></param>
        /// <param name="SortKey"></param>
        public HeapScribeTable(Host Host, string Name, string Dir, Schema Columns, int PageSize)
            : base(Host, Name, Dir, Columns, PageSize)
        {
            this._Header.OriginPageID = 0;
            this._Header.TerminalPageID = 0;
            this._Terminis = new Page(PageSize, 0, -1, -1, Columns);
            this.SetPage(this._Terminis);
            this._TableType = "HEAP_SCRIBE";
        }

        public override void Insert(Record Value)
        {

            RecordKey ptr = RecordKey.RecordNotFound;

            this.Insert(Value, out ptr);

        }

        public void Insert(Record Value, out RecordKey  Pointer)
        {

            if (this.State == TableState.ReadOnly || this.State == TableState.FullLock)
                throw new Exception("BaseTable is locked for writing");

            // Handle the terminal page being full //
            if (this._Terminis.IsFull)
            {

                Page p = new Page(this.PageSize, this.GenerateNewPageID, this._Terminis.PageID, -1, this.Columns);
                this._Terminis.NextPageID = p.PageID;
                this.SetPage(p);
                this._Terminis = p;
                this._Header.TerminalPageID = p.PageID;

            }

            // Add the actual record //
            this._Terminis.Insert(Value);
            this.RecordCount++;

            // Get the pointer //
            Pointer = new RecordKey(this._Terminis.PageID, this._Terminis.Count - 1);

        }

        public override BaseTable Partition(int PartitionIndex, int PartitionCount)
        {

            throw new NotImplementedException();

        }

        public override void PreSerialize()
        {
            if (this._Terminis != null)
                this.SetPage(this._Terminis);
        }

        public override Page TerminalPage
        {
            get
            {
                return this._Terminis;
            }
        }

        public override int TerminalPageID
        {
            get
            {
                return this._Terminis.PageID;
            }
        }

    }

    /// <summary>
    /// Creates a table sorted usina a b+tree that can be saved to disk
    /// </summary>
    public class ClusteredScribeTable : ScribeTable
    {

        protected BPlusTree _Cluster;
        
        /// <summary>
        /// This method should be used for creating a table object from an existing table on disk
        /// </summary>
        /// <param name="Host"></param>
        /// <param name="Header"></param>
        /// <param name="SortKey"></param>
        public ClusteredScribeTable(Host Host, TableHeader Header)
            : base(Host, Header)
        {
            
            if (Header.RootPageID == -1)
                throw new ArgumentException("The root page ID cannot be null");

            // Get the sort key //
            Key k = Header.SortKey;

            // Get the root page ID //
            BPlusTreePage root = BPlusTreePage.Mutate(this.GetPage(Header.RootPageID), k);

            // Cluster //
            this._Cluster = new BPlusTree(this, this.Columns, k, root, this.Header);

            this._TableType = "CLUSTER_SCRIBE";

        }

        /// <summary>
        /// This method should be used for creating a brand new table object
        /// </summary>
        /// <param name="Host"></param>
        /// <param name="Name"></param>
        /// <param name="Dir"></param>
        /// <param name="Columns"></param>
        /// <param name="PageSize"></param>
        /// <param name="SortKey"></param>
        public ClusteredScribeTable(Host Host, string Name, string Dir, Schema Columns, Key ClusterColumns, int PageSize)
            : base(Host, Name, Dir, Columns, PageSize)
        {

            this._Cluster = BPlusTree.CreateClusteredIndex(this, ClusterColumns);
            this._TableType = "CLUSTER_SCRIBE";
            this._Header.SortKey = ClusterColumns;

        }

        /// <summary>
        /// This method should be used for creating a brand new table object
        /// </summary>
        /// <param name="Host"></param>
        /// <param name="Name"></param>
        /// <param name="Dir"></param>
        /// <param name="Columns"></param>
        /// <param name="ClusterColumns"></param>
        public ClusteredScribeTable(Host Host, string Name, string Dir, Schema Columns, Key ClusterColumns)
            : this(Host, Name, Dir, Columns, ClusterColumns, Page.DEFAULT_SIZE)
        {
        }

        /// <summary>
        /// Inner B+Tree
        /// </summary>
        public BPlusTree BaseTree
        {
            get { return this._Cluster; }
        }

        /// <summary>
        /// Appends a record to a table
        /// </summary>
        /// <param name="Value"></param>
        public override void Insert(Record Value)
        {
            this._Cluster.Insert(Value);
        }

        /// <summary>
        /// Opens a record reader that focuses on a single key
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public override ReadStream OpenReader(Record Key)
        {

            if (Key.Count != this._Cluster.IndexColumns.Count)
                return this.OpenReader();
            RecordKey l = this._Cluster.SeekFirst(Key);
            RecordKey u = this._Cluster.SeekLast(Key);
            return new VanillaReadStream(this, l, u);

        }

        /// <summary>
        /// Opens a record reader to focus on records between A and B (inclusive)
        /// </summary>
        /// <param name="LKey"></param>
        /// <param name="UKey"></param>
        /// <returns></returns>
        public override ReadStream OpenReader(Record LKey, Record UKey)
        {

            if (LKey.Count != UKey.Count || LKey.Count != this._Cluster.IndexColumns.Count)
                return this.OpenReader();
            RecordKey lk = this._Cluster.SeekFirst(LKey);
            RecordKey uk = this._Cluster.SeekLast(UKey);
            return new VanillaReadStream(this, lk, uk);

        }

        /// <summary>
        /// 
        /// </summary>
        public override void PreSerialize()
        {
            this.SetPage(this._Cluster.Root);
        }

        // Methods not implemented //
        /// <summary>
        /// Splits a table into N sub tables
        /// </summary>
        /// <param name="PartitionIndex"></param>
        /// <param name="PartitionCount"></param>
        /// <returns></returns>
        public override BaseTable Partition(int PartitionIndex, int PartitionCount)
        {
            throw new NotImplementedException();
        }

    }

    // Derived tables: tables that save pages to other tables //
    // -----------------------------------------------------------------------

    public abstract class DerivedTable : BaseTable
    {

        /*
         * The following are turned off and will throw an exception:
         *      AddIndex
         *      GetIndex
         *      Partition
         *      
         * The following are taken from the parent table:
         *      GeneratePageID
         *      
         * The following must be overwritten:
         *      GenerateNewPage
         *      InsertKey
         *      
         */

        protected BaseTable _Parent;

        public DerivedTable(Host Host, TableHeader DerivedHeader, BaseTable Parent)
            : base(Host, DerivedHeader)
        {
            this._Parent = Parent;
        }

        public override int GenerateNewPageID
        {
            get
            {
                return this._Parent.GenerateNewPageID;
            }
        }

        public override void SetPage(Page Element)
        {
            this._Parent.SetPage(Element);
        }

        public override Page GetPage(int PageID)
        {
            return this._Parent.GetPage(PageID);
        }

        public override void AddIndex(string Name, Index Value)
        {
            throw new InvalidOperationException("Indexes cannot exist on derived tables");
        }

        public override Index GetIndex(string Name)
        {
            throw new InvalidOperationException("Indexes cannot exist on derived tables");
        }

        public override BaseTable Partition(int PartitionIndex, int PartitionCount)
        {
            throw new InvalidOperationException("Cannot partition derived tables");
        }

    }

}
