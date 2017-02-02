using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace Rye.Data.Spectre
{


    // Keys //
    [System.Runtime.InteropServices.StructLayout(LayoutKind.Explicit)]
    public struct RecordKey
    {

        [System.Runtime.InteropServices.FieldOffset(0)]
        internal long U_ID;

        [System.Runtime.InteropServices.FieldOffset(0)]
        internal int ROW_ID;

        [System.Runtime.InteropServices.FieldOffset(4)]
        internal int PAGE_ID;

        public RecordKey(int PageID, int RowID)
        {
            this.U_ID = 0;
            this.PAGE_ID = PageID;
            this.ROW_ID = RowID;
        }

        public RecordKey(long UID)
        {
            this.PAGE_ID = 0;
            this.ROW_ID = 0;
            this.U_ID = UID;
        }

        public RecordKey(Cell Element)
            : this(Element.INT_A, Element.INT_B)
        {
        }

        public long UID
        {
            get { return this.U_ID; }
        }

        public int PageID
        {
            get { return this.PAGE_ID; }
        }

        public int RowID
        {
            get { return this.ROW_ID; }
        }

        public Cell Element
        {
            get { return new Cell(this.PAGE_ID, this.ROW_ID); }
        }

        public bool IsNotFound
        {
            get { return this.PAGE_ID == -1 && this.ROW_ID == -1; }
        }

        public static long GetUID(int PageID, int RowID)
        {
            return new RecordKey(PageID, RowID).U_ID;
        }

        public static long GetPageID(long UID)
        {
            return new RecordKey(UID).PAGE_ID;
        }

        public static long GetRowID(long UID)
        {
            return new RecordKey(UID).ROW_ID;
        }

        public static RecordKey RecordNotFound
        {
            get { return new RecordKey(-1, -1); }
        }

    }

    public class IndexHeader : IElementHeader
    {

        /*
            Index table record:
            Name: 36 (32 chars + 4 length)
            PageSize: 4 bytes
            OriginPageID: 4 bytes
            TerminalPageID: 4 bytes
            RootPageID: 4 bytes
            RecordCount: 4 bytes
            PageCount: 4 bytes
            Key: 4 byte count, n x 8 bytes length (up to 8 columns)
         
            128 bytes 
         
         */
        
        public const int MAX_INDEX_COLUMNS = 8;
        public const int MAX_NAME_LEN = 32;
        public const int OFFSET_NAME = 0;
        public const int OFFSET_ORIGIN_PAGE_ID = 36;
        public const int OFFSET_TERMINAL_PAGE_ID = 40;
        public const int OFFSET_ROOT_PAGE_ID = 44;
        public const int OFFSET_RECORD_COUNT = 48;
        public const int OFFSET_PAGE_COUNT = 56;
        public const int OFFSET_INDEX_COLUMNS = 64;
        public const int SIZE_HASH = 128;
        
        private string _Name;
        private int _OriginPageID;
        private int _TerminalPageID;
        private int _RootPageID;
        private long _RecordCount;
        private int _PageCount;
        private Key _IndexColumns;

        private IndexHeader()
        {
            this._Name = "";
            this._IndexColumns = new Key();
        }

        public IndexHeader(string Name, int OriginPageID, int TerminalPageID, int RootPageID, long RecordCount, int PageCount, Key IndexColumns)
        {
            this.Name = Name;
            this.OriginPageID = OriginPageID;
            this.TerminalPageID = TerminalPageID;
            this.RootPageID = RootPageID;
            this.RecordCount = RecordCount;
            this.PageCount = PageCount;
            this.IndexColumns = IndexColumns;
        }

        public string Name
        {
            get 
            { 
                return this._Name; 
            }
            set
            {
                if (value.Length > MAX_NAME_LEN)
                {
                    value = value.Substring(0, MAX_NAME_LEN);
                }
                this._Name = value;
            }

        }

        public int OriginPageID
        {
            get { return this._OriginPageID; }
            set { this._OriginPageID = value; }
        }

        public int TerminalPageID
        {
            get { return this._TerminalPageID; }
            set { this._TerminalPageID = value; }
        }

        public int RootPageID
        {
            get { return this._RootPageID; }
            set { this._RootPageID = value; }
        }

        public long RecordCount
        {
            get { return this._RecordCount; }
            set { this._RecordCount = value; }
        }

        public int PageCount
        {
            get { return this._PageCount; }
            set { this._PageCount = value; }
        }

        public Key IndexColumns
        {
            get 
            { 
                return this._IndexColumns; 
            }
            set 
            {
                if (this._IndexColumns.Count > MAX_INDEX_COLUMNS)
                    throw new IndexOutOfRangeException("Can't index more than eight columns");
                this._IndexColumns = value; 
            }
        }

        public int PointerIndex
        {
            get { return this._IndexColumns.Count; }
        }

        public static void Write(byte[] Buffer, int Location, IndexHeader Header)
        {

            Array.Copy(BitConverter.GetBytes(Header.Name.Length), 0, Buffer, Location + OFFSET_NAME, 4);
            Array.Copy(ASCIIEncoding.ASCII.GetBytes(Header.Name), 0, Buffer, Location + OFFSET_NAME + 4, Header.Name.Length);
            Array.Copy(BitConverter.GetBytes(Header.OriginPageID), 0, Buffer, Location + OFFSET_ORIGIN_PAGE_ID, 4);
            Array.Copy(BitConverter.GetBytes(Header.TerminalPageID), 0, Buffer, Location + OFFSET_TERMINAL_PAGE_ID, 4);
            Array.Copy(BitConverter.GetBytes(Header.RootPageID), 0, Buffer, Location + OFFSET_ROOT_PAGE_ID, 4);
            Array.Copy(BitConverter.GetBytes(Header.RecordCount), 0, Buffer, Location + OFFSET_RECORD_COUNT, 8);
            Array.Copy(BitConverter.GetBytes(Header.PageCount), 0, Buffer, Location + OFFSET_PAGE_COUNT, 4);
            Array.Copy(Header.IndexColumns.Bash(), 0, Buffer, Location + OFFSET_INDEX_COLUMNS, 4);
            
        }

        public static IndexHeader Read(byte[] Buffer, int Location)
        {

            IndexHeader h = new IndexHeader();
            int len = BitConverter.ToInt32(Buffer, Location + OFFSET_NAME);
            h.Name = ASCIIEncoding.ASCII.GetString(Buffer, Location + OFFSET_NAME + 4, len);
            h.OriginPageID = BitConverter.ToInt32(Buffer, Location + OFFSET_ORIGIN_PAGE_ID);
            h.TerminalPageID = BitConverter.ToInt32(Buffer, Location + OFFSET_TERMINAL_PAGE_ID);
            h.RootPageID = BitConverter.ToInt32(Buffer, Location + OFFSET_ROOT_PAGE_ID);
            h.RecordCount = BitConverter.ToInt64(Buffer, Location + OFFSET_RECORD_COUNT);
            h.PageCount = BitConverter.ToInt32(Buffer, Location + OFFSET_PAGE_COUNT);
            h.IndexColumns = Key.Read(Buffer, Location + OFFSET_INDEX_COLUMNS);

            return h;

        }

    }

    // Collections //
    public class IndexCollection
    {

        public IndexCollection()
        {
        }

        public void Insert(RecordKey Key, Record Value)
        {
        }

        public ReadStream OpenRead(Key Optimial)
        {
            return null;
        }

        public Index this[string Name]
        {
            get { return null; }
            set { }
        }

        public int Count
        {
            get { return 0; }
        }

    }

    // Indexes //
    public class Index
    {

        /// <summary>
        /// Represents the base b+tree object
        /// </summary>
        private BPlusTree _Tree;

        /// <summary>
        /// Represents the table where the index will be stored
        /// </summary>
        private BaseTable _Storage;

        /// <summary>
        /// Represents the table that the index is
        /// </summary>
        private BaseTable _Parent;

        private Key _IndexColumns;

        private IndexHeader _Header;

        /// <summary>
        /// Opens an existing index
        /// </summary>
        /// <param name="Storage"></param>
        /// <param name="Parent"></param>
        /// <param name="Header"></param>
        public Index(BaseTable Storage, BaseTable Parent, IndexHeader Header)
        {

            this._Storage = Storage;
            this._Parent = Parent;
            this._Header = Header;
            this._IndexColumns = Header.IndexColumns;
            BPlusTreePage root = BPlusTreePage.Mutate(this._Storage.GetPage(Header.RootPageID), Header.IndexColumns);
            Schema s = BPlusTree.NonClusteredIndexColumns(this._Parent.Columns, Header.IndexColumns);
            this._Tree = new BPlusTree(Storage, s, Key.Build(this._IndexColumns.Count), root, Header);

        }

        /// <summary>
        /// Creates a new index
        /// </summary>
        /// <param name="Storage"></param>
        /// <param name="Parent"></param>
        /// <param name="IndexColumns"></param>
        public Index(BaseTable Storage, BaseTable Parent, string Name, Key IndexColumns)
        {

            this._Header = new IndexHeader(Name, -1, -1, -1, 0, 0, IndexColumns);
            this._Storage = Storage;
            this._Parent = Parent;
            this._IndexColumns = IndexColumns;
            Schema s = BPlusTree.NonClusteredIndexColumns(this._Parent.Columns, this._IndexColumns);
            this._Tree = new BPlusTree(this._Storage, s, this._IndexColumns, null, this._Header);

        }

        // Properties //
        public BaseTable Storage
        {
            get { return this._Storage; }
        }

        public BaseTable Parent
        {
            get { return this._Parent; }
        }

        public Key IndexColumns
        {
            get { return this._IndexColumns; }
        }

        public IndexHeader Header
        {
            get { return this._Header; }
        }

        public BPlusTree Tree
        {
            get { return this._Tree; }
        }

        // Methods //
        public void Insert(Record Element, RecordKey Key)
        {
            Record x = Index.GetIndexElement(Element, Key, this._IndexColumns);
            this._Tree.Insert(Element);
        }

        public ReadStream OpenReader()
        {
            return new IndexDataReadStream(this._Header, this._Storage, this._Parent);
        }

        public ReadStream OpenReader(Record Key)
        {
            RecordKey l = this._Tree.SeekFirst(Key);
            RecordKey u = this._Tree.SeekLast(Key);
            return new IndexDataReadStream(this._Header, this._Storage, this._Parent, l, u);
        }

        public ReadStream OpenReader(Record LKey, Record UKey)
        {
            RecordKey l = this._Tree.SeekFirst(LKey);
            RecordKey u = this._Tree.SeekLast(UKey);
            return new IndexDataReadStream(this._Header, this._Storage, this._Parent, l, u);
        }

        // Statics //
        public static Record GetIndexElement(Record Element, RecordKey Pointer, Key IndexColumns)
        {

            Cell[] c = new Cell[IndexColumns.Count + 1];
            for (int i = 0; i < IndexColumns.Count; i++)
            {
                c[i] = Element[IndexColumns[i]];
            }
            c[c.Length - 1] = Pointer.Element;
            return new Record(c);

        }


    }



}
