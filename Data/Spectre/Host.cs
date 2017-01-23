using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Rye.Structures;

namespace Rye.Data.Spectre
{


    // Support //
    public sealed class Host
    {

        public const string GLOBAL = "PSY";

        private PageCache _PageCache;
        private Communicator _IO;
        private Heap<Cell> _Scalars;
        private Heap<CellMatrix> _Matrixes;
        private Heap<string> _Connects;

        public Host()
        {

            this._PageCache = new PageCache();
            this._IO = new CommandLineCommunicator();

            this._Scalars = new Heap<Cell>();
            this._Matrixes = new Heap<CellMatrix>();
            this._Connects = new Heap<string>();

        }

        public void ShutDown()
        {
            this.PageCache.ShutDown();
        }

        public PageCache PageCache
        {
            get { return this._PageCache; }
        }

        public Communicator IO
        {
            get { return this._IO; }
        }

        public Heap<Cell> Scalars
        {
            get { return this._Scalars; }
        }

        public Heap<CellMatrix> Matrixes
        {
            get { return this._Matrixes; }
        }

        public Heap<string> Connections
        {
            get { return this._Connects; }
        }

        // Connection Support //
        public void AddConnection(string Alias, string Connection)
        {
            this._Connects.Allocate(Alias, Connection);
        }

        // Table Support //
        public BaseTable GetTable(string Key)
        {
            return null;
        }

        public BaseTable GetTable(string Alias, string Name)
        {
            return null;
        }

    }

    /// <summary>
    /// This class holds all in memory pages and manages buffering pages and tables from disk
    /// </summary>
    public sealed class PageCache
    {

        /// <summary>
        /// The default capacity is 128 MB
        /// </summary>
        public const long DEFAULT_CAPACITY = 1024 * 1024 * 128;

        private Dictionary<string, Entry> _Elements;
        private long _Capacity = 0;

        public PageCache(long Capacity)
        {
            this._Elements = new Dictionary<string, Entry>(StringComparer.OrdinalIgnoreCase);
            this._Capacity = Capacity;
        }

        public PageCache()
            : this(DEFAULT_CAPACITY)
        {
        }

        // Creation //
        /// <summary>
        /// Adds a table to the cache
        /// </summary>
        /// <param name="Table"></param>
        public void AddTable(BaseTable Table)
        {

            if (this.ElementExists(Table.Key))
            {
                throw new ElementDoesNotExistException(Table.Key);
            }

            this._Elements.Add(Table.Key, new Entry(Table));

        }

        // Exists methods //
        /// <summary>
        /// Checks if an element exists
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public bool ElementExists(string Key)
        {
            return this._Elements.ContainsKey(Key);
        }

        /// <summary>
        /// Checks if a page exists
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="PageID"></param>
        /// <returns></returns>
        public bool PageExists(string Key, int PageID)
        {

            if (!this._Elements.ContainsKey(Key))
                return false;
            return this._Elements[Key].Exists(PageID);

        }

        /// <summary>
        /// Checks if a table is in-memory only
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public bool IsDream(string Key)
        {
            if (!this.ElementExists(Key))
                return false;
            return this._Elements[Key].IsDream;
        }

        // Request / Push Methods //
        public Page RequestPage(string Key, int PageID)
        {

            // Check for the object //
            if (!this.ElementExists(Key))
                throw new ElementDoesNotExistException(Key);

            // Check if the page is in memory already //
            if (this._Elements[Key].Exists(PageID))
            {
                return this._Elements[Key].Peek(PageID);
            }

            // Otherwise, check if this is hard object and buffer //
            if (!this.IsDream(Key))
            {
                Page p = this.Buffer(Key, PageID);
                this.PushPage(Key, p);
                return p;
            }

            // Otherwise, the page doesnt exist //
            throw new PageDoesNotExistException(Key, PageID);

        }

        public BaseTable RequestTable(Host Host, string Key)
        {

            if (this.ElementExists(Key))
                return this._Elements[Key].Parent;

            if (!File.Exists(Key))
            {
                throw new ElementDoesNotExistException(Key);
            }

            return this.Buffer(Host, Key, true);

        }

        public void PushPage(string Key, Page Element)
        {

            this.PushPage(Key, Element, false);

        }

        public void PushPage(string Key, Page Element, bool Write)
        {

            if (!this.ElementExists(Key))
            {
                throw new ElementDoesNotExistException(Key);
            }

            this._Elements[Key].Push(Element, Write);

        }

        // Memory Methods //
        public void FreeAll(string Key)
        {

            if (!this.ElementExists(Key))
                throw new ElementDoesNotExistException(Key);

            if (!this._Elements[Key].IsDream)
            {
                this.FreePageSpace(Key, this._Elements[Key].Count);
            }

            this._Elements.Remove(Key);

        }

        public int FreePageSpace(string Key, int ElementCount)
        {

            if (!this.ElementExists(Key))
                throw new ElementDoesNotExistException(Key);
            if (this._Elements[Key].IsDream)
                return 0;

            int DiskHits = 0;

            int BurtPageCount = 0;
            for (int i = 0; i < ElementCount; i++)
            {

                int PageID = this._Elements[Key].SuggestBurnPage();
                if (PageID == -1)
                    break;

                // Check if this needs to be flushed to disk //
                if (this._Elements[Key].WriteCount(PageID) > 0)
                {
                    Page p = this._Elements[Key].Pop(PageID);
                    this.Flush(Key, p);
                    DiskHits++;
                }
                else
                {
                    this._Elements[Key].Burn(PageID);
                }
                BurtPageCount++;

            }

            // If we hit the disk, we need to flush the header //
            if (DiskHits > 0)
            {
                this.Flush(Key, this._Elements[Key].Parent.Header);
            }

            return BurtPageCount;

        }

        public int InMemoryPageCount(string Key)
        {

            if (!this.ElementExists(Key))
                throw new ElementDoesNotExistException(Key);

            return this._Elements[Key].Count;

        }

        public long MemoryUsage()
        {
            long t = 0;
            // Note: the dictionary enumerator breaks if the dictionary is modified; use .ToArray() to get around that
            foreach (Entry x in this._Elements.Values.ToArray())
            {
                t += x.MemoryUsage;
            }
            return t;
        }

        public long FreeMemory()
        {
            return this._Capacity - this.MemoryUsage();
        }

        public void ShutDown()
        {

            string[] Elements = this._Elements.Keys.ToArray();
            foreach (string k in Elements)
            {
                this.FreeAll(k);
            }

        }

        // Table Drops //
        /// <summary>
        /// Removes dream tables from memory; removes scribe tables from memory and disk
        /// </summary>
        /// <param name="Key"></param>
        public void DropTable(string Key)
        {

            // Take care of the entry //
            if (this.ElementExists(Key))
            {
                this._Elements.Remove(Key);
            }

            // Take care of the file on disk //
            if (File.Exists(Key))
            {
                File.Delete(Key);
            }

        }

        // Disk methods //
        /// <summary>
        /// Buffers a page
        /// </summary>
        /// <param name="Path"></param>
        /// <param name="PageID"></param>
        /// <returns></returns>
        internal Page Buffer(string Path, int PageID)
        {

            // Get the header //
            TableHeader h = this._Elements[Path].Parent.Header;

            // Get the location on disk of the page //
            long Location = PageAddress(PageID, h.PageSize);

            // Buffer the page //
            byte[] b = new byte[h.PageSize];
            using (FileStream x = File.Open(Path, FileMode.Open, FileAccess.Read, FileShare.None))
            {

                // Go to the file offset //
                x.Position = Location;

                // Buffer the bytes //
                x.Read(b, 0, h.PageSize);

            }

            Page p = Page.Read(b, h.PageSize);

            return p;

        }

        /// <summary>
        /// Flushes a page to disk
        /// </summary>
        /// <param name="Path"></param>
        /// <param name="Key"></param>
        internal void Flush(string Path, Page Element)
        {

            // Get the disk location //
            long Position = this.PageAddress(Element.PageID, Element.PageSize);

            // Convert to a hash //
            byte[] b = new byte[Element.PageSize];
            Page.Write(b, 0, Element);

            // Hit the disk //
            using (FileStream x = File.Open(Path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
            {
                x.Position = Position;
                x.Write(b, 0, Element.PageSize);
            }


        }

        /// <summary>
        /// Reads the table header from disk, but does NOT allocate in the current heap
        /// </summary>
        /// <param name="Path"></param>
        /// <returns></returns>
        internal TableHeader Buffer(string Path)
        {

            byte[] buffer = new byte[TableHeader.SIZE];
            using (FileStream x = File.Open(Path, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                x.Read(buffer, 0, TableHeader.SIZE);
            }

            TableHeader h = TableHeader.FromHash(buffer, 0);

            return h;

        }

        /// <summary>
        /// Flushes a table header to disk
        /// </summary>
        /// <param name="Path"></param>
        /// <param name="Key"></param>
        internal void Flush(string Path, TableHeader Element)
        {

            // Convert to a hash //
            byte[] b = new byte[TableHeader.SIZE];
            TableHeader.ToHash(b, 0, Element);

            // Hit the disk //
            using (FileStream x = File.Open(Path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
            {
                x.Write(b, 0, b.Length);
            }

        }

        /// <summary>
        /// Gets the on disk address of a given page
        /// </summary>
        /// <param name="PageID"></param>
        /// <param name="PageSize"></param>
        /// <returns></returns>
        internal long PageAddress(int PageID, int PageSize)
        {

            long HeaderOffset = TableHeader.SIZE;
            long pid = (long)PageID;
            long ps = (long)PageSize;
            return HeaderOffset + pid * ps;

        }

        /// <summary>
        /// Creates a map of all pages in memory
        /// </summary>
        /// <returns></returns>
        internal string ElementMap()
        {

            StringBuilder sb = new StringBuilder();
            foreach (Entry e in this._Elements.Values)
            {
                sb.AppendLine(string.Format("Object: {0}", e.Key));
                sb.AppendLine(e.PageMap());
            }
            return sb.ToString();

        }

        /// <summary>
        /// Pulls a table in from disk
        /// </summary>
        /// <param name="Host"></param>
        /// <param name="Path"></param>
        /// <param name="CacheAsMuchAsPossible"></param>
        /// <returns></returns>
        internal ScribeTable Buffer(Host Host, string Path, bool CacheAsMuchAsPossible)
        {

            // Get the table header //
            TableHeader h = this.Buffer(Path);
            ScribeTable t = new HeapScribeTable(Host, h);
            if (!this.ElementExists(h.Key))
            {
                this.AddTable(t);
            }

            // Check to see how many pages we can buffer //
            int MaxPages = (int)(this.FreeMemory() / h.PageSize);
            int Pages = Math.Min(h.PageCount, MaxPages);

            // Buffer a block of pages //
            if (CacheAsMuchAsPossible)
            {
                this.BufferBlock(h, 0, Pages);
            }

            return new HeapScribeTable(Host, h);

        }

        /// <summary>
        /// Buffers a block of pages from disk
        /// </summary>
        /// <param name="Header"></param>
        /// <param name="PageOffset"></param>
        /// <param name="PageCount"></param>
        private void BufferBlock(TableHeader Header, int PageOffset, int PageCount)
        {

            long Offset = TableHeader.SIZE + (long)(PageOffset) * (long)Header.PageSize;
            long ByteCount = (long)PageCount * (long)Header.PageSize;
            if (ByteCount > (long)int.MaxValue)
                throw new IndexOutOfRangeException("Cannot read more than 2gb into memory at once");
            
            byte[] b = new byte[ByteCount];
            using (FileStream fs = File.Open(Header.Path, FileMode.Open, FileAccess.Read, FileShare.None))
            {
                fs.Position = Offset;
                fs.Read(b, 0, (int)ByteCount);
            }

            RecordMatcher matcher = new RecordMatcher(Header.SortKey);
            long Location = 0;
            for (int i = 0; i < PageCount; i++)
            {

                Page p = Page.Read(b, Location);
                if (p.PageType == Page.SORTED_PAGE_TYPE)
                {
                    p = new SortedPage(p, matcher);
                }
                Location += Header.PageSize;
                this.PushPage(Header.Key, p);

            }

        }

        // Sub-Classes //
        /// <summary>
        /// An entry holds all pages for a given database object
        /// </summary>
        public class Entry
        {

            private Dictionary<int, Page> _Pages; // Key = page id, value = page
            private Dictionary<int, Tuple<int, int>> _Requests; // Value1 = read requests, value2 = write requests
            private Queue<int> _BurnQueue;
            private string _Key; // the name of the database object
            private int _PageSize; // the database object's page size
            private bool _IsDream = false;
            private BaseTable _Parent;

            public Entry(BaseTable Table)
            {
                this._Key = Table.Key;
                this._PageSize = Table.PageSize;
                this._Pages = new Dictionary<int, Page>();
                this._Requests = new Dictionary<int, Tuple<int, int>>();
                this._BurnQueue = new Queue<int>();
                this._IsDream = Table.Header.IsMemoryOnly;
                this._Parent = Table;
            }

            // Properties //
            public int Count
            {
                get { return this._Pages.Count; }
            }

            public int PageSize
            {
                get { return this._PageSize; }
            }

            public long MemoryUsage
            {
                get { return (long)(this._Pages.Count) * (long)(this._PageSize); }
            }

            public string Key
            {
                get { return this._Key; }
            }

            public bool IsDream
            {
                get { return this._IsDream; }
            }

            public BaseTable Parent
            {
                get { return this._Parent; }
            }

            // Methods //
            public bool Exists(int PageID)
            {
                return this._Pages.ContainsKey(PageID);
            }

            public int ReadCount(int PageID)
            {
                return this._Requests[PageID].Item1;
            }

            public void IncrementReadCount(int PageID)
            {
                Tuple<int, int> x = this._Requests[PageID];
                this._Requests[PageID] = new Tuple<int, int>(x.Item1 + 1, x.Item2);
            }

            public int WriteCount(int PageID)
            {
                return this._Requests[PageID].Item2;
            }

            public void IncrementWriteCount(int PageID)
            {
                Tuple<int, int> x = this._Requests[PageID];
                this._Requests[PageID] = new Tuple<int, int>(x.Item1, x.Item2 + 1);
            }

            public void Push(Page Element, bool Write)
            {

                // Check to see if the page exists in memory already //
                if (this.Exists(Element.PageID))
                {
                    this._Pages[Element.PageID] = Element;
                    this.IncrementWriteCount(Element.PageID);
                    return;
                }

                // Otherwise allocate //
                this._Pages.Add(Element.PageID, Element);
                this._Requests.Add(Element.PageID, new Tuple<int, int>(0, 0));
                this._BurnQueue.Enqueue(Element.PageID);
                if (Write) this.IncrementWriteCount(Element.PageID);

            }

            public void Push(Page Element)
            {
                this.Push(Element, false);
            }

            public Page Peek(int PageID)
            {

                this.IncrementReadCount(PageID);
                return this._Pages[PageID];

            }

            public void Burn(int PageID)
            {

                if (!this.Exists(PageID))
                    return;

                this._Pages.Remove(PageID);
                this._Requests.Remove(PageID);

            }

            public Page Pop(int PageID)
            {
                Page p = this.Peek(PageID);
                this.Burn(PageID);
                return p;
            }

            public int SuggestBurnPage()
            {

                if (this.Count == 0)
                    return -1;

                int id = this._BurnQueue.Dequeue();
                while (!this.Exists(id))
                {
                    id = this._BurnQueue.Dequeue();
                }

                return id;
            }

            public string PageMap()
            {

                StringBuilder sb = new StringBuilder();
                foreach (Page p in this._Pages.Values)
                {
                    sb.AppendLine(p.MapElement());
                }
                return sb.ToString();

            }

        }

        /// <summary>
        /// Thrown if a given page does not exist
        /// </summary>
        public class PageDoesNotExistException : Exception
        {

            public PageDoesNotExistException(string ObjectName, int PageID)
                : base(string.Format("Page {0} does not exist for '{1}'", PageID, ObjectName))
            {
            }

        }

        /// <summary>
        /// Throw when an object doesnt exist
        /// </summary>
        public class ElementDoesNotExistException : Exception
        {

            public ElementDoesNotExistException(string ObjectName)
                : base(string.Format("Object '{0}' does not exist", ObjectName))
            {

            }

        }

        /// <summary>
        /// Exception for duplicate elements
        /// </summary>
        public class ElementExistsException : Exception
        {

            public ElementExistsException(string ObjectName)
                : base(string.Format("Object '{0}' already exists", ObjectName))
            {

            }

        }


    }


}
