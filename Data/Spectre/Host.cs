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

        private PageManager _PageCache;
        private Communicator _IO;
        private Heap<Cell> _Scalars;
        private Heap<CellMatrix> _Matrixes;
        private Heap<string> _Connects;

        public Host()
        {

            this._PageCache = new PageManager(this);
            this._IO = new CommandLineCommunicator();

            this._Scalars = new Heap<Cell>();
            this._Matrixes = new Heap<CellMatrix>();
            this._Connects = new Heap<string>();

        }

        public void ShutDown()
        {
            this.PageCache.ShutDown();
        }

        public PageManager PageCache
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
    public sealed class PageManager
    {

        /// <summary>
        /// The default capacity is 128 MB
        /// </summary>
        public const long DEFAULT_CAPACITY = 1024 * 1024 * 32;

        //private Dictionary<string, Entry> _Elements;
        //private BurnQueue<PageUID> _WoodPile;
        private long _MaxMemory = 0;
        private long _Memory = 0;
        private Host _Host;

        // It's easier to keep two sets of books, one for the dream tables and one for scribe tables //
        private Dictionary<string, ScribeTable> _ScribeTables;
        private Dictionary<PageUID, Page> _ScribePages;
        private Dictionary<PageUID, int> _ScribeWrites;
        private Dictionary<string, DreamTable> _DreamTables;
        private Dictionary<PageUID, Page> _DreamPages;

        // Constructors //
        public PageManager(Host Host, long Capacity)
        {
            //this._Elements = new Dictionary<string, Entry>(StringComparer.OrdinalIgnoreCase);
            //this._WoodPile = new BurnQueue<PageUID>(PageUID.DefaultComparer);

            this._ScribeTables = new Dictionary<string, ScribeTable>(StringComparer.OrdinalIgnoreCase);
            this._ScribePages = new Dictionary<PageUID, Page>(PageUID.DefaultComparer);
            this._ScribeWrites = new Dictionary<PageUID, int>(PageUID.DefaultComparer);
            this._DreamTables = new Dictionary<string, DreamTable>(StringComparer.OrdinalIgnoreCase);
            this._DreamPages = new Dictionary<PageUID, Page>(PageUID.DefaultComparer);
            
            this._MaxMemory = Capacity;
            this._Host = Host;

        }

        public PageManager(Host Host)
            : this(Host, DEFAULT_CAPACITY)
        {
        }

        // Properties //
        public long MaxMemory
        {
            get { return this._MaxMemory; }
        }

        public long UsedMemory
        {
            get { return this._Memory; }
        }

        public long FreeMemory
        {
            get { return this._MaxMemory - this._Memory; }
        }

        // Scribe Tables //
        public void AddScribeTable(ScribeTable Table)
        {

            if (this.ScribeTableExists(Table.Key))
            {
                throw new ElementDoesNotExistException(Table.Key);
            }

            this._ScribeTables.Add(Table.Key, Table);
            this._Memory += TableHeader.SIZE;

        }

        public ScribeTable RequestScribeTable(string Key)
        {
            
            if (this.ScribeTableExists(Key))
            {
                return this._ScribeTables[Key];
            }
            else
            {

                // Get the table header //
                TableHeader h = this.Buffer(Key);

                // Create the table //
                ScribeTable t = new HeapScribeTable(this._Host, h); // The ctor adds the table to the cache
            
                // Check to see how many pages we can buffer //
                int MaxPages = (int)(this.FreeMemory / h.PageSize);
                int Pages = Math.Min(h.PageCount, MaxPages);

                // Buffer a block of pages //
                this.BufferBlock(h, 0, Pages);

                return t;

            }

            throw new ElementDoesNotExistException(Key);


        }

        public bool ScribeTableExists(string Key)
        {
            return this._ScribeTables.ContainsKey(Key);
        }

        public bool ScribePageExists(PageUID PID)
        {
            return this._ScribePages.ContainsKey(PID);
        }

        public void PushScribePage(string Key, Page Element, bool Write)
        {

            // Check if the element key doesnt exist //
            if (!this.ScribeTableExists(Key))
                throw new ElementDoesNotExistException(Key);

            // Build a PID //
            PageUID pid = new PageUID(Key, Element.PageID);

            // If the page exists already //
            if (this.ScribePageExists(pid))
            {
                this._ScribePages[pid] = Element;
                this._ScribeWrites[pid]++;
                Element.Cached = true; // just in case...
            }
            // Otherwise, add the page //
            else
            {
                this._ScribePages.Add(pid, Element);
                this._ScribeWrites.Add(pid, Write ? 1 : 0);
                this._Memory += Element.PageID;
                Element.Cached = true;
            }


        }

        public void PushScribePage(string Key, Page Element)
        {
            this.PushScribePage(Key, Element, true);
        }

        public Page RequestScribePage(string Key, int PageID)
        {

            PageUID pid = new PageUID(Key, PageID);

            if (!this.ScribeTableExists(Key))
                throw new ElementDoesNotExistException(Key);

            if (this.ScribePageExists(pid))
            {
                return this._ScribePages[pid];
            }
            else
            {
                Page p = this.Buffer(Key, PageID);
                this.PushScribePage(Key, p, false);
                return p;
            }
                
        }

        public void BurnScribePage(string Key, int PageID, bool Flush)
        {

            PageUID pid = new PageUID(Key, PageID);

            // See if the page exists //
            if (!this.ScribePageExists(pid))
                throw new Exception("Page does not exist");

            // Get the page //
            Page p = this._ScribePages[pid];
            int Writes = this._ScribeWrites[pid];

            // Remove from the cache //
            this._ScribePages.Remove(pid);
            this._ScribeWrites.Remove(pid);

            // Remove from memory //
            this._Memory -= p.PageSize;

            // Set the page to non-cached //
            p.Cached = false;

            // Actually hit disk //
            if (Flush && Writes > 0)
                this.Flush(Key, p);

        }

        public void BurnScribeTable(string Key, bool Flush)
        {

            if (!this.ScribeTableExists(Key))
                throw new ElementDoesNotExistException(Key);

            // Get the table //
            BaseTable t = this._ScribeTables[Key];

            // Remove from memory //
            this._ScribeTables.Remove(Key);
            this._Memory -= TableHeader.SIZE;

            // Get all the pages //
            var Pages = this._ScribePages.Select((x) => { return x.Key.Key == Key; });

            // Burn every page //
            foreach (KeyValuePair<PageUID, Page> kv in this._ScribePages.ToList())
            {
                this.BurnScribePage(kv.Key.Key, kv.Key.PageID, Flush);
            }

            // Dump the table to disk //
            if (Flush)
                this.Flush(Key, t.Header);


        }

        // Dream Tables //
        public void AddDreamTable(DreamTable Table)
        {

            if (this.DreamTableExists(Table.Key))
            {
                throw new ElementDoesNotExistException(Table.Key);
            }

            this._DreamTables.Add(Table.Key, Table);

        }

        public DreamTable RequestDreamTable(string Key)
        {
            if (!this.DreamTableExists(Key))
                throw new ElementDoesNotExistException(Key);
            return this._DreamTables[Key];
        }

        public bool DreamTableExists(string Key)
        {
            return this._DreamTables.ContainsKey(Key);
        }

        public bool DreamPageExists(PageUID PID)
        {
            return this._DreamPages.ContainsKey(PID);
        }

        public void PushDreamPage(string Key, Page Element)
        {

            // Check if the element key doesnt exist //
            if (!this.ScribeTableExists(Key))
                throw new ElementDoesNotExistException(Key);

            // Build a PID //
            PageUID pid = new PageUID(Key, Element.PageID);

            // If the page exists already //
            if (this.DreamPageExists(pid))
            {
                this._DreamPages[pid] = Element;
                Element.Cached = true;
            }
            // Otherwise, add the page //
            else
            {
                this._DreamPages.Add(pid, Element);
                this._Memory += Element.PageID;
                Element.Cached = true;
            }

        }

        public Page RequestDreamPage(string Key, int PageID)
        {

            PageUID pid = new PageUID(Key, PageID);

            if (!this.ScribeTableExists(Key))
                throw new ElementDoesNotExistException(Key);

            if (this.ScribePageExists(pid))
            {
                return this._ScribePages[pid];
            }

            throw new Exception("Page does not exist");

        }

        public void BurnDreamPage(string Key, int PageID)
        {

            PageUID pid = new PageUID(Key, PageID);

            // See if the page exists //
            if (!this.DreamPageExists(pid))
                throw new Exception("Page does not exist");

            // Get the page //
            Page p = this._ScribePages[pid];

            // Remove from the cache //
            this._DreamPages.Remove(pid);

            // Remove from memory //
            this._Memory -= p.PageSize;

            // Set the page to non-cached //
            p.Cached = false;

        }

        public void BurnDreamTable(string Key)
        {

            if (!this.DreamTableExists(Key))
                throw new ElementDoesNotExistException(Key);

            // Get the table //
            BaseTable t = this._DreamTables[Key];

            // Remove from memory //
            this._DreamTables.Remove(Key);
            this._Memory -= TableHeader.SIZE;

            // Get all the pages //
            var Pages = this._DreamPages.Select((x) => { return x.Key.Key == Key; });

            // Burn every page //
            foreach (KeyValuePair<PageUID, Page> kv in this._DreamPages)
            {
                this.BurnDreamPage(kv.Key.Key, kv.Key.PageID);
            }

        }

        // Freeing Methods //
        public void ShutDown()
        {

            List<string> keys = this._ScribeTables.Keys.ToList();
            foreach (string s in keys)
                this.BurnScribeTable(s, true);

            keys = this._DreamTables.Keys.ToList();
            foreach (string s in keys)
                this.BurnDreamTable(s);

        }

        // Table Drops //
        /// <summary>
        /// Removes dream tables from memory; removes scribe tables from memory and disk
        /// </summary>
        /// <param name="Key"></param>
        public void DropTable(string Key)
        {

            // Take care of the entry //
            this.BurnScribeTable(Key, false);

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
            TableHeader h = this._ScribeTables[Path].Header;

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

            Page p = Page.Read(b, 0);

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
                this.PushScribePage(Header.Key, p, false);

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

    public class LRUQueue<TKey, TValue>
    {

        private class LRUNode<TKey, TValue>
        {

            public LRUNode<TKey, TValue> LastNode;
            public LRUNode<TKey, TValue> NextNode;
            public TKey Key;
            public TValue Value;

        }

        private Dictionary<TKey, LRUNode<TKey,TValue>> _Dictionary;
        private LRUNode<TKey, TValue> _Highest;
        private LRUNode<TKey, TValue> _Lowest;
        private int _Count;

        public LRUQueue(IEqualityComparer<TKey> Comparer)
        {
            this._Count = 0;
            this._Dictionary = new Dictionary<TKey,LRUNode<TKey,TValue>>(Comparer);
        }

        public LRUQueue()
            :this(EqualityComparer<TKey>.Default)
        {
        }

        public void Mark(TKey Key, TValue Value)
        {

            if (this._Dictionary.ContainsKey(Key))
            {

                LRUNode<TKey, TValue> oldhead = this._Highest;
                this._Highest = this._Dictionary[Key];

            }

        }

    }

    public class PageUID
    {

        public PageUID(string Key, int PageID)
        {
            this.Key = Key;
            this.PageID = PageID;
        }

        public string Key
        {
            get;
            set;
        }

        public int PageID
        {
            get;
            set;
        }

        public static IEqualityComparer<PageUID> DefaultComparer
        {
            get { return new PageUIDComparer(); }
        }

        private sealed class PageUIDComparer : IEqualityComparer<PageUID>
        {

            public bool Equals(PageUID A, PageUID B)
            {

                return (StringComparer.OrdinalIgnoreCase.Compare(A.Key, B.Key) == 0 && A.PageID == B.PageID);

            }

            public int GetHashCode(PageUID A)
            {
                return StringComparer.OrdinalIgnoreCase.GetHashCode(A.Key) ^ A.PageID;
            }

        }

    }

}
