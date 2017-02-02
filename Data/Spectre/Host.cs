using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Rye.Structures;

namespace Rye.Data.Spectre
{


    // Support //
    /// <summary>
    /// 
    /// </summary>
    public sealed class Host
    {

        public const string GLOBAL = "PSY";

        private PageManager _PageCache;
        private Communicator _IO;
        private Heap<Cell> _Scalars;
        private Heap<CellMatrix> _Matrixes;
        private Heap<string> _Connections;

        public Host()
        {

            this._PageCache = new PageManager(this);
            this._IO = new CommandLineCommunicator();

            this._Scalars = new Heap<Cell>();
            this._Matrixes = new Heap<CellMatrix>();
            this._Connections = new Heap<string>();

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
            get { return this._Connections; }
        }

        // Connection Support //
        public void AddConnection(string Alias, string Connection)
        {
            this._Connections.Allocate(Alias, Connection);
        }

        // Table Support //
        public BaseTable OpenTable(string Key)
        {

            if (this._PageCache.ScribeTableExists(Key))
                return this._PageCache.RequestScribeTable(Key);
            else if (this._PageCache.DreamTableExists(Key))
                return this._PageCache.RequestDreamTable(Key);

            throw new Exception(string.Format("Table does not exist '{0}'", Key));

        }

        public BaseTable OpenTable(string Alias, string Name)
        {

            if (StringComparer.OrdinalIgnoreCase.Compare(Alias, GLOBAL) == 0)
                return this.OpenTable(Name);

            if (this.Connections.Exists(Alias))
                return this.OpenTable(TableHeader.DeriveV1Path(this.Connections[Alias], Name));

            throw new Exception(string.Format("Table does not exist '{0}.{1}'", Alias, Name));

        }

        public ClusteredScribeTable CreateTable(string Alias, string Name, Schema Columns, Key ClusterColumns, int PageSize)
        {
            ClusteredScribeTable t = new ClusteredScribeTable(this, Name, this._Connections[Alias], Columns, ClusterColumns, PageSize);
            return t;
        }

        public ClusteredScribeTable CreateTable(string Alias, string Name, Schema Columns, Key ClusterColumns)
        {
            ClusteredScribeTable t = new ClusteredScribeTable(this, Name, this._Connections[Alias], Columns, ClusterColumns, Page.DEFAULT_SIZE);
            return t;
        }

        public HeapScribeTable CreateTable(string Alias, string Name, Schema Columns)
        {
            return new HeapScribeTable(this, Name, this._Connections[Alias], Columns, Page.DEFAULT_SIZE);
        }

    }

    /// <summary>
    /// This class holds all in memory pages and manages buffering pages and tables from disk
    /// </summary>
    public sealed class PageManager
    {

        /// <summary>
        /// The default capacity is 23 MB
        /// </summary>
        public const long DEFAULT_CAPACITY = 1024 * 1024 * 32;

        /// <summary>
        /// The minimum memory capacity is 8 MB
        /// </summary>
        public const long MIN_CAPACITY = 1024 * 1024 * 8;

        private long _MaxMemory = 0;
        private long _Memory = 0;
        private Host _Host;

        // It's easier to keep two sets of books, one for the dream tables and one for scribe tables //
        private Dictionary<string, ScribeTable> _ScribeTables;
        private Dictionary<PageUID, Page> _ScribePages;
        private Dictionary<PageUID, int> _ScribeWrites;
        private Dictionary<string, DreamTable> _DreamTables;
        private Dictionary<PageUID, Page> _DreamPages;

        // This is the collection that holds pages to be burnt //
        private FloatingQueue<PageUID> _BurnPile;

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
            this._BurnPile = new FloatingQueue<PageUID>(4096, PageUID.DefaultComparer);
            
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
            
            if (this._ScribeTables.ContainsKey(Table.Key))
            {
                throw new ElementDoesNotExistException(Table.Key);
            }

            this._ScribeTables.Add(Table.Key, Table);
            this._Memory += TableHeader.SIZE;

        }

        public ScribeTable RequestScribeTable(string Key)
        {
            
            if (this._ScribeTables.ContainsKey(Key))
            {
                return this._ScribeTables[Key];
            }
            else if (File.Exists(Key))
            {

                // Get the table header //
                TableHeader h = this.Buffer(Key);

                // Create the table //
                ScribeTable t;
                if(h.RootPageID == -1)
                {
                    t = new HeapScribeTable(this._Host, h);
                }
                else
                {
                    t = new ClusteredScribeTable(this._Host, h); // The ctor adds the table to the cache
                }

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
            if (this._ScribeTables.ContainsKey(Key))
                return true;
            return File.Exists(Key);
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

            // Check if we have space //
            if (Element.PageSize > this.FreeMemory)
            {
                this.ReleaseMemory(Element.PageSize);
            }

            // Build a PID //
            PageUID pid = new PageUID(Key, Element.PageID);
            this._BurnPile.EnqueueOrTag(pid);

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
            this._BurnPile.EnqueueOrTag(pid);

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
            this._BurnPile.Remove(pid);

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

            if (!this._ScribeTables.ContainsKey(Key))
                throw new ElementDoesNotExistException(Key);

            // Get the table //
            BaseTable t = this._ScribeTables[Key];

            // Close it //
            t.PreSerialize();

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
            {
                this.Flush(Key, t.Header);
            }


        }

        public void ReleaseMemory(int MemoryNeeded)
        {

            while (this.FreeMemory < (long)MemoryNeeded)
            {

                if (this._ScribePages.Count == 0)
                    throw new OutOfMemoryException("Cannot free enough memory");

                PageUID pid = this._BurnPile.Dequeue();

                this.BurnScribePage(pid.Key, pid.PageID, true);

            }

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
            if (!this.DreamTableExists(Key))
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

            if (!this.DreamTableExists(Key))
                throw new ElementDoesNotExistException(Key);

            if (this.DreamPageExists(pid))
            {
                return this._DreamPages[pid];
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
            Page p = this._DreamPages[pid];

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
            foreach (KeyValuePair<PageUID, Page> kv in this._DreamPages.ToList())
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

            // Don't throw an error if the table doesnt exist //
            if (!this.ScribeTableExists(Key))
                return;

            // Take care of the entry //
            if (this._ScribeTables.ContainsKey(Key))
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

    /// <summary>
    /// Represents a Queue where elements can move up or down
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class FloatingQueue<T>
    {

        public enum State
        {
            LeastRecentlyUsed,
            MostRecentlyUsed
        }

        private Dictionary<T, LinkedListNode<T>> _Index;
        LinkedList<T> _Trail;
        private int _Capacity;
        private State _State = State.LeastRecentlyUsed;
        
        public FloatingQueue(int Capacity, IEqualityComparer<T> Comparer)
        {
            this._Index = new Dictionary<T, LinkedListNode<T>>(Comparer);
            this._Trail = new LinkedList<T>();
            this._Capacity = Capacity;
        }

        public FloatingQueue(int Capacity)
            : this(Capacity, EqualityComparer<T>.Default)
        {
        }

        public FloatingQueue()
            : this(128, EqualityComparer<T>.Default)
        {
        }

        public bool IsEmpty
        {
            get { return this._Index.Count == 0; }
        }

        public bool IsFull
        {
            get { return this._Index.Count >= this._Capacity; }
        }

        public int Count
        {
            get { return this._Index.Count; }
        }

        // Public //
        public T Peek()
        {
            if (this._State == State.LeastRecentlyUsed)
                return this._Trail.Last.Value;
            else
                return this._Trail.First.Value;
        }

        public T Dequeue()
        {

            if (this.IsEmpty)
                throw new IndexOutOfRangeException();
           
            T element;
            if (this._State == State.LeastRecentlyUsed)
            {
                element = this._Trail.Last.Value;
                this._Trail.RemoveLast();
            }
            else
            {
                element = this._Trail.First.Value;
                this._Trail.RemoveFirst();
            }
            this._Index.Remove(element);

            return element;

        }

        public void Enqueue(T Value)
        {

            LinkedListNode<T> node = new LinkedListNode<T>(Value);
            this._Index.Add(Value, node);
            this._Trail.AddFirst(node);

        }

        public void Tag(T Value)
        {

            LinkedListNode<T> node = this._Index[Value];
            this._Trail.Remove(node);
            this._Index.Remove(Value);
            this.Enqueue(Value);

        }

        public void EnqueueOrTag(T Value)
        {

            if (this._Index.ContainsKey(Value))
            {
                this.Tag(Value);
            }
            else
            {
                this.Enqueue(Value);
            }

        }

        public void Remove(T Value)
        {
            
            if (!this._Index.ContainsKey(Value))
                return;

            LinkedListNode<T> x = this._Index[Value];
            this._Trail.Remove(x);
            this._Index.Remove(Value);

        }

    }

    /// <summary>
    /// Represents a Key and a PageID
    /// </summary>
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
