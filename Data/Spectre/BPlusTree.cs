using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace Rye.Data.Spectre
{

    public enum BPlusTreeSearchType : int
    {
        FirstElement = -1,
        LastElement = 1,
        AnyElement = 0
    }

    /// <summary>
    /// Represents a single page in a b+ tree
    /// </summary>
    public class BPlusTreePage : Page
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

        public BPlusTreePage(int PageSize, int PageID, int LastPageID, int NextPageID, int FieldCount, int DataDiskCost, Key KeyColumns, bool IsLeaf)
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
                this._WeakMatcher = new RecordMatcher(BranchObjectiveClone(KeyColumns, true)); // Designed to match keys and keys
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

        public List<Record> SelectAll(Record Element)
        {

            int Lower = this.SearchLeaf(Record.Split(Element, this._WeakKeyColumns), BPlusTreeSearchType.FirstElement, true);
            int Upper = this.SearchLeaf(Record.Split(Element, this._WeakKeyColumns), BPlusTreeSearchType.LastElement, true);

            List<Record> elements = new List<Record>();
            if (Lower < 0 || Upper < 0)
                return elements;

            elements.AddRange(this._Elements.GetRange(Lower, Upper - Lower));
            return elements;

        }

        public BPlusTreePage GenerateXPage(int PageID, int LastPageID, int NextPageID)
        {
            BPlusTreePage x = new BPlusTreePage(this.PageSize, PageID, LastPageID, NextPageID, this._FieldCount, this._DataDiskCost, this._OriginalKeyColumns, this.IsLeaf);
            x.IsLeaf = this.IsLeaf;
            return x;
        }

        public BPlusTreePage SplitXPage(int PageID, int LastPageID, int NextPageID, int Pivot)
        {

            if (this.Count < 2)
                throw new IndexOutOfRangeException("Cannot split a page with fewer than 2 records");
            if (Pivot == 0 || Pivot == this.Count - 1)
                throw new IndexOutOfRangeException("Cannot split on the first or last record");
            if (Pivot < 0)
                throw new IndexOutOfRangeException(string.Format("Pivot ({0}) must be greater than 0", Pivot));
            if (Pivot >= this.Count)
                throw new IndexOutOfRangeException(string.Format("The pivot ({0}) cannot be greater than the element count ({1})", Pivot, this.Count));

            BPlusTreePage p = this.GenerateXPage(PageID, LastPageID, NextPageID);
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
        /// <summary>
        /// Inserts a key into the page
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="PageID"></param>
        public void InsertKey(Record Key, int PageID)
        {

            if (this._WeakMatcher.Compare(Key, this._Elements.Last()) > 0 && !this.IsHighest)
                throw new Exception("Can't insert a record greater the max record unless this is the highest page");

            // InsertKey as usual //
            this.InsertKeyUnsafe(Key, PageID);

        }

        /// <summary>
        /// Inserts a key into the table without checking if it is within the bounds of the tree
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="PageID"></param>
        public void InsertKeyUnsafe(Record Key, int PageID)
        {

            // Find the insertion point //
            Record k = Composite(Key, PageID);
            int idx = this._Elements.BinarySearch(k, this._StrongMatcher);
            if (idx < 0) idx = ~idx;

            // InsertKey as usual //
            this._Elements.Insert(idx, k);

        }

        /// <summary>
        /// Given an element, this finds the page ID of the page it belongs on
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public int GetPageID(Record Key)
        {

            if (this.IsLeaf)
                throw new Exception("Cannot page search a leaf");

            int idx = this._Elements.BinarySearch(Key, this._PageSearchMatcher);
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

        /// <summary>
        /// Gets the page ID given an index
        /// </summary>
        /// <param name="Index"></param>
        /// <returns></returns>
        public int GetPageID(int Index)
        {
            return this._Elements[Index][this._RefColumn].INT_A;
        }

        /// <summary>
        /// Gets all the page IDs
        /// </summary>
        /// <returns></returns>
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

            // Key must be the entire data record //
            int idx = this._Elements.BinarySearch(Element, this._StrongMatcher);
            if (idx < 0)
            {
                throw new IndexOutOfRangeException("Key is not in this page");
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

        // Searches //
        /// <summary>
        /// Finds a page ID given a key
        /// </summary>
        /// <param name="Key">The value to find</param>
        /// <param name="SearchType">The method of search</param>
        /// <returns>A page ID; if the value doesnt exist</returns>
        public int SearchBranch(Record Key, BPlusTreeSearchType SearchType, bool Exact)
        {

            int idx = this._Elements.BinarySearch(Key, this._WeakMatcher);

            // If we didnt find the element, we dont have to search for multiple keys
            if (idx < 0) 
            {
                if (!Exact) idx = ~idx;
                return idx;
            }

            // If we really don't care about anything, then return the index //
            if (SearchType == BPlusTreeSearchType.AnyElement)
                return idx;

            // Search Lower //
            int pos = 0;
            if (SearchType == BPlusTreeSearchType.FirstElement)
            {

                while (true)
                {
                    pos = this._Elements.BinarySearch(0, idx, Key, this._WeakMatcher);
                    if (pos < 0) break;
                    idx = pos;
                }

            }
            else
            {

                pos = idx;
                while (true)
                {
                    pos = this._Elements.BinarySearch(pos + 1, this.Count - idx - 1, Key, this._WeakMatcher);
                    if (pos < 0) break;
                    idx = pos;
                }

            }

            return this.GetPageID(idx);

        }

        /// <summary>
        /// Finds a record position in a page
        /// </summary>
        /// <param name="Key">The key to search for</param>
        /// <param name="SearchType">The method of search</param>
        /// <returns>The index of the record on this page</returns>
        public int SearchLeaf(Record Key, BPlusTreeSearchType SearchType, bool Exact)
        {

            int idx = this._Elements.BinarySearch(Key, this._WeakMatcher);

            // If we didnt find the element, we dont have to search for multiple keys
            if (idx < 0)
            {
                if (!Exact) idx = ~idx;
                return idx;
            }

            // If we really don't care about anything, then return the index //
            if (SearchType == BPlusTreeSearchType.AnyElement)
                return idx;

            // Search Lower //
            int pos = 0;
            if (SearchType == BPlusTreeSearchType.FirstElement)
            {

                while (true)
                {
                    pos = this._Elements.BinarySearch(0, idx, Key, this._WeakMatcher);
                    if (pos < 0) break;
                    idx = pos;
                }

            }
            else
            {

                pos = idx;
                while (true)
                {
                    pos = this._Elements.BinarySearch(pos + 1, this.Count - idx - 1, Key, this._WeakMatcher);
                    if (pos < 0) break;
                    idx = pos;
                }

            }

            return idx;

        }

        // Statics //
        public static BPlusTreePage Mutate(Page Primitive, Key KeyColumns)
        {

            if (Primitive is BPlusTreePage)
                return Primitive as BPlusTreePage;

            BPlusTreePage x = new BPlusTreePage(Primitive.PageSize, Primitive.PageID, Primitive.LastPageID, Primitive.NextPageID, Primitive.FieldCount, Primitive.DataDiskCost, KeyColumns, Primitive.X1 == 1);
            x._X0 = Primitive.X0;
            x._X1 = Primitive.X1;
            x._X2 = Primitive.X2;
            x._X3 = Primitive.X3;
            x._Elements = Primitive.Cache;

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

    /// <summary>
    /// Represents a B+ Tree
    /// </summary>
    public class BPlusTree
    {

        /*
         * Note:
         *      Key = data record
         *      Key = just the key piece of element
         *      Value = just the value piece of element
         * 
         */

        protected BaseTable _Storage;
        protected BPlusTreePage _Root;
        protected Key _IndexColumns;
        protected Record _MaxRecord;
        protected IElementHeader _Header;
        
        /// <summary>
        /// This creates a B+Tree given a root page from an already existing B+Tree
        /// </summary>
        /// <param name="Storage"></param>
        /// <param name="IndexColumns"></param>
        public BPlusTree(BaseTable Storage, Schema ParentSchema, Key IndexColumns, BPlusTreePage Root, IElementHeader Header)
        {

            this._Storage = Storage;
            this._Header = Header;
            this._IndexColumns = IndexColumns;
            if (Root == null)
            {
                this._Root = this.NewRootAsLeaf();
                this._Storage.SetPage(this._Root);
                this._Storage.Header.PageCount++;
            }
            else
            {
                this._Root = Root;
            }
            this._MaxRecord = Schema.Split(ParentSchema, this._IndexColumns).MaxRecord;
            this.IsUnique = false;
            this.OriginBTreePageID = this.SeekOriginPageID();
            this.TerminalBTreePageID = this.SeekTerminalPageID();
            
        }

        /// <summary>
        /// Gets the table that stores the b+tree
        /// </summary>
        public BaseTable Storage
        {
            get { return this._Storage; }
        }

        /// <summary>
        /// Gets the root page
        /// </summary>
        public BPlusTreePage Root
        {
            get { return this._Root; }
        }

        /// <summary>
        /// Gets a key describing the index columns
        /// </summary>
        public Key IndexColumns
        {
            get { return this._IndexColumns; }
        }

        /// <summary>
        /// True if unigue, false otherwise
        /// </summary>
        public virtual bool IsUnique
        {
            get;
            set;
        }

        /// <summary>
        /// Gets the first data page ID
        /// </summary>
        public virtual int OriginBTreePageID
        {
            get { return this._Header.OriginPageID; }
            set { this._Header.OriginPageID = value; }
        }

        /// <summary>
        /// Gets the last data page ID
        /// </summary>
        public virtual int TerminalBTreePageID
        {
            get { return this._Header.TerminalPageID; }
            set { this._Header.TerminalPageID = value; }
        }

        /// <summary>
        /// Gets the root page id
        /// </summary>
        public virtual int RootPageID
        {
            get { return this._Root.PageID; }
        }

        // Seek Methods //
        /// <summary>
        /// Finds the leaf page this record belongs on
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public virtual BPlusTreePage SeekPage(Record Element)
        {

            // If the root page is a leaf node //
            if (this._Root.IsLeaf)
                return this._Root;

            // Otherwise, starting at root, find the page //
            Record Key = Record.Split(Element, this._IndexColumns);
            BPlusTreePage x = this._Root;
            while (true)
            {
                int PageID = x.GetPageID(Key);
                x = this.GetPage(PageID);
                if (x.IsLeaf)
                    return x;

            }

            // ## For debuggin ##
            //BPlusTreePage x = this._Root;
            //while (true)
            //{
            //    int PageID = x.GetPageID(Key);
            //    BPlusTreePage y = this.GetPage(PageID);
            //    if (y.IsLeaf)
            //    {
            //        return y;
            //    }
            //    else
            //    {
            //        x = y;
            //    }
            //}

        }

        /// <summary>
        /// Finds the page with the FIRST instance of the key
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public virtual BPlusTreePage SeekFirstPage(Record Key)
        {

            // If the root page is a leaf node //
            if (this._Root.IsLeaf)
                return this._Root;

            // Otherwise, starting at root, find the page //
            BPlusTreePage x = this._Root;
            while (true)
            {
                int PageID = x.SearchBranch(Key, BPlusTreeSearchType.FirstElement, false);
                x = this.GetPage(PageID);
                if (x.IsLeaf)
                    return x;

            }

        }

        /// <summary>
        /// Finds the page with the LAST instance of the key on it
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public virtual BPlusTreePage SeekLastPage(Record Key)
        {

            // If the root page is a leaf node //
            if (this._Root.IsLeaf)
                return this._Root;

            // Otherwise, starting at root, find the page //
            BPlusTreePage x = this._Root;
            while (true)
            {
                int PageID = x.SearchBranch(Key, BPlusTreeSearchType.LastElement, false);
                x = this.GetPage(PageID);
                if (x.IsLeaf)
                    return x;

            }

        }

        /// <summary>
        /// Finds the first location of a record
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public virtual RecordKey SeekFirst(Record Key)
        {

            BPlusTreePage x = this.SeekFirstPage(Key);
            int location = x.SearchLeaf(Key, BPlusTreeSearchType.FirstElement, false);
            while (location == 0 && x.LastPageID != -1)
            {
                x = this.GetPage(x.LastPageID);
                location = x.SearchLeaf(Key, BPlusTreeSearchType.FirstElement, false);
            }
            return new RecordKey(x.PageID, location);

        }

        /// <summary>
        /// Finds the last location of a record
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public virtual RecordKey SeekLast(Record Key)
        {

            BPlusTreePage x = this.SeekLastPage(Key);
            int location = x.SearchLeaf(Key, BPlusTreeSearchType.LastElement, false);
            while (location == (x.Count - 1) && x.NextPageID != -1)
            {
                x = this.GetPage(x.NextPageID);
                location = x.SearchLeaf(Key, BPlusTreeSearchType.LastElement, false);
            }
            return new RecordKey(x.PageID, location);

        }

        /// <summary>
        /// Checks if a key exists
        /// </summary>
        /// <param name="Key"></param>
        /// <returns></returns>
        public virtual bool Exists(Record Key)
        {

            // If the root page is a leaf node //
            if (this._Root.IsLeaf)
                return this._Root.KeyExists(Key);

            // Otherwise, starting at root, find the page //
            BPlusTreePage x = this._Root;
            while (true)
            {
                int PageID = x.GetPageID(Key);
                x = this.GetPage(PageID);
                if (x.IsLeaf)
                    return x.KeyExists(Key);

            }

        }

        /// <summary>
        /// Gets the very first leaf page id in the data stream
        /// </summary>
        /// <returns></returns>
        public virtual int SeekOriginPageID()
        {

            if (this.Root.IsLeaf)
                return this.Root.PageID;

            BPlusTreePage p = this.Root;
            while (true)
            {
                p = this.GetPage(p.GetPageID(0));
                if (p.IsLeaf)
                    return p.PageID;
            }

        }

        /// <summary>
        /// Gets the very last page id in the data stream
        /// </summary>
        /// <returns></returns>
        public virtual int SeekTerminalPageID()
        {

            if (this.Root.IsLeaf)
                return this.Root.PageID;

            BPlusTreePage p = this.Root;
            while (true)
            {
                int id = p.GetPageID(p.Count - 1); // Note that the last record is always MAX_RECORD, so we want the second to last
                p = this.GetPage(id);
                if (p.IsLeaf)
                    return p.PageID;
            }

        }

        // Inserts //
        /// <summary>
        /// Core insertion step; inserts a value into the b+ tree and splits any nodes if needed
        /// </summary>
        /// <param name="Key"></param>
        public virtual void Insert(Record Element)
        {

            // Check if it exists only if this is unqiue //
            if (this.IsUnique)
            {
                if (this.Exists(Record.Split(Element, this._IndexColumns)))
                    throw new DuplicateKeyException(string.Format("Key exists {0}", Record.Split(Element, this._IndexColumns)));
            }

            // Finde the leaf node to insert into //
            BPlusTreePage node = this.SeekPage(Element);

            // Actually insert the value //
            this.InsertValue(node, Element);

            // Tick up the records //
            this._Header.RecordCount++;

        }

        /// <summary>
        /// Adds a key/page id to the tree; if the node is currently full, this method will split it, and update it's parent; if the parent is full, it'll also split; if the parent is the root and it's
        /// full, it'll split that too;
        /// </summary>
        /// <param name="Node">A branch node to append</param>
        /// <param name="Key">The key value of the index</param>
        /// <param name="PageID">The page id linking to the key</param>
        private void InsertKey(BPlusTreePage Node, Record Key, int PageID)
        {

            if (Node.IsLeaf)
                throw new Exception("Node passed must be a branch node");

            // Get the child page //
            BPlusTreePage Child = this.GetPage(PageID);

            // The node is not full; note that the node will handle flipping the overflow ID if needed //
            if (!Node.IsFull)
            {
                Child.ParentPageID = Node.PageID;
                Node.InsertKey(Key, PageID);
                return;
            }

            // Otherwise, the node is full and we need to split //
            BPlusTreePage y = this.SplitBranch(Node);

            // But... we don't know for sure if we should insert into 'Node' or 'y', so we need to check //
            if (Node.LessThanTerminal(Key))
            {
                Child.ParentPageID = Node.PageID;
                Node.InsertKey(Key, PageID);
            }
            else
            {
                Child.ParentPageID = y.PageID;
                y.InsertKey(Key, PageID);
            }

        }

        /// <summary>
        /// Inserts a value into a node;
        /// If the node is full, this method will split it and update it's parent; using 'InsertKey'
        /// </summary>
        /// <param name="Node">The node to insert</param>
        /// <param name="Key">The data record (having the same schema as the parent table)</param>
        private void InsertValue(BPlusTreePage Node, Record Element)
        {

            if (!Node.IsLeaf)
                throw new Exception("Node passed must be a branch node");

            // InsertKey if the node isnt full //
            if (!Node.IsFull)
            {
                Node.Insert(Element);
                return;
            }

            // Otherwise, the node is full and we need to split //
            BPlusTreePage y = this.SplitLeaf(Node);

            // But... we don't know for sure if we should insert into 'Node' or 'y', so we need to check //
            if (Node.LessThanTerminal(Element))
            {
                Node.Insert(Element);
            }
            else
            {
                y.Insert(Element);
            }

        }

        // Splits //
        /// <summary>
        /// Splits a branch node; this will re-balance all nodes above it;
        /// </summary>
        /// <param name="OriginalNode">The node to be split; after this method is called, this node will have the LOWER half of all records before the split</param>
        /// <returns>A new node with the UPPER half of all record in the OriginalNode; this method will append the parent nodes</returns>
        private BPlusTreePage SplitBranch(BPlusTreePage OriginalNode)
        {

            if (OriginalNode.IsLeaf)
                throw new Exception("Node passed must be a branch node");

            // Split up the page; splitting will set the ParentID and the Leafness, but it won't set the overflow page id //
            BPlusTreePage NewNode = OriginalNode.SplitXPage(this._Storage.GenerateNewPageID, OriginalNode.PageID, OriginalNode.NextPageID, OriginalNode.Count / 2);
            this._Storage.SetPage(NewNode);
            this._Storage.Header.PageCount++;

            // Marry the new and og nodes //
            NewNode.LastPageID = OriginalNode.PageID;
            OriginalNode.NextPageID = NewNode.PageID;

            // Set the last page id for the next page id
            if (NewNode.NextPageID != -1)
            {
                BPlusTreePage UpPage = this.GetPage(NewNode.NextPageID);
                UpPage.LastPageID = NewNode.PageID;
            }

            // Now we have to go through all NewNode's children and update their parent page id //
            List<int> PageIDs = NewNode.AllPageIDs();
            foreach (int PageID in PageIDs)
            {
                BPlusTreePage q = this.GetPage(PageID);
                q.ParentPageID = NewNode.PageID;
            }

            // Finally, we need to handle introducing NewNode to OriginalNode's parent //
            if (OriginalNode.ParentPageID != -1) // OriginalNode isnt the root node
            {

                // Get the parent //
                BPlusTreePage parent = this.GetPage(OriginalNode.ParentPageID);


                // Need to update the key for OriginalNode to be it's last record //
                parent.Delete(BPlusTreePage.Composite(Record.Split(NewNode.TerminalRecord, NewNode.WeakKeyColumns), OriginalNode.PageID));

                // Note: we know because we just removed a record that the parent node is not full, so we do a direct insert
                //      we would run into trouble if we didnt do this and the record we were deleting was the last in the tree's node
                //      becuase down stream the nodes will error out if we try to insert something higher than they are
                parent.InsertKeyUnsafe(Record.Split(NewNode.TerminalRecord, NewNode.WeakKeyColumns), NewNode.PageID);

                // If it is the highest //
                if (OriginalNode.IsHighest)
                {
                    OriginalNode.IsHighest = false;
                    NewNode.IsHighest = true;
                }

                // Need to add in NewNode //
                this.InsertKey(parent, Record.Split(OriginalNode.TerminalRecord, OriginalNode.WeakKeyColumns), OriginalNode.PageID);

            }
            // if this is the root page //
            else
            {

                // Create a new root page //
                this._Root = this.NewRootAsBranch();
                OriginalNode.ParentPageID = this._Root.PageID;
                NewNode.ParentPageID = this._Root.PageID;

                // Need to insert the last row of the current record //
                this._Root.Insert(BPlusTreePage.Composite(Record.Split(OriginalNode.TerminalRecord, OriginalNode.WeakKeyColumns), OriginalNode.PageID));

                // Need to add the max record to this layer //
                this._Root.Insert(BPlusTreePage.Composite(this._MaxRecord, NewNode.PageID));

                // Set the new node to be the highest //
                NewNode.IsHighest = true;
                OriginalNode.IsHighest = false;

                // Set the root //
                this._Storage.SetPage(this._Root);
                this._Storage.Header.PageCount++;

            }

            return NewNode;

        }

        /// <summary>
        /// Splits a leaf node
        /// </summary>
        /// <param name="OriginalNode">The node to split; this node will have the LOWER half of the original nodes record after the split</param>
        /// <returns>A new node with the UPPER half of all records on the original node</returns>
        private BPlusTreePage SplitLeaf(BPlusTreePage OriginalNode)
        {

            if (!OriginalNode.IsLeaf)
                throw new Exception("Node passed must be a leaf node");

            Record OriginalTerminal = OriginalNode.TerminalRecord;

            // Split up the page; splitting will set the ParentID and the Leafness, but it won't set the overflow page id //
            BPlusTreePage NewNode = OriginalNode.SplitXPage(this._Storage.GenerateNewPageID, OriginalNode.PageID, OriginalNode.NextPageID, OriginalNode.Count / 2);
            this._Storage.SetPage(NewNode);
            this._Storage.Header.PageCount++;

            // Check the last ID //
            if (this.TerminalBTreePageID == OriginalNode.PageID)
            {
                this.TerminalBTreePageID = NewNode.PageID;
            }

            // Marry the new and og nodes //
            NewNode.LastPageID = OriginalNode.PageID;
            OriginalNode.NextPageID = NewNode.PageID;

            // Set the last page id for the next page id
            if (NewNode.NextPageID != -1)
            {
                BPlusTreePage UpPage = this.GetPage(NewNode.NextPageID);
                UpPage.LastPageID = NewNode.PageID;
            }

            // We need to introduce NewNode to it's parent //
            if (OriginalNode.ParentPageID != -1) // OriginalNode isnt the root node
            {

                // Get the parent //
                BPlusTreePage parent = this.GetPage(OriginalNode.ParentPageID);

                // If it is the highest //
                if (OriginalNode.IsHighest)
                {

                    // Turn off the highest flag //
                    OriginalNode.IsHighest = false;
                    NewNode.IsHighest = true;

                    // The record in the parent is NOT the terminal record, it's the highest record //
                    parent.Delete(BPlusTreePage.Composite(this._MaxRecord, OriginalNode.PageID));

                    // Note: we know because we just removed a record that the parent node is not full, so we do a direct insert
                    //      we would run into trouble if we didnt do this and the record we were deleting was the last in the tree's node
                    //      becuase down stream the nodes will error out if we try to insert something higher than they are
                    parent.InsertKeyUnsafe(this._MaxRecord, NewNode.PageID);


                }
                else  // Note that becuase the child nodes are leafs, we do want to use the 'OriginalKeyColumns' not the 'Weak Key Columns'
                {

                    // Need to update the key for OriginalNode to be it's last record //
                    parent.Delete(BPlusTreePage.Composite(Record.Split(NewNode.TerminalRecord, NewNode.OriginalKeyColumns), OriginalNode.PageID));

                    // Note: we know because we just removed a record that the parent node is not full, so we do a direct insert
                    //      we would run into trouble if we didnt do this and the record we were deleting was the last in the tree's node
                    //      becuase down stream the nodes will error out if we try to insert something higher than they are
                    parent.InsertKeyUnsafe(Record.Split(NewNode.TerminalRecord, NewNode.OriginalKeyColumns), NewNode.PageID);

                }

                // add in OriginalNode because it didnt exist before
                this.InsertKey(parent, Record.Split(OriginalNode.TerminalRecord, OriginalNode.OriginalKeyColumns), OriginalNode.PageID);

            }
            // if this is the root page - note that this piece of code only get's triggered once in the tree development //
            else
            {

                // Create a new root page //
                this._Root = this.NewRootAsBranch();
                OriginalNode.ParentPageID = this._Root.PageID;
                NewNode.ParentPageID = this._Root.PageID;

                // Need to insert
                this._Root.Insert(BPlusTreePage.Composite(Record.Split(OriginalNode.TerminalRecord, OriginalNode.OriginalKeyColumns), OriginalNode.PageID));

                // Need to add the max record to this layer //
                this._Root.Insert(BPlusTreePage.Composite(this._MaxRecord, NewNode.PageID));

                // Set the new node to be the highest //
                NewNode.IsHighest = true;
                OriginalNode.IsHighest = false;

                // Set the root //
                this._Storage.SetPage(this._Root);
                this._Storage.Header.PageCount++;

            }

            return NewNode;

        }

        /// <summary>
        /// Gets a b+tree page; this method calls the page from the parent table and will either cast or do a hard convert to the b+tree method
        /// </summary>
        /// <param name="PageID">The page ID requested</param>
        /// <returns></returns>
        private BPlusTreePage GetPage(int PageID)
        {
            return BPlusTreePage.Mutate(this._Storage.GetPage(PageID), this._IndexColumns);
        }

        // New Root Methods //
        /// <summary>
        /// Generates a new root page as if it were a branch node
        /// </summary>
        /// <returns></returns>
        private BPlusTreePage NewRootAsBranch()
        {

            Schema s = Schema.Split(this._Storage.Columns, this._IndexColumns);
            s.Add("@IDX", CellAffinity.INT);

            BPlusTreePage NewRoot = new BPlusTreePage(this._Storage.PageSize, this._Storage.GenerateNewPageID, -1, -1, s.Count, s.RecordDiskCost, this._IndexColumns, false);
            NewRoot.ParentPageID = -1;
            NewRoot.IsHighest = true;
            this._Header.RootPageID = NewRoot.PageID;

            return NewRoot;

        }

        /// <summary>
        /// Generates a new root node as a leaf node; note that this only called in the ctor method
        /// </summary>
        /// <returns></returns>
        private BPlusTreePage NewRootAsLeaf()
        {

            Schema s = this._Storage.Columns;

            BPlusTreePage NewRoot = new BPlusTreePage(this._Storage.PageSize, this._Storage.GenerateNewPageID, -1, -1, s.Count, s.RecordDiskCost, this._IndexColumns, true);
            NewRoot.ParentPageID = -1;
            NewRoot.IsHighest = true;
            this._Header.RootPageID = NewRoot.PageID;

            return NewRoot;

        }

        // Debugging //
        internal void Print(StreamWriter writer, BPlusTreePage Page)
        {

            // Print this data //
            if (Page.IsLeaf)
            {

                writer.WriteLine("--- Leaf {0} <{1},{2}> ---", Page.PageID, Page.LastPageID, Page.NextPageID);
                foreach (Record r in Page.Elements)
                {
                    writer.WriteLine(r);
                }

            }
            else
            {

                writer.WriteLine("--- Branch {0} <{1},{2}> ---", Page.PageID, Page.LastPageID, Page.NextPageID);
                foreach (Record r in Page.Elements)
                {
                    writer.WriteLine("Key {0} : Page ID {1}", Record.Split(r, Page.StrongKeyColumns), r._data.Last().INT_A);
                }

                // Write all the child nodes //
                List<int> pages = Page.AllPageIDs();
                foreach (int pageid in pages)
                {
                    this.Print(writer, this.GetPage(pageid));
                }

            }



        }

        internal void Print(string Path)
        {

            using (StreamWriter sw = new StreamWriter(Path))
            {
                this.Print(sw, this._Root);
                sw.Flush();
            }

        }

        internal void AppendString(StringBuilder sb, BPlusTreePage Node)
        {

            if (Node.IsLeaf)
            {
                sb.AppendLine(string.Format("\tLeaf: {0} | {1}", Node.PageID, Node.ParentPageID));
                return;
            }

            List<int> nodes = Node.AllPageIDs();
            int j = 0;
            sb.AppendLine(string.Format("Storage PageID {0}", Node.PageID));
            foreach (int i in nodes)
            {
                sb.AppendLine(string.Format("Child PageID {0} ", i));
            }
            sb.AppendLine("---------------");

            foreach (int i in nodes)
            {
                BPlusTreePage x = this.GetPage(i);
                this.AppendString(sb, x);
            }

        }

        internal string Tree()
        {
            StringBuilder sb = new StringBuilder();
            this.AppendString(sb, this._Root);
            return sb.ToString();
        }

        internal string PageMap()
        {

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < this._Storage.PageCount; i++)
            {
                Page p = this._Storage.GetPage(i);
                sb.AppendLine(string.Format("PageID: {0} | ParentPageID {1} | IsLeaf {2}", p.PageID, p.X0, p.X1 == 1));
            }

            return sb.ToString();

        }

        // Exceptions //
        public class DuplicateKeyException : Exception
        {

            public DuplicateKeyException(string Message)
                : base(Message)
            {
            }

        }

        // Statics //
        public static BPlusTree CreateClusteredIndex(BaseTable Parent, Key IndexColumns)
        {
            return new BPlusTree(Parent, Parent.Columns, IndexColumns, null, Parent.Header);
        }

        public static BPlusTree OpenClusteredIndex(BaseTable Parent)
        {

            if (Parent.Header.RootPageID == -1)
                throw new ArgumentException("Cannot open a clustered index; no such index exists");
            Key k = Parent.Header.SortKey;
            BPlusTreePage root = BPlusTreePage.Mutate(Parent.GetPage(Parent.Header.RootPageID), k);
            return new BPlusTree(Parent, Parent.Columns, k, root, Parent.Header);

        }

        public static BPlusTree CreateNonClusteredIndex(BaseTable StorageAndParent, Key IndexColumns)
        {

            Key k = Key.Build(IndexColumns.Count);
            Schema s = Schema.Split(StorageAndParent.Columns, IndexColumns);
            s.Add("@PTR", CellAffinity.INT, true, 8);
            return new BPlusTree(StorageAndParent, s, k, null, StorageAndParent.Header);

        }

        public static BPlusTree OpenNonClusteredIndex(BaseTable StorageAndParent, IndexHeader Header)
        {

            Key k = Key.Build(Header.IndexColumns.Count);
            Schema s = Schema.Split(StorageAndParent.Columns, Header.IndexColumns);
            s.Add("@PTR", CellAffinity.INT, true, 8);
            BPlusTreePage root = BPlusTreePage.Mutate(StorageAndParent.GetPage(Header.RootPageID), Header.IndexColumns);
            return new BPlusTree(StorageAndParent, s, k, root, Header);

        }

        public static Schema NonClusteredIndexColumns(Schema Columns, Key IndexColumns)
        {
            Key k = Key.Build(IndexColumns.Count);
            Schema s = Schema.Split(Columns, IndexColumns);
            s.Add("@PTR", CellAffinity.INT, true, 8);
            return s;
        }

    }

}
