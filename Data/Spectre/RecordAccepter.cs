using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace Rye.Data.Spectre
{

    public sealed class RecordAccepterHeader
    {

        public const int RA_HEADER_LEN = 196;
        public const int RA_HEAP_TYPE = 0;
        public const int RA_BPTREE_TYPE = 1;

        /* 0:3: type
         * 4:4: first data page ID
         * 8:4: last data page ID
         * 12:4: root page ID
         * 16:4: key count (N), max 16
         * 20:4: is unique
         * 24: N x 8 (up to 128, puts us at position 152) 
         * 152:44: dead space
         * 
         */

        private RecordAccepterHeader()
        {
        }

        public RecordAccepterHeader(int Type, bool IsUnique, int OriginPageID, int TerminalPageID, int RootPageID, Key IndexColumns)
        {
            this.Type = Type;
            this.IsUnique = IsUnique;
            this.OriginDataPageID = OriginPageID;
            this.TerminalDataPageID = TerminalPageID;
            this.RootPageID = RootPageID;
            this.IndexColumns = IndexColumns;
        }

        public int Type
        {
            get;
            set;
        }

        public int OriginDataPageID
        {
            get;
            set;
        }

        public int TerminalDataPageID
        {
            get;
            set;
        }

        public int RootPageID
        {
            get;
            set;
        }

        public bool IsUnique
        {
            get;
            set;
        }

        public Key IndexColumns
        {
            get;
            set;
        }

        public static RecordAccepterHeader Read(int Location, byte[] Hash)
        {

            RecordAccepterHeader h = new RecordAccepterHeader();
            h.TerminalDataPageID = BitConverter.ToInt32(Hash, Location + 0);
            h.OriginDataPageID = BitConverter.ToInt32(Hash, Location + 4);
            h.TerminalDataPageID = BitConverter.ToInt32(Hash, Location + 8);
            h.RootPageID = BitConverter.ToInt32(Hash, Location + 12);
            h.IsUnique = (BitConverter.ToInt32(Hash, Location + 16) == 1);

            int KeyCount = BitConverter.ToInt32(Hash, Location + 20);
            h.IndexColumns = new Key();
            int pos = 0;
            for (int i = 0; i < KeyCount; i++)
            {
                pos = Location + 24 + i * 8;
                int idx = BitConverter.ToInt32(Hash, pos);
                KeyAffinity ka = (KeyAffinity)BitConverter.ToInt32(Hash, pos + 4);
                h.IndexColumns.Add(idx, ka);
            }

            return h;

        }

        public static void Write(int Location, byte[] Hash, RecordAccepterHeader Value)
        {

            Array.Copy(BitConverter.GetBytes(Value.Type), 0, Hash, Location, 4);
            Array.Copy(BitConverter.GetBytes(Value.OriginDataPageID), 0, Hash, Location + 4, 4);
            Array.Copy(BitConverter.GetBytes(Value.TerminalDataPageID), 0, Hash, Location + 8, 4);
            Array.Copy(BitConverter.GetBytes(Value.RootPageID), 0, Hash, Location + 12, 4);
            Array.Copy(BitConverter.GetBytes(Value.IsUnique ? (int)1 : (int)0), 0, Hash, Location + 16, 4);
            Array.Copy(BitConverter.GetBytes(Value.IndexColumns.Count), 0, Hash, Location + 20, 4);
            for (int i = 0; i < Value.IndexColumns.Count; i++)
            {
                int pos = Location + 24 + i * 8;
                Array.Copy(BitConverter.GetBytes(Value.IndexColumns[i]), 0, Hash, pos, 4);
                Array.Copy(BitConverter.GetBytes((int)Value.IndexColumns.Affinity(i)), 0, Hash, pos + 4, 4);
            }

        }

    }

    public interface IRecordAccepter
    {

        int Type { get; }

        int OriginDataPageID { get; }

        int TerminalDataPageID { get; }

        int RootPageID { get; }

        bool IsUnique { get; }

        void Insert(Record Element);

        ReadStream OpenReader();

        ReadStream OpenReader(Record Key);

        ReadStream OpenReader(Record LowerKey, Record UpperKey);

    }

    public class RecordHeap
    {

        private BaseTable _Parent;
        private Page _Terminis;

        public RecordHeap(BaseTable Parent, Page Terminis)
        {
            this._Parent = Parent;
            this._Terminis = Terminis;
        }

        public void Insert(Record Element)
        {

            // Handle the terminal page being full //
            if (this._Terminis.IsFull)
            {

                Page p = new Page(this._Parent.PageSize, this._Parent.GenerateNewPageID, this._Terminis.PageID, -1, this._Parent.Columns);
                this._Terminis.NextPageID = p.PageID;
                this._Parent.SetPage(p);
                this._Terminis = p;
                this._Parent.Header.TerminalPageID = p.PageID;

            }

            // Add the actual record //
            this._Terminis.Insert(Element);
            this._Parent.RecordCount++;

        }

    }

    public class BPlusTree
    {

        /*
         * Note:
         *      Element = data record
         *      Key = just the key piece of element
         *      Value = just the value piece of element
         * 
         */

        protected BaseTable _T;
        protected BPTreePage _Root;
        protected Key _IndexColumns;
        protected Record _MaxRecord;

        public BPlusTree(BaseTable Parent, Key IndexColumns)
        {
            this._T = Parent;
            this._IndexColumns = IndexColumns;
            this._Root = this.NewRootAsLeaf();
            this._T.SetPage(this._Root);
            this._MaxRecord = Schema.Split(this._T.Columns, this._IndexColumns).MaxRecord;
            
        }

        public BaseTable Parent
        {
            get { return this._T; }
        }

        public BPTreePage Root
        {
            get { return this._Root; }
        }

        public Key IndexColumns
        {
            get { return this._IndexColumns; }
        }

        // Seek Methods //
        /// <summary>
        /// Finds the leaf page this record belongs on
        /// </summary>
        /// <param name="Element"></param>
        /// <returns></returns>
        public BPTreePage SeekPage(Record Element)
        {

            // If the root page is a leaf node //
            if (this._Root.IsLeaf)
                return this._Root;

            // Otherwise, starting at root, find the page //
            BPTreePage x = this._Root;
            while (true)
            {
                int PageID = x.PageSearch(Element);
                x = this.GetPage(PageID);
                if (x.IsLeaf)
                    return x;
                
            }

            // ## For debuggin ##
            //BPTreePage x = this._Root;
            //while (true)
            //{
            //    int PageID = x.PageSearch(Element);
            //    BPTreePage y = this.GetPage(PageID);
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
        public BPTreePage SeekFirstPage(Record Key)
        {

            // If the root page is a leaf node //
            if (this._Root.IsLeaf)
                return this._Root;

            // Otherwise, starting at root, find the page //
            BPTreePage x = this._Root;
            while (true)
            {
                int PageID = x.SearchLowerKey(Key);
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
        public BPTreePage SeekLast(Record Key)
        {

            // If the root page is a leaf node //
            if (this._Root.IsLeaf)
                return this._Root;

            // Otherwise, starting at root, find the page //
            BPTreePage x = this._Root;
            while (true)
            {
                int PageID = x.SearchUpperKey(Key);
                x = this.GetPage(PageID);
                if (x.IsLeaf)
                    return x;

            }

        }

        // Inserts //
        /// <summary>
        /// Adds a key/page id to the tree; if the node is currently full, this method will split it, and update it's parent; if the parent is full, it'll also split; if the parent is the root and it's
        /// full, it'll split that too;
        /// </summary>
        /// <param name="Node">A branch node to append</param>
        /// <param name="Key">The key value of the index</param>
        /// <param name="PageID">The page id linking to the key</param>
        private void InsertKey(BPTreePage Node, Record Key, int PageID)
        {

            if (Node.IsLeaf)
                throw new Exception("Node passed must be a branch node");

            // Get the child page //
            BPTreePage Child = this.GetPage(PageID);

            // The node is not full; note that the node will handle flipping the overflow ID if needed //
            if (!Node.IsFull)
            {
                Child.ParentPageID = Node.PageID;
                Node.InsertKey(Key, PageID);
                return;
            }

            // Otherwise, the node is full and we need to split //
            BPTreePage y = this.SplitBranch(Node);

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
        /// <param name="Element">The data record (having the same schema as the parent table)</param>
        private void InsertValue(BPTreePage Node, Record Element)
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
            BPTreePage y = this.SplitLeaf(Node);

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

        /// <summary>
        /// Core insertion step; inserts a value into the b+ tree and splits any nodes if needed
        /// </summary>
        /// <param name="Element"></param>
        public void Insert(Record Element)
        {

            BPTreePage node = this.SeekPage(Element);

            this.InsertValue(node, Element);

        }

        // Splits //
        /// <summary>
        /// Splits a branch node; this will re-balance all nodes above it;
        /// </summary>
        /// <param name="OriginalNode">The node to be split; after this method is called, this node will have the LOWER half of all records before the split</param>
        /// <returns>A new node with the UPPER half of all record in the OriginalNode; this method will append the parent nodes</returns>
        private BPTreePage SplitBranch(BPTreePage OriginalNode)
        {

            if (OriginalNode.IsLeaf)
                throw new Exception("Node passed must be a branch node");

            // Split up the page; splitting will set the ParentID and the Leafness, but it won't set the overflow page id //
            BPTreePage NewNode = OriginalNode.SplitXPage(this._T.GenerateNewPageID, OriginalNode.PageID, OriginalNode.NextPageID, OriginalNode.Count / 2);
            this._T.SetPage(NewNode);

            // Marry the new and og nodes //
            NewNode.LastPageID = OriginalNode.PageID;
            OriginalNode.NextPageID = NewNode.PageID;

            // Set the last page id for the next page id
            if (NewNode.NextPageID != -1)
            {
                BPTreePage UpPage = this.GetPage(NewNode.NextPageID);
                UpPage.LastPageID = NewNode.PageID;
            }

            // Now we have to go through all NewNode's children and update their parent page id //
            List<int> PageIDs = NewNode.AllPageIDs();
            foreach (int PageID in PageIDs)
            {
                BPTreePage q = this.GetPage(PageID);
                q.ParentPageID = NewNode.PageID;
            }

            // Finally, we need to handle introducing NewNode to OriginalNode's parent //
            if (OriginalNode.ParentPageID != -1) // OriginalNode isnt the root node
            {

                // Get the parent //
                BPTreePage parent = this.GetPage(OriginalNode.ParentPageID);


                // Need to update the key for OriginalNode to be it's last record //
                parent.Delete(BPTreePage.Composite(Record.Split(NewNode.TerminalRecord, NewNode.WeakKeyColumns), OriginalNode.PageID));

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
                this._Root.Insert(BPTreePage.Composite(Record.Split(OriginalNode.TerminalRecord, OriginalNode.WeakKeyColumns), OriginalNode.PageID));

                // Need to add the max record to this layer //
                this._Root.Insert(BPTreePage.Composite(this._MaxRecord, NewNode.PageID));

                // Set the new node to be the highest //
                NewNode.IsHighest = true;
                OriginalNode.IsHighest = false;

                // Set the root //
                this._T.SetPage(this._Root);

            }

            return NewNode;

        }

        /// <summary>
        /// Splits a leaf node
        /// </summary>
        /// <param name="OriginalNode">The node to split; this node will have the LOWER half of the original nodes record after the split</param>
        /// <returns>A new node with the UPPER half of all records on the original node</returns>
        private BPTreePage SplitLeaf(BPTreePage OriginalNode)
        {

            if (!OriginalNode.IsLeaf)
                throw new Exception("Node passed must be a leaf node");

            Record OriginalTerminal = OriginalNode.TerminalRecord;

            // Split up the page; splitting will set the ParentID and the Leafness, but it won't set the overflow page id //
            BPTreePage NewNode = OriginalNode.SplitXPage(this._T.GenerateNewPageID, OriginalNode.PageID, OriginalNode.NextPageID, OriginalNode.Count / 2);
            this._T.SetPage(NewNode);

            // Marry the new and og nodes //
            NewNode.LastPageID = OriginalNode.PageID;
            OriginalNode.NextPageID = NewNode.PageID;

            // Set the last page id for the next page id
            if (NewNode.NextPageID != -1)
            {
                BPTreePage UpPage = this.GetPage(NewNode.NextPageID);
                UpPage.LastPageID = NewNode.PageID;
            }

            // We need to introduce NewNode to it's parent //
            if (OriginalNode.ParentPageID != -1) // OriginalNode isnt the root node
            {

                // Get the parent //
                BPTreePage parent = this.GetPage(OriginalNode.ParentPageID);

                // If it is the highest //
                if (OriginalNode.IsHighest)
                {

                    // Turn off the highest flag //
                    OriginalNode.IsHighest = false;
                    NewNode.IsHighest = true;

                    // The record in the parent is NOT the terminal record, it's the highest record //
                    parent.Delete(BPTreePage.Composite(this._MaxRecord, OriginalNode.PageID));

                    // Note: we know because we just removed a record that the parent node is not full, so we do a direct insert
                    //      we would run into trouble if we didnt do this and the record we were deleting was the last in the tree's node
                    //      becuase down stream the nodes will error out if we try to insert something higher than they are
                    parent.InsertKeyUnsafe(this._MaxRecord, NewNode.PageID);


                }
                else  // Note that becuase the child nodes are leafs, we do want to use the 'OriginalKeyColumns' not the 'Weak Key Columns'
                {

                    // Need to update the key for OriginalNode to be it's last record //
                    parent.Delete(BPTreePage.Composite(Record.Split(NewNode.TerminalRecord, NewNode.OriginalKeyColumns), OriginalNode.PageID));

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
                this._Root.Insert(BPTreePage.Composite(Record.Split(OriginalNode.TerminalRecord, OriginalNode.OriginalKeyColumns), OriginalNode.PageID));

                // Need to add the max record to this layer //
                this._Root.Insert(BPTreePage.Composite(this._MaxRecord, NewNode.PageID));

                // Set the new node to be the highest //
                NewNode.IsHighest = true;
                OriginalNode.IsHighest = false;

                // Set the root //
                this._T.SetPage(this._Root);

            }

            return NewNode;

        }

        /// <summary>
        /// Gets a b+tree page; this method calls the page from the parent table and will either cast or do a hard convert to the b+tree method
        /// </summary>
        /// <param name="PageID">The page ID requested</param>
        /// <returns></returns>
        private BPTreePage GetPage(int PageID)
        {
            return BPTreePage.Mutate(this._T.GetPage(PageID), this._IndexColumns);
        }

        // New Root Methods //
        /// <summary>
        /// Generates a new root page as if it were a branch node
        /// </summary>
        /// <returns></returns>
        private BPTreePage NewRootAsBranch()
        {

            Schema s = Schema.Split(this._T.Columns, this._IndexColumns);
            s.Add("@IDX", CellAffinity.INT);

            BPTreePage NewRoot = new BPTreePage(this._T.PageSize, this._T.GenerateNewPageID, -1, -1, s.Count, s.RecordDiskCost, this._IndexColumns, false);
            NewRoot.ParentPageID = -1;
            NewRoot.IsHighest = true;

            return NewRoot;

        }

        /// <summary>
        /// Generates a new root node as a leaf node; note that this only called in the ctor method
        /// </summary>
        /// <returns></returns>
        private BPTreePage NewRootAsLeaf()
        {

            Schema s = this._T.Columns;

            BPTreePage NewRoot = new BPTreePage(this._T.PageSize, this._T.GenerateNewPageID, -1, -1, s.Count, s.RecordDiskCost, this._IndexColumns, true);
            NewRoot.ParentPageID = -1;
            NewRoot.IsHighest = true;

            return NewRoot;

        }

        // Debugging //
        internal void Print(StreamWriter writer, BPTreePage Page)
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

        internal void AppendString(StringBuilder sb, BPTreePage Node)
        {

            if (Node.IsLeaf)
            {
                sb.AppendLine(string.Format("\tLeaf: {0} | {1}", Node.PageID, Node.ParentPageID));
                return;
            }

            List<int> nodes = Node.AllPageIDs();
            int j = 0;
            sb.AppendLine(string.Format("Parent PageID {0}", Node.PageID));
            foreach (int i in nodes)
            {
                sb.AppendLine(string.Format("Child PageID {0} ", i));
            }
            sb.AppendLine("---------------");

            foreach (int i in nodes)
            {
                BPTreePage x = this.GetPage(i);
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
            for (int i = 0; i < this._T.PageCount; i++)
            {
                Page p = this._T.GetPage(i);
                sb.AppendLine(string.Format("PageID: {0} | ParentPageID {1} | IsLeaf {2}", p.PageID, p.X0, p.X1 == 1));
            }

            return sb.ToString();

        }

    }


}
