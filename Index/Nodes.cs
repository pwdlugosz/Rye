using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Index
{

    public enum IndexNodeAffinity : byte
    {
        Internal,
        Leaf
    }

    public struct RecordPointer
    {
        
        internal int _PageID;
        internal int _RecordID;

        public RecordPointer(int PageID, int RecordID)
        {
            this._PageID = PageID;
            this._RecordID = RecordID;
        }

        public int PageID
        {
            get { return this._PageID; }
        }

        public int RecordID
        {
            get { return this._RecordID; }
        }

    }

    public abstract class IndexNode
    {

        protected IndexNode _Parent;
        protected IndexNodeAffinity _Affinity;
        protected int _Level = 0;

        protected IndexNode(IndexNode Parent, IndexNodeAffinity Affinity, int Level)
        {
            this._Parent = Parent;
            this._Affinity = Affinity;
            this._Level = Level;
        }

        // Properties //
        public IndexNode Parent
        {
            get { return this._Parent; }
        }
        
        public IndexNodeAffinity Affinity
        {
            get { return this._Affinity; }
        }

        public int Level
        {
            get { return this._Level; }
        }

        public abstract LinkedList<IndexNode> Children
        {
            get;
        }

        public abstract LinkedList<RecordPointer> Entries
        {
            get;
        }

        public abstract bool AtMaxEntries
        {
            get;
        }

        public abstract bool AtMinEntries
        {
            get;
        }

    }



}
