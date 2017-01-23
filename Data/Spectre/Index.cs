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

    public class IndexHeader
    {

        /*
            Index table record:
            Name: 36 byets (32 chars + 4 length)
            FirstPageID: 4
            LastPageID: 4
            PageCount: 4
            RecordCount: 4
            IsUnique: 4 bytes
            Type: 4 bytes
            KeyCount: 4 bytes
            KeyRecords: 8 bytes OriginalNode KeyCount
            128 bytes: total
         */

        private string _Name;
        private int _FirstPageID;

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
    /// <summary>
    /// Base class for all indexes
    /// </summary>
    public abstract class Index
    {

        protected Host _Session;

        public Index(Host Session)
        {
            this._Session = Session;
        }

        public abstract BaseTable Parent { get; }

        public abstract bool IsUnique { get; }

        public abstract Key IndexColumns { get; }

        public abstract string Name { get; }

        public abstract void AppendRecord(RecordKey Key, Record Value);

        public abstract ReadStream OpenReader();

        public abstract ReadStream OpenReader(Record Criteria);

        public abstract ReadStream OpenReader(Record Lower, Record Upper);

    }


}
