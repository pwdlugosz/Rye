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

    public interface IClusteredIndex
    {

        int Type { get; }

        int OriginDataPageID { get; }

        int TerminalDataPageID { get; }

        void Insert(Record Element);

        void Commit();

    }

    public interface IOptimizedRecordReader
    {

        RecordReader OpenReader();

        RecordReader OpenReader(Record Key, Key Columns);

        RecordReader OpenReader(Record LowerKey, Record UpperKey, Key Columns);

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
                this._Parent.Header.PageCount++;

            }

            // Add the actual record //
            this._Terminis.Insert(Element);
            this._Parent.RecordCount++;

        }

        public void Commit()
        {
            if (this._Terminis != null)
                this._Parent.SetPage(this._Terminis);
        }

    }


}
