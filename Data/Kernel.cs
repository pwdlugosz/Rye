using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Rye.Helpers;
using Rye.Query;

namespace Rye.Data
{
    
    public sealed class Kernel
    {

        private int _DISK_READS = 0;
        private int _DISK_WRITES = 0;
        private int _VIRTUAL_READS = 0;
        private int _VIRTUAL_WRITES = 0;

        private Dictionary<string, Extent> _ExtentCache = new Dictionary<string, Extent>(StringComparer.OrdinalIgnoreCase);
        private Dictionary<string, Table> _TableCache = new Dictionary<string, Table>(StringComparer.OrdinalIgnoreCase);

        private DataSerializationProvider _Provider = new BasicDataSerializationProvider();

        private string _TempDir = null;

        public Kernel(string TempDB)
        {
            this._TempDir = TempDB;
        }

        // Meta Data //
        public int DiskReads
        {
            get { return _DISK_READS; }
        }

        public int DiskWrites
        {
            get { return _DISK_WRITES; }
        }

        public int VirtualReads
        {
            get { return this._VIRTUAL_READS; }
        }

        public int VirtualWrites
        {
            get { return this._VIRTUAL_WRITES; }
        }

        public long MaxMemory
        {
            get
            {
                return this._MaxMemory;
            }
            set
            {
                if (value < 0)
                    this._MaxMemory = 0;
                else
                    this._MaxMemory = value;
            }
        }

        public long MaxMemoryKB
        {
            get
            {
                return this.MaxMemory / 1024;
            }
            set
            {
                this.MaxMemory = value * 1024;
            }
        }

        public long MaxMemoryMB
        {
            get { return this.MaxMemory / 1024 / 1024; }
            set { this.MaxMemory = value * 1024 * 1024; }
        }

        public int MaxThreadCount
        {
            get { return Environment.ProcessorCount; }
        }

        public string TempDirectory
        {
            get { return this._TempDir; }
            set { this._TempDir = value; }
        }

        public string DefaultExtension
        {
            get { return this._Provider.Extension; }
        }

        public long DefaultPageSize
        {
            get { return this._Provider.DefaultPageSize; }
        }

        // Public Methods //
        public void RequestFlushExtent(Extent E)
        {

            if (this.CanExceptExtent(E))
            {
                this.VirtualFlushExtent(E);
                return;
            }

            this.Flush(E);

        }

        public void RequestFlushTable(Table T)
        {

            if (this.CanExceptTable(T))
            {
                this.VirtualFlushTable(T);
                return;
            }

            this.Flush(T);

        }

        public Extent RequestBufferExtent(Table Parent, long ID)
        {

            string key = Parent.Header.CreateChild(ID).LookUpKey;

            if (this.IsExtentCached(key))
            {
                return this.VirtualBufferExtent(key);
            }

            Extent e = this.BufferExtent(Parent, ID);

            if (this.CanExceptExtent(e))
                this.VirtualFlushExtent(e);

            return e;

        }

        public Table RequestBufferTable(string Path)
        {

            if (this.IsTableCached(Path))
            {
                return this.VirtualBufferTable(Path);
            }

            Table t = this.BufferTable(Path);

            if (this.CanExceptTable(t))
                this.VirtualFlushTable(t);

            return t;

        }

        public void RequestDropTable(string Path)
        {

            // Delete the virtual table //
            if (this.IsTableCached(Path))
            {
                this.ReleaseTable(Path, false);
            }

            // Delete the disk based table //
            if (File.Exists(Path))
            {
                File.Delete(Path);
            }

        }

        public void FlushCache()
        {

            foreach (KeyValuePair<string, Table> kv in this._TableCache)
            {
                this.Flush(kv.Value);
            }
            foreach (KeyValuePair<string, Extent> kv in this._ExtentCache)
            {
                this.Flush(kv.Value);
            }

        }

        public void ClearCache()
        {
            this._ExtentCache = new Dictionary<string, Extent>(StringComparer.OrdinalIgnoreCase);
            this._TableCache = new Dictionary<string, Table>(StringComparer.OrdinalIgnoreCase);
            this._CurrentMemory = 0;
        }

        public void FlushAndClearCache()
        {
            this.FlushCache();
            this.ClearCache();
        }

        public void ShutDown()
        {
            this.FlushAndClearCache();
        }

        public string Status
        {
            get
            {

                StringBuilder sb = new StringBuilder();
                sb.AppendLine("Kernel Statistics");
                sb.AppendLine(string.Format("Disk Reads: {0}", this.DiskReads));
                sb.AppendLine(string.Format("Disk Writes: {0}", this.DiskWrites));
                sb.AppendLine(string.Format("Virtual Reads: {0}", this._VIRTUAL_READS));
                sb.AppendLine(string.Format("Virtual Writes: {0}", this._VIRTUAL_WRITES));
                sb.AppendLine("Shard Cache:");
                foreach (KeyValuePair<string, Extent> kv in this._ExtentCache)
                {
                    sb.AppendLine(kv.Key);
                }
                sb.AppendLine("ShartTable Cache:");
                foreach (KeyValuePair<string, Table> kv in this._TableCache)
                {
                    sb.AppendLine(kv.Key);
                }
                return sb.ToString();

            }
        }

        public bool TableExists(string Path)
        {
            return File.Exists(Path) || this.IsTableCached(Path);
        }

        #region IO_Manager

        private long _MaxMemory = 256 * 1024 * 1024; // 256 mb
        private long _CurrentMemory = 0;
        private int _ExtentCacheLimit = 16;
        private int _TableCacheLimit = 16;

        // ----------------------- Extents ----------------------- //
        private bool IsExtentCached(string Path)
        {
            return this._ExtentCache.ContainsKey(Path);
        }

        private bool CanExceptExtent(Extent Data)
        {

            // Shard is already cached //
            if (this._ExtentCache.ContainsKey(Data.Header.Path))
                return true;

            // We are over the cache the limit //
            if (this._ExtentCache.Count >= this._ExtentCacheLimit)
                return false;

            // We are over the memory limit //
            //if (this._MaxMemory < this._CurrentMemory + Data.MemCost)
            //    return false;

            // Otherwise we can accept it //
            return true;

        }

        private void VirtualFlushExtent(Extent Data)
        {

            if (this._ExtentCache.ContainsKey(Data.Header.LookUpKey))
            {
                this._CurrentMemory += (Data.MemCost - this._ExtentCache[Data.Header.LookUpKey].MemCost);
                this._ExtentCache[Data.Header.LookUpKey] = Data;
                this._VIRTUAL_WRITES++;
            }
            else
            {
                this._CurrentMemory += Data.MemCost;
                this._ExtentCache.Add(Data.Header.LookUpKey, Data);
                this._VIRTUAL_WRITES++;
            }

        }

        private Extent VirtualBufferExtent(string Key)
        {

            this._VIRTUAL_READS++;
            return this._ExtentCache[Key];

        }

        private void ReleaseExtent(string Key, bool Flush)
        {

            if (!this.IsExtentCached(Key))
                return;

            if (Flush)
                this.Flush(this.VirtualBufferExtent(Key));

            this._CurrentMemory -= this.VirtualBufferExtent(Key).MemCost;

            this._ExtentCache.Remove(Key);

        }

        private void ReleaseAllExtents(bool Flush)
        {

            List<string> paths = this._ExtentCache.Keys.ToList();

            foreach (string path in paths)
            {
                this.ReleaseExtent(path, Flush);
            }

        }

        // ----------------------- Tables ----------------------- //
        private bool IsTableCached(string Path)
        {
            return this._TableCache.ContainsKey(Path);
        }

        private bool CanExceptTable(Table Data)
        {
            if (this._TableCache.ContainsKey(Data.Header.Path))
                return true;
            if (this._TableCache.Count >= this._TableCacheLimit)
                return false;
            if (this._MaxMemory < this._CurrentMemory + Data.MemCost)
                return false;
            return true;
        }

        private void VirtualFlushTable(Table Data)
        {

            if (this._TableCache.ContainsKey(Data.Header.Path))
            {
                this._CurrentMemory += (Data.MemCost - this._TableCache[Data.Header.Path].MemCost);
                this._TableCache[Data.Header.Path] = Data;
                this._VIRTUAL_WRITES++;
            }
            else
            {
                this._CurrentMemory += Data.MemCost;
                this._TableCache.Add(Data.Header.Path, Data);
                this._VIRTUAL_WRITES++;
            }

        }

        private Table VirtualBufferTable(string Path)
        {

            this._VIRTUAL_READS++;
            return this._TableCache[Path];

        }

        private void ReleaseTable(string Path, bool Flush)
        {

            if (!this.IsTableCached(Path))
                return;

            if (Flush)
                this.Flush(this.VirtualBufferTable(Path));

            this._CurrentMemory -= this.VirtualBufferTable(Path).MemCost;

            this._TableCache.Remove(Path);

        }

        private void ReleaseAllTables(bool Flush)
        {

            List<string> paths = this._TableCache.Keys.ToList();

            foreach (string path in paths)
            {
                this.ReleaseTable(path, Flush);
            }

        }

        #endregion

        #region DiskProcess

        private void Flush(Extent E)
        {

            this._Provider.FlushExtent(E);
            this._DISK_WRITES++;

        }

        private void Flush(Table T)
        {

            this._Provider.FlushTable(T);
            this._DISK_WRITES++;

        }

        private Extent BufferExtent(Table Parent, long ID)
        {

            Extent e = this._Provider.BufferExtent(Parent, ID);
            this._DISK_READS++;

            return e;

        }

        private Table BufferTable(string Path)
        {

            Table t = this._Provider.BufferTable(this, Path);
            this._DISK_READS++;

            return t;

        }

        #endregion

        public void TextDump(TabularData Data, string OutPath, char Delim, char Escape, Expressions.Filter Where, Expressions.Register Memory)
        {

            StreamWriter sw = new StreamWriter(OutPath);

            RecordReader stream = Data.CreateVolume().OpenReader(Memory, Where);

            sw.WriteLine(Data.Columns.ToNameString(Delim));

            while (!stream.EndOfData)
            {

                sw.WriteLine(stream.ReadNext().ToString(Delim, Escape));

            }

            sw.Close();

        }

        public void TextDump(TabularData Data, string OutPath, char Delim, Expressions.Filter Where, Expressions.Register Memory)
        {

            StreamWriter sw = new StreamWriter(OutPath);

            RecordReader stream = Data.CreateVolume().OpenReader(Memory, Where);

            sw.WriteLine(Data.Columns.ToNameString(Delim));

            while (!stream.EndOfData)
            {

                sw.WriteLine(stream.ReadNext().ToString(Delim));

            }

            sw.Close();

        }

        public void TextDump(TabularData Data, string OutPath, char Delim, char Escape)
        {
            this.TextDump(Data, OutPath, Delim, Escape, Expressions.Filter.TrueForAll, new Expressions.Register("THIS_DOESN'T MATTER!", Data.Columns));
        }

        public void TextDump(TabularData Data, string OutPath, char Delim)
        {
            this.TextDump(Data, OutPath, Delim, Expressions.Filter.TrueForAll, new Expressions.Register("THIS_DOESN'T MATTER!", Data.Columns));
        }

        public void TextPop(TabularData Data, string InPath, char[] Delim, char Escape, int Skip)
        {

            RecordWriter writer = Data.OpenWriter();
            using (StreamReader reader = new StreamReader(InPath))
            {

                for (int i = 0; i < Skip; i++)
                {
                    if (reader.EndOfStream)
                        break;
                    string t = reader.ReadLine();
                }

                while (!reader.EndOfStream)
                {

                    string s = reader.ReadLine();
                    Record r = Splitter.ToRecord(s, Data.Columns, Delim, Escape);
                    writer.Insert(r);

                }

            }

            writer.Close();

        }

    }

    public abstract class DataSerializationProvider
    {

        public const long META_DATA_PAGE_SIZE = 1024 * 1024;
        public const long META_DATA_PAGE_OFFSET = 0;
        public const long DEFAULT_PAGE_SIZE = Extent.DEFAULT_PAGE_SIZE;
        
        // Properties //
        public abstract int Version { get; }

        public abstract string Extension { get; }

        public virtual long DefaultPageSize
        {
            get { return DEFAULT_PAGE_SIZE; }
        }

        public abstract int WriteCell(byte[] Mem, int Location, Cell C);

        public abstract int WriteRecord(byte[] Mem, int Location, Record R);

        public abstract int WriteRecordCollection(byte[] Mem, int Location, List<Record> Cache);

        public int WriteShardedExtent(byte[] Mem, int Location, Extent Data)
        {

            /*
             * Write:
             *      Header
             *      Schema
             *      SortKey
             *      Record Collection
             */

            // Update the data //
            Data.PreSerialize();

            // Write header //
            Location = this.WriteRecord(Mem, Location, Data.Header);

            // Write columns //
            Location = this.WriteRecordCollection(Mem, Location, Data.Columns._Cache);

            // Write sort key //
            Location = this.WriteRecord(Mem, Location, Data.SortBy.ToRecord());

            // Write cache //
            Location = this.WriteRecordCollection(Mem, Location, Data.Cache);

            return Location;

        }

        public int WriteVanillaExtent(byte[] Mem, int Location, Extent Data)
        {

            // Update the data //
            Data.PreSerialize();

            // Write cache //
            Location = this.WriteRecordCollection(Mem, Location, Data.Cache);

            return Location;

        }

        public int WriteTable(byte[] Mem, int Location, Table Data)
        {

            /*
             * Write:
             *      Header
             *      Schema
             *      SortKey
             *      Record Collection
             */

            // Update the data //
            //Data.PreSerialize();

            // Write header //
            Location = this.WriteRecord(Mem, Location, Data.Header);

            // Write columns //
            Location = this.WriteRecordCollection(Mem, Location, Data.Columns._Cache);

            // Write sort key //
            Location = this.WriteRecord(Mem, Location, Data.SortBy.ToRecord());

            // Write cache //
            Location = this.WriteRecordCollection(Mem, Location, Data.ReferenceTable.Cache);

            return Location;

        }

        public void FlushExtent(Extent Data)
        {

            // Get the path //
            string ParentPath = Data.Header.Path;

            // Get the disk location //
            long Location = this.GetDataPageLocation(Data.Header);

            // Create the buffer //
            byte[] b = new byte[Data.Header.PageSize];

            // Load the buffer //
            this.WriteRecordCollection(b, 0, Data.Cache);

            // Get the open method //
            FileMode mode = FileMode.Open;
            if (!File.Exists(ParentPath))
                mode = FileMode.Create;

            // Write the buffer to disk //
            using (FileStream fs = new FileStream(ParentPath, mode, FileAccess.Write, FileShare.None))
            {

                this.WritePage(fs, Location, b);

            }

        }

        public void FlushTable(Table Data)
        {

            // Get the path //
            string ParentPath = Data.Header.Path;

            // Create the buffer //
            byte[] b = new byte[META_DATA_PAGE_SIZE];

            // Load the buffer //
            this.WriteTable(b, 0, Data);

            // Get the open method //
            FileMode mode = FileMode.Open;
            if (!File.Exists(ParentPath))
                mode = FileMode.Create;

            // Write the buffer to disk //
            using (FileStream fs = new FileStream(ParentPath, mode, FileAccess.Write, FileShare.None))
            {
                this.WritePage(fs, META_DATA_PAGE_OFFSET, b);
            }

        }

        // Disk Reading Methods //
        public abstract int ReadCell(byte[] Mem, int Location, out Cell C);

        public abstract int ReadRecord(byte[] Mem, int Location, int Length, out Record Datum);

        public abstract int ReadRecordCollection(byte[] Mem, int Location, long Count, int Length, List<Record> Cache);

        public abstract int ReadHeaderCollection(byte[] Mem, int Location, long Count, int Length, List<Header> Cache);

        public Extent ReadShardedExtent(byte[] Mem, int Location)
        {

            /*
             * Read:
             *      Header
             *      Schema
             *      SortKey
             *      Record Collection
             */

            // Read header //
            Record rh;
            Location = this.ReadRecord(Mem, Location, Header.RECORD_LEN, out rh);
            Header h = new Header(rh);

            // Read schema //
            List<Record> s_cache = new List<Record>();
            Location = this.ReadRecordCollection(Mem, Location, h.ColumnCount, Schema.RECORD_LEN, s_cache);
            Schema s = new Schema(s_cache);

            // Read key //
            Record rk;
            Location = this.ReadRecord(Mem, Location, (int)h.KeyCount, out rk);
            Key k = new Key(rk);

            // Read record cache //
            List<Record> d_cache = new List<Record>();
            Location = this.ReadRecordCollection(Mem, Location, (int)h.RecordCount, (int)h.ColumnCount, d_cache);

            // Return recordset //
            return new Extent(s, h, d_cache, k);

        }

        public Extent ReadVanillaExtent(byte[] Mem, int Location, Table Parent, long ID)
        {


            // Generate the header //
            Header h = Parent.Header.CreateChild(ID);

            // Read record cache //
            List<Record> d_cache = new List<Record>();
            Location = this.ReadRecordCollection(Mem, Location, (int)h.RecordCount, (int)h.ColumnCount, d_cache);

            // Return recordset //
            return new Extent(Parent.Columns, h, d_cache, Parent.SortBy);

        }

        public Table ReadTable(Kernel Driver, byte[] Mem, int Location)
        {

            /*
             * Read:
             *      Header
             *      Schema
             *      SortKey
             *      Record Collection
             */

            // Read header //
            Record rh;
            Location = this.ReadRecord(Mem, Location, Header.RECORD_LEN, out rh);
            Header h = new Header(rh);

            // Read schema //
            List<Record> s_cache = new List<Record>();
            Location = this.ReadRecordCollection(Mem, Location, h.ColumnCount, Schema.RECORD_LEN, s_cache);
            Schema s = new Schema(s_cache);

            // Read key //
            Record rk;
            Location = this.ReadRecord(Mem, Location, (int)h.KeyCount, out rk);
            Key k = new Key(rk);

            // Read record cache //
            List<Record> d_cache = new List<Record>();
            Location = this.ReadRecordCollection(Mem, Location, (int)h.RecordCount, Table.RECORD_LEN, d_cache);

            // Return recordset //
            return new Table(Driver, h, s, d_cache, k);

        }

        public Extent BufferExtent(Table Parent, long ID)
        {

            // Child header //
            Header h = Parent.RenderHeader((int)ID);
            
            // Create a buffer //
            byte[] buffer;

            // Open a file stream //
            using (FileStream fs = File.Open(Parent.Header.Path, FileMode.Open, FileAccess.Read, FileShare.None))
            {

                // Get the page location //
                long position = this.GetDataPageLocation(h);

                // Load the buffer //
                buffer = this.BufferPage(fs, position, h.PageSize);

            }

            // Serialize the extent //
            List<Record> cache = new List<Record>();
            int idx = this.ReadRecordCollection(buffer, 0, h.RecordCount, (int)h.ColumnCount, cache);

            // Return //
            return new Extent(Parent.Columns, h, cache, Parent.SortBy);

        }

        public Table BufferTable(Kernel Driver, string Path)
        {

            // Create a buffer //
            byte[] buffer;

            // Open a file stream //
            using (FileStream fs = File.Open(Path, FileMode.Open, FileAccess.Read, FileShare.None))
            {

                // Read in a whole page, since we're at 0, this will be the meta data page //
                buffer = this.BufferPage(fs, 0, META_DATA_PAGE_SIZE);

            }

            return this.ReadTable(Driver, buffer, 0);

        }

        // Disk Support //
        public byte[] BufferPage(FileStream Reader, long Location, long PageSize)
        {

            // Check position //
            if (Reader.Position != Location)
                Reader.Position = Location;

            // Create the buffer //
            byte[] page = new byte[PageSize];

            // Load it //
            Reader.Read(page, 0, (int)PageSize);

            return page;


        }

        public void WritePage(FileStream Writer, long Location, byte[] Page)
        {

            if (Writer.Position != Location)
                Writer.Position = Location;

            Writer.Write(Page, 0, Page.Length);

        }

        public long GetDataPageLocation(Header H)
        {

            return META_DATA_PAGE_SIZE + H.ID * H.PageSize;

        }

    }

    public sealed class BasicDataSerializationProvider : DataSerializationProvider
    {

        public const string EXTENSION = "ryedatv1";
        public const int VERSION = 1;

        public override string Extension
        {
            get { return EXTENSION; }
        }

        public override int Version
        {
            get { return VERSION; }
        }

        // Reads //
        public override int ReadCell(byte[] Mem, int Location, out Cell C)
        {

            // Read the affinity //
            CellAffinity a = (CellAffinity)Mem[Location];
            Location++;

            // Read nullness //
            bool b = (Mem[Location] == 1);
            Location++;

            // If we are null, then exit
            // for security reasons, we do not want to write any ghost data if the cell is null //
            if (b == true)
            {
                C = new Cell(a);
                return Location;
            }

            // Cell c //
            C = new Cell(a);
            C.NULL = 0;

            if (a == CellAffinity.BOOL)
            {
                C.B0 = Mem[Location];
                Location++;
                return Location;
            }

            // BLOB //
            if (a == CellAffinity.BLOB)
            {

                C.B4 = Mem[Location];
                C.B5 = Mem[Location + 1];
                C.B6 = Mem[Location + 2];
                C.B7 = Mem[Location + 3];
                Location += 4;
                byte[] blob = new byte[C.INT_B];
                for (int i = 0; i < blob.Length; i++)
                {
                    blob[i] = Mem[Location];
                    Location++;
                }
                C = new Cell(blob);
                return Location;

            }

            // STRING //
            if (a == CellAffinity.STRING)
            {

                C.B4 = Mem[Location];
                C.B5 = Mem[Location + 1];
                C.B6 = Mem[Location + 2];
                C.B7 = Mem[Location + 3];
                Location += 4;
                char[] chars = new char[C.INT_B];
                for (int i = 0; i < C.INT_B; i++)
                {
                    byte c1 = Mem[Location];
                    Location++;
                    byte c2 = Mem[Location];
                    Location++;
                    chars[i] = (char)(((int)c2) | (int)(c1 << 8));
                }
                C = new Cell(new string(chars));
                return Location;

            }

            // Double, Ints, Dates //
            C.B0 = Mem[Location];
            C.B1 = Mem[Location + 1];
            C.B2 = Mem[Location + 2];
            C.B3 = Mem[Location + 3];
            C.B4 = Mem[Location + 4];
            C.B5 = Mem[Location + 5];
            C.B6 = Mem[Location + 6];
            C.B7 = Mem[Location + 7];
            Location += 8;
            return Location;

        }

        public override int ReadRecord(byte[] Mem, int Location, int Length, out Record Datum)
        {

            // Array //
            Cell[] q = new Cell[Length];

            // Get cells //
            for (int j = 0; j < Length; j++)
            {

                Cell C;

                // Read the affinity //
                CellAffinity a = (CellAffinity)Mem[Location];
                Location++;

                // Read nullness //
                bool b = (Mem[Location] == 1);
                Location++;

                // If we are null, then exit
                // for security reasons, we do not want to write any ghost data if the cell is null //
                if (b == true)
                {
                    C = new Cell(a);
                }
                else
                {

                    // Cell c //
                    C = new Cell(a);
                    C.NULL = 0;

                    // BOOL //
                    if (a == CellAffinity.BOOL)
                    {
                        C.B0 = Mem[Location];
                        Location++;
                    }

                    // BLOB //
                    else if (a == CellAffinity.BLOB)
                    {

                        C.B4 = Mem[Location];
                        C.B5 = Mem[Location + 1];
                        C.B6 = Mem[Location + 2];
                        C.B7 = Mem[Location + 3];
                        Location += 4;
                        byte[] blob = new byte[C.INT_B];
                        for (int i = 0; i < blob.Length; i++)
                        {
                            blob[i] = Mem[Location];
                            Location++;
                        }
                        C = new Cell(blob);

                    }

                    // STRING //
                    else if (a == CellAffinity.STRING)
                    {

                        C.B4 = Mem[Location];
                        C.B5 = Mem[Location + 1];
                        C.B6 = Mem[Location + 2];
                        C.B7 = Mem[Location + 3];
                        Location += 4;
                        char[] chars = new char[C.INT_B];
                        for (int i = 0; i < C.INT_B; i++)
                        {
                            byte c1 = Mem[Location];
                            Location++;
                            byte c2 = Mem[Location];
                            Location++;
                            chars[i] = (char)(((int)c2) | (int)(c1 << 8));
                        }
                        C = new Cell(new string(chars));

                    }

                    // Double, Ints, Dates //
                    else
                    {
                        C.B0 = Mem[Location];
                        C.B1 = Mem[Location + 1];
                        C.B2 = Mem[Location + 2];
                        C.B3 = Mem[Location + 3];
                        C.B4 = Mem[Location + 4];
                        C.B5 = Mem[Location + 5];
                        C.B6 = Mem[Location + 6];
                        C.B7 = Mem[Location + 7];
                        Location += 8;
                    }

                }

                q[j] = C;

            }

            Datum = new Record(q);
            return Location;

        }

        public override int ReadRecordCollection(byte[] Mem, int Location, long Count, int Length, List<Record> Cache)
        {

            // Loop through //
            for (int k = 0; k < Count; k++)
            {

                // Array //
                Cell[] q = new Cell[Length];

                // Get cells //
                for (int j = 0; j < Length; j++)
                {

                    Cell C;

                    // Read the affinity //
                    CellAffinity a = (CellAffinity)Mem[Location];
                    Location++;

                    // Read nullness //
                    bool b = (Mem[Location] == 1);
                    Location++;

                    // If we are null, then exit
                    // for security reasons, we do not want to write any ghost data if the cell is null //
                    if (b == true)
                    {
                        C = new Cell(a);
                    }
                    else
                    {

                        // Cell c //
                        C = new Cell(a);
                        C.NULL = 0;

                        // BOOL //
                        if (a == CellAffinity.BOOL)
                        {
                            C.B0 = Mem[Location];
                            Location++;
                        }

                        // BLOB //
                        else if (a == CellAffinity.BLOB)
                        {

                            C.B4 = Mem[Location];
                            C.B5 = Mem[Location + 1];
                            C.B6 = Mem[Location + 2];
                            C.B7 = Mem[Location + 3];
                            Location += 4;
                            byte[] blob = new byte[C.INT_B];
                            for (int i = 0; i < blob.Length; i++)
                            {
                                blob[i] = Mem[Location];
                                Location++;
                            }
                            C = new Cell(blob);

                        }

                        // STRING //
                        else if (a == CellAffinity.STRING)
                        {

                            C.B4 = Mem[Location];
                            C.B5 = Mem[Location + 1];
                            C.B6 = Mem[Location + 2];
                            C.B7 = Mem[Location + 3];
                            Location += 4;
                            char[] chars = new char[C.INT_B];
                            for (int i = 0; i < C.INT_B; i++)
                            {
                                byte c1 = Mem[Location];
                                Location++;
                                byte c2 = Mem[Location];
                                Location++;
                                chars[i] = (char)(((int)c2) | (int)(c1 << 8));
                            }
                            C = new Cell(new string(chars));

                        }

                        // Double, Ints, Dates //
                        else
                        {
                            C.B0 = Mem[Location];
                            C.B1 = Mem[Location + 1];
                            C.B2 = Mem[Location + 2];
                            C.B3 = Mem[Location + 3];
                            C.B4 = Mem[Location + 4];
                            C.B5 = Mem[Location + 5];
                            C.B6 = Mem[Location + 6];
                            C.B7 = Mem[Location + 7];
                            Location += 8;
                        }

                    }

                    q[j] = C;

                }

                Cache.Add(new Record(q));

            }

            return Location;

        }

        public override int ReadHeaderCollection(byte[] Mem, int Location, long Count, int Length, List<Header> Cache)
        {

            // Loop through //
            for (int k = 0; k < Count; k++)
            {

                // Array //
                Cell[] q = new Cell[Length];

                // Get cells //
                for (int j = 0; j < Length; j++)
                {

                    Cell C;

                    // Read the affinity //
                    CellAffinity a = (CellAffinity)Mem[Location];
                    Location++;

                    // Read nullness //
                    bool b = (Mem[Location] == 1);
                    Location++;

                    // If we are null, then exit
                    // for security reasons, we do not want to write any ghost data if the cell is null //
                    if (b == true)
                    {
                        C = new Cell(a);
                    }
                    else
                    {

                        // Cell c //
                        C = new Cell(a);
                        C.NULL = 0;

                        // BOOL //
                        if (a == CellAffinity.BOOL)
                        {
                            C.B0 = Mem[Location];
                            Location++;
                        }

                        // BLOB //
                        else if (a == CellAffinity.BLOB)
                        {

                            C.B4 = Mem[Location];
                            C.B5 = Mem[Location + 1];
                            C.B6 = Mem[Location + 2];
                            C.B7 = Mem[Location + 3];
                            Location += 4;
                            byte[] blob = new byte[C.INT_B];
                            for (int i = 0; i < blob.Length; i++)
                            {
                                blob[i] = Mem[Location];
                                Location++;
                            }
                            C = new Cell(blob);

                        }

                        // STRING //
                        else if (a == CellAffinity.STRING)
                        {

                            C.B4 = Mem[Location];
                            C.B5 = Mem[Location + 1];
                            C.B6 = Mem[Location + 2];
                            C.B7 = Mem[Location + 3];
                            Location += 4;
                            char[] chars = new char[C.INT_B];
                            for (int i = 0; i < C.INT_B; i++)
                            {
                                byte c1 = Mem[Location];
                                Location++;
                                byte c2 = Mem[Location];
                                Location++;
                                chars[i] = (char)(((int)c2) | (int)(c1 << 8));
                            }
                            C = new Cell(new string(chars));

                        }

                        // Double, Ints, Dates //
                        else
                        {
                            C.B0 = Mem[Location];
                            C.B1 = Mem[Location + 1];
                            C.B2 = Mem[Location + 2];
                            C.B3 = Mem[Location + 3];
                            C.B4 = Mem[Location + 4];
                            C.B5 = Mem[Location + 5];
                            C.B6 = Mem[Location + 6];
                            C.B7 = Mem[Location + 7];
                            Location += 8;
                        }

                    }

                    q[j] = C;

                }

                Cache.Add(new Header(new Record(q)));

            }

            return Location;

        }

        // Writes //
        public override int WriteCell(byte[] Mem, int Location, Cell C)
        {

            // Write the affinity //
            Mem[Location] = ((byte)C.AFFINITY);
            Location++;

            // Write nullness //
            Mem[Location] = C.NULL;
            Location++;

            // If we are null, then exit
            // for security reasons, we do not want to write any ghost data if the cell is null //
            if (C.IsNull)
                return Location;

            // Bool //
            if (C.AFFINITY == CellAffinity.BOOL)
            {
                Mem[Location] = (C.BOOL == true ? (byte)1 : (byte)0);
                Location++;
                return Location;
            }

            // BLOB //
            if (C.AFFINITY == CellAffinity.BLOB)
            {

                C.INT_B = C.BLOB.Length;
                Mem[Location] = (C.B4);
                Mem[Location + 1] = (C.B5);
                Mem[Location + 2] = (C.B6);
                Mem[Location + 3] = (C.B7);
                Location += 4;

                for (int i = 0; i < C.BLOB.Length; i++)
                {
                    Mem[Location + i] = C.BLOB[i];
                }

                Location += C.BLOB.Length;
                return Location;

            }

            // STRING //
            if (C.AFFINITY == CellAffinity.STRING)
            {

                C.INT_B = C.STRING.Length;
                Mem[Location] = (C.B4);
                Mem[Location + 1] = (C.B5);
                Mem[Location + 2] = (C.B6);
                Mem[Location + 3] = (C.B7);
                Location += 4;

                for (int i = 0; i < C.STRING.Length; i++)
                {
                    byte c1 = (byte)(C.STRING[i] >> 8);
                    byte c2 = (byte)(C.STRING[i] & 255);
                    Mem[Location] = c1;
                    Location++;
                    Mem[Location] = c2;
                    Location++;
                }
                return Location;

            }

            // Double, int, date //
            Mem[Location] = C.B0;
            Mem[Location + 1] = C.B1;
            Mem[Location + 2] = C.B2;
            Mem[Location + 3] = C.B3;
            Mem[Location + 4] = C.B4;
            Mem[Location + 5] = C.B5;
            Mem[Location + 6] = C.B6;
            Mem[Location + 7] = C.B7;

            return Location + 8;

        }

        public override int WriteRecord(byte[] Mem, int Location, Record R)
        {

            // Write each cell //
            for (int j = 0; j < R.Count; j++)
            {

                Cell C = R[j];

                // Write the affinity //
                Mem[Location] = ((byte)C.AFFINITY);
                Location++;

                // Write nullness //
                Mem[Location] = C.NULL;
                Location++;

                // If we are null, then exit
                // for security reasons, we do not want to write any ghost data if the cell is null //
                if (C.NULL == 0)
                {

                    // Bool //
                    if (C.AFFINITY == CellAffinity.BOOL)
                    {
                        Mem[Location] = (C.BOOL == true ? (byte)1 : (byte)0);
                        Location++;
                    }

                    // BLOB //
                    else if (C.AFFINITY == CellAffinity.BLOB)
                    {

                        C.INT_B = C.BLOB.Length;
                        Mem[Location] = (C.B4);
                        Mem[Location + 1] = (C.B5);
                        Mem[Location + 2] = (C.B6);
                        Mem[Location + 3] = (C.B7);
                        Location += 4;

                        for (int i = 0; i < C.BLOB.Length; i++)
                        {
                            Mem[Location + i] = C.BLOB[i];
                        }

                        Location += C.BLOB.Length;

                    }

                    // STRING //
                    else if (C.AFFINITY == CellAffinity.STRING)
                    {

                        C.INT_B = C.STRING.Length;
                        Mem[Location] = (C.B4);
                        Mem[Location + 1] = (C.B5);
                        Mem[Location + 2] = (C.B6);
                        Mem[Location + 3] = (C.B7);
                        Location += 4;

                        for (int i = 0; i < C.STRING.Length; i++)
                        {
                            byte c1 = (byte)(C.STRING[i] >> 8);
                            byte c2 = (byte)(C.STRING[i] & 255);
                            Mem[Location] = c1;
                            Location++;
                            Mem[Location] = c2;
                            Location++;
                        }

                    }

                    // Double, int, date //
                    else
                    {

                        Mem[Location] = C.B0;
                        Mem[Location + 1] = C.B1;
                        Mem[Location + 2] = C.B2;
                        Mem[Location + 3] = C.B3;
                        Mem[Location + 4] = C.B4;
                        Mem[Location + 5] = C.B5;
                        Mem[Location + 6] = C.B6;
                        Mem[Location + 7] = C.B7;
                        Location += 8;
                    }

                }

            }

            return Location;

        }

        public override int WriteRecordCollection(byte[] Mem, int Location, List<Record> Cache)
        {

            // Do NOT write the record count; assume the reader knows what the record count is //
            foreach (Record R in Cache)
            {

                // Write each cell //
                for (int j = 0; j < R.Count; j++)
                {

                    Cell C = R[j];

                    // Write the affinity //
                    Mem[Location] = ((byte)C.AFFINITY);
                    Location++;

                    // Write nullness //
                    Mem[Location] = C.NULL;
                    Location++;

                    // If we are null, then exit
                    // for security reasons, we do not want to write any ghost data if the cell is null //
                    if (C.NULL == 0)
                    {

                        // Bool //
                        if (C.AFFINITY == CellAffinity.BOOL)
                        {
                            Mem[Location] = (C.BOOL == true ? (byte)1 : (byte)0);
                            Location++;
                        }

                        // BLOB //
                        else if (C.AFFINITY == CellAffinity.BLOB)
                        {

                            C.INT_B = C.BLOB.Length;
                            Mem[Location] = (C.B4);
                            Mem[Location + 1] = (C.B5);
                            Mem[Location + 2] = (C.B6);
                            Mem[Location + 3] = (C.B7);
                            Location += 4;

                            for (int i = 0; i < C.BLOB.Length; i++)
                            {
                                Mem[Location + i] = C.BLOB[i];
                            }

                            Location += C.BLOB.Length;

                        }

                        // STRING //
                        else if (C.AFFINITY == CellAffinity.STRING)
                        {

                            C.INT_B = C.STRING.Length;
                            Mem[Location] = (C.B4);
                            Mem[Location + 1] = (C.B5);
                            Mem[Location + 2] = (C.B6);
                            Mem[Location + 3] = (C.B7);
                            Location += 4;

                            for (int i = 0; i < C.STRING.Length; i++)
                            {
                                byte c1 = (byte)(C.STRING[i] >> 8);
                                byte c2 = (byte)(C.STRING[i] & 255);
                                Mem[Location] = c1;
                                Location++;
                                Mem[Location] = c2;
                                Location++;
                            }

                        }

                        // Double, int, date //
                        else
                        {

                            Mem[Location] = C.B0;
                            Mem[Location + 1] = C.B1;
                            Mem[Location + 2] = C.B2;
                            Mem[Location + 3] = C.B3;
                            Mem[Location + 4] = C.B4;
                            Mem[Location + 5] = C.B5;
                            Mem[Location + 6] = C.B6;
                            Mem[Location + 7] = C.B7;
                            Location += 8;
                        }

                    }

                }

            }

            return Location;

        }


    }

}
