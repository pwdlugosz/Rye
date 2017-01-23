using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Rye.Helpers;
using Rye.Query;
using Rye.Structures;

namespace Rye.Data
{

    public sealed class Kernel
    {

        private TablixBuffer<Extent> _ExtentBuffer = new TablixBuffer<Extent>();
        private TablixBuffer<Table> _TableBuffer = new TablixBuffer<Table>();
        private DataSerializationProvider _Provider = new BasicDataSerializationProvider();
        private string _TempDir = null;
        private Communicator _Logger;
        private System.Diagnostics.Stopwatch _Timer;

        public Kernel(string TempDB)
        {
            this._TempDir = TempDB;
            this._Logger = new FileCommunicator(Kernel.KernelLogFilePath());
            this._Timer = System.Diagnostics.Stopwatch.StartNew();
        }

        // Header Data //
        public int DiskReads
        {
            get { return this._Provider.Reads; }
        }

        public int DiskWrites
        {
            get { return this._Provider.Writes; }
        }

        public int VirtualReads
        {
            get { return this._ExtentBuffer.Reads + this._TableBuffer.Reads; }
        }

        public int VirtualWrites
        {
            get { return this._ExtentBuffer.Writes + this._TableBuffer.Writes; }
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

        public string[] ExtentNames
        {
            get
            {
                return this._ExtentBuffer.Keys.ToArray();
            }
        }

        public string[] TableNames
        {
            get
            {
                return this._TableBuffer.Keys.ToArray();
            }
        }

        // Public Methods //
        public void RequestFlushExtent(Extent E)
        {

            E.PreSerialize();

            // Check if we need to free up space //
            if (!this._ExtentBuffer.HasCapacity(E.MemCost))
            {
                this._Logger.WriteLine("RequestFlushExtent {0}: Extent not in memory, buffering {1}.{2}", this._Timer.Elapsed, E.Header.Path, E.Header.ID);
                this.FreeExtentSpace(E.MemCost);
            }

            // Push into the buffer //
            this._Logger.WriteLine("RequestFlushExtent {0}: Extent not in memory, buffering {1}.{2}", this._Timer.Elapsed, E.Header.Path, E.Header.ID);
            this._ExtentBuffer.AllocateHard(E);

        }

        public void RequestFlushTable(Table T)
        {

            // Check if we need to free up space //
            if (!this._TableBuffer.HasCapacity(T.MemCost))
            {
                this._Logger.WriteLine("RequestFlushTable {0}: Freeing space to hold table {1}", this._Timer.Elapsed, T.Header.Path);
                this.FreeTableSpace(T.MemCost);
            }

            // Push the table into the buffer //
            this._Logger.WriteLine("RequestFlushTable {0}: BaseTable allocated to memory {1}", this._Timer.Elapsed, T.Header.Path); 
            this._TableBuffer.AllocateHard(T);

        }

        public Extent RequestBufferExtent(Table Parent, long ID)
        {

            // Get the key //
            string key = Parent.Header.CreateChild(ID).LookUpKey;

            // Check if this already exists //
            if (this._ExtentBuffer.Exists(key))
            {
                this._Logger.WriteLine("RequestBufferExtent {0}: Extent not in memory, buffering {1}.{2}", this._Timer.Elapsed, Parent.Header.Path, ID);
                return this._ExtentBuffer.Request(key);
            }

            // Get the extent from the disk //
            Extent e = this._Provider.BufferExtent(Parent, ID);
            
            // Check if we need to free up space //
            if (!this._ExtentBuffer.HasCapacity(e.MemCost))
            {
                this._Logger.WriteLine("RequestBufferExtent {0}: Freeing memory to hold extent {1}.{2}", this._Timer.Elapsed, Parent.Header.Path, ID);
                this.FreeExtentSpace(e.MemCost);
            }

            // Push the extent into the buffer //
            this._Logger.WriteLine("RequestBufferExtent {0}: Extent allocated to memory {1}.{2}", this._Timer.Elapsed, Parent.Header.Path, ID);
            this._ExtentBuffer.Allocate(e);

            return e;

        }

        public Table RequestBufferTable(string Path)
        {

            // Check if this table is already in memory //
            if (this._TableBuffer.Exists(Path))
            {
                this._Logger.WriteLine("RequestBufferTable {0}: BaseTable does not exist in memory, buffering from {1}", this._Timer.Elapsed, Path);
                return this._TableBuffer.Request(Path);
            }

            // Pull from disk //
            Table t = this._Provider.BufferTable(this, Path);

            // Check if we need to free up space //
            if (!this._TableBuffer.HasCapacity(t.MemCost))
            {
                this._Logger.WriteLine("RequestBufferTable {0}: Freeing space to hold table {1}", this._Timer.Elapsed, Path);
                this.FreeTableSpace(t.MemCost);
            }

            // Push the table onto the buffer //
            this._Logger.WriteLine("RequestBufferTable {0}: BaseTable allocated to memory {1}", this._Timer.Elapsed, Path);
            this._TableBuffer.Allocate(t);
            
            // Return the table //
            return t;

        }

        public void RequestDropTable(string Path)
        {

            // Check if the table even exists
            if (!this._TableBuffer.Exists(Path) && !File.Exists(Path))
            {
                this._Logger.WriteLine("RequestDropTable {0}: File does not exist {1}", this._Timer.Elapsed, Path);
                return;
            }

            // Check if the table is in memory //
            if (this._TableBuffer.Exists(Path))
            {
                this._Logger.WriteLine("RequestDropTable {0}: Clearing table and extents from memory {1}", this._Timer.Elapsed, Path);
                Table t = this._TableBuffer.RequestAndDeallocate(Path);
                this._ExtentBuffer.DeallocateChildren(t.Header);
            }

            // Delete the disk based table //
            if (File.Exists(Path))
            {
                this._Logger.WriteLine("RequestDropTable {0}: Delete hard file {1}", this._Timer.Elapsed, Path);
                File.Delete(Path);
            }

        }

        // Space freeing methods //
        public void FreeExtentSpace(long SpaceNeeded)
        {

            while (!this._ExtentBuffer.HasCapacity(SpaceNeeded) && this._ExtentBuffer.Count > 0)
            {

                // Get the version //
                int version = 0;
                
                // Get the extent //
                Extent e = this._ExtentBuffer.RequestAndDeallocateNext(out version);
                
                // Flush if the version is > 0; this means the table was written to or updated at least once //
                if (version != 0)
                {
                    this._Logger.WriteLine("FreeAllExtentSpace {0}: Flushing {1}, ID {2}, Version {3}", this._Timer.Elapsed, e.Header.Path, e.Header.ID, version);
                    this._Provider.FlushExtent(e);
                }
                else
                {
                    this._Logger.WriteLine("FreeAllExtentSpace {0}: Removing w/o flushing {1}, ID {2}", this._Timer.Elapsed, e.Header.Path, e.Header.ID);
                }

            }

        }

        public void FreeAllExtentSpace()
        {

            while (this._ExtentBuffer.Count != 0)
            {

                int version = 0;
                Extent e = this._ExtentBuffer.RequestAndDeallocateNext(out version);

                if (version != 0)
                {
                    this._Logger.WriteLine("FreeAllExtentSpace {0}: Flushing {1}, ID {2}, Version {3}", this._Timer.Elapsed, e.Header.Path, e.Header.ID, version);
                    this._Provider.FlushExtent(e);
                }
                else
                {
                    this._Logger.WriteLine("FreeAllExtentSpace {0}: Removing w/o flushing {1}, ID {2}", this._Timer.Elapsed, e.Header.Path, e.Header.ID);
                }

            }

        }

        public void FreeTableSpace(long SpaceNeeded)
        {

            while (!this._TableBuffer.HasCapacity(SpaceNeeded) && this._TableBuffer.Count > 0)
            {

                // Get the version //
                int version = 0;

                // Get the extent //
                Table t = this._TableBuffer.RequestAndDeallocateNext(out version);

                // Flush if the version is > 0; this means the table was written to or updated at least once //
                if (version != 0)
                {
                    this._Logger.WriteLine("FreeTableSpace {0}: Flushing {1} with version {2}", this._Timer.Elapsed, t.Header.Path, version);
                    this._Provider.FlushTable(t);
                }
                else
                {
                    this._Logger.WriteLine("FreeTableSpace {0}: Removing w/o flushing {1}", this._Timer.Elapsed, t.Header.Path);
                }

            }

        }

        public void FreeAllTableSpace()
        {
            this._Logger.WriteLine("FreeAllTableSpace {0}", this._Timer.Elapsed);
            while (this._TableBuffer.Count != 0)
            {
                Table t = this._TableBuffer.RequestAndDeallocateNext();
                this._Logger.WriteLine("FreeAllTableSpace {0}: Flushing {1}", this._Timer.Elapsed, t.Header.Path);
                this._Provider.FlushTable(t);
            }

        }

        // Flushing and clearing the cache //
        public void FlushCache()
        {
            this._Logger.WriteLine("FlushCache {0}", this._Timer.Elapsed);
            this.FreeAllExtentSpace();
            this.FreeAllTableSpace();
        }

        public void ClearCache()
        {
            this._Logger.WriteLine("ClearCache {0}", this._Timer.Elapsed);
            this._ExtentBuffer = new TablixBuffer<Extent>();
            this._TableBuffer = new TablixBuffer<Table>();
        }

        public void FlushAndClearCache()
        {
            this._Logger.WriteLine("FlushAndClearCache {0}", this._Timer.Elapsed);
            this.FlushCache();
            this.ClearCache();
        }

        public void ShutDown()
        {
            this._Logger.WriteLine("ShutDown {0}", this._Timer.Elapsed);
            this.FlushAndClearCache();
            this._Logger.ShutDown();
        }

        public string Status
        {
            get
            {

                StringBuilder sb = new StringBuilder();
                sb.AppendLine("Kernel Statistics");
                sb.AppendLine(string.Format("Disk Reads: {0}", this.DiskReads));
                sb.AppendLine(string.Format("Disk Writes: {0}", this.DiskWrites));
                sb.AppendLine(string.Format("Virtual Reads: {0}", this.VirtualReads));
                sb.AppendLine(string.Format("Virtual Writes: {0}", this.VirtualWrites));
                sb.AppendLine("Shard Cache:");
                foreach (string x in this._ExtentBuffer.Keys)
                {
                    sb.AppendLine(x);
                }
                sb.AppendLine("ShartTable Cache:");
                foreach (string x in this._TableBuffer.Keys)
                {
                    sb.AppendLine(x);
                }
                return sb.ToString();

            }
        }

        public bool TableExists(string Path)
        {
            return File.Exists(Path) || this._TableBuffer.Exists(Path);
        }

        public void MarkTable(string Path, bool PriorityOne)
        {

            // Buffer the table //
            Table t = this.RequestBufferTable(Path);
            this._Logger.WriteLine("MarkTable {0}: {1}", this._Timer.Elapsed, Path);

            // Check if there are any extents to buffer //
            if (t.ExtentCount == 0)
                return;

            // Call all extents //
            int id = 0;
            while (id < t.ExtentCount)
            {

                // Check if the extent is in memory //
                Header h = t.RenderHeader(id);
                if (!this._ExtentBuffer.Exists(h.LookUpKey))
                {

                    Extent e = this.RequestBufferExtent(t, id);
                    this._Logger.WriteLine("MarkTable {0}: buffer extent {1}", this._Timer.Elapsed, id);

                }

            }

        }

        // Text dumping //
        public void TextDump(TabularData Data, string OutPath, char Delim, char Escape, Expressions.Filter Where, Expressions.Register Memory)
        {

            StreamWriter sw = new StreamWriter(OutPath);

            RecordReader stream = Data.CreateVolume().OpenReader(Memory, Where);

            sw.WriteLine(Data.Columns.ToNameString(Delim));

            while (!stream.EndOfData)
            {

                Record r = stream.ReadNext();
                string q = r.ToString(Delim, Escape);

                sw.WriteLine(q);

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

        internal static string RyeProjectsDir
        {
            get { return System.Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\Rye Projects\"; }
        }

        internal static string RyeTempDir
        {
            get { return Kernel.RyeProjectsDir + @"Temp\"; }
        }

        internal static string RyeFlatFilesDir
        {
            get { return Kernel.RyeProjectsDir + @"Flat Files\"; }
        }

        internal static string RyeScriptsDir
        {
            get { return Kernel.RyeProjectsDir + @"Scripts\"; }
        }

        internal static string RyeLogDir
        {
            get { return Kernel.RyeProjectsDir + @"Log Files\"; }
        }

        internal static void CheckDir()
        {

            // Temp Dir //
            if (!Directory.Exists(Kernel.RyeTempDir))
                Directory.CreateDirectory(Kernel.RyeTempDir);

            // Flat Files //
            if (!Directory.Exists(Kernel.RyeFlatFilesDir))
                Directory.CreateDirectory(Kernel.RyeFlatFilesDir);

            // Scripts //
            if (!Directory.Exists(Kernel.RyeScriptsDir))
                Directory.CreateDirectory(Kernel.RyeScriptsDir);

            // Log //
            if (!Directory.Exists(Kernel.RyeLogDir))
                Directory.CreateDirectory(Kernel.RyeLogDir);

        }

        internal static string KernelLogFilePath()
        {

            string Dir = Kernel.RyeLogDir;
            DateTime now = DateTime.Now;
            string Name = string.Format("Kernel_Log_{0}{1}{2}_{3}{4}{5}.txt", now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Millisecond);
            return Dir + Name;

        }

    }

    public abstract class DataSerializationProvider
    {

        public const long META_DATA_PAGE_SIZE = 1024 * 128;
        public const long META_DATA_PAGE_OFFSET = 0;
        public const long DEFAULT_PAGE_SIZE = Extent.DEFAULT_PAGE_SIZE;

        public int _Reads = 0;
        public int _Writes = 0;

        // Properties //
        public abstract int Version { get; }

        public abstract string Extension { get; }

        public int Reads
        {
            get { return this._Reads; }
        }

        public int Writes
        {
            get { return this._Writes; }
        }

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
            Data.PreSerialize();

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

            this._Writes++;

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

            this._Writes++;

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

            this._Reads++;

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

            this._Reads++;

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

    public sealed class TablixBuffer<T> where T : TabularData
    {

        /*
            67108864
            134217728
            268435456
            536870912
            1073741824
         */
        public const long MEM_1MB = 1048576;
        public const long MEM_64MB = 67108864;
        public const long MEM_128MB = 134217728;
        public const long MEM_256MB = 268435456;
        public const long MEM_512MB = 536870912;
        public const long MEM_1024MB = 1073741824;

        private long _MaxMemoryCapacity = MEM_128MB; // Bytes
        private long _CurrentMemory = 0;
        private Quack<string> _PriorityQueue = new Quack<string>(Quack<string>.QuackState.FIFO);
        private Dictionary<string, T> _Buffer = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
        private Dictionary<string, long> _MemoryValueCache = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        private Dictionary<string, int> _VersionCache = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        private int _Reads = 0;
        private int _Writes = 0;
        private int _Burns = 0;

        private void Allocate(T Data, int Version)
        {

            // Get the lookup key //
            string key = Data.Header.LookUpKey;

            // Check if this already has the key, only update the memory and the actual data //
            if (this.Exists(key))
            {
                this._CurrentMemory += (Data.MemCost - this._MemoryValueCache[key]);
                this._MemoryValueCache[key] = Data.MemCost;
                this._Buffer[key] = Data;
                this._VersionCache[key]++;
                return;
            }

            // Check if we have capacity, if not, then remove an element from the cache //
            if (!this.HasCapacity(Data.MemCost))
            {
                throw new Rye.Interpreter.RyeDataException("The extent buffer is at capacity");
            }

            // Add to the queue //
            this._PriorityQueue.Allocate(key);
            this._Writes++;

            // Add to the version buffer //
            this._VersionCache.Add(key, Version);

            // Add the buffer Value //
            this._Buffer.Add(key, Data);

            // Handle memory //
            this._MemoryValueCache.Add(key, Data.MemCost);
            this._CurrentMemory += Data.MemCost;

        }

        public void Allocate(T Data)
        {
            this.Allocate(Data, 0);
        }

        public void AllocateHard(T Data)
        {
            this.Allocate(Data, 1);
        }

        public void Deallocate(string Key)
        {

            // Remove the key //
            if (!this._Buffer.ContainsKey(Key))
                return;

            // Remove the buffer //
            this._Buffer.Remove(Key);
            
            // Remove from the memory count //
            this._CurrentMemory -= (this._MemoryValueCache[Key]);
            this._MemoryValueCache.Remove(Key);

            // Remove from the version stack //
            this._VersionCache.Remove(Key);

            // Remove from the quack //
            this._PriorityQueue.Remove(Key);
            this._Burns++;

        }

        public void DeallocateNext()
        {

            if (this._PriorityQueue.Count == 0)
                return;

            string key = this._PriorityQueue.Deallocate();

            this.Deallocate(key);

        }

        public void DeallocateChildren(Header H)
        {

            foreach(string key in this._PriorityQueue.ToCache)
            {

                T val = this._Buffer[key];
                if (val.Header.Path == H.Path)
                {
                    this.Deallocate(val.Header.LookUpKey);
                }

            }

        }

        public T Request(string Key)
        {
            if (!this._Buffer.ContainsKey(Key))
                throw new IndexOutOfRangeException(string.Format("Buffer does not contain '{0}'", Key));
            this._Reads++;
            return this._Buffer[Key];
        }

        public T RequestAndDeallocate(string Key)
        {
            T value = this.Request(Key);
            this.Deallocate(Key);
            return value;
        }

        public T RequestAndDeallocateNext(out int Version)
        {

            // Get the next key //
            string Key = this._PriorityQueue.Deallocate();
            
            // Get the table/extent //
            T value = this.Request(Key);

            // Get the version //
            Version = this.Version(Key);
            
            // Deallocate //
            this.Deallocate(Key);

            return value;

        }

        public T RequestAndDeallocateNext()
        {
            int ver = 0;
            return this.RequestAndDeallocateNext(out ver);
        }

        public bool Exists(string Key)
        {
            return this._Buffer.ContainsKey(Key);
        }

        public bool HasCapacity(long MemorySize)
        {
            return (this._CurrentMemory + MemorySize < this._MaxMemoryCapacity);
        }

        public bool CanEverHaveCapacity(long MemorySize)
        {
            return this._MaxMemoryCapacity > MemorySize;
        }

        public int Version(string Key)
        {
            if (this._VersionCache.ContainsKey(Key))
                return this._VersionCache[Key];
            return -1;
        }

        public int Reads
        {
            get { return this._Reads; }
        }

        public int Writes
        {
            get { return this._Writes; }
        }

        public int Burns
        {
            get { return this._Burns; }
        }

        public int Count
        {
            get
            {
                return this._Buffer.Count;
            }
        }

        public bool IsEmpty
        {
            get { return this._Buffer.Count == 0; }
        }

        public bool IsFull
        {
            get 
            {
                return this._CurrentMemory >= this._MaxMemoryCapacity;
            }
        }

        public List<string> Keys
        {
            get { return this._Buffer.Keys.ToList(); }
        }

        public void BurnBuffer()
        {

            this._Buffer = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
            this._PriorityQueue = new Quack<string>(Quack<string>.QuackState.FIFO);
            this._MemoryValueCache = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            this._CurrentMemory = 0;

        }

        public Queue<T> GetObjects()
        {
            return new Queue<T>(this._Buffer.Values);
        }


    }


}
