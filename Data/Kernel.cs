using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Rye.Helpers;

namespace Rye.Data
{
    
    public static class Kernel
    {

        private static int _DISK_READS = 0;
        private static int _DISK_WRITES = 0;
        private static int _VIRTUAL_READS = 0;
        private static int _VIRTUAL_WRITES = 0;

        private static Dictionary<string, Extent> _ExtentCache = new Dictionary<string,Extent>(StringComparer.OrdinalIgnoreCase);
        private static Dictionary<string, Table> _TableCache = new Dictionary<string, Table>(StringComparer.OrdinalIgnoreCase);
        private static Quack<string> _PriorityCache = new Quack<string>(Quack<string>.QuackState.LIFO);

        private static string _TempDir = null;

        // Meta Data //
        public static int DiskReads
        {
            get { return _DISK_READS; }
        }

        public static int DiskWrites
        {
            get { return _DISK_WRITES; }
        }

        public static int VirtualReads
        {
            get { return Kernel._VIRTUAL_READS; }
        }

        public static int VirtualWrites
        {
            get { return Kernel._VIRTUAL_WRITES; }
        }

        public static long MaxMemory
        {
            get
            {
                return Kernel._MaxMemory;
            }
            set
            {
                if (value < 0)
                    Kernel._MaxMemory = 0;
                else
                    Kernel._MaxMemory = value;
            }
        }

        public static long MaxMemoryKB
        {
            get
            {
                return Kernel.MaxMemory / 1024;
            }
            set
            {
                Kernel.MaxMemory = value * 1024;
            }
        }

        public static long MaxMemoryMB
        {
            get { return Kernel.MaxMemory / 1024 / 1024; }
            set { Kernel.MaxMemory = value * 1024 * 1024; }
        }

        public static int MaxThreadCount
        {
            get { return Environment.ProcessorCount; }
        }

        public static string TempDirectory
        {
            get { return Kernel._TempDir; }
            set { Kernel._TempDir = value; }
        }

        // Public Methods //
        public static void RequestFlushExtent(Extent E)
        {

            if (Kernel.CanExceptExtent(E))
            {
                Kernel.VirtualFlushExtent(E);
                return;
            }

            Kernel.FlushExtentSafeVersion1(E);

        }

        public static void RequestFlushTable(Table T)
        {

            if (Kernel.CanExceptTable(T))
            {
                Kernel.VirtualFlushTable(T);
                return;
            }

            Kernel.FlushTableSafeVersion1(T);

        }

        public static Extent RequestBufferExtent(string Path)
        {

            if (Kernel.IsExtentCached(Path))
            {
                return Kernel.VirtualBufferExtent(Path);
            }

            Extent e = Kernel.BufferExtent(Path);

            if (Kernel.CanExceptExtent(e))
                Kernel.VirtualFlushExtent(e);

            return e;

        }

        public static Table RequestBufferTable(string Path)
        {

            if (Kernel.IsTableCached(Path))
            {
                return Kernel.VirtualBufferTable(Path);
            }

            Table t = Kernel.BufferTable(Path);

            if (Kernel.CanExceptTable(t))
                Kernel.VirtualFlushTable(t);

            return t;

        }

        public static void RequestDropTable(string Path)
        {

            // Remove from disk //
            Table t;
            if (File.Exists(Path))
            {
                t = RequestBufferTable(Path);
            }
            else if (Kernel.IsTableCached(Path))
            {
                t = Kernel.VirtualBufferTable(Path);
            }
            else
            {
                return;
            }

            foreach (Header h in t.Headers)
            {

                if (File.Exists(h.Path))
                    File.Delete(h.Path);

                if (Kernel.IsExtentCached(h.Path))
                    Kernel.ReleaseExtent(h.Path, false);

            }

            if (File.Exists(t.Header.Path))
                File.Delete(t.Header.Path);

            if (Kernel.IsTableCached(t.Header.Path))
                File.Delete(t.Header.Path);

            foreach (Extent e in Kernel._ExtentCache.Values)
            {
                if (t.Header.IsMemberOf(e.Header))
                    Kernel.ReleaseExtent(e.Header.Path, false);
            }


        }
        
        public static void FlushCache()
        {

            foreach (KeyValuePair<string, Extent> kv in Kernel._ExtentCache)
            {
                Kernel.FlushExtentSafeVersion1(kv.Value);
            }
            foreach (KeyValuePair<string, Table> kv in Kernel._TableCache)
            {
                Kernel.FlushTableSafeVersion1(kv.Value);
            }

        }

        public static void ClearCache()
        {
            Kernel._ExtentCache = new Dictionary<string, Extent>(StringComparer.OrdinalIgnoreCase);
            Kernel._TableCache = new Dictionary<string, Table>(StringComparer.OrdinalIgnoreCase);
            Kernel._CurrentMemory = 0;
        }

        public static void FlushAndClearCache()
        {
            Kernel.FlushCache();
            Kernel.ClearCache();
        }

        public static void ShutDown()
        {
            Kernel.FlushAndClearCache();
        }

        public static string Status
        {
            get
            {

                StringBuilder sb = new StringBuilder();
                sb.AppendLine("Kernel Statistics");
                sb.AppendLine(string.Format("Disk Reads: {0}", Kernel.DiskReads));
                sb.AppendLine(string.Format("Disk Writes: {0}", Kernel.DiskWrites));
                sb.AppendLine(string.Format("Virtual Reads: {0}", Kernel._VIRTUAL_READS));
                sb.AppendLine(string.Format("Virtual Writes: {0}", Kernel._VIRTUAL_WRITES));
                sb.AppendLine("Extent Cache:");
                foreach (KeyValuePair<string, Extent> kv in Kernel._ExtentCache)
                {
                    sb.AppendLine(kv.Key);
                }
                sb.AppendLine("Table Cache:");
                foreach (KeyValuePair<string, Table> kv in Kernel._TableCache)
                {
                    sb.AppendLine(kv.Key);
                }
                return sb.ToString();

            }
        }

        #region IO_Manager

        private static long _MaxMemory = 256 * 1024 * 1024; // 256 mb
        private static long _CurrentMemory = 0;
        private static int _ExtentCacheLimit = 16;
        private static int _TableCacheLimit = 16;

        // ----------------------- Extents ----------------------- //
        private static bool IsExtentCached(string Path)
        {
            return Kernel._ExtentCache.ContainsKey(Path);
        }

        private static bool CanExceptExtent(Extent Data)
        {

            // Extent is already cached //
            if (Kernel._ExtentCache.ContainsKey(Data.Header.Path))
                return true;

            // We are over the cache the limit //
            if (Kernel._ExtentCache.Count >= Kernel._ExtentCacheLimit)
                return false;

            // We are over the memory limit //
            if (Kernel._MaxMemory < Kernel._CurrentMemory + Data.MemCost)
                return false;

            // Otherwise we can accept it //
            return true;

        }

        private static void VirtualFlushExtent(Extent Data)
        {

            if (Kernel._ExtentCache.ContainsKey(Data.Header.Path))
            {
                Kernel._CurrentMemory += (Data.MemCost - Kernel._ExtentCache[Data.Header.Path].MemCost);
                Kernel._ExtentCache[Data.Header.Path] = Data;
                Kernel._VIRTUAL_WRITES++;
            }
            else
            {
                Kernel._CurrentMemory += Data.MemCost;
                Kernel._ExtentCache.Add(Data.Header.Path, Data);
                Kernel._VIRTUAL_WRITES++;
            }

        }

        private static Extent VirtualBufferExtent(string Path)
        {

            Kernel._VIRTUAL_READS++;
            return Kernel._ExtentCache[Path];
            
        }

        private static void ReleaseExtent(string Path, bool Flush)
        {

            if (!Kernel.IsExtentCached(Path))
                return;

            if (Flush)
                Kernel.FlushExtentSafeVersion1(Kernel.VirtualBufferExtent(Path));

            Kernel._CurrentMemory -= Kernel.VirtualBufferExtent(Path).MemCost;

            Kernel._ExtentCache.Remove(Path);

        }

        private static void ReleaseAllExtents(bool Flush)
        {

            List<string> paths = Kernel._ExtentCache.Keys.ToList();

            foreach (string path in paths)
            {
                Kernel.ReleaseExtent(path, Flush);
            }

        }

        // ----------------------- Tables ----------------------- //
        private static bool IsTableCached(string Path)
        {
            return Kernel._TableCache.ContainsKey(Path);
        }

        private static bool CanExceptTable(Table Data)
        {
            if (Kernel._TableCache.ContainsKey(Data.Header.Path))
                return true;
            if (Kernel._TableCache.Count >= Kernel._TableCacheLimit)
                return false;
            if (Kernel._MaxMemory < Kernel._CurrentMemory + Data.MemCost)
                return false;
            return true;
        }

        private static void VirtualFlushTable(Table Data)
        {

            if (Kernel._TableCache.ContainsKey(Data.Header.Path))
            {
                Kernel._CurrentMemory += (Data.MemCost - Kernel._TableCache[Data.Header.Path].MemCost);
                Kernel._TableCache[Data.Header.Path] = Data;
                Kernel._VIRTUAL_WRITES++;
            }
            else
            {
                Kernel._CurrentMemory += Data.MemCost;
                Kernel._TableCache.Add(Data.Header.Path, Data);
                Kernel._VIRTUAL_WRITES++;
            }

        }

        private static Table VirtualBufferTable(string Path)
        {

            Kernel._VIRTUAL_READS++;
            return Kernel._TableCache[Path];

        }

        private static void ReleaseTable(string Path, bool Flush)
        {

            if (!Kernel.IsTableCached(Path))
                return;

            if (Flush)
                Kernel.FlushTableSafeVersion1(Kernel.VirtualBufferTable(Path));

            Kernel._CurrentMemory -= Kernel.VirtualBufferTable(Path).MemCost;

            Kernel._TableCache.Remove(Path);

        }

        private static void ReleaseAllTables(bool Flush)
        {

            List<string> paths = Kernel._TableCache.Keys.ToList();

            foreach (string path in paths)
            {
                Kernel.ReleaseTable(path, Flush);
            }

        }

        #endregion

        #region Version1_Serialization

        /*
         * Binary serialization:
         * Cells: affinity, nullness, data (if not null)
         * Records: record count as int, each cell
         * RecordSets:
         *      -- Header
         *      -- Columns (Header has the correct count)
         *      -- Sort key (Header has the correct count)
         *      -- Records (Header has the correct count)
         * 
         */

        private const int HEADER_LEN = 11;
        private const int SCHEMA_LEN = 4;
        private const int TABLE_LEN = 2;
        private const byte VERSION1 = 1;
        private const byte STATE1 = 0;
        private const int VERSION_LOC = 0;
        private const int STATE_LOC = 1;
        private const int LOCATION_BEGIN = 2;
        private const int META_SIZE = 2;

        // Version 1 Serialization - Writes //
        private static int WriteCellSafeVersion1(byte[] Mem, int Location, Cell C)
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

        private static int WriteRecordSafeVersion1(byte[] Mem, int Location, Record R)
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

        private static int WriteRecordsSafeVersion1(byte[] Mem, int Location, List<Record> Cache)
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

        private static int WriteExtentSafeVersion1(byte[] Mem, int Location, Extent Data)
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
            Location = Kernel.WriteRecordSafeVersion1(Mem, Location, Data.Header);
            //Console.WriteLine("Header Terminator: {0}", Location);

            // Write columns //
            Location = Kernel.WriteRecordsSafeVersion1(Mem, Location, Data.Columns._Cache);
            //Console.WriteLine("Schema Terminator: {0}", Location);

            // Write sort key //
            Location = Kernel.WriteRecordSafeVersion1(Mem, Location, Data.SortBy.ToRecord());
            //Console.WriteLine("Keys Terminator: {0}", Location);

            // Write cache //
            Location = Kernel.WriteRecordsSafeVersion1(Mem, Location, Data.Cache);
            //Console.WriteLine("Data Terminator: {0}", Location);

            return Location;

        }

        private static int WriteTableSafeVersion1(byte[] Mem, int Location, Table Data)
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
            Location = Kernel.WriteRecordSafeVersion1(Mem, Location, Data.Header);
            //Console.WriteLine("Header Terminator: {0}", Location);

            // Write columns //
            Location = Kernel.WriteRecordsSafeVersion1(Mem, Location, Data.Columns._Cache);
            //Console.WriteLine("Schema Terminator: {0}", Location);

            // Write sort key //
            Location = Kernel.WriteRecordSafeVersion1(Mem, Location, Data.SortBy.ToRecord());
            //Console.WriteLine("Keys Terminator: {0}", Location);

            // Write cache //
            Location = Kernel.WriteRecordsSafeVersion1(Mem, Location, Data.ReferenceTable.Cache);
            //Console.WriteLine("Data Terminator: {0}", Location);

            return Location;

        }

        private static void FlushExtentSafeVersion1(Extent Data)
        {

            // Check if attached //
            if (Data.IsMemoryOnly)
                throw new Exception("Extent passed is a memory only set");

            // Update the header //
            Data.PreSerialize();

            // Estimate the size //
            int size = Data.DiskCost + META_SIZE;

            // Memory stack //
            byte[] memory = new byte[size];

            // Write the meta data //
            memory[VERSION_LOC] = VERSION1;
            memory[STATE_LOC] = STATE1;

            // Write the data to memory //
            int location = Kernel.WriteExtentSafeVersion1(memory, LOCATION_BEGIN, Data);

            // Create a file stream //
            using (FileStream fs = File.Create(Data.Header.Path, size))
            {
                fs.Write(memory, 0, location);
            }

            // Increment writes //
            _DISK_WRITES++;

        }

        private static void FlushTableSafeVersion1(Table Data)
        {

            // Update the header //
            Data.PreSerialize();

            // Get the size //
            int size = Data.DiskCost + META_SIZE;

            // Memory //
            byte[] memory = new byte[size];

            // Write the meta data //
            memory[VERSION_LOC] = VERSION1;
            memory[STATE_LOC] = STATE1;

            // Create a file stream //
            using (FileStream fs = File.Create(Data.Header.Path))
            {
                int location = Kernel.WriteTableSafeVersion1(memory, LOCATION_BEGIN, Data);
                fs.Write(memory, 0, location);
            }

            // Increment writes //
            _DISK_WRITES++;

        }

        // Version 1 Serialization - Reads //
        private static int ReadCellSafeVersion1(byte[] Mem, int Location, out Cell C)
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

        private static int ReadRecordSafeVersion1(byte[] Mem, int Location, int Length, out Record Datum)
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

        private static int ReadRecordsSafeVersion1(byte[] Mem, int Location, long Count, int Length, List<Record> Cache)
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

        private static int ReadHeadersSafeVersion1(byte[] Mem, int Location, long Count, int Length, List<Header> Cache)
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

        private static Extent ReadRecordSetSafeVersion1(byte[] Mem, int Location)
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
            Location = ReadRecordSafeVersion1(Mem, Location, HEADER_LEN, out rh);
            Header h = new Header(rh);

            // Read schema //
            List<Record> s_cache = new List<Record>();
            Location = Kernel.ReadRecordsSafeVersion1(Mem, Location, h.ColumnCount, SCHEMA_LEN, s_cache);
            Schema s = new Schema(s_cache);

            // Read key //
            Record rk;
            Location = ReadRecordSafeVersion1(Mem, Location, (int)h.KeyCount, out rk);
            Key k = new Key(rk);

            // Read record cache //
            List<Record> d_cache = new List<Record>();
            Location = Kernel.ReadRecordsSafeVersion1(Mem, Location, (int)h.RecordCount, (int)h.ColumnCount, d_cache);

            // Return recordset //
            return new Extent(s, h, d_cache, k);

        }

        private static Table ReadTableSafeVersion1(byte[] Mem, int Location)
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
            Location = ReadRecordSafeVersion1(Mem, Location, 11, out rh);
            Header h = new Header(rh);

            // Read schema //
            List<Record> s_cache = new List<Record>();
            Location = Kernel.ReadRecordsSafeVersion1(Mem, Location, h.ColumnCount, 4, s_cache);
            Schema s = new Schema(s_cache);

            // Read key //
            Record rk;
            Location = ReadRecordSafeVersion1(Mem, Location, (int)h.KeyCount, out rk);
            Key k = new Key(rk);

            // Read record cache //
            List<Record> d_cache = new List<Record>();
            Location = Kernel.ReadRecordsSafeVersion1(Mem, Location, (int)h.RecordCount, 2, d_cache);

            // Return recordset //
            return new Table(h, s, d_cache, k);

        }

        private static Extent BufferExtent(string FullPath)
        {

            // Open a stream //
            byte[] b = File.ReadAllBytes(FullPath);

            // Get the version Bytes //
            byte version = b[VERSION_LOC];
            byte state = b[STATE_LOC];

            // Check the version //
            if (version != VERSION1)
                throw new InvalidDataException(string.Format("The table passed is encoded with version {0} but only version {1} serialization is available", version, VERSION1));

            // Buffer Data //
            Extent e = ReadRecordSetSafeVersion1(b, LOCATION_BEGIN);

            // Increment Reads //
            _DISK_READS++;

            return e;

        }

        private static Table BufferTable(string FullPath)
        {

            // Open a stream //
            byte[] b = File.ReadAllBytes(FullPath);

            // Get the version Bytes //
            byte version = b[VERSION_LOC];
            byte state = b[STATE_LOC];

            // Check the version //
            if (version != VERSION1)
                throw new InvalidDataException(string.Format("The table passed is encoded with version {0} but only version {1} serialization is available", version, VERSION1));

            Table t = Kernel.ReadTableSafeVersion1(b, LOCATION_BEGIN);

            // Increment Reads //
            _DISK_READS++;

            return t;

        }

        #endregion

        public static void TextDump(DataSet Data, string OutPath, char Delim, char Escape, Expressions.Filter Where, Expressions.Register Memory)
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

        public static void TextDump(DataSet Data, string OutPath, char Delim, Expressions.Filter Where, Expressions.Register Memory)
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

        public static void TextDump(DataSet Data, string OutPath, char Delim, char Escape)
        {
            Kernel.TextDump(Data, OutPath, Delim, Escape, Expressions.Filter.TrueForAll, new Expressions.Register("THIS_DOESN'T MATTER!", Data.Columns));
        }

        public static void TextDump(DataSet Data, string OutPath, char Delim)
        {
            Kernel.TextDump(Data, OutPath, Delim, Expressions.Filter.TrueForAll, new Expressions.Register("THIS_DOESN'T MATTER!", Data.Columns));
        }

        public static void TextPop(DataSet Data, string InPath, char[] Delim, char Escape, int Skip)
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

    public class Quack<T>
    {

        public enum QuackState
        {
            LIFO,
            FIFO
        }

        private LinkedList<T> _Cache;

        public Quack(QuackState NewState)
        {
            this._Cache = new LinkedList<T>();
            this.State = NewState;
        }

        public QuackState State
        {
            get;
            set;
        }

        public void Allocate(T Value)
        {

            // Append to the begining, which is akin to Stack.Push //
            if (this.State == QuackState.FIFO)
            {
                this._Cache.AddFirst(Value);
            }
            // Otherwise, this is Queue.Enqueue
            else
            {
                this._Cache.AddLast(Value);
            }

        }

        public T Deallocate()
        {

            if (this.State == QuackState.LIFO)
            {
                T v = this._Cache.First.Value;
                this._Cache.RemoveFirst();
                return v;
            }
            else
            {
                T v = this._Cache.Last.Value;
                this._Cache.RemoveLast();
                return v;
            }

        }


    }

}
