using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data
{

    public enum HeaderType : long
    {
        Extent,
        Table
    }

    public sealed class Header : Record
    {

        public const int OFFSET_NAME = 0;
        public const int OFFSET_ID = 1;
        public const int OFFSET_DIRECTORY = 2;
        public const int OFFSET_EXTENSION = 3;
        public const int OFFSET_COLUMN_COUNT = 4;
        public const int OFFSET_RECORD_COUNT = 5;
        public const int OFFSET_TIME_STAMP = 6;
        public const int OFFSET_KEY_COUNT = 7;
        public const int OFFSET_MAX_RECORDS = 8;
        public const int OFFSET_BIG_RECORD_COUNT = 9;
        public const int OFFSET_TYPE = 10;

        public const int RECORD_LEN = 11;

        private const char DOT = '.';
        internal const string DEFAULT_EXTENSION = "ryedat";
        private const char TYPE_E = 'E';
        private const char TYPE_T = 'T';
        
        // Constructor //
        public Header(string NewDirectory, string NewName, long NewID, long ColumnCount, long RecordCount, long KeyCount, long NewMaxCount, long NewExtentCount, HeaderType NewType)
            : base(RECORD_LEN)
        {

            // Fix the directory //
            if (NewDirectory != null)
            {
                if (NewDirectory.Last() != '\\') 
                    NewDirectory = NewDirectory.Trim() + '\\';
            }

            // Fix the name //
            if (Name.Contains(DOT) == true) 
                Name = Name.Split(DOT)[0].Trim();

            this[OFFSET_NAME] = new Cell(NewName);
            this[OFFSET_ID] = new Cell(NewID);
            this[OFFSET_DIRECTORY] = (NewDirectory == null ? Cell.NULL_STRING : new Cell(NewDirectory));
            this[OFFSET_EXTENSION] = new Cell(DEFAULT_EXTENSION);
            this[OFFSET_COLUMN_COUNT] = new Cell(ColumnCount);
            this[OFFSET_RECORD_COUNT] = new Cell(RecordCount);
            this[OFFSET_TIME_STAMP] = new Cell(DateTime.Now);
            this[OFFSET_KEY_COUNT] = new Cell(KeyCount);
            this[OFFSET_MAX_RECORDS] = new Cell(NewMaxCount);
            this[OFFSET_BIG_RECORD_COUNT] = new Cell(NewExtentCount);
            this[OFFSET_TYPE] = new Cell((long)NewType);

        }

        public Header(Record R)
            : base(RECORD_LEN)
        {
            if (R.Count != RECORD_LEN) throw new Exception("Header-Record supplied has an invalid length");
            this._data = R.BaseArray;
        }

        // Properties //
        public string Path
        {
            get
            {
                return this.Directory + this.Name + DOT + (this.Affinity == HeaderType.Extent ? TYPE_E : TYPE_T) + this.ID.ToString() + DOT + this.Extension;
            }
        }

        public string Name
        {
            get
            {
                return this[OFFSET_NAME].valueSTRING;
            }
            set
            {
                this[OFFSET_NAME] = new Cell(value);
            }
        }

        public long ID
        {
            get
            {
                return this[OFFSET_ID].INT;
            }
            set
            {
                this[OFFSET_ID] = new Cell(value);
            }
        }

        public string Directory
        {
            get
            {
                return this[OFFSET_DIRECTORY].valueSTRING;
            }
            set
            {
                this[OFFSET_DIRECTORY] = new Cell(value);
            }
        }

        public string Extension
        {
            get
            {
                return this[OFFSET_EXTENSION].valueSTRING;
            }
            set
            {
                this[OFFSET_EXTENSION] = new Cell(value);
            }
        }

        public long ColumnCount
        {
            get
            {
                return this[OFFSET_COLUMN_COUNT].INT;
            }
        }

        public long RecordCount
        {
            get
            {
                return this[OFFSET_RECORD_COUNT].INT;
            }
            set
            {
                this._data[OFFSET_RECORD_COUNT] = new Cell(value);
            }
        }

        public long BigRecordCount
        {
            get
            {
                return this._data[OFFSET_BIG_RECORD_COUNT].INT;
            }
            set
            {
                this._data[OFFSET_BIG_RECORD_COUNT].INT = value;
            }
        }

        public long KeyCount
        {
            get
            {
                return this[OFFSET_KEY_COUNT].INT;
            }
            set
            {
                this[OFFSET_KEY_COUNT] = new Cell(value);
            }
        }

        public DateTime TimeStamp
        {
            get
            {
                return this[OFFSET_TIME_STAMP].valueDATE_TIME;
            }
        }

        public long MaxRecordCount
        {
            get
            {
                return this[OFFSET_MAX_RECORDS].INT;
            }
            set
            {
                this[OFFSET_MAX_RECORDS] = new Cell(value);
            }
        }

        public HeaderType Affinity
        {
            get { return (HeaderType)this._data[OFFSET_TYPE].INT; }
        }

        public bool Exists
        {
            get
            {
                return System.IO.File.Exists(this.Path);
            }
        }

        public bool IsMemoryOnly
        {
            get { return this._data[OFFSET_DIRECTORY].IsNull; }
        }

        // Methods //
        public void Stamp()
        {
            this[OFFSET_TIME_STAMP] = new Cell(DateTime.Now);
        }

        public string ToMetaString()
        {

            StringBuilder sb = new StringBuilder();
            sb.AppendLine("Name: " + this.Name);
            sb.AppendLine("ID: " + this.ID);
            sb.AppendLine("Directory: " + this.Directory);
            sb.AppendLine("Extension: " + this.Extension);
            sb.AppendLine("Columns: " + this.ColumnCount.ToString());
            sb.AppendLine("Records: " + this.RecordCount.ToString());
            sb.AppendLine("Keys: " + this.KeyCount.ToString());
            sb.AppendLine("Timestamp: " + this.TimeStamp.ToString());
            sb.AppendLine("Type: Extent");
            return sb.ToString();
        }

        public bool IsMemberOf(Header H)
        {
            
            if (this.Affinity != HeaderType.Table)
                return false;
            if (H.Affinity != HeaderType.Extent)
                return false;

            // Check both the directory, name and extension match; don't care about ID or type //
            return (string.Compare(this.Directory, H.Directory, true) == 0) && (string.Compare(this.Name, H.Name, true) == 0) && (string.Compare(this.Extension, H.Extension) == 0);

        }

        // Statics //
        public static string FilePath(string Dir, string Name, HeaderType Affinity)
        {
            Header h = new Header(Dir.Trim(), Name, 0, 0, 0, 0, 0, 0, Affinity);
            return h.Path;
        }

        public static string TempName()
        {
            Guid g = Guid.NewGuid();
            return g.ToString().Replace("-", "");
        }

        public static Header NewExtentHeader(string Dir, string Name, long ID, int ColumnCount, long MaxCount)
        {
            return new Header(Dir, Name, ID, (long)ColumnCount, 0, 0, MaxCount, 1, HeaderType.Extent);
        }

        public static Header NewMemoryOnlyExtentHeader(string Name, int ColumnCount, long MaxCount)
        {
            return new Header(null, Name, 0, ColumnCount, 0, 0, MaxCount, 0, HeaderType.Extent);
        }

        public static Header NewTableHeader(string Dir, string Name, int ColumnCount, long MaxCount)
        {
            return new Header(Dir, Name, 0, (long)ColumnCount, 0, 0, (long)MaxCount, 0, HeaderType.Table);
        }

    }




}
