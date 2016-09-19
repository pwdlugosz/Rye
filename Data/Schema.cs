using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data
{

    /// <summary>
    /// Represents a collection of Name, Type, Type Size, and Nullness; this forms the basis of horse structured datasets
    /// </summary>
    public sealed class Schema
    {

        public const char SCHEMA_DELIM = ',';
        public const char ALIAS_DELIM = '.';
        public const int OFFSET_NAME = 0;
        public const int OFFSET_AFFINITY = 1;
        public const int OFFSET_NULL = 2;
        public const int OFFSET_SIZE = 3;
        public const int RECORD_LEN = 4;
        public const int MAX_COLUMNS = 1024;

        /* Sizing variables:
         * -- Represent the maxium size of the data on disk, not in memory
         * -- Each element takes up two meta data bytes
         *      -- Affinity
         *      -- Null bit
         * -- Bools (BOOL) take up 1 byte
         * -- Longs (INT), Dates (DATE), floating points (DOUBLE) all take up 8 bytes
         * -- Strings take up 4 + 2 x n bytes of data (4 == length, each n takes up 2 bytes)
         * -- Blobs take up 4 + n bytes of data
         * 
         */
        internal const int MAX_VARIABLE_SIZE = 8192; // 8k
        internal const int DEFAULT_STRING_SIZE = 64;
        internal const int DEFAULT_BLOB_SIZE = 16; // 256 bit

        internal List<Record> _Cache;
        private int _HashCode = int.MaxValue ^ (int)short.MaxValue;

        // Schema //
        /// <summary>
        /// Initializes an empty schema
        /// </summary>
        public Schema()
        {
            this._Cache = new List<Record>();
        }

        /// <summary>
        /// Initializes a schema given a set of recors; note: this is designed only to be called by the serializer.
        /// </summary>
        /// <param name="Cache">A list of records, each with four elements</param>
        public Schema(List<Record> Cache)
            : this()
        {
            for (int i = 0; i < Cache.Count; i++)
            {
                if (Cache[i].Count == RECORD_LEN)
                {
                    this._Cache.Add(Cache[i]);
                    this._HashCode += Cache[i].GetHashCode(new Key(1, 2)) * this.Count;
                }
            }
        }

        /// <summary>
        /// Initializes a schema based on text passed
        /// </summary>
        /// <param name="Text">A string representing the schema</param>
        public Schema(string Text)
            : this()
        {
            string[] s = Text.Split(SCHEMA_DELIM);
            foreach (string t in s)
            {
                this.Add(t);
            }
        }

        // Properties //
        /// <summary>
        /// Returns the count of columns in the schema
        /// </summary>
        public int Count
        {
            get
            {
                return this._Cache.Count;
            }
        }

        /// <summary>
        /// Returns a record filled with null cell elements based on the current schema
        /// </summary>
        public Record NullRecord
        {
            get
            {
                List<Cell> c = new List<Cell>();
                for (int i = 0; i < this.Count; i++)
                {
                    c.Add(new Cell(this.ColumnAffinity(i)));
                }
                return new Record(c.ToArray());
            }
        }

        public int RecordDataCost
        {
            get
            {
                int Size = 0;
                for (int i = 0; i < this.Count; i++)
                {
                    CellAffinity ca = this.ColumnAffinity(i);
                    if (ca == CellAffinity.STRING)
                    {
                        Size += 2 * this.ColumnSize(i);
                    }
                    else if (ca == CellAffinity.BLOB)
                    {
                        Size += this.ColumnSize(i);
                    }
                    else
                    {
                        Size += this.ColumnSize(i);
                    }
                }
                return Size;
            }
        }

        public int RecordMemCost
        {
            get
            {
                int Size = 0; // the cost of a list //
                for (int i = 0; i < this.Count; i++)
                {
                    CellAffinity ca = this.ColumnAffinity(i);
                    if (ca == CellAffinity.STRING)
                    {
                        Size += 22 + 16 + 2 * this.ColumnSize(i);
                    }
                    else if (ca == CellAffinity.BLOB)
                    {
                        Size += 4 + 16 + this.ColumnSize(i);
                    }
                    else
                    {
                        Size += 16;
                    }
                }
                return Size;
            }
        }

        public int RecordDiskCost
        {
            get
            {
                int Size = 2 * this._Cache.Count; // one byte for nullness, one for affinity //
                for (int i = 0; i < this.Count; i++)
                {

                    CellAffinity ca = this.ColumnAffinity(i);
                    if (ca == CellAffinity.STRING)
                    {
                        Size += 4 + 2 * this.ColumnSize(i);
                    }
                    else if (ca == CellAffinity.BLOB)
                    {
                        Size += 4 + this.ColumnSize(i);
                    }
                    else if (ca == CellAffinity.BOOL)
                    {
                        Size += 1;
                    }
                    else // INT, DOUBLE, DATE //
                    {
                        Size += 8;
                    }

                }
                return Size;
            }
        }

        public int DataCost
        {

            get 
            { 
                return Schema.SchemaSchema().RecordDataCost * this.Count; 
            }

        }

        public int MemCost
        {

            get 
            { 
                return Schema.SchemaSchema().RecordMemCost * this.Count; 
            }

        }

        public int DiskCost
        {

            get 
            { 
                return Schema.SchemaSchema().RecordDiskCost * this.Count; 
            }

        }

        // Property Functions //
        /// <summary>
        /// Finds the index of the column in the schema; the seek is not case sensative
        /// </summary>
        /// <param name="Name">A column name</param>
        /// <returns>An integer index of the column; -1 if the column does not exist</returns>
        public int ColumnIndex(string Name)
        {
            int i = 0;
            string t = Name.Trim();
            foreach (Record r in this._Cache)
            {
                if (string.Compare(t, r[OFFSET_NAME].valueSTRING, true) == 0) return i;
                i++;
            }
            return -1;
        }

        /// <summary>
        /// Gets a column name given an index
        /// </summary>
        /// <param name="Index">A column's index in the schema</param>
        /// <returns>A column</returns>
        public string ColumnName(int Index)
        {
            if (Index < 0 || Index >= this.Count)
                throw new Exception("Index supplied is invalid: " + Index.ToString() + " : " + this.Count.ToString());
            return this._Cache[Index][OFFSET_NAME].valueSTRING;
        }

        /// <summary>
        /// Checks to see if the column exists
        /// </summary>
        /// <param name="Name">A column name</param>
        /// <returns>True if the column exists; false if the column does not</returns>
        public bool Contains(string Name)
        {
            return this.ColumnIndex(Name) != -1;
        }

        /// <summary>
        /// Gets a column affinity given given an index
        /// </summary>
        /// <param name="Index">A column's index in the schema</param>
        /// <returns>A cell affinity</returns>
        public CellAffinity ColumnAffinity(int Index)
        {
            if (Index < 0 || Index >= this.Count)
                throw new Exception("Index supplied is invalid: " + Index.ToString());
            return (CellAffinity)this._Cache[Index][OFFSET_AFFINITY].INT;
        }

        /// <summary>
        /// Gets a column's affinity
        /// </summary>
        /// <param name="Name">A column name</param>
        /// <returns>A cell affinity</returns>
        public CellAffinity ColumnAffinity(string Name)
        {
            return this.ColumnAffinity(this.ColumnIndex(Name));
        }

        /// <summary>
        /// Gets a column nullness given given an index
        /// </summary>
        /// <param name="Index">A column's index in the schema</param>
        /// <returns>True if the column can be nulled, false if it cant</returns>
        public bool ColumnNull(int Index)
        {
            if (Index < 0 || Index >= this.Count)
                throw new Exception("Index supplied is invalid: " + Index.ToString());
            return this._Cache[Index][OFFSET_NULL].valueBOOL;
        }

        /// <summary>
        /// Gets a column's nullness
        /// </summary>
        /// <param name="Name">A column name</param>
        /// <returns>True if the column can be nulled, false otherwise</returns>
        public bool ColumnNull(string Name)
        {
            return this.ColumnNull(this.ColumnIndex(Name));
        }

        /// <summary>
        /// Gets a column size given given an index
        /// </summary>
        /// <param name="Index">A column's index in the schema</param>
        /// <returns>The column's type's size</returns>
        public int ColumnSize(int Index)
        {
            if (Index < 0 || Index >= this.Count)
                throw new Exception("Index supplied is invalid: " + Index.ToString());
            return (int)this._Cache[Index][OFFSET_SIZE].INT;
        }

        /// <summary>
        /// Gets a column's size
        /// </summary>
        /// <param name="Name">A column name</param>
        /// <returns>The column's type's size</returns>
        public int ColumnSize(string Name)
        {
            return this.ColumnSize(this.ColumnIndex(Name));
        }

        // Adds //
        /// <summary>
        /// Adds a column to the schema; will throw an exception if a column name passed already exists in the schema
        /// </summary>
        /// <param name="Name">The column name</param>
        /// <param name="Affinity">The column affinity</param>
        /// <param name="Nullable">A boolean, true means the column can be nulls, false means the column cannot be null</param>
        /// <param name="Size">The size in bytes; this will be ignored if the affinity is not variable (not string or blob)</param>
        public void Add(string Name, CellAffinity Affinity, bool Nullable, int Size)
        {

            // Check if exists //
            if (this.Contains(Name))
                throw new Exception("Column already exists: " + Name);

            // Check for capacity //
            if (this.Count >= MAX_COLUMNS)
                throw new Exception("Schema cannot accept any more columns");

            // Get the size //
            Size = FixSize(Affinity, Size);

            // Build record //
            Record r = Record.Stitch(new Cell(Name), new Cell((byte)Affinity), new Cell(Nullable), new Cell(Size));

            // Accumulate record //
            this._Cache.Add(r);

            // Hash code //
            this._HashCode += r.GetHashCode(new Key(1, 2)) * this.Count;

        }

        /// <summary>
        /// Adds a column to the schema; will throw an exception if a column name passed already exists in the schema; assumes the column is nullable
        /// </summary>
        /// <param name="Name">The column name</param>
        /// <param name="Affinity">The column affinity</param>
        /// <param name="Size">The size in bytes; this will be ignored if the affinity is not variable (not string or blob)</param>
        public void Add(string Name, CellAffinity Affinity, int Size)
        {
            this.Add(Name, Affinity, false, Size);
        }

        /// <summary>
        /// Adds a column to the schema; will throw an exception if a column name passed already exists in the schema; assumes the column is nullable; assumes a default type size
        /// </summary>
        /// <param name="Name">The column name</param>
        /// <param name="Affinity">The column affinity</param>
        public void Add(string Name, CellAffinity Affinity)
        {
            this.Add(Name, Affinity, true, -1);
        }

        /// <summary>
        /// Adds a columns based on a text expression
        /// </summary>
        /// <param name="Expression">A text expression [Name] [Type].[Size] [Nullable]</param>
        public void Add(string Expression)
        {

            /*
             * <NAME> <TYPE> (<'.'> <INT>)? (<BOOL>)?
             * 
             * Len 2 = name, type
             * Len 3 = name, type, size
             * Len 4 = name, table, size, bool
             * 
             */

            // Split //
            string[] s = Expression.Trim().Split(' ', '.');

            // Check length //
            if (s.Length < 2 || s.Length > 5)
                throw new Exception("Expression is invalid: " + Expression);

            // Set defaults //
            string name = s[0].Trim();
            CellAffinity ca = CellAffinityHelper.Parse(s[1].ToUpper().Trim());
            bool n = true;
            int Size = -1;

            // nullability //
            if (s.Length == 3)
            {
                Size = int.Parse(s[2]);
            }
            else if (s.Length == 4)
            {
                Size = int.Parse(s[2]);
                if (s[3].Trim().ToUpper() == "FALSE")
                    n = false;
            }

            // Accumulate the value //
            this.Add(name, ca, n, Size);

        }

        // Special methods //
        /// <summary>
        /// Renames a column to another name
        /// </summary>
        /// <param name="OldName">The current name</param>
        /// <param name="NewName">The proposed name</param>
        public void Rename(string OldName, string NewName)
        {
            if (this.Contains(NewName))
                throw new Exception("Rename-Column already exists: " + NewName);
            if (!this.Contains(OldName))
                throw new Exception("Rename-Column does not exist: " + OldName);
            int i = this.ColumnIndex(OldName);
            this._Cache[i][OFFSET_NAME] = new Cell(NewName);
        }

        /// <summary>
        /// Checks a record for nullness, type affinity, size overflow, and field count
        /// </summary>
        /// <param name="R">A record to check</param>
        /// <param name="FixAffinity">If true, the method will cast an invalid affinity cell</param>
        /// <returns>A boolean describing whether or not the check fails</returns>
        public bool Check(Record R, bool FixAffinity)
        {

            // Check length //
            if (R.Count != this.Count)
                throw new Exception(String.Format("Record size {0} does not match schema size {1}", R.Count, this.Count));

            // Check nullness and affinity //
            for (int i = 0; i < R.Count; i++)
            {

                // Check affinity //
                if (R[i].Affinity != this.ColumnAffinity(i) && !FixAffinity)
                    throw new Exception(String.Format("Column Affinities do not match {0} : {1} != {2}", i, R[i].Affinity, this.ColumnAffinity(i)));
                else if (R[i].Affinity != this.ColumnAffinity(i))
                    R[i] = Cell.Cast(R[i], this.ColumnAffinity(i));
                else if (this.ColumnSize(i) <= R[i].DataCost && this.ColumnAffinity(i) == CellAffinity.STRING)
                    R._data[i].STRING = R[i].STRING.Substring(0, this.ColumnSize(i));
                else if (this.ColumnSize(i) <= R[i].DataCost && this.ColumnAffinity(i) == CellAffinity.BLOB)
                {
                    Array.Resize(ref R._data[i].BLOB, this.ColumnSize(i));
                }

                // Check nullness //
                if (!this.ColumnNull(i) && R[i].IsNull)
                    throw new Exception(String.Format("Column {0} is null", i));

            }

            return true;
        }

        /// <summary>
        /// Checks a record for nullness, type affinity, size overflow, and field count; turns off auto-casting
        /// </summary>
        /// <param name="R">A record to check</param>
        /// <returns>A boolean describing whether or not the check fails</returns>
        public bool Check(Record R)
        {
            return this.Check(R, false);
        }

        /// <summary>
        /// Checks a record for nullness, type affinity, size overflow, and field count; wont throw an exception
        /// </summary>
        /// <param name="R">A record to check</param>
        /// <returns>A boolean describing whether or not the check fails</returns>
        public bool TryCheck(Record R)
        {

            try
            {
                return this.Check(R);
            }
            catch
            {
                return false;
            }

        }

        /// <summary>
        /// Fixing any affinity issues by auto casting the cell type to the correct type based on the column
        /// </summary>
        /// <param name="R">A record to check</param>
        /// <returns>A record</returns>
        public Record Fix(Record R)
        {

            // Check nullness and affinity //
            for (int i = 0; i < R.Count; i++)
            {

                // Cast to a new affinity if need by //
                if (R[i].Affinity != this.ColumnAffinity(i))
                    R[i] = Cell.Cast(R[i], this.ColumnAffinity(i));

            }

            return R;

        }

        /// <summary>
        /// Parses a string into a key
        /// </summary>
        /// <param name="Text">The text list of columns</param>
        /// <returns>A key</returns>
        public Key KeyParse(string Text)
        {

            Key k = new Key();

            if (Text == "*")
                return Key.Build(this.Count);

            string[] t = Text.Split(',');

            foreach (string s in t)
            {

                // Parse out the 'NAME KEY_AFFINITY' logic //
                string[] u = s.Trim().Split(' ');
                string v = u[0]; // column name
                string w = "A"; // affinity (Optional)
                if (u.Length > 1)
                    w = u[1];

                // get index and affinity
                int j = this.ColumnIndex(v);
                KeyAffinity a = Key.ParseAffinity(w);

                // Accumulate values //
                if (j != -1)
                    k.Add(j, a);
                else if (v.ToList().TrueForAll((c) => { return "1234567890".Contains(c); }))
                    k.Add(int.Parse(v), a);

            }
            return k;

        }

        /// <summary>
        /// Parses a string into a key
        /// </summary>
        /// <param name="Columns">A variable list of columns</param>
        /// <returns>A key</returns>
        public Key KeyParse(string[] Columns)
        {
            Key k = new Key();
            foreach (string s in Columns)
            {
                k.Add(this.ColumnIndex(s));
            }
            return k;
        }

        // Prints //
        /// <summary>
        /// Prints the contents of a schema
        /// </summary>
        public void Print()
        {
            for (int i = 0; i < this.Count; i++)
            {
                Console.WriteLine("{0} : {1} : {2} : {3}", this.ColumnName(i), this.ColumnAffinity(i), this.ColumnSize(i), this.ColumnNull(i));
            }
        }

        // Meta data //
        /// <summary>
        /// Returns a string representation of the schema: NAME TYPE(.SIZE)? NULLABLE
        /// </summary>
        /// <param name="Delim">A character to deliminate the fields</param>
        /// <returns>A string</returns>
        public string ToString(char Delim)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < this._Cache.Count; i++)
            {
                sb.Append(this.ColumnName(i) + " " + this.ColumnAffinity(i).ToString());
                if (this.ColumnAffinity(i) == CellAffinity.BLOB || this.ColumnAffinity(i) == CellAffinity.STRING)
                    sb.Append("." + this.ColumnSize(i).ToString());
                if (this.ColumnNull(i) == true)
                {
                    sb.Append(" TRUE");
                }
                else
                {
                    sb.Append(" FALSE");
                }
                if (i != this._Cache.Count - 1)
                    sb.Append(Delim);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Returns a string with a ',' deliminator
        /// </summary>
        /// <returns>A string representation of the scheam</returns>
        public override string ToString()
        {
            return this.ToString(SCHEMA_DELIM);
        }

        /// <summary>
        /// Returns a string representation of the schema: NAME TYPE(.SIZE)? NULLABLE
        /// </summary>
        /// <param name="K">A key to filter on</param>
        /// <param name="Delim">A character to deliminate the fields</param>
        /// <returns>A string</returns>
        public string ToString(Key K, char Delim)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < K.Count; i++)
            {
                sb.Append(this.ColumnName(K[i]) + " " + this.ColumnAffinity(K[i]).ToString());
                if (this.ColumnNull(K[i]) == true)
                {
                    sb.Append(" NULL");
                }
                else
                {
                    sb.Append(" NOT NULL");
                }
                if (i != K.Count - 1) sb.Append(Delim);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Returns a string representation of the schema: NAME TYPE(.SIZE)? NULLABLE
        /// </summary>
        /// <param name="K">A key to filter on</param>
        /// <returns>A string</returns>
        public string ToString(Key K)
        {
            return this.ToString(K, SCHEMA_DELIM);
        }

        /// <summary>
        /// Returns a string of column names
        /// </summary>
        /// <param name="Delim">A character to deliminate the fields</param>
        /// <returns>A string</returns>
        public string ToNameString(char Delim)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < this._Cache.Count; i++)
            {
                sb.Append(this.ColumnName(i));
                if (i != this._Cache.Count - 1) sb.Append(Delim);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Returns a string of column names
        /// </summary>
        /// <returns>A string</returns>
        public string ToNameString()
        {
            return this.ToNameString(SCHEMA_DELIM);
        }

        /// <summary>
        /// Returns a string of column names
        /// </summary>
        /// <param name="K">A key to filter on</param>
        /// <param name="Delim">A character to deliminate the fields</param>
        /// <returns>A string</returns>
        public string ToNameString(Key K, char Delim)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < K.Count; i++)
            {
                sb.Append(this.ColumnName(K[i]));
                if (i != K.Count - 1) sb.Append(Delim);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Returns a string of column names
        /// </summary>
        /// <param name="K">A key to filter on</param>
        /// <returns>A string</returns>
        public string ToNameString(Key K)
        {
            return this.ToNameString(K, SCHEMA_DELIM);
        }

        /// <summary>
        /// Returns a string of column affinities
        /// </summary>
        /// <param name="Delim">A character to deliminate the fields</param>
        /// <returns>A string</returns>
        public string ToAffinityString(char Delim)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < this._Cache.Count; i++)
            {
                sb.Append(this.ColumnAffinity(i).ToString());
                if (i != this._Cache.Count - 1) sb.Append(Delim);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Returns a string of column affinities
        /// </summary>
        /// <returns>A string</returns>
        public string ToAffinityString()
        {
            return this.ToAffinityString(SCHEMA_DELIM);
        }

        /// <summary>
        /// Returns a string of column affinities
        /// </summary>
        /// <param name="K">A key to filter on</param>
        /// <param name="Delim">A character to deliminate the fields</param>
        /// <returns>A string</returns>
        public string ToAffinityString(Key K, char Delim)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < K.Count; i++)
            {
                sb.Append(this.ColumnAffinity(K[i]).ToString());
                if (i != K.Count - 1) sb.Append(Delim);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Returns a string of column affinities
        /// </summary>
        /// <returns>A string</returns>
        public string ToAffinityString(Key K)
        {
            return this.ToAffinityString(K, SCHEMA_DELIM);
        }

        /// <summary>
        /// Returns a unique hash code value based on the schema's type and order; two schema with identical field types and 
        /// </summary>
        /// <returns>An integer hash code</returns>
        public override int GetHashCode()
        {
            return this._HashCode;
        }

        /// <summary>
        /// Checks if a given object's hash code matches another's; does not attempt to cast obj as a schema
        /// </summary>
        /// <param name="obj">An object passed</param>
        /// <returns>An bool indicating if the objects have the same hash code</returns>
        public override bool Equals(object obj)
        {
            return this._HashCode == obj.GetHashCode();
        }

        // Statics //
        /// <summary>
        /// Combines two schemas; throws an exception if two columns have the same name.
        /// </summary>
        /// <param name="S1">The left schema</param>
        /// <param name="S2">The right schema</param>
        /// <returns>A combined schema</returns>
        public static Schema Join(Schema S1, Schema S2)
        {

            Schema s = new Schema();
            for (int i = 0; i < S1.Count; i++)
            {
                s.Add(S1.ColumnName(i), S1.ColumnAffinity(i), S1.ColumnNull(i), S1.ColumnSize(i));
            }
            for (int i = 0; i < S2.Count; i++)
            {
                s.Add(S2.ColumnName(i), S2.ColumnAffinity(i), S2.ColumnNull(i), S2.ColumnSize(i));
            }
            return s;

        }

        /// <summary>
        /// Creates a schema from another schema
        /// </summary>
        /// <param name="S">The starting point schema</param>
        /// <param name="K">A key representing the columns to keep</param>
        /// <returns>A schema</returns>
        public static Schema Split(Schema S, Key K)
        {

            Schema s = new Schema();
            for (int i = 0; i < K.Count; i++)
            {
                s.Add(S.ColumnName(K[i]), S.ColumnAffinity(K[i]), S.ColumnNull(K[i]), S.ColumnSize(K[i]));
            }
            return s;

        }

        /// <summary>
        /// Checks if two columns have compatible schemas
        /// </summary>
        /// <param name="S1">The left schema</param>
        /// <param name="S2">The right schema</param>
        /// <returns>A boolean indicating whether or not each schema's type arrays match</returns>
        public static bool operator ==(Schema S1, Schema S2)
        {
            int hc1 = (S1 ?? new Schema())._HashCode;
            int hc2 = (S2 ?? new Schema())._HashCode;
            return hc1 == hc2;
        }

        /// <summary>
        /// Checks if two columns have incompatible schemas
        /// </summary>
        /// <param name="S1">The left schema</param>
        /// <param name="S2">The right schema</param>
        /// <returns>A boolean indicating whether or not each schema's type arrays match</returns>
        public static bool operator !=(Schema S1, Schema S2)
        {
            int hc1 = (S1 ?? new Schema())._HashCode;
            int hc2 = (S2 ?? new Schema())._HashCode;
            return hc1 != hc2;
        }

        /// <summary>
        /// Fixes a (potentially) invalid column size passed
        /// </summary>
        /// <param name="Affinity">The desired affinity</param>
        /// <param name="Size">The initial size</param>
        /// <returns>The fixed size</returns>
        public static int FixSize(CellAffinity Affinity, int Size)
        {

            if (Affinity == CellAffinity.BOOL)
                Size = 1;
            else if (Affinity == CellAffinity.DATE_TIME || Affinity == CellAffinity.DOUBLE || Affinity == CellAffinity.INT)
                Size = 8;
            else if (Size < 0 && Affinity == CellAffinity.STRING)
                Size = DEFAULT_STRING_SIZE;
            else if (Size < 0 && Affinity == CellAffinity.BLOB)
                Size = DEFAULT_BLOB_SIZE;
            else if (Size > MAX_VARIABLE_SIZE)
                Size = MAX_VARIABLE_SIZE;

            return Size;

        }

        /// <summary>
        /// Returns the schema's inner schema type NAME, TYPE, ISNULL, SIZE
        /// </summary>
        /// <returns>A schema</returns>
        public static Schema SchemaSchema()
        {
            string text = "name string.64, type int, isnull bool, size int";
            return new Schema(text);
        }

    }

}
