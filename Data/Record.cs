using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data
{

    /// <summary>
    /// Represents an array of cells; unlike a CellVector, each cell is allowed to have different types
    /// </summary>
    public class Record 
    {

        public const char DELIM = '\t';

        // Private //
        internal Cell[] _data;

        // Constructor //
        /// <summary>
        /// Creates a record with a pre-defined size
        /// </summary>
        /// <param name="Size">The size of the record's element cache</param>
        public Record(int Size)
        {
            this._data = new Cell[Size];
        }

        /// <summary>
        /// Creates a record based on an array of cells
        /// </summary>
        /// <param name="Data">The data to load the record with</param>
        public Record(Cell[] Data)
        {
            this._data = Data;
        }

        // Properties //
        /// <summary>
        /// The number of elements in the record
        /// </summary>
        public int Count
        {
            get
            {
                return this._data.Length;
            }
        }

        /// <summary>
        /// Gets or sets an element in a record
        /// </summary>
        /// <param name="Index">The offset of the record</param>
        /// <returns>A cell</returns>
        public Cell this[int Index]
        {
            get
            {
                return this._data[Index];
            }
            set
            {
                this._data[Index] = value;
            }
        }

        /// <summary>
        /// Returns the inner array supporting the record
        /// </summary>
        internal Cell[] BaseArray
        {
            get
            {
                return this._data;
            }
        }

        // Overrides //
        public string ToString(char Delim, char Escape)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < this.Count; i++)
            {
                sb.Append(Escape);
                sb.Append(this[i].ToString());
                sb.Append(Escape);
                if (i != this.Count - 1) 
                    sb.Append(Delim);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Converts the record to a string with a defined deliminator
        /// </summary>
        /// <param name="Delim">The delim to space each cell value</param>
        /// <returns>A string representation of the record</returns>
        public string ToString(char Delim)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < this.Count; i++)
            {
                sb.Append(this[i].ToString());
                if (i != this.Count - 1) sb.Append(Delim);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Returns a string representation of a record using '\t' as the deliminator
        /// </summary>
        /// <returns>A string</returns>
        public override string ToString()
        {
            return this.ToString(DELIM);
        }

        public string ToString(Key K, char Delim, char Escape)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < K.Count; i++)
            {
                sb.Append(Escape);
                sb.Append(this[K[i]].ToString());
                sb.Append(Escape);
                if (i != K.Count - 1) sb.Append(Delim);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Turns a record into a string
        /// </summary>
        /// <param name="K">A key to filter the record on</param>
        /// <param name="Delim">A delim to composite the string based on</param>
        /// <returns>A string</returns>
        public string ToString(Key K, char Delim)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < K.Count; i++)
            {
                sb.Append(this[K[i]].ToString());
                if (i != K.Count - 1) sb.Append(Delim);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Turns a record into a string
        /// </summary>
        /// <param name="K">A key to filter the record on</param>
        /// <returns>A string</returns>
        public string ToString(Key K)
        {
            return this.ToString(K, DELIM);
        }

        /// <summary>
        /// Returns the hash code for the entire record
        /// </summary>
        /// <returns>An interger hash code</returns>
        public override int GetHashCode()
        {
            int hxc = 0;
            for (int i = 0; i < this.Count; i++)
            {
                hxc += (i + 1) * this[i].GetHashCode();
            }
            return hxc;
        }

        /// <summary>
        /// Returns the hash code for the entire record
        /// </summary>
        /// <param name="K">A key to filter the record on</param>
        /// <returns>An interger hash code</returns>
        public int GetHashCode(Key K)
        {
            int hxc = 0;
            for (int i = 0; i < K.Count; i++)
            {
                hxc += (i + 1) * this[K[i]].GetHashCode();
            }
            return hxc;
        }

        // Comparers //
        public static int Compare(Record R1, Key K1, Record R2, Key K2)
        {

            if (K1.Count != K2.Count)
                throw new Exception("Keys size do not match " + K1.Count.ToString() + " : " + K2.Count.ToString());

            int c = 0;
            for (int i = 0; i < K1.Count; i++)
            {
                c = Cell.Compare(R1[K1[i]], R2[K2[i]]);
                if (K1.Affinity(i) == KeyAffinity.Descending || K2.Affinity(i) == KeyAffinity.Descending) c = -c;
                if (c != 0) return c;
            }
            return 0;

        }

        public static int Compare(Record R1, Record R2, Key K)
        {

            int c = 0;
            for (int i = 0; i < K.Count; i++)
            {
                c = Cell.Compare(R1[K[i]], R2[K[i]]);
                if (K.Affinity(i) == KeyAffinity.Descending) c = -c;
                if (c != 0) return c;
            }
            return 0;

        }

        public static int Compare(Record R1, Record R2)
        {

            if (R1.Count != R2.Count)
                throw new Exception("Record sizes do not match " + R1.Count.ToString() + " : " + R2.Count.ToString());

            int c = 0;
            for (int i = 0; i < R1.Count; i++)
            {
                c = Cell.Compare(R1[i], R2[i]);
                if (c != 0) return c;
            }
            return 0;

        }

        public static bool Equals(Record R1, Key K1, Record R2, Key K2)
        {

            if (K1.Count != K2.Count)
                throw new Exception("Keys size do not match " + K1.Count.ToString() + " : " + K2.Count.ToString());

            for (int i = 0; i < K1.Count; i++)
            {

                if (R1[K1[i]].INT_A != R2[K2[i]].INT_A)
                    return false;
                else if (R1[K1[i]] != R2[K2[i]])
                    return false;

            }
            return true;

        }

        public static bool Equals(Record R1, Record R2)
        {

            if (R1.Count != R2.Count)
                throw new Exception("Keys size do not match " + R1.Count.ToString() + " : " + R2.Count.ToString());

            for (int i = 0; i < R1.Count; i++)
            {

                if (R1[i].INT_A != R2[i].INT_A)
                    return false;
                else if (R1[i] != R2[i])
                    return false;

            }
            return true;

        }

        // Builders //
        /// <summary>
        /// Unboxes an object into a cell; throws an exception if the unbox fails
        /// </summary>
        /// <param name="Data">An array of objects</param>
        /// <returns>A record representation of each object</returns>
        public static Record Unbox(params object[] Data)
        {

            int len = Data.Length;
            Cell[] c = new Cell[len];

            for (int i = 0; i < len; i++)
            {
                c[i] = Cell.UnBox(Data[i]);
            }
            return new Record(c);

        }

        /// <summary>
        /// Unboxes an object into a cell; puts a null cell into the record if the unbox fails
        /// </summary>
        /// <param name="Data">An array of objects</param>
        /// <returns>A record representation of each object</returns>
        public static Record TryUnbox(params object[] Data)
        {

            int len = Data.Length;
            Cell[] c = new Cell[len];

            for (int i = 0; i < len; i++)
            {
                c[i] = Cell.TryUnBox(Data[i]);
            }
            return new Record(c);

        }

        /// <summary>
        /// Unboxes an object into a specific type; throws an exception if the unbox fails
        /// </summary>
        /// <param name="Columns">A schema representing the types to unbox</param>
        /// <param name="Data">The collection of objects</param>
        /// <returns>A record</returns>
        public static Record UnboxInto(Schema Columns, params object[] Data)
        {

            int len = Data.Length;
            if (len != Columns.Count)
                throw new Exception("Column count does not match data length");
            Cell[] c = new Cell[len];

            for (int i = 0; i < len; i++)
            {
                c[i] = Cell.UnBoxInto(Data[i], Columns.ColumnAffinity(i));
            }
            return new Record(c);

        }

        /// <summary>
        /// Unboxes an object into a specific type; returns null if the unbox fails
        /// </summary>
        /// <param name="Columns">A schema representing the types to unbox</param>
        /// <param name="Data">The collection of objects</param>
        /// <returns>A record</returns>
        public static Record TryUnboxInto(Schema Columns, params object[] Data)
        {

            int len = Data.Length;
            if (len != Columns.Count) throw new Exception("Column count does not match data length");
            Cell[] c = new Cell[len];

            for (int i = 0; i < len; i++)
            {
                c[i] = Cell.TryUnBoxInto(Data[i], Columns.ColumnAffinity(i));
            }
            return new Record(c);

        }

        /// <summary>
        /// Combines a variables length array of cells into a record
        /// </summary>
        /// <param name="Data">A variable array of cells</param>
        /// <returns>A record</returns>
        public static Record Stitch(params Cell[] Data)
        {
            return new Record(Data);
        }

        /// <summary>
        /// Parses a string into a record; throws an exception if the parse fails
        /// </summary>
        /// <param name="Columns">A schema representing the record types</param>
        /// <param name="Text">Text representing the record</param>
        /// <param name="Delim">A delim value partitioning fields</param>
        /// <returns>A record</returns>
        public static Record Parse(Schema Columns, string Text, char Delim)
        {
            string[] s = Text.Split(Delim);
            if (s.Length != Columns.Count)
                throw new Exception(string.Format("Text passed has {0} elements, but the schema contains {1} elements \n\t{2}", s.Length, Columns.Count, Text));
            List<Cell> c = new List<Cell>();
            for (int i = 0; i < Columns.Count; i++)
            {
                c.Add(Cell.Parse(s[i], Columns.ColumnAffinity(i)));
            }
            return new Record(c.ToArray());
        }

        /// <summary>
        /// Parses a string into a record; throws an exception if the parse fails
        /// </summary>
        /// <param name="Columns">A schema representing the record types</param>
        /// <param name="Text">Text representing the record</param>
        /// <returns>A record</returns>
        public static Record Parse(Schema Columns, string Text)
        {
            return Parse(Columns, Text, DELIM);
        }

        /// <summary>
        /// Parses a string into a record; returns null if the parse failes
        /// </summary>
        /// <param name="Columns">A schema representing the record types</param>
        /// <param name="Text">Text representing the record</param>
        /// <param name="Delim">A delim value partitioning fields</param>
        /// <returns>A record</returns>
        public static Record TryParse(Schema Columns, string Text, char Delim)
        {
            string[] s = Text.Split(Delim);
            if (s.Length != Columns.Count) throw new Exception("Parse-Expression is invalid: " + Text);
            List<Cell> c = new List<Cell>();
            for (int i = 0; i < Columns.Count; i++)
            {
                c.Add(Cell.TryParse(s[i], Columns.ColumnAffinity(i)));
            }
            return new Record(c.ToArray());
        }

        /// <summary>
        /// Parses a string into a record; returns null if the parse failes
        /// </summary>
        /// <param name="Columns">A schema representing the record types</param>
        /// <param name="Text">Text representing the record</param>
        /// <returns>A record</returns>
        public static Record TryParse(Schema Columns, string Text)
        {
            return TryParse(Columns, Text, DELIM);
        }

        // Others //
        /// <summary>
        /// Combines two records
        /// </summary>
        /// <param name="R1">The left record</param>
        /// <param name="R2">The right record</param>
        /// <returns>A record</returns>
        public static Record Join(Record R1, Record R2)
        {

            List<Cell> c = new List<Cell>();
            for (int i = 0; i < R1.Count; i++)
            {
                c.Add(R1[i]);
            }
            for (int i = 0; i < R2.Count; i++)
            {
                c.Add(R2[i]);
            }
            return new Record(c.ToArray());

        }

        /// <summary>
        /// Combines two records
        /// </summary>
        /// <param name="R1">The left record</param>
        /// <param name="K1">A key filter to apply to the left record</param>
        /// <param name="R2">The right record</param>
        /// <param name="K2">A key filter to apply to the right record</param>
        /// <returns>A record</returns>
        public static Record Join(Record R1, Key K1, Record R2, Key K2)
        {

            List<Cell> c = new List<Cell>();
            for (int i = 0; i < K1.Count; i++)
            {
                c.Add(R1[K1[i]]);
            }
            for (int i = 0; i < K2.Count; i++)
            {
                c.Add(R2[K2[i]]);
            }
            return new Record(c.ToArray());

        }

        /// <summary>
        /// Cuts a reocrd into a smaller record
        /// </summary>
        /// <param name="R">A record to split</param>
        /// <param name="K">A key to filter the record on</param>
        /// <returns>A record</returns>
        public static Record Split(Record R, Key K)
        {
            List<Cell> c = new List<Cell>();
            for (int i = 0; i < K.Count; i++)
            {
                c.Add(R[K[i]]);
            }
            return new Record(c.ToArray());
        }

        /// <summary>
        /// Chops a record into a smaller record
        /// </summary>
        /// <param name="R">The record to chop</param>
        /// <param name="Start">The starting index of the record</param>
        /// <param name="Length">The length of the new record</param>
        /// <returns>A record</returns>
        public static Record Subrecord(Record R, int Start, int Length)
        {
            Cell[] c = new Cell[Length];
            Array.Copy(R.BaseArray, Start, c, 0, Length);
            return new Record(c);
        }

        // Costs //
        public int MemCost
        {
            get
            {
                int cost = 0;
                foreach (Cell c in this._data)
                    cost += c.MemCost;
                return cost;
            }
        }

        public int DiskCost
        {
            get
            {
                int cost = 0;
                foreach (Cell c in this._data)
                    cost += c.DiskCost;
                return cost;
            }
        }

        public int DataCost
        {
            get
            {
                int cost = 0;
                foreach (Cell c in this._data)
                    cost += c.DataCost;
                return cost;
            }
        }



    }

    /// <summary>
    /// Akin to StringBuilder, this is a dynamic record builder
    /// </summary>
    public sealed class RecordBuilder
    {

        private List<Cell> _cache;

        /// <summary>
        /// Creates an empty RecordBuilder
        /// </summary>
        public RecordBuilder()
        {
            this._cache = new List<Cell>();
        }

        /// <summary>
        /// Creates a RecordBuilder and loads it with data
        /// </summary>
        /// <param name="Data">The cells to load the builder with</param>
        public RecordBuilder(params Cell[] Data)
            : this()
        {
            this.Add(Data);
        }

        /// <summary>
        /// Adds a cell to the right of the record
        /// </summary>
        /// <param name="Data">The cell to add</param>
        public void Add(Cell Data)
        {
            this._cache.Add(Data);
        }

        /// <summary>
        /// Adds a collection of cells to the data
        /// </summary>
        /// <param name="Data">The cells to add</param>
        public void Add(IEnumerable<Cell> Data)
        {
            foreach (Cell c in Data)
                this.Add(c);
        }

        /// <summary>
        /// Adds a collection of cells, derived from the record, to the data
        /// </summary>
        /// <param name="Data">The cells to add</param>
        public void Add(Record Data)
        {
            this.Add(Data.BaseArray);
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">An element which will be cast as a HORSE.INT type, which is a .NET long</param>
        public void Add(sbyte Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">An element which will be cast as a HORSE.INT type, which is a .NET long</param>
        public void Add(short Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">An element which will be cast as a HORSE.INT type, which is a .NET long</param>
        public void Add(int Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">An element which will be cast as a HORSE.INT type, which is a .NET long</param>
        public void Add(long Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">An element which will be cast as a HORSE.INT type, which is a .NET long</param>
        public void Add(byte Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">An element which will be cast as a HORSE.INT type, which is a .NET long</param>
        public void Add(ushort Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">An element which will be cast as a HORSE.INT type, which is a .NET long</param>
        public void Add(uint Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">An element which will be cast as a HORSE.INT type, which is a .NET long</param>
        public void Add(ulong Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">A boolean element</param>
        public void Add(bool Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">An element which will be cast as a HORSE.DOUBLE type, which is a .NET double</param>
        public void Add(float Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">An element which will be cast as a HORSE.DOUBLE type, which is a .NET double</param>
        public void Add(double Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">A date element</param>
        public void Add(DateTime Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">A string element</param>
        public void Add(string Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Adds an element to the right of the data
        /// </summary>
        /// <param name="Data">A byte array element</param>
        public void Add(byte[] Data)
        {
            this.Add(new Cell(Data));
        }

        /// <summary>
        /// Renders the current collection of cells into a record
        /// </summary>
        /// <returns>A record</returns>
        public Record ToRecord()
        {
            return new Record(this._cache.ToArray());
        }

    }


}
