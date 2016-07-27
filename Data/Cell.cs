using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data
{


    /// <summary>
    /// The basic unit of in memory data within Rye
    /// </summary>
    [System.Runtime.InteropServices.StructLayout(LayoutKind.Explicit)]
    public struct Cell : IComparable<Cell>, IComparer<Cell>
    {

        // Cell constants //
        public const String NULL_STRING_TEXT = "@@NULL"; // the null value text
        public const string HEX_LITERARL = "0x"; // the expected qualifier for a hex string 
        public const int MAX_STRING_LENGTH = 64 * 1024; // maximum length of a string, 64k

        // Cell internal statics //
        internal static int NUMERIC_ROUNDER = 5; // used for rounding double values 
        internal static int DATE_FORMAT = 1; // 0 = full date time, 1 = date only, 2 = time only
        internal static string TRUE_STRING = "TRUE";
        internal static string FALSE_STRING = "FALSE";

        // Static common cells //
        public static Cell TRUE = new Cell(true);
        public static Cell FALSE = new Cell(false);
        public static Cell NULL_BOOL = new Cell(CellAffinity.BOOL);
        public static Cell NULL_INT = new Cell(CellAffinity.INT);
        public static Cell NULL_DOUBLE = new Cell(CellAffinity.DOUBLE);
        public static Cell NULL_DATE = new Cell(CellAffinity.DATE_TIME);
        public static Cell NULL_STRING = new Cell(CellAffinity.STRING);
        public static Cell NULL_BLOB = new Cell(CellAffinity.BLOB);

        #region Runtime_Variables

        /* Offset:      0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15
         * 
         * NullFlag     x
         * Affinity         x
         * INT64                x   x   x   x   x   x   x   x
         * DATE                 x   x   x   x   x   x   x   x
         * DOUBLE               x   x   x   x   x   x   x   x
         * BOOL                 x
         * STRING                                                       x   x   x   x
         * BLOB                                                         x   x   x   x
         * INT32A               x   x   x   x   
         * INT32B                               x   x   x   x
         * ULONG                x   x   x   x   x   x   x   x
         * 
         */

        // Metadata elements //
        /// <summary>
        /// The cell affinity, offset 0
        /// </summary>
        [System.Runtime.InteropServices.FieldOffset(0)]
        internal CellAffinity AFFINITY;

        /// <summary>
        /// The null byte indicator, offset 1
        /// </summary>
        [System.Runtime.InteropServices.FieldOffset(1)]
        internal byte NULL;

        // Data variables //
        /// <summary>
        /// The .Net bool value, offset 2
        /// </summary>
        [System.Runtime.InteropServices.FieldOffset(2)]
        internal bool BOOL;

        /// <summary>
        /// The .Net long value, offset 2
        /// </summary>
        [System.Runtime.InteropServices.FieldOffset(2)]
        internal long INT;

        /// <summary>
        /// The .Net double value, offset 2
        /// </summary>
        [System.Runtime.InteropServices.FieldOffset(2)]
        internal double DOUBLE;

        /// <summary>
        /// The .Net DateTime variable, offset 2
        /// </summary>
        [System.Runtime.InteropServices.FieldOffset(2)]
        internal DateTime DATE_TIME;

        /// <summary>
        /// The .Net string variable, offset 12
        /// </summary>
        [System.Runtime.InteropServices.FieldOffset(12)]
        internal string STRING;

        /// <summary>
        /// The .Net byte[] variable, offset 12
        /// </summary>
        [System.Runtime.InteropServices.FieldOffset(12)]
        internal byte[] BLOB;

        // Extended elements //
        /// <summary>
        /// The .Net integer value at offset 2
        /// </summary>
        [System.Runtime.InteropServices.FieldOffset(2)]
        internal int INT_A;

        /// <summary>
        /// The .Net integer value at offset 6
        /// </summary>
        [System.Runtime.InteropServices.FieldOffset(6)]
        internal int INT_B;

        /// <summary>
        /// The .Net ulong value at offset 12
        /// </summary>
        [System.Runtime.InteropServices.FieldOffset(2)]
        internal ulong ULONG;

        [System.Runtime.InteropServices.FieldOffset(2)]
        internal byte B0;

        [System.Runtime.InteropServices.FieldOffset(3)]
        internal byte B1;

        [System.Runtime.InteropServices.FieldOffset(4)]
        internal byte B2;

        [System.Runtime.InteropServices.FieldOffset(5)]
        internal byte B3;

        [System.Runtime.InteropServices.FieldOffset(6)]
        internal byte B4;

        [System.Runtime.InteropServices.FieldOffset(7)]
        internal byte B5;

        [System.Runtime.InteropServices.FieldOffset(8)]
        internal byte B6;

        [System.Runtime.InteropServices.FieldOffset(9)]
        internal byte B7;

        #endregion

        #region Constructor

        /// <summary>
        /// Creates a boolean cell
        /// </summary>
        /// <param name="Value">A .Net bool</param>
        public Cell(bool Value)
            : this()
        {
            this.BOOL = Value;
            this.AFFINITY = CellAffinity.BOOL;
            this.NULL = 0;
        }

        /// <summary>
        /// Creates a 64 bit integer cell
        /// </summary>
        /// <param name="Value">A .Net long or int64</param>
        public Cell(long Value)
            : this()
        {

            this.INT = Value;
            this.AFFINITY = CellAffinity.INT;
            this.NULL = 0;
        }

        /// <summary>
        /// Creates a 64 bit numeric cell
        /// </summary>
        /// <param name="Value">A .Net double or Double</param>
        public Cell(double Value)
            : this()
        {
            this.DOUBLE = Value;
            this.AFFINITY = CellAffinity.DOUBLE;
            this.NULL = 0;
        }

        /// <summary>
        /// Creates a 64 bit date-time cell
        /// </summary>
        /// <param name="Value">A .Net DateTime</param>
        public Cell(DateTime Value)
            : this()
        {
            this.DATE_TIME = Value;
            this.AFFINITY = CellAffinity.DATE_TIME;
            this.NULL = 0;
        }

        /// <summary>
        /// Creates a string cell; strings greater than 1024 chars will be truncated
        /// </summary>
        /// <param name="Value">A .Net string value to be converted to a cell</param>
        /// <param name="TrimQuotes">True will conver 'ABCD' to ABCD</param>
        public Cell(string Value, bool TrimQuotes)
            : this()
        {

            // Set the affinity //
            this.AFFINITY = CellAffinity.STRING;

            // Handle null strings //
            if (Value == null)
            {
                this.STRING = "\0";
                this.NULL = 1;
            }

            // Remove the quotes //
            if (TrimQuotes)
                Value = Cell.RemoveFirstLastQuotes(Value);
            //Console.WriteLine("{0} : {1}", TrimQuotes, Value);

            // Fix the values
            if (Value.Length == 0) // fix instances that are zero length
                Value = "\0";
            else if (Value.Length >= MAX_STRING_LENGTH) // Fix strings that are too long
                Value = Value.Substring(0, MAX_STRING_LENGTH);

            this.STRING = Value;
            this.NULL = 0;

            this.INT_A = Value.GetHashCode();
            this.INT_B = Value.Length;

        }

        /// <summary>
        /// Creates a string cell; strings greater than 1024 chars will be truncated
        /// </summary>
        /// <param name="Value">A .Net string value to be converted to a cell</param>
        public Cell(string Value)
            : this(Value, false)
        {
        }

        /// <summary>
        /// Creats a BLOB cell
        /// </summary>
        /// <param name="Value">A .Net array of bytes</param>
        public Cell(byte[] Value)
            : this()
        {
            this.BLOB = Value;
            this.NULL = 0;
            this.AFFINITY = CellAffinity.BLOB;
            for (int i = 0; i < Value.Length; i++)
                this.INT_A += Value[i] * i;
            this.INT_A = this.INT_A ^ Value.Length;
            this.INT_B = Value.Length;
        }

        /// <summary>
        /// Creates a cell of a given affinity that is null
        /// </summary>
        /// <param name="Type">An affinity of the new cell</param>
        public Cell(CellAffinity Type)
            : this()
        {
            this.AFFINITY = Type;
            this.NULL = 1;
            if (Type == CellAffinity.STRING)
                this.STRING = "";
            if (Type == CellAffinity.BLOB)
                this.BLOB = new byte[0];
        }

        // -- Auto Casts -- //
        /// <summary>
        /// Creates a 64 integer cell
        /// </summary>
        /// <param name="Value">A .Net int or Int32 which will be cast to a .Net long</param>
        public Cell(int Value)
            : this()
        {
            this.INT = (long)Value;
            this.AFFINITY = CellAffinity.INT;
            this.NULL = 0;
        }

        /// <summary>
        /// Creates a 64 bit integer cell
        /// </summary>
        /// <param name="ValueA">A .Net 32 bit integer that will make up the first 4 bytes of integer</param>
        /// <param name="ValueB"></param>
        internal Cell(int ValueA, int ValueB)
            : this()
        {

            // Set these values //
            this.INT_A = ValueA;
            this.INT_B = ValueB;
            this.AFFINITY = CellAffinity.INT;
            this.NULL = 0;

        }

        internal Cell(ulong Value, CellAffinity Affinity)
            : this()
        {
            this.AFFINITY = Affinity;
            this.ULONG = Value;
            this.NULL = 0;
        }

        #endregion

        #region Properties

        /// <summary>
        /// The cell's current affinity
        /// </summary>
        public CellAffinity Affinity
        {
            get { return this.AFFINITY; }
        }

        /// <summary>
        /// True == null, False == not null
        /// </summary>
        public bool IsNull
        {
            get { return NULL == 1; }
        }

        /// <summary>
        /// True if the numeric value is 0 or if the variable length value has a zero length
        /// </summary>
        public bool IsZero
        {
            get
            {
                if (this.IsNull) return false;
                switch (this.Affinity)
                {
                    case CellAffinity.INT: return this.INT == 0;
                    case CellAffinity.DOUBLE: return this.DOUBLE == 0;
                    case CellAffinity.BOOL: return !this.BOOL;
                    case CellAffinity.STRING: return this.STRING.Length == 0;
                    case CellAffinity.BLOB: return this.BLOB.Length == 0;
                    default: return false;
                }
            }

        }

        /// <summary>
        /// Returns true if the integer value or double value is 1, or if the boolean is true, false otherwise
        /// </summary>
        public bool IsOne
        {

            get
            {
                if (this.IsNull) return false;
                switch (this.Affinity)
                {
                    case CellAffinity.INT: return this.INT == 1;
                    case CellAffinity.DOUBLE: return this.DOUBLE == 1;
                    case CellAffinity.BOOL: return this.BOOL;
                    default: return false;
                }
            }

        }

        #endregion

        #region SafeValues

        /// <summary>
        /// Returns the bool value if the affinity is 'BOOL', true if the INT property is 0, false otherwise
        /// </summary>
        public bool valueBOOL
        {
            get
            {
                if (this.AFFINITY == CellAffinity.BOOL) return this.BOOL;
                return this.INT == 0;
            }
        }

        /// <summary>
        /// Return the INT value if the affinity is INT, casts the DOUBLE as an INT if the affinity is a DOUBLE, 0 otherwise
        /// </summary>
        public long valueINT
        {
            get
            {
                if (this.AFFINITY == CellAffinity.INT)
                    return this.INT;
                if (this.AFFINITY == CellAffinity.DOUBLE)
                    return (long)this.DOUBLE;
                if (this.AFFINITY == CellAffinity.BOOL)
                    return this.BOOL ? 1 : 0;
                return 0;
            }
        }

        /// <summary>
        /// Return the DOUBLE value if the affinity is DOUBLE, casts the INT as an DOUBLE if the affinity is a INT, 0 otherwise
        /// </summary>
        public double valueDOUBLE
        {
            get
            {
                if (this.AFFINITY == CellAffinity.DOUBLE)
                    return this.DOUBLE;
                if (this.AFFINITY == CellAffinity.INT)
                    return (double)this.INT;
                if (this.AFFINITY == CellAffinity.BOOL)
                    return this.BOOL ? 1D : 0D;
                return 0;
            }
        }

        /// <summary>
        /// Returns the current DATE_TIME if the affinity is DATE_TIME, otherwise return the minimum date time .Net value
        /// </summary>
        public DateTime valueDATE_TIME
        {
            get
            {
                if (this.Affinity == CellAffinity.DATE_TIME) return this.DATE_TIME;
                return DateTime.MinValue;
            }
        }

        /// <summary>
        /// If the cell is null, returns '@@NULL'; otherwise, casts the value as a string
        /// </summary>
        public string valueSTRING
        {
            get
            {

                if (this.IsNull)
                    return Cell.NULL_STRING_TEXT;

                switch (this.Affinity)
                {

                    case CellAffinity.INT:
                        return this.INT.ToString();

                    case CellAffinity.DOUBLE:
                        return Math.Round(this.DOUBLE, NUMERIC_ROUNDER).ToString();

                    case CellAffinity.BOOL:
                        return this.BOOL ? TRUE_STRING : FALSE_STRING;

                    case CellAffinity.DATE_TIME:
                        return Cell.DateString(this.DATE_TIME, Cell.DATE_FORMAT);

                    case CellAffinity.STRING:
                        return this.STRING;

                    case CellAffinity.BLOB:
                        return HEX_LITERARL + BitConverter.ToString(this.BLOB).Replace("-", "");

                    default:
                        return "";

                }

            }

        }

        /// <summary>
        /// If the affinity is null, returns an empty byte array; if the value is a BLOB, returns the BLOB; if the value is a stirng, returns the string as a byte array, unless the string has a hex prefix, then it converts the hex string to a byte array; otherwise it converts an INT, DOUBLE, BOOL to a byte array.
        /// </summary>
        public byte[] valueBLOB
        {
            get
            {

                if (this.AFFINITY == CellAffinity.BLOB)
                    return this.NULL == 1 ? new byte[0] : this.BLOB;

                if (this.AFFINITY == CellAffinity.BOOL)
                    return this.BOOL == true ? new byte[1] { 1 } : new byte[1] { 0 };
                else if (this.AFFINITY == CellAffinity.DATE_TIME || this.AFFINITY == CellAffinity.INT || this.AFFINITY == CellAffinity.DOUBLE)
                    return BitConverter.GetBytes(this.INT);
                else // STRING
                    return ASCIIEncoding.BigEndianUnicode.GetBytes(this.STRING);

            }
        }

        #endregion

        #region Overrides

        /// <summary>
        /// Returns the valueString property
        /// </summary>
        /// <returns>A string reprsentation of a cell</returns>
        public override string ToString()
        {

            // Check if null //
            if (this.IsNull == true) return Cell.NULL_STRING_TEXT;

            return this.valueSTRING;

        }

        /// <summary>
        /// Casts an object as a cell then compares it to the current instance
        /// </summary>
        /// <param name="obj">The object being tested for equality</param>
        /// <returns>A boolean indicating both objects are equal</returns>
        public override bool Equals(object obj)
        {
            return Compare(this, (Cell)obj) == 0;
        }

        /// <summary>
        /// If null, return int.MinValue, for INT, DOUBLE, BOOL, and DATE_TIME, return INT_A; for blobs, returns the sum of all bytes; for strings, returns the sum of the (i + 1) x char[i]
        /// </summary>
        /// <returns>An integer hash code</returns>
        public override int GetHashCode()
        {

            if (this.NULL == 1)
                return int.MinValue;

            if (this.Affinity != CellAffinity.STRING && this.AFFINITY != CellAffinity.BLOB)
                return this.INT_A;

            if (this.AFFINITY == CellAffinity.BLOB)
                return this.BLOB.Sum<byte>((x) => { return (int)x; });

            int t = 0;
            for (int i = 0; i < this.STRING.Length; i++)
                t += (i + 1) * this.STRING[i];

            return t;

        }

        #endregion

        #region Statics

        /// <summary>
        /// Compares two cells; a negative number indicates the C1 less than C2, a positive numbers indicates C1 greater than C2, zero indicates both are equal
        /// </summary>
        /// <param name="C1">The left comparing cell</param>
        /// <param name="C2">The right comparing cell</param>
        /// <returns>An integer comparison metric</returns>
        public static int Compare(Cell C1, Cell C2)
        {

            // Check nulls //
            if (C1.IsNull == true && C2.IsNull == false)
                return -1;
            else if (C1.IsNull == false && C2.IsNull == true)
                return 1;
            else if (C1.IsNull == true && C2.IsNull == true)
                return 0;


            // Non-strings //
            if (C1.AFFINITY == CellAffinity.STRING)
                return string.Compare(C1.STRING, C2.STRING);
            // BLOBs //
            else if (C1.AFFINITY == CellAffinity.BLOB)
                return ByteArrayCompare(C1.BLOB, C2.valueBLOB);

            // Check equality //
            if (C1.INT == C2.INT)
                return 0;

            // Doubles //
            else if (C1.Affinity == CellAffinity.DOUBLE)
                return C1.DOUBLE < C2.DOUBLE ? -1 : 1;

            // Ints //
            else if (C1.Affinity == CellAffinity.INT)
                return C1.INT < C2.INT ? -1 : 1;

            // Booleans //
            else if (C1.AFFINITY == CellAffinity.BOOL)
                return C1.BOOL ? 1 : -1;

            // Dates //
            else if (C1.AFFINITY == CellAffinity.DATE_TIME)
                return DateTime.Compare(C1.DATE_TIME, C2.DATE_TIME);

            return 0;

        }

        /// <summary>
        /// Converts a string value to a cell with a given affinity
        /// </summary>
        /// <param name="Value">The string representation of the cell</param>
        /// <param name="NewAffinity">The affinity the string will be cast to</param>
        /// <returns>The cell form the converted value</returns>
        public static Cell Parse(string Value, CellAffinity NewAffinity)
        {

            // Check for nulls //
            if (Value == null)
                return new Cell(NewAffinity);

            // Check nulls //
            if (Value.ToUpper() == Cell.NULL_STRING_TEXT)
                return new Cell(NewAffinity);

            //string Value2 = RemoveFirstLastQuotes(Value);

            // Negative //
            bool Minus = false;
            if (Value[0] == '~' && (NewAffinity == CellAffinity.DOUBLE || NewAffinity == CellAffinity.INT))
            {
                Value = Value.Substring(1, Value.Length - 1);
                Minus = true;
            }

            // Clean up numerics //
            if (NewAffinity == CellAffinity.DOUBLE || NewAffinity == CellAffinity.INT)
                Value = Value.Replace("%", "").Replace(",", "");

            // Set value //
            switch (NewAffinity)
            {

                case CellAffinity.INT:
                    if (!IsNumeric(Value, false))
                    {
                        return NULL_INT;
                    }
                    if (Minus)
                    {
                        return new Cell(-long.Parse(Value.Trim()));
                    }
                    else
                    {
                        return new Cell(long.Parse(Value.Trim()));
                    }

                case CellAffinity.DOUBLE:
                    if (!IsNumeric(Value, true))
                        return NULL_DOUBLE;
                    if (Minus)
                        return new Cell(-double.Parse(Value.Trim()));
                    return new Cell(double.Parse(Value.Trim()));

                case CellAffinity.BOOL:
                    return new Cell(bool.Parse(Value.Trim()));

                case CellAffinity.DATE_TIME:
                    return DateParse(Value.Trim());

                case CellAffinity.STRING:
                    return new Cell(Value); // No parsing

                case CellAffinity.BLOB:
                    return ByteParse(Value.Trim());

                default:
                    return new Cell(NewAffinity);

            }

        }

        /// <summary>
        /// Tries to convert a string to a cell, returning a null cell if the parse fails
        /// </summary>
        /// <param name="Value">The string representation of the cell</param>
        /// <param name="NewAffinity">The affinity the string will be cast to</param>
        /// <returns>The cell form the converted value</returns>
        public static Cell TryParse(string Value, CellAffinity NewAffinity)
        {
            try { return Cell.Parse(Value, NewAffinity); }
            catch { return new Cell(NewAffinity); }
        }

        /// <summary>
        /// Unboxes an object to a cell
        /// </summary>
        /// <param name="Value">The object that will be cast to a cell</param>
        /// <returns>The cell form the converted value</returns>
        public static Cell UnBox(object Value)
        {

            if (Value is byte[])
                return new Cell((byte[])Value);

            // Type //
            TypeCode tc = Type.GetTypeCode(Value.GetType());

            // Set value //
            switch (tc)
            {

                case TypeCode.Boolean:
                    return new Cell((bool)Value);

                case TypeCode.Single:
                case TypeCode.Double:
                    return new Cell((double)Value);

                case TypeCode.Byte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.SByte:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                    return new Cell((long)Value);

                case TypeCode.DateTime:
                    return new Cell((DateTime)Value);

                case TypeCode.String:
                    return new Cell(Value.ToString());

                default:
                    return new Cell(CellAffinity.INT);

            }

        }

        /// <summary>
        /// Tries to unbox an object, if it fails it returns a new cell that is null
        /// </summary>
        /// <param name="Value">Object that will be cast to a cell</param>
        /// <returns>The cell form the converted value</returns>
        public static Cell TryUnBox(object Value)
        {
            try { return UnBox(Value); }
            catch { return new Cell(CellAffinity.INT); }
        }

        /// <summary>
        /// Unboxes an object into a specific affinity's cell
        /// </summary>
        /// <param name="Value">The object to be cast</param>
        /// <param name="NewAffinity">The affinity to be cast into</param>
        /// <returns>The cell form the converted value</returns>
        public static Cell UnBoxInto(object Value, CellAffinity NewAffinity)
        {

            // Check for null //
            if (Value == null)
                return new Cell(NewAffinity);

            // Set value //
            switch (NewAffinity)
            {
                case CellAffinity.BOOL: return new Cell((bool)Value);
                case CellAffinity.INT: return new Cell((int)Value);
                case CellAffinity.DOUBLE: return new Cell(Convert.ToDouble(Value));
                case CellAffinity.DATE_TIME: return new Cell((DateTime)Value);
                case CellAffinity.STRING: return new Cell(Value.ToString());
                case CellAffinity.BLOB: return new Cell((byte[])Value);
                default: return new Cell(CellAffinity.INT);
            }

        }

        /// <summary>
        /// Tries to unbox an object into a specific affinity's cell; if the unboxing failes, returns a null cell
        /// </summary>
        /// <param name="Value">The object to be cast</param>
        /// <param name="NewAffinity">The affinity to be cast into</param>
        /// <returns>The cell form the converted value</returns>
        public static Cell TryUnBoxInto(object Value, CellAffinity NewAffinity)
        {
            try { return UnBoxInto(Value, NewAffinity); }
            catch { return new Cell(NewAffinity); }
        }

        /// <summary>
        /// Convets the string 'xyz' to xyz, abc to abc, '123 to '123, 789' to 789'
        /// </summary>
        /// <param name="t">The string whose quotes will be removed</param>
        /// <returns>A string with the first and last quotes removed</returns>
        internal static string RemoveFirstLastQuotes(string t)
        {
            if (t.Length < 2)
                return t;
            if (t.First() == '\'' && t.Last() == '\'')
            {
                if (t.Length <= 2) return string.Empty;
                return t.Substring(1, t.Length - 2);
            }
            return t;
        }

        /// <summary>
        /// Compares to two byte arrays
        /// </summary>
        /// <param name="A">The left byte array</param>
        /// <param name="B">The right byte array</param>
        /// <returns>An interger</returns>
        internal static int ByteArrayCompare(byte[] A, byte[] B)
        {

            if (A.Length != B.Length)
                return A.Length - B.Length;

            int c = 0;
            for (int i = 0; i < A.Length; i++)
            {
                c = A[i] - B[i];
                if (c != 0)
                    return c;
            }
            return 0;

        }

        /// <summary>
        /// Parses a string into a date time variable with the form YYYY-MM-DD or YYYY-MM-DD:HH:MM:SS or YYYY-MM-DD:HH:MM:SS:LL, where '-' may be '-','\','/', or '#'
        /// </summary>
        /// <param name="Value">The string to be parsed</param>
        /// <returns>A date time cell</returns>
        internal static Cell DateParse(string Value)
        {

            char delim = '-';
            if (Value.Contains('-'))
                delim = '-';
            else if (Value.Contains('\\'))
                delim = '\\';
            else if (Value.Contains('/'))
                delim = '/';
            else if (Value.Contains('#'))
                delim = '#';
            else
                throw new FormatException("Expecting the data string to contain either -, \\, / or #");

            string[] s = Value.Replace("'", "").Split(delim, ':', '.', ' ');
            int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0, millisecond = 0;
            if (s.Length == 3)
            {
                year = int.Parse(s[0]);
                month = int.Parse(s[1]);
                day = int.Parse(s[2]);
            }
            else if (s.Length == 6)
            {
                year = int.Parse(s[0]);
                month = int.Parse(s[1]);
                day = int.Parse(s[2]);
                hour = int.Parse(s[3]);
                minute = int.Parse(s[4]);
                second = int.Parse(s[5]);
            }
            else if (s.Length == 7)
            {
                year = int.Parse(s[0]);
                month = int.Parse(s[1]);
                day = int.Parse(s[2]);
                hour = int.Parse(s[3]);
                minute = int.Parse(s[4]);
                second = int.Parse(s[5]);
                millisecond = int.Parse(s[6]);
            }
            else
                return NULL_DATE;

            if (year >= 1 && year <= 9999 && month >= 1 && month <= 12 && day >= 1 && day <= 31 && hour >= 0 && minute >= 0 && second >= 0 && millisecond >= 0)
            {
                return new Cell(new DateTime(year, month, day, hour, minute, second, millisecond));
            }
            return NULL_DATE;

        }

        /// <summary>
        /// Converts a date to either YYYY-MM-DD or YYYY-MM-DD:HH:MM:SS.LLLLLLLLL 
        /// </summary>
        /// <param name="Value">A .Net date time</param>
        /// <param name="DataTimeStringType">0 == YearMonthDayHourMinuteSecondMillisecond, 1 == YearMonthDay, 2 == include HourMinuteSecondMillisecond</param>
        /// <returns>A string value of the date</returns>
        internal static string DateString(DateTime Value, int DataTimeStringType)
        {

            StringBuilder sb = new StringBuilder();

            // Handle YYYY-MM-DD //
            if (Cell.DATE_FORMAT == 0 || Cell.DATE_FORMAT == 1)
            {
                sb.Append(Value.Year.ToString().PadLeft(4, '0'));
                sb.Append("-");
                sb.Append(Value.Month.ToString().PadLeft(2, '0'));
                sb.Append("-");
                sb.Append(Value.Day.ToString().PadLeft(2, '0'));
            }

            // Handle needing HH:MM:SS.LLLLLLL //
            if (Cell.DATE_FORMAT == 0 || Cell.DATE_FORMAT == 2)
            {
                sb.Append(":");
                sb.Append(Value.Hour.ToString().PadLeft(2, '0'));
                sb.Append(":");
                sb.Append(Value.Minute.ToString().PadLeft(2, '0'));
                sb.Append(":");
                sb.Append(Value.Second.ToString().PadLeft(2, '0'));
                sb.Append(".");
                sb.Append(Value.Millisecond);
            }

            return sb.ToString();

        }

        /// <summary>
        /// Converts a hex literal string '0x0000' to a byte array
        /// </summary>
        /// <param name="Value">Hexidecimal string</param>
        /// <returns>Byte array</returns>
        internal static Cell ByteParse(string Value)
        {
            if (Value.Length == 0)
                return new Cell(CellAffinity.BLOB);

            Value = Value.Replace("0x", "").Replace("0X", "");
            byte[] b = new byte[(Value.Length) / 2];

            for (int i = 0; i < Value.Length; i += 2)
                b[i / 2] = Convert.ToByte(Value.Substring(i, 2), 16);

            return new Cell(b);
        }

        /// <summary>
        /// Tests if a string is numeric
        /// </summary>
        /// <param name="Value">The text to test</param>
        /// <param name="AllowDot">Allows a '.' in the 'Value' parameter; if true then it will return true for 3.1415; false would return false for the same number; either true or false will return true for 20161105</param>
        /// <returns></returns>
        internal static bool IsNumeric(string Value, bool AllowDot)
        {

            if (Value == "." && AllowDot)
                return false;

            for (int i = 0; i < Value.Length; i++)
            {
                if (!char.IsDigit(Value, i) && ((AllowDot && Value[i] != '.') || !AllowDot))
                    return false;
            }
            return true;
        }

        #endregion

        #region Operators

        /*
         * if any of the below say they 'dont work', that just means they return a null value
         * 
         * All opperations returning a Cell will return a null Cell if either A or B are null (the affinity based on A)
         * +: add, returns null for date, blob and boolean; returns concatenate for strings
         * -: subtract, returns null for boolean and blob and string; resturn a long for ticks difference for dates
         * *: multiply, returns null for date, blob, string and boolean
         * /: divide, returns null for date, blob, string and boolean
         * %: mod, returns null for date, blob, string, boolean, and double
         * ^: xor, works for all types, may return nonsense for double/datetime, good for encrypting strings
         * &: and, works for all types, may return nonsense for double/datetime, good for encrypting strings
         * |: or, works for all types, may return nonsense for double/datetime, good for encrypting strings
         * ==: equals, works for all types
         * !=: not equals, works for all types
         * <: less than, works for all types
         * <=: less than or equals, works for all types
         * >: greater than, works for all types
         * >=: greater than or equals, works for all types
         * true/false: all types
         * ++/--: only for numerics
         */

        /// <summary>
        /// Performs the 'NOT' opperation, will return for null for DATE_TIME, STRING, and BLOBs
        /// </summary>
        /// <param name="C">A cell</param>
        /// <returns>A cell</returns>
        public static Cell operator !(Cell C)
        {

            // Check nulls //
            if (C.NULL == 1)
                return C;

            if (C.AFFINITY == CellAffinity.DOUBLE)
                C.DOUBLE = -C.DOUBLE;
            else if (C.AFFINITY == CellAffinity.INT)
                C.INT = -C.INT;
            else if (C.AFFINITY == CellAffinity.BOOL)
                C.BOOL = !C.BOOL;
            else
                C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Adds two cells together for INT and DOUBLE, concatentates strings, returns null otherwise
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>Cell result</returns>
        public static Cell operator +(Cell C1, Cell C2)
        {

            // Check nulls //
            if (C1.NULL == 1)
                return C1;
            else if (C2.NULL == 1)
                return C2;

            // If affinities match //
            if (C1.AFFINITY == C2.AFFINITY)
            {

                if (C1.AFFINITY == CellAffinity.DOUBLE)
                {
                    C1.DOUBLE += C2.DOUBLE;
                }
                else if (C1.AFFINITY == CellAffinity.INT)
                {
                    C1.INT += C2.INT;
                }
                else if (C1.AFFINITY == CellAffinity.STRING)
                {
                    C1.STRING += C2.STRING;
                }
                else if (C1.AFFINITY == CellAffinity.BLOB)
                {
                    byte[] b = new byte[C1.BLOB.Length + C2.BLOB.Length];
                    Array.Copy(C1.BLOB, 0, b, 0, C1.BLOB.Length);
                    Array.Copy(C2.BLOB, 0, b, C1.BLOB.Length, C2.BLOB.Length);
                    C1 = new Cell(b);
                }
                else
                    C1.NULL = 1;

            }
            else
            {

                if (C1.AFFINITY == CellAffinity.STRING || C2.AFFINITY == CellAffinity.STRING)
                {
                    C1.STRING = C1.valueSTRING + C2.valueSTRING;
                    C1.AFFINITY = CellAffinity.STRING;
                }
                else if (C1.AFFINITY == CellAffinity.BLOB || C2.AFFINITY == CellAffinity.BLOB)
                {
                    byte[] b = new byte[C1.valueBLOB.Length + C2.valueBLOB.Length];
                    Array.Copy(C1.valueBLOB, 0, b, 0, C1.valueBLOB.Length);
                    Array.Copy(C2.valueBLOB, 0, b, C1.valueBLOB.Length, C2.valueBLOB.Length);
                    C1 = new Cell(b);
                }
                else if (C1.AFFINITY == CellAffinity.DOUBLE || C2.AFFINITY == CellAffinity.DOUBLE)
                {
                    C1.DOUBLE = C1.valueDOUBLE + C2.valueDOUBLE;
                    C1.AFFINITY = CellAffinity.DOUBLE;
                }
                else if (C1.AFFINITY == CellAffinity.INT || C2.AFFINITY == CellAffinity.INT)
                {
                    C1.INT = C1.valueINT + C2.valueINT;
                    C1.AFFINITY = CellAffinity.INT;
                }
                else if (C1.AFFINITY == CellAffinity.DATE_TIME || C2.AFFINITY == CellAffinity.DATE_TIME)
                {
                    C1.AFFINITY = CellAffinity.DATE_TIME;
                    C1.NULL = 1;
                }
                else
                {
                    C1.AFFINITY = CellAffinity.BOOL;
                    C1.NULL = 1;
                }

            }

            // Fix nulls //
            if (C1.NULL == 1)
            {
                C1.ULONG = 0;
                C1.STRING = "";
            }
            return C1;

        }

        /// <summary>
        /// Converts either an INT or DOUBLE to a positve value, returns the cell passed otherwise
        /// </summary>
        /// <param name="C">A cell</param>
        /// <returns>Cell result</returns>
        public static Cell operator +(Cell C)
        {

            // Check nulls //
            if (C.NULL == 1)
                return C;

            if (C.AFFINITY == CellAffinity.DOUBLE)
                C.DOUBLE = +C.DOUBLE;
            else if (C.AFFINITY == CellAffinity.INT)
                C.INT = +C.INT;
            else
                C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Adds one to the given cell for an INT or DOUBLE, returns the cell passed otherwise
        /// </summary>
        /// <param name="C">The cell argument</param>
        /// <returns>Cell result</returns>
        public static Cell operator ++(Cell C)
        {
            if (C.NULL == 1)
                return C;
            if (C.AFFINITY == CellAffinity.DOUBLE)
                C.DOUBLE++;
            else if (C.AFFINITY == CellAffinity.INT)
                C.INT++;
            return C;
        }

        /// <summary>
        /// Subtracts two cells together for INT and DOUBLE, repalces instances of C2 in C1, for date times, return the tick count difference as an INT
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>Cell result</returns>
        public static Cell operator -(Cell C1, Cell C2)
        {

            // Check nulls //
            if (C1.NULL == 1) return C1;
            else if (C2.NULL == 1) return C2;

            // If affinities match //
            if (C1.AFFINITY == C2.AFFINITY)
            {
                if (C1.AFFINITY == CellAffinity.DOUBLE)
                {
                    C1.DOUBLE -= C2.DOUBLE;
                }
                else if (C1.AFFINITY == CellAffinity.INT)
                {
                    C1.INT -= C2.INT;
                }
                else if (C1.AFFINITY == CellAffinity.DATE_TIME)
                {
                    C1.INT = C1.INT - C2.INT;
                    C1.AFFINITY = CellAffinity.INT;
                }
                else if (C1.AFFINITY == CellAffinity.STRING)
                {
                    C1.STRING = C1.STRING.Replace(C2.STRING, "");
                }
                else
                {
                    C1.NULL = 1;
                }

            }
            else
            {

                if (C1.AFFINITY == CellAffinity.STRING || C2.AFFINITY == CellAffinity.STRING)
                {
                    C1.STRING = C1.valueSTRING.Replace(C2.valueSTRING, "");
                    C1.AFFINITY = CellAffinity.STRING;
                }
                else if (C1.AFFINITY == CellAffinity.BLOB || C2.AFFINITY == CellAffinity.BLOB)
                {
                    C1.AFFINITY = CellAffinity.BLOB;
                    C1.NULL = 1;
                }
                else if (C1.AFFINITY == CellAffinity.DOUBLE || C2.AFFINITY == CellAffinity.DOUBLE)
                {
                    C1.DOUBLE = C1.valueDOUBLE - C2.valueDOUBLE;
                    C1.AFFINITY = CellAffinity.DOUBLE;
                }
                else if (C1.AFFINITY == CellAffinity.INT || C2.AFFINITY == CellAffinity.INT)
                {
                    C1.INT = C1.valueINT - C2.valueINT;
                    C1.AFFINITY = CellAffinity.INT;
                }
                else if (C1.AFFINITY == CellAffinity.DATE_TIME || C2.AFFINITY == CellAffinity.DATE_TIME)
                {
                    C1.AFFINITY = CellAffinity.DATE_TIME;
                    C1.NULL = 1;
                }
                else
                {
                    C1.AFFINITY = CellAffinity.BOOL;
                    C1.NULL = 1;
                }

            }

            // Fix nulls //
            if (C1.NULL == 1)
            {
                C1.ULONG = 0;
                C1.STRING = "";
            }

            return C1;

        }

        /// <summary>
        /// Converts either an INT or DOUBLE to a negative value, returns the cell passed otherwise
        /// </summary>
        /// <param name="C">A cell</param>
        /// <returns>Cell result</returns>
        public static Cell operator -(Cell C)
        {

            // Check nulls //
            if (C.NULL == 1)
                return C;

            if (C.AFFINITY == CellAffinity.DOUBLE)
                C.DOUBLE = -C.DOUBLE;
            else if (C.AFFINITY == CellAffinity.INT)
                C.INT = -C.INT;
            else if (C.AFFINITY == CellAffinity.STRING)
                C.STRING = new string(C.STRING.Reverse().ToArray());
            else
                C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Subtracts one to the given cell for an INT or DOUBLE, returns the cell passed otherwise
        /// </summary>
        /// <param name="C">The cell argument</param>
        /// <returns>Cell result</returns>
        public static Cell operator --(Cell C)
        {
            if (C.NULL == 1)
                return C;
            if (C.AFFINITY == CellAffinity.DOUBLE)
                C.DOUBLE--;
            else if (C.AFFINITY == CellAffinity.INT)
                C.INT--;
            return C;
        }

        /// <summary>
        /// Multiplies two cells together for INT and DOUBLE; if C1 is a string and C2 is either int/double, repeats the string C2 times; 
        /// otherwise, returns the cell passed otherwise
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>Cell result</returns>
        public static Cell operator *(Cell C1, Cell C2)
        {

            // Check nulls //
            if (C1.NULL == 1) return C1;
            else if (C2.NULL == 1) return C2;

            // If affinities match //
            if (C1.AFFINITY == C2.AFFINITY)
            {

                if (C1.AFFINITY == CellAffinity.DOUBLE)
                {
                    C1.DOUBLE *= C2.DOUBLE;
                }
                else if (C1.AFFINITY == CellAffinity.INT)
                {
                    C1.INT *= C2.INT;
                }
                else
                {
                    C1.NULL = 1;
                }

            }
            else
            {

                if (C1.AFFINITY == CellAffinity.STRING || C2.AFFINITY == CellAffinity.STRING)
                {
                    C1.AFFINITY = CellAffinity.STRING;
                    C1.NULL = 1;
                }
                else if (C1.AFFINITY == CellAffinity.BLOB || C2.AFFINITY == CellAffinity.BLOB)
                {
                    C1.AFFINITY = CellAffinity.BLOB;
                    C1.NULL = 1;
                }
                else if (C1.AFFINITY == CellAffinity.DOUBLE || C2.AFFINITY == CellAffinity.DOUBLE)
                {
                    C1.DOUBLE = C1.valueDOUBLE * C2.valueDOUBLE;
                    C1.AFFINITY = CellAffinity.DOUBLE;
                }
                else if (C1.AFFINITY == CellAffinity.INT || C2.AFFINITY == CellAffinity.INT)
                {
                    C1.INT = C1.valueINT * C2.valueINT;
                    C1.AFFINITY = CellAffinity.INT;
                }
                else if (C1.AFFINITY == CellAffinity.DATE_TIME || C2.AFFINITY == CellAffinity.DATE_TIME)
                {
                    C1.AFFINITY = CellAffinity.DATE_TIME;
                    C1.NULL = 1;
                }
                else
                {
                    C1.AFFINITY = CellAffinity.BOOL;
                    C1.NULL = 1;
                }

            }

            // Fix nulls //
            if (C1.NULL == 1)
            {
                C1.ULONG = 0;
                C1.STRING = "";
            }

            return C1;

        }

        /// <summary>
        /// Divides two cells together for INT and DOUBLE, returns the cell passed otherwise as null
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>Cell result</returns>
        public static Cell operator /(Cell C1, Cell C2)
        {

            // Check nulls //
            if (C1.NULL == 1) return C1;
            else if (C2.NULL == 1) return C2;

            // If affinities match //
            if (C1.AFFINITY == C2.AFFINITY)
            {

                if (C1.AFFINITY == CellAffinity.DOUBLE && C2.DOUBLE != 0)
                {
                    C1.DOUBLE /= C2.DOUBLE;
                }
                else if (C1.AFFINITY == CellAffinity.INT && C2.INT != 0)
                {
                    C1.INT /= C2.INT;
                }
                else
                {
                    C1.NULL = 1;
                }

            }
            else
            {

                if (C1.AFFINITY == CellAffinity.STRING || C2.AFFINITY == CellAffinity.STRING)
                {
                    C1.AFFINITY = CellAffinity.STRING;
                    C1.NULL = 1;
                }
                else if (C1.AFFINITY == CellAffinity.BLOB || C2.AFFINITY == CellAffinity.BLOB)
                {
                    C1.AFFINITY = CellAffinity.BLOB;
                    C1.NULL = 1;
                }
                else if (C1.AFFINITY == CellAffinity.DOUBLE || C2.AFFINITY == CellAffinity.DOUBLE)
                {

                    if (C2.valueDOUBLE != 0)
                    {
                        C1.DOUBLE = C1.valueDOUBLE / C2.valueDOUBLE;
                    }
                    else
                    {
                        C1.NULL = 1;
                    }
                    C1.AFFINITY = CellAffinity.DOUBLE;


                }
                else if (C1.AFFINITY == CellAffinity.INT || C2.AFFINITY == CellAffinity.INT)
                {

                    if (C2.valueINT != 0)
                    {
                        C1.INT = C1.valueINT / C2.valueINT;
                    }
                    else
                    {
                        C1.NULL = 1;
                    }
                    C1.AFFINITY = CellAffinity.INT;

                }
                else if (C1.AFFINITY == CellAffinity.DATE_TIME || C2.AFFINITY == CellAffinity.DATE_TIME)
                {
                    C1.AFFINITY = CellAffinity.DATE_TIME;
                    C1.NULL = 1;
                }
                else
                {
                    C1.AFFINITY = CellAffinity.BOOL;
                    C1.NULL = 1;
                }

            }

            // Fix nulls //
            if (C1.NULL == 1)
            {
                C1.ULONG = 0;
                C1.STRING = "";
            }

            return C1;

        }

        /// <summary>
        /// Divides two cells together for INT and DOUBLE, returns the cell passed otherwise as null; if C2 is 0, then it returns 0
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>Cell result</returns>
        public static Cell CheckDivide(Cell C1, Cell C2)
        {

            // Check nulls //
            if (C1.NULL == 1)
                return C1;
            else if (C2.NULL == 1)
                return C2;

            // If affinities match //
            if (C1.AFFINITY == C2.AFFINITY)
            {

                if (C1.AFFINITY == CellAffinity.DOUBLE && C2.DOUBLE != 0)
                {
                    C1.DOUBLE /= C2.DOUBLE;
                }
                else if (C1.AFFINITY == CellAffinity.DOUBLE)
                {
                    C1.DOUBLE = 0;
                }
                else if (C1.AFFINITY == CellAffinity.INT && C2.INT != 0)
                {
                    C1.INT /= C2.INT;
                }
                else if (C1.AFFINITY == CellAffinity.INT)
                {
                    C1.INT = 0;
                }
                else
                {
                    C1.NULL = 1;
                }

            }
            else
            {

                if (C1.AFFINITY == CellAffinity.STRING || C2.AFFINITY == CellAffinity.STRING)
                {
                    C1.AFFINITY = CellAffinity.STRING;
                    C1.NULL = 1;
                }
                else if (C1.AFFINITY == CellAffinity.BLOB || C2.AFFINITY == CellAffinity.BLOB)
                {
                    C1.AFFINITY = CellAffinity.BLOB;
                    C1.NULL = 1;
                }
                else if (C1.AFFINITY == CellAffinity.DOUBLE || C2.AFFINITY == CellAffinity.DOUBLE)
                {

                    if (C2.valueDOUBLE != 0)
                    {
                        C1.DOUBLE = C1.valueDOUBLE / C2.valueDOUBLE;
                    }
                    else
                    {
                        C1.DOUBLE = 0D;
                    }
                    C1.AFFINITY = CellAffinity.DOUBLE;

                }
                else if (C1.AFFINITY == CellAffinity.INT || C2.AFFINITY == CellAffinity.INT)
                {

                    if (C2.valueINT != 0)
                    {
                        C1.INT = C1.valueINT / C2.valueINT;
                    }
                    else
                    {
                        C1.INT = 0;
                    }
                    C1.AFFINITY = CellAffinity.INT;

                }
                else if (C1.AFFINITY == CellAffinity.DATE_TIME || C2.AFFINITY == CellAffinity.DATE_TIME)
                {
                    C1.AFFINITY = CellAffinity.DATE_TIME;
                    C1.NULL = 1;
                }
                else
                {
                    C1.AFFINITY = CellAffinity.BOOL;
                    C1.NULL = 1;
                }

            }

            // Fix nulls //
            if (C1.NULL == 1)
            {
                C1.ULONG = 0;
                C1.STRING = "";
            }

            return C1;

        }

        /// <summary>
        /// Perform modulo between two cells together for INT and DOUBLE, returns the cell passed otherwise
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>Cell result</returns>
        public static Cell operator %(Cell C1, Cell C2)
        {

            // Check nulls //
            if (C1.NULL == 1) return C1;
            else if (C2.NULL == 1) return C2;

            // If affinities match //
            if (C1.AFFINITY == C2.AFFINITY)
            {

                if (C1.AFFINITY == CellAffinity.DOUBLE && C2.DOUBLE != 0)
                {
                    C1.DOUBLE %= C2.DOUBLE;
                }
                else if (C1.AFFINITY == CellAffinity.INT && C2.INT != 0)
                {
                    C1.INT %= C2.INT;
                }
                else
                {
                    C1.NULL = 1;
                }

            }
            else
            {

                if (C1.AFFINITY == CellAffinity.STRING || C2.AFFINITY == CellAffinity.STRING)
                {
                    C1.AFFINITY = CellAffinity.STRING;
                    C1.NULL = 1;
                }
                else if (C1.AFFINITY == CellAffinity.BLOB || C2.AFFINITY == CellAffinity.BLOB)
                {
                    C1.AFFINITY = CellAffinity.BLOB;
                    C1.NULL = 1;
                }
                else if (C1.AFFINITY == CellAffinity.DOUBLE || C2.AFFINITY == CellAffinity.DOUBLE)
                {

                    if (C2.valueDOUBLE != 0)
                    {
                        C1.DOUBLE = C1.valueDOUBLE % C2.valueDOUBLE;
                    }
                    else
                    {
                        C1.NULL = 1;
                    }
                    C1.AFFINITY = CellAffinity.DOUBLE;

                }
                else if (C1.AFFINITY == CellAffinity.INT || C2.AFFINITY == CellAffinity.INT)
                {

                    if (C2.valueINT != 0)
                    {
                        C1.INT = C1.valueINT % C2.valueINT;
                    }
                    else
                    {
                        C1.NULL = 1;
                    }
                    C1.AFFINITY = CellAffinity.INT;

                }
                else if (C1.AFFINITY == CellAffinity.DATE_TIME || C2.AFFINITY == CellAffinity.DATE_TIME)
                {
                    C1.AFFINITY = CellAffinity.DATE_TIME;
                    C1.NULL = 1;
                }
                else
                {
                    C1.AFFINITY = CellAffinity.BOOL;
                    C1.NULL = 1;
                }

            }

            // Fix nulls //
            if (C1.NULL == 1)
            {
                C1.ULONG = 0;
                C1.STRING = "";
            }

            return C1;

        }

        /// <summary>
        /// Return the bitwise AND for all types
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>Cell result</returns>
        public static Cell operator &(Cell C1, Cell C2)
        {

            // Check nulls //
            if (C1.NULL == 1)
                return C1;
            else if (C2.NULL == 1)
                return C2;

            // If neither a string or blob //
            if (C1.AFFINITY != CellAffinity.STRING && C2.AFFINITY != CellAffinity.STRING
                && C1.AFFINITY != CellAffinity.BLOB && C2.AFFINITY != CellAffinity.BLOB)
            {

                C1.INT = C1.INT & C2.INT;
                if (C1.AFFINITY == CellAffinity.DOUBLE || C2.AFFINITY == CellAffinity.DOUBLE)
                    C1.AFFINITY = CellAffinity.DOUBLE;
                else if (C1.AFFINITY == CellAffinity.INT || C2.AFFINITY == CellAffinity.INT)
                    C1.AFFINITY = CellAffinity.INT;
                else if (C1.AFFINITY == CellAffinity.DATE_TIME || C2.AFFINITY == CellAffinity.DATE_TIME)
                    C1.AFFINITY = CellAffinity.DATE_TIME;
                else if (C1.AFFINITY == CellAffinity.BOOL || C2.AFFINITY == CellAffinity.BOOL)
                    C1.AFFINITY = CellAffinity.BOOL;

            }
            else if (C1.AFFINITY == CellAffinity.STRING || C2.AFFINITY == CellAffinity.STRING)
            {

                StringBuilder sb = new StringBuilder();
                int t = 0;
                for (int i = 0; i < C1.STRING.Length; i++)
                {
                    if (t >= C2.valueSTRING.Length)
                        t = 0;
                    sb.Append((char)(C1.valueSTRING[i] & C2.valueSTRING[t]));
                    t++;
                }
                C1.STRING = sb.ToString();

            }
            else
            {

                int t = 0;
                byte[] b = C2.valueBLOB;
                for (int i = 0; i < C1.BLOB.Length; i++)
                {
                    if (t >= b.Length)
                        t = 0;
                    C1.BLOB[i] = (byte)(C1.BLOB[i] & b[t]);
                    t++;
                }

            }

            return C1;

        }

        /// <summary>
        /// Returns the bitwise OR for all types
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>Cell result</returns>
        public static Cell operator |(Cell C1, Cell C2)
        {

            // Check nulls //
            if (C1.NULL == 1) return C1;
            else if (C2.NULL == 1) return C2;

            // If neither a string or blob //
            if (C1.AFFINITY != CellAffinity.STRING && C2.AFFINITY != CellAffinity.STRING
                && C1.AFFINITY != CellAffinity.BLOB && C2.AFFINITY != CellAffinity.BLOB)
            {

                C1.INT = C1.INT | C2.INT;
                if (C1.AFFINITY == CellAffinity.DOUBLE || C2.AFFINITY == CellAffinity.DOUBLE)
                    C1.AFFINITY = CellAffinity.DOUBLE;
                else if (C1.AFFINITY == CellAffinity.INT || C2.AFFINITY == CellAffinity.INT)
                    C1.AFFINITY = CellAffinity.INT;
                else if (C1.AFFINITY == CellAffinity.DATE_TIME || C2.AFFINITY == CellAffinity.DATE_TIME)
                    C1.AFFINITY = CellAffinity.DATE_TIME;
                else if (C1.AFFINITY == CellAffinity.BOOL || C2.AFFINITY == CellAffinity.BOOL)
                    C1.AFFINITY = CellAffinity.BOOL;

            }
            else if (C1.AFFINITY == CellAffinity.STRING || C2.AFFINITY == CellAffinity.STRING)
            {
                StringBuilder sb = new StringBuilder();
                int t = 0;
                for (int i = 0; i < C1.STRING.Length; i++)
                {
                    if (t >= C2.valueSTRING.Length)
                        t = 0;
                    sb.Append((char)(C1.STRING[i] | C2.valueSTRING[t]));
                    t++;
                }
                C1.STRING = sb.ToString();

            }
            else
            {

                int t = 0;
                byte[] b = C2.valueBLOB;
                for (int i = 0; i < C1.BLOB.Length; i++)
                {
                    if (t >= b.Length) t = 0;
                    C1.BLOB[i] = (byte)(C1.BLOB[i] | b[t]);
                    t++;
                }

            }

            return C1;

        }

        /// <summary>
        /// Returns the bitwise XOR for all types
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>Cell result</returns>
        public static Cell operator ^(Cell C1, Cell C2)
        {

            // Check nulls //
            if (C1.NULL == 1) return C1;
            else if (C2.NULL == 1) return C2;

            // If neither a string or blob //
            if (C1.AFFINITY != CellAffinity.STRING && C2.AFFINITY != CellAffinity.STRING
                && C1.AFFINITY != CellAffinity.BLOB && C2.AFFINITY != CellAffinity.BLOB)
            {

                C1.INT = C1.INT ^ C2.INT;
                if (C1.AFFINITY == CellAffinity.DOUBLE || C2.AFFINITY == CellAffinity.DOUBLE)
                    C1.AFFINITY = CellAffinity.DOUBLE;
                else if (C1.AFFINITY == CellAffinity.INT || C2.AFFINITY == CellAffinity.INT)
                    C1.AFFINITY = CellAffinity.INT;
                else if (C1.AFFINITY == CellAffinity.DATE_TIME || C2.AFFINITY == CellAffinity.DATE_TIME)
                    C1.AFFINITY = CellAffinity.DATE_TIME;
                else if (C1.AFFINITY == CellAffinity.BOOL || C2.AFFINITY == CellAffinity.BOOL)
                    C1.AFFINITY = CellAffinity.BOOL;

            }
            else if (C1.AFFINITY == CellAffinity.STRING || C2.AFFINITY == CellAffinity.STRING)
            {

                StringBuilder sb = new StringBuilder();
                int t = 0;
                for (int i = 0; i < C1.STRING.Length; i++)
                {
                    if (t >= C2.valueSTRING.Length) t = 0;
                    sb.Append((char)(C1.STRING[i] ^ C2.valueSTRING[t]));
                    t++;
                }
                C1.STRING = sb.ToString();

            }
            else
            {

                int t = 0;
                byte[] b = C2.valueBLOB;
                for (int i = 0; i < C1.BLOB.Length; i++)
                {
                    if (t >= b.Length) t = 0;
                    C1.BLOB[i] = (byte)(C1.BLOB[i] ^ b[t]);
                    t++;
                }

            }

            return C1;

        }

        /// <summary>
        /// Checks if two cells are equal
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>A boolean</returns>
        public static bool operator ==(Cell C1, Cell C2)
        {

            if (C1.NULL == 1 && C2.NULL == 1)
                return true;

            //if (C1.ULONG != C2.ULONG)
            //    return false;

            if (C1.AFFINITY != CellAffinity.STRING && C1.AFFINITY != CellAffinity.BLOB)
                return C1.INT == C2.INT;
            else if (C1.AFFINITY == CellAffinity.STRING)
                return C1.STRING == C2.valueSTRING;

            return Compare(C1, C2) == 0;

        }

        /// <summary>
        /// Checks if two cells are not equal
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>A boolean</returns>
        public static bool operator !=(Cell C1, Cell C2)
        {

            if (C1.NULL != C2.NULL)
                return true;

            if (C1.AFFINITY != CellAffinity.STRING && C1.AFFINITY != CellAffinity.BLOB)
                return C1.INT != C2.INT;
            else if (C1.AFFINITY == CellAffinity.STRING)
                return C1.STRING != C2.STRING;

            return Compare(C1, C2) != 0;

        }

        /// <summary>
        /// Checks if C1 is less than C2
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>A boolean</returns>
        public static bool operator <(Cell C1, Cell C2)
        {
            return Compare(C1, C2) < 0;
        }

        /// <summary>
        /// Checks if C1 is less than or equal to C2
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>A boolean</returns>
        public static bool operator <=(Cell C1, Cell C2)
        {
            return Compare(C1, C2) <= 0;
        }

        /// <summary>
        /// Checks if C1 is greater than C2
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>A boolean</returns>
        public static bool operator >(Cell C1, Cell C2)
        {
            return Compare(C1, C2) > 0;
        }

        /// <summary>
        /// Checks if C1 is greater than or equal to C2
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>A boolean</returns>
        public static bool operator >=(Cell C1, Cell C2)
        {
            return Compare(C1, C2) >= 0;
        }

        /// <summary>
        /// Determines whether or not a cell is 'TRUE'; if the cell is not null it returns the boolean value
        /// </summary>
        /// <param name="C">The cell value</param>
        /// <returns></returns>
        public static bool operator true(Cell C)
        {
            return C.NULL == 0 && C.BOOL;
        }

        /// <summary>
        /// Determines whether or not a cell is 'FALSE'; if the cell is null or the BOOL value is false, returns false
        /// </summary>
        /// <param name="C">The cell value</param>
        /// <returns></returns>
        public static bool operator false(Cell C)
        {
            return !(C.NULL == 0 && C.BOOL);
        }

        #endregion

        #region Functions

        /*
         * 
         * need:
         * -- Coalesce, IsNull, NullIf: works for all Cells
         * -- RequiredParameterCount, Max: works for all Cells
         * -- Cast: works for all Cells
         * -- Log, Log2, Log10: only works for numerics
         * -- Exp, Exp2, Exp10: only works for numerics
         * 
         */

        /// <summary>
        /// Performs the log base E; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Log(Cell C)
        {

            if (C.NULL == 1)
                return C;

            double d = Math.Log(C.valueDOUBLE);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C.NULL = 1;
                return C;
            }

            if (C.AFFINITY == CellAffinity.DOUBLE) C.DOUBLE = d;
            else if (C.AFFINITY == CellAffinity.INT) C.INT = (long)d;
            else C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the log base 2; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Log2(Cell C)
        {

            if (C.NULL == 1)
                return C;

            double d = Math.Log(C.valueDOUBLE) / Math.Log(2);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C.NULL = 1;
                return C;
            }

            if (C.AFFINITY == CellAffinity.DOUBLE) C.DOUBLE = d;
            else if (C.AFFINITY == CellAffinity.INT) C.INT = (long)d;
            else C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the log base 10; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Log10(Cell C)
        {

            if (C.NULL == 1)
                return C;

            double d = Math.Log10(C.valueDOUBLE);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C.NULL = 1;
                return C;
            }

            if (C.AFFINITY == CellAffinity.DOUBLE) C.DOUBLE = d;
            else if (C.AFFINITY == CellAffinity.INT) C.INT = (long)d;
            else C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the exponential base E; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Exp(Cell C)
        {

            if (C.NULL == 1)
                return C;

            double d = Math.Exp(C.valueDOUBLE);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C.NULL = 1;
                return C;
            }

            if (C.AFFINITY == CellAffinity.DOUBLE) C.DOUBLE = d;
            else if (C.AFFINITY == CellAffinity.INT) C.INT = (long)d;
            else C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the exponential base 2; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Exp2(Cell C)
        {

            if (C.NULL == 1)
                return C;

            double d = Math.Pow(2, C.valueDOUBLE);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C.NULL = 1;
                return C;
            }

            if (C.AFFINITY == CellAffinity.DOUBLE) C.DOUBLE = d;
            else if (C.AFFINITY == CellAffinity.INT) C.INT = (long)d;
            else C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the exponential base 10; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Exp10(Cell C)
        {

            if (C.NULL == 1)
                return C;

            double d = Math.Pow(10, C.valueDOUBLE);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C.NULL = 1;
                return C;
            }

            if (C.AFFINITY == CellAffinity.DOUBLE) C.DOUBLE = d;
            else if (C.AFFINITY == CellAffinity.INT) C.INT = (long)d;
            else C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the trigonomic sine; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Sin(Cell C)
        {

            if (C.NULL == 1)
                return C;

            double d = Math.Sin(C.valueDOUBLE);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C.NULL = 1;
                return C;
            }

            if (C.AFFINITY == CellAffinity.DOUBLE) C.DOUBLE = d;
            else if (C.AFFINITY == CellAffinity.INT) C.INT = (long)d;
            else C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the trigonomic cosine; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Cos(Cell C)
        {

            if (C.NULL == 1)
                return C;

            double d = Math.Cos(C.valueDOUBLE);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C.NULL = 1;
                return C;
            }

            if (C.AFFINITY == CellAffinity.DOUBLE) C.DOUBLE = d;
            else if (C.AFFINITY == CellAffinity.INT) C.INT = (long)d;
            else C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the trigonomic tangent; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Tan(Cell C)
        {

            if (C.NULL == 1)
                return C;

            double d = Math.Tan(C.valueDOUBLE);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C.NULL = 1;
                return C;
            }

            if (C.AFFINITY == CellAffinity.DOUBLE) C.DOUBLE = d;
            else if (C.AFFINITY == CellAffinity.INT) C.INT = (long)d;
            else C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the hyperbolic sine; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Sinh(Cell C)
        {

            if (C.NULL == 1)
                return C;

            double d = Math.Sinh(C.valueDOUBLE);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C.NULL = 1;
                return C;
            }

            if (C.AFFINITY == CellAffinity.DOUBLE) C.DOUBLE = d;
            else if (C.AFFINITY == CellAffinity.INT) C.INT = (long)d;
            else C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the hyperbolic cosine; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Cosh(Cell C)
        {

            if (C.NULL == 1)
                return C;

            double d = Math.Cosh(C.valueDOUBLE);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C.NULL = 1;
                return C;
            }

            if (C.AFFINITY == CellAffinity.DOUBLE) C.DOUBLE = d;
            else if (C.AFFINITY == CellAffinity.INT) C.INT = (long)d;
            else C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the hyperbolic tangent; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Tanh(Cell C)
        {

            if (C.NULL == 1)
                return C;

            double d = Math.Tanh(C.valueDOUBLE);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C.NULL = 1;
                return C;
            }

            if (C.AFFINITY == CellAffinity.DOUBLE) C.DOUBLE = d;
            else if (C.AFFINITY == CellAffinity.INT) C.INT = (long)d;
            else C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the square root; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Sqrt(Cell C)
        {

            if (C.NULL == 1)
                return C;

            double d = Math.Sqrt(C.valueDOUBLE);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C.NULL = 1;
                return C;
            }

            if (C.AFFINITY == CellAffinity.DOUBLE) C.DOUBLE = d;
            else if (C.AFFINITY == CellAffinity.INT) C.INT = (long)d;
            else C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the power; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C1">The base</param>
        /// <param name="C2">The exponent</param>
        /// <returns>Cell value</returns>
        public static Cell Power(Cell C1, Cell C2)
        {

            if (C1.NULL == 1)
                return C1;
            else if (C2.NULL == 1)
                return C2;

            double d = Math.Pow(C1.valueDOUBLE, C2.valueDOUBLE);
            if (double.IsInfinity(d) || double.IsNaN(d))
            {
                C1.NULL = 1;
                return C1;
            }

            if (C1.AFFINITY == CellAffinity.DOUBLE)
                C1.DOUBLE = d;
            else if (C1.AFFINITY == CellAffinity.INT)
                C1.INT = (long)d;
            else
                C1.NULL = 1;

            return C1;

        }

        /// <summary>
        /// Returns the absolute value of a cell's numeric value; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value</returns>
        public static Cell Abs(Cell C)
        {

            if (C.NULL == 1)
                return C;

            if (C.AFFINITY == CellAffinity.DOUBLE)
                C.DOUBLE = Math.Abs(C.DOUBLE);
            else if (C.AFFINITY == CellAffinity.INT)
                C.INT = Math.Abs(C.INT);
            else
                C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Returns the sign of a cell's numeric value; the resulting value will be null if the result is either nan or infinity; casts the result back to original affinity passed
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell value, NULL, +1, -1, or 0</returns>
        public static Cell Sign(Cell C)
        {

            if (C.NULL == 1)
                return C;

            if (C.AFFINITY == CellAffinity.DOUBLE)
                C.DOUBLE = Math.Sign(C.DOUBLE);
            else if (C.AFFINITY == CellAffinity.INT)
                C.INT = Math.Sign(C.INT);
            else
                C.NULL = 1;

            return C;

        }

        /// <summary>
        /// Performs the logic 'IF'
        /// </summary>
        /// <param name="A">Predicate: uses A.BOOL to perform the logical if</param>
        /// <param name="B">The value returned if A is true</param>
        /// <param name="C">The value returned if A is false</param>
        /// <returns>Aither B or C</returns>
        public static Cell If(Cell A, Cell B, Cell C)
        {
            if (A.BOOL)
                return B;
            if (B.AFFINITY != C.AFFINITY)
                return Cell.Cast(C, B.Affinity);
            return C;
        }

        /// <summary>
        /// Returns the smallest value of an array of cells
        /// </summary>
        /// <param name="Data">A collection of cells</param>
        /// <returns>A cell</returns>
        public static Cell Min(params Cell[] Data)
        {

            // Empty //
            if (Data == null) new Cell(CellAffinity.INT);
            if (Data.Length == 0) new Cell(CellAffinity.INT);

            // One value //
            if (Data.Length == 1) return Data[0];

            // Two values //
            if (Data.Length == 2 && Data[0] < Data[1]) return Data[0];
            if (Data.Length == 2) return Data[1];

            // Three or more //
            Cell c = Data[0];
            for (int i = 1; i < Data.Length; i++)
            {
                if (Data[i] < c)
                    c = Data[i];
            }
            if (Data[0].Affinity != c.Affinity)
                return Cell.Cast(c, Data[0].Affinity);
            return c;

        }

        /// <summary>
        /// Returns the largest value of an array of cells
        /// </summary>
        /// <param name="Data">A collection of cells</param>
        /// <returns>A cell</returns>
        public static Cell Max(params Cell[] Data)
        {

            // Empty //
            if (Data == null) new Cell(CellAffinity.INT);
            if (Data.Length == 0) new Cell(CellAffinity.INT);

            // One value //
            if (Data.Length == 1) return Data[0];

            // Two values //
            if (Data.Length == 2 && Data[0] > Data[1]) return Data[0];
            if (Data.Length == 2) return Data[1];

            // Three or more //
            Cell c = Data[0];
            for (int i = 1; i < Data.Length; i++)
            {
                if (Data[i] > c) c = Data[i];
            }
            if (Data[0].Affinity != c.Affinity)
                return Cell.Cast(c, Data[0].Affinity);
            return c;

        }

        /// <summary>
        /// Returns the cumulative AND value of an array of cells
        /// </summary>
        /// <param name="Data">A collection of cells</param>
        /// <returns>A cell</returns>
        public static Cell And(params Cell[] Data)
        {

            // Empty //
            if (Data == null) new Cell(false);
            if (Data.Length == 0) new Cell(false);

            // Three or more //
            bool b = true;
            for (int i = 0; i < Data.Length; i++)
            {
                b = b && Data[i].valueBOOL;
                if (!b) return new Cell(b);
            }
            return new Cell(b);

        }

        /// <summary>
        /// Returns the cumulative OR value of an array of cells
        /// </summary>
        /// <param name="Data">A collection of cells</param>
        /// <returns>A cell</returns>
        public static Cell Or(params Cell[] Data)
        {

            // Empty //
            if (Data == null) new Cell(false);
            if (Data.Length == 0) new Cell(false);

            // Three or more //
            bool b = false;
            for (int i = 0; i < Data.Length; i++)
            {
                b = b || Data[i].valueBOOL;
                if (b) return new Cell(b);
            }
            return new Cell(b);

        }

        /// <summary>
        /// Returns the sum of an array of cells
        /// </summary>
        /// <param name="Data">A collection of cells</param>
        /// <returns>A cell</returns>
        public static Cell Sum(params Cell[] Data)
        {

            // Empty //
            if (Data == null) new Cell(CellAffinity.INT);
            if (Data.Length == 0) new Cell(CellAffinity.INT);

            // One value //
            if (Data.Length == 1) return Data[0];

            // Two values //
            if (Data.Length == 2) return Data[0] + Data[1];

            // Three or more //
            Cell c = Data[0];
            for (int i = 1; i < Data.Length; i++)
            {
                c += Data[i];
            }
            return c;

        }

        /// <summary>
        /// Returns the first non-null cell in a collection
        /// </summary>
        /// <param name="Data">A collection of cells</param>
        /// <returns>A cell</returns>
        public static Cell Coalesce(params Cell[] Data)
        {

            // Empty //
            if (Data == null) new Cell(CellAffinity.INT);
            if (Data.Length == 0) new Cell(CellAffinity.INT);

            for (int i = 0; i < Data.Length; i++)
            {
                if (!Data[i].IsNull)
                {
                    if (Data[i].Affinity != Data[0].Affinity)
                        return Cell.Cast(Data[i], Data[0].Affinity);
                    return Data[i];
                }
            }
            return new Cell(Data[0].Affinity);

        }

        /// <summary>
        /// Casts a cell to another affinity
        /// </summary>
        /// <param name="C">The cell to cast</param>
        /// <param name="Type">The new affinity</param>
        /// <returns>Cell value</returns>
        public static Cell Cast(Cell C, CellAffinity Type)
        {

            // Check if types are the same //
            if (C.AFFINITY == Type)
                return C;

            // Check for nulls right away //
            if (C.NULL == 1)
            {
                C.AFFINITY = Type;
                return C;
            }

            /* BOOL:
             *  -> INT: 0 or 1
             *  -> DOUBLE: 0 or 1
             *  -> DATE: NULL
             *  -> STRING: ToString
             *  -> BLOB: Byte[0] = {0}
             * 
             */
            if (C.AFFINITY == CellAffinity.BOOL)
            {

                C.AFFINITY = Type;
                if (Type == CellAffinity.INT)
                {
                    C.INT = (C.BOOL ? 1 : 0);
                    return C;
                }
                else if (Type == CellAffinity.DOUBLE)
                {
                    C.DOUBLE = (C.BOOL ? 1D : 0D);
                    return C;
                }
                else if (Type == CellAffinity.STRING)
                {
                    C.STRING = C.BOOL.ToString();
                    C.INT_A = C.STRING.GetHashCode();
                    C.INT_B = C.STRING.Length;
                    return C;
                }
                else if (Type == CellAffinity.BLOB)
                {
                    C.INT = 0;
                    return new Cell(new byte[1] { C.BOOL ? (byte)1 : (byte)0 });
                }
                else
                {
                    C.INT = 0;
                    C.NULL = 1;
                    return C;
                }

            }
            /* INT:
             *  -> BOOL: 0 = false, true otherwise
             *  -> DOUBLE: (double)INT
             *  -> DATE: Date(INT)
             *  -> STRING: ToString
             *  -> BLOB: BitConverter
             * 
             */
            else if (C.AFFINITY == CellAffinity.INT)
            {

                C.AFFINITY = Type;
                if (Type == CellAffinity.BOOL)
                {
                    C.BOOL = (C.INT == 0 ? false : true);
                    return C;
                }
                else if (Type == CellAffinity.DOUBLE)
                {
                    C.DOUBLE = (double)C.INT;
                    return C;
                }
                else if (Type == CellAffinity.DATE_TIME)
                {
                    // Handle date out of range values
                    if (C.INT > 3155378975999999999L || C.INT < 0L)
                    {
                        C.INT = 0;
                        C.NULL = 1;
                    }
                    return C;
                }
                else if (Type == CellAffinity.STRING)
                {
                    C.STRING = C.INT.ToString();
                    C.INT_A = C.STRING.GetHashCode();
                    C.INT_B = C.STRING.Length;
                    return C;
                }
                else if (Type == CellAffinity.BLOB)
                {
                    return new Cell(BitConverter.GetBytes(C.INT)); // INT/DOUBLE/DATE_TIME have the same bytes[]
                }
                else
                {
                    C.INT = 0;
                    C.NULL = 1;
                    return C;
                }

            }
            /* DOUBLE:
             *  -> BOOL: 0 = false, true otherwise
             *  -> INT: (long)DOUBLE
             *  -> DATE: NULL
             *  -> STRING: ToString
             *  -> BLOB: BitConverter
             * 
             */
            else if (C.AFFINITY == CellAffinity.DOUBLE)
            {

                C.AFFINITY = Type;
                if (Type == CellAffinity.BOOL)
                {
                    C.BOOL = (C.DOUBLE == 0D ? false : true);
                    return C;
                }
                else if (Type == CellAffinity.INT)
                {
                    C.INT = (long)C.DOUBLE;
                    return C;
                }
                else if (Type == CellAffinity.DATE_TIME)
                {
                    C.INT = 0;
                    C.NULL = 1;
                    return C;
                }
                else if (Type == CellAffinity.STRING)
                {
                    C.STRING = C.DOUBLE.ToString();
                    C.INT_A = C.STRING.GetHashCode();
                    C.INT_B = C.STRING.Length;
                    return C;
                }
                else if (Type == CellAffinity.BLOB)
                {
                    return new Cell(BitConverter.GetBytes(C.INT)); // INT/DOUBLE/DATE_TIME have the same bytes[]
                }
                else
                {
                    C.INT = 0;
                    C.NULL = 1;
                    return C;
                }

            }
            /* DATE:
             *  -> BOOL: NULL
             *  -> INT: TICKS
             *  -> DOUBLE: NULL
             *  -> STRING: ToString
             *  -> BLOB: BitConverter
             * 
             */
            else if (C.AFFINITY == CellAffinity.DATE_TIME)
            {

                C.AFFINITY = Type;
                if (Type == CellAffinity.BOOL || Type == CellAffinity.DOUBLE)
                {
                    C.INT = 0;
                    C.NULL = 1;
                    return C;
                }
                else if (Type == CellAffinity.INT)
                {
                    return C;
                }
                else if (Type == CellAffinity.STRING)
                {
                    C.STRING = C.DATE_TIME.ToString();
                    C.INT_A = C.STRING.GetHashCode();
                    C.INT_B = C.STRING.Length;
                    return C;
                }
                else if (Type == CellAffinity.BLOB)
                {
                    return new Cell(BitConverter.GetBytes(C.INT)); // INT/DOUBLE/DATE_TIME have the same bytes[]
                }
                else
                {
                    C.INT = 0;
                    C.NULL = 1;
                    return C;
                }

            }
            /* STRING:
             *  -> BOOL: Parse
             *  -> INT: Parse
             *  -> DOUBLE: Parse
             *  -> DATE: Parse
             *  -> BLOB: BitConverter
             * 
             */
            else if (C.AFFINITY == CellAffinity.STRING)
            {

                C.AFFINITY = Type;
                if (Type == CellAffinity.BOOL || Type == CellAffinity.INT || Type == CellAffinity.DATE_TIME || Type == CellAffinity.DOUBLE)
                {
                    C = Cell.TryParse(C.STRING == null ? null : C.STRING.Trim(), Type);
                    //C.INT = 0;
                    return C;
                }
                else if (Type == CellAffinity.BLOB)
                {
                    return new Cell(System.Text.Encoding.Unicode.GetBytes(C.STRING.ToCharArray()));
                }
                else
                {
                    C.INT = 0;
                    C.NULL = 1;
                    return C;
                }

            }
            /* BLOB:
             *  -> BOOL: BitConverter
             *  -> INT: BitConverter
             *  -> DOUBLE: BitConverter
             *  -> DATE: BitConverter
             *  -> STRING: BitConverter
             * 
             */
            else if (C.AFFINITY == CellAffinity.BLOB)
            {

                C.AFFINITY = Type;
                if (Type == CellAffinity.INT || Type == CellAffinity.DOUBLE || Type == CellAffinity.DATE_TIME)
                {
                    if (C.BLOB.Length < 8)
                    {
                        byte[] b = new byte[8];
                        Array.Copy(C.BLOB, b, C.BLOB.Length);
                        C.BLOB = b;
                    }
                    long l = BitConverter.ToInt64(C.BLOB, 0);

                    // Handle date out of range values
                    if (Type == CellAffinity.DATE_TIME && (l > 3155378975999999999L || l < 0L))
                    {
                        C.INT = 0;
                        C.NULL = 1;
                        return C;
                    }
                    C.INT = l;
                    return C;

                }
                else if (Type == CellAffinity.BOOL)
                {

                    if (C.BLOB.Length < 1)
                    {
                        C.INT = 0;
                        C.NULL = 1;
                        return C;
                    }
                    C.BOOL = (C.BLOB[1] != 0);
                }
                else if (Type == CellAffinity.STRING)
                {

                    if (C.BLOB.Length % 2 != 0)
                    {
                        C.INT = 0;
                        C.NULL = 1;
                        return C;
                    }
                    C.STRING = System.Text.Encoding.Unicode.GetString(C.BLOB);
                    C.INT_A = C.STRING.GetHashCode();
                    C.INT_B = C.STRING.Length;
                    return C;
                }

                else
                {
                    C.INT = 0;
                    C.NULL = 1;
                    return C;
                }

            }

            C.INT = 0;
            C.NULL = 1;
            return C;

        }

        /// <summary>
        /// Extracts the year value of a date time cell, returns null for non-date cells
        /// </summary>
        /// <param name="C">A cell value</param>
        /// <returns>An integer cell</returns>
        public static Cell Year(Cell C)
        {
            if (C.Affinity != CellAffinity.DATE_TIME)
                return new Cell(CellAffinity.INT);
            return new Cell(C.valueDATE_TIME.Year);
        }

        /// <summary>
        /// Extracts the month value of a date time cell, returns null for non-date cells
        /// </summary>
        /// <param name="C">A cell value</param>
        /// <returns>An integer cell</returns>
        public static Cell Month(Cell C)
        {
            if (C.Affinity != CellAffinity.DATE_TIME) return new Cell(CellAffinity.INT);
            return new Cell(C.valueDATE_TIME.Month);
        }

        /// <summary>
        /// Extracts the day value of a date time cell, returns null for non-date cells
        /// </summary>
        /// <param name="C">A cell value</param>
        /// <returns>An integer cell</returns>
        public static Cell Day(Cell C)
        {
            if (C.Affinity != CellAffinity.DATE_TIME) return new Cell(CellAffinity.INT);
            return new Cell(C.valueDATE_TIME.Day);
        }

        /// <summary>
        /// Extracts the hour value of a date time cell, returns null for non-date cells
        /// </summary>
        /// <param name="C">A cell value</param>
        /// <returns>An integer cell</returns>
        public static Cell Hour(Cell C)
        {
            if (C.Affinity != CellAffinity.DATE_TIME) return new Cell(CellAffinity.INT);
            return new Cell(C.valueDATE_TIME.Hour);
        }

        /// <summary>
        /// Extracts the minute value of a date time cell, returns null for non-date cells
        /// </summary>
        /// <param name="C">A cell value</param>
        /// <returns>An integer cell</returns>
        public static Cell Minute(Cell C)
        {
            if (C.Affinity != CellAffinity.DATE_TIME) return new Cell(CellAffinity.INT);
            return new Cell(C.valueDATE_TIME.Minute);
        }

        /// <summary>
        /// Extracts the second value of a date time cell, returns null for non-date cells
        /// </summary>
        /// <param name="C">A cell value</param>
        /// <returns>An integer cell</returns>
        public static Cell Second(Cell C)
        {
            if (C.Affinity != CellAffinity.DATE_TIME) return new Cell(CellAffinity.INT);
            return new Cell(C.valueDATE_TIME.Second);
        }

        /// <summary>
        /// Extracts the millisecond value of a date time cell, returns null for non-date cells
        /// </summary>
        /// <param name="C">A cell value</param>
        /// <returns>An integer cell</returns>
        public static Cell Millisecond(Cell C)
        {
            if (C.Affinity != CellAffinity.DATE_TIME) return new Cell(CellAffinity.INT);
            return new Cell(C.valueDATE_TIME.Millisecond);
        }

        /// <summary>
        /// Converts a byte array to a string using UTF16 encoding
        /// </summary>
        /// <param name="Hash"></param>
        /// <returns></returns>
        internal static string ByteArrayToUTF16String(byte[] Hash)
        {

            byte[] to_convert = Hash;
            if (Hash.Length % 2 != 0)
            {
                to_convert = new byte[Hash.Length + 1];
                Array.Copy(Hash, to_convert, Hash.Length);
            }

            return ASCIIEncoding.BigEndianUnicode.GetString(to_convert);

        }

        #endregion

        #region StaticValues

        /// <summary>
        /// Returns the lowest possible value for the given affinity
        /// </summary>
        /// <param name="NewAffinity">Data type</param>
        /// <returns>Cell value</returns>
        public static Cell MinValue(CellAffinity NewAffinity)
        {

            Cell C = new Cell(NewAffinity);
            C.NULL = 0;
            switch (NewAffinity)
            {
                case CellAffinity.INT: C.INT = long.MinValue; break;
                case CellAffinity.DOUBLE: C.DOUBLE = double.MinValue; break;
                case CellAffinity.BOOL: C.BOOL = false; break;
                case CellAffinity.DATE_TIME: C.ULONG = 0; break;
                case CellAffinity.BLOB: C.BLOB = new byte[0]; break;
                default: C.STRING = ""; break;
            }
            return C;
        }

        /// <summary>
        /// Returns the highest possible value for the given affinity
        /// </summary>
        /// <param name="NewAffinity">Data type</param>
        /// <returns>Cell value</returns>
        public static Cell MaxValue(CellAffinity NewAffinity)
        {
            Cell C = new Cell(NewAffinity);
            C.NULL = 0;
            switch (NewAffinity)
            {
                case CellAffinity.INT: C.INT = long.MaxValue; break;
                case CellAffinity.DOUBLE: C.DOUBLE = double.MaxValue; break;
                case CellAffinity.BOOL: C.BOOL = true; break;
                case CellAffinity.DATE_TIME: C.DATE_TIME = DateTime.MaxValue; break;
                default: C.STRING = new string('\xFFFF', 1024); break; // will take care of max byte too
            }
            return C;
        }

        /// <summary>
        /// Returns a value representing zero
        /// </summary>
        /// <param name="NewAffinity">Data type</param>
        /// <returns>Cell value</returns>
        public static Cell ZeroValue(CellAffinity NewAffinity)
        {
            Cell C = new Cell(NewAffinity);
            C.NULL = 0;
            switch (NewAffinity)
            {
                case CellAffinity.INT: C.INT = 0; break;
                case CellAffinity.DOUBLE: C.DOUBLE = 0; break;
                case CellAffinity.BOOL: C.BOOL = false; break;
                case CellAffinity.DATE_TIME: C.ULONG = 0; break;
                case CellAffinity.BLOB: C.BLOB = new byte[0]; break;
                default: C.STRING = ""; break;
            }
            return C;
        }

        /// <summary>
        /// Returns a value representing one
        /// </summary>
        /// <param name="NewAffinity">Data type</param>
        /// <returns>Cell value</returns>
        public static Cell OneValue(CellAffinity NewAffinity)
        {
            Cell C = new Cell(NewAffinity);
            C.NULL = 0;
            switch (NewAffinity)
            {
                case CellAffinity.INT: C.INT = 1; break;
                case CellAffinity.DOUBLE: C.DOUBLE = 1; break;
                case CellAffinity.BOOL: C.BOOL = true; break;
                case CellAffinity.DATE_TIME: C.ULONG = 1; break;
                case CellAffinity.BLOB: C.BLOB = new byte[1] { 1 }; break;
                default: C.STRING = ""; break;
            }
            return C;
        }

        #endregion

        #region Implementations

        /// <summary>
        /// IComparable implementation
        /// </summary>
        /// <param name="C">A cell to compare to the current instance</param>
        /// <returns>An integer value</returns>
        int IComparable<Cell>.CompareTo(Cell C)
        {
            return Cell.Compare(this, C);
        }

        /// <summary>
        /// IComparer implementation that compares two cells
        /// </summary>
        /// <param name="C1">Left cell</param>
        /// <param name="C2">Right cell</param>
        /// <returns>An integer representing </returns>
        int IComparer<Cell>.Compare(Cell C1, Cell C2)
        {
            return Compare(C1, C2);
        }

        #endregion

        #region StringFunctions

        /// <summary>
        /// Trims a given string value
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell with a stirng affinity</returns>
        public static Cell Trim(Cell C)
        {
            return new Cell(C.valueSTRING.Trim());
        }

        /// <summary>
        /// Converts a given string to uppercase
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell with a stirng affinity</returns>
        public static Cell ToUpper(Cell C)
        {
            return new Cell(C.valueSTRING.ToUpper());
        }

        /// <summary>
        /// Converts a given string to lowercase
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <returns>Cell with a stirng affinity</returns>
        public static Cell ToLower(Cell C)
        {
            return new Cell(C.valueSTRING.ToLower());
        }

        /// <summary>
        /// Returns all characters/bytes left of given point
        /// </summary>
        /// <param name="C">The string or BLOB value</param>
        /// <param name="Length">The maximum number of chars/bytes</param>
        /// <returns>A string or blob cell</returns>
        public static Cell Left(Cell C, long Length)
        {
            int len = Math.Min(C.AFFINITY == CellAffinity.BLOB ? C.BLOB.Length : C.valueSTRING.Length, (int)Length);
            return Cell.Substring(C, 0, len);
        }

        /// <summary>
        /// Returns all characters/bytes right of given point
        /// </summary>
        /// <param name="C">The string or BLOB value</param>
        /// <param name="Length">The maximum number of chars/bytes</param>
        /// <returns>A string or blob cell</returns>
        public static Cell Right(Cell C, long Length)
        {
            int l = C.AFFINITY == CellAffinity.BLOB ? C.BLOB.Length : C.valueSTRING.Length;
            int begin = Math.Max(l - (int)Length, 0);
            int len = (int)Length;
            if (begin + Length > l) len = l - begin;
            return Cell.Substring(C, begin, len);
        }

        /// <summary>
        /// Checks if a given string contains another string
        /// </summary>
        /// <param name="Source">The string to be checked</param>
        /// <param name="Check">The string being check for</param>
        /// <returns>Cell with boolean type</returns>
        public static Cell Contains(Cell Source, Cell Check)
        {
            return new Cell(Source.valueSTRING.Contains(Check.valueSTRING));
        }

        #endregion

        #region StringBlob

        /// <summary>
        /// Returns either the sub stirng or sub blob
        /// </summary>
        /// <param name="C">Cell value</param>
        /// <param name="Position">The starting point</param>
        /// <param name="Length">The maximum length of the new string</param>
        /// <returns>Either a string or blob value</returns>
        public static Cell Substring(Cell C, long Position, long Length)
        {

            if (C.AFFINITY == CellAffinity.BLOB)
            {
                if (Position + Length > C.BLOB.Length || Position < 0 || Length < 0)
                    return Cell.NULL_BLOB;
                byte[] b = new byte[Length];
                Array.Copy(C.BLOB, Position, b, 0, Length);
                C.BLOB = b;
                return C;
            }
            else //if (C.AFFINITY == CellAffinity.STRING)
            {
                if (Position + Length > C.valueSTRING.Length || Position < 0 || Length < 0)
                    return Cell.NULL_STRING;
                return new Cell(C.valueSTRING.Substring((int)Position, (int)Length));
            }
            //return new Cell(C.AFFINITY);

        }

        /// <summary>
        /// Replaces all occurances of a string value with another string value
        /// </summary>
        /// <param name="Source">The string to be searched</param>
        /// <param name="LookFor">The string being searched for</param>
        /// <param name="ReplaceWith">The string that serves as the replacement</param>
        /// <returns>Cell string value</returns>
        public static Cell Replace(Cell Source, Cell LookFor, Cell ReplaceWith)
        {

            if (Source.AFFINITY == CellAffinity.BOOL || Source.AFFINITY == CellAffinity.DATE_TIME
                || Source.AFFINITY == CellAffinity.DOUBLE || Source.AFFINITY == CellAffinity.INT)
                return new Cell(Source.AFFINITY);

            if (!(Source.AFFINITY == CellAffinity.BLOB && LookFor.AFFINITY == CellAffinity.BLOB && ReplaceWith.AFFINITY == CellAffinity.BLOB))
            {
                Source.STRING = Source.valueSTRING.Replace(LookFor.valueSTRING, ReplaceWith.valueSTRING);
                Source.AFFINITY = CellAffinity.STRING;
                Source.ULONG = 0;
                return Source;
            }

            string t = BitConverter.ToString(Source.BLOB);
            string u = BitConverter.ToString(LookFor.BLOB);
            string v = BitConverter.ToString(ReplaceWith.BLOB);
            t = t.Replace(u, v).Replace("-", "");
            Source.BLOB = Cell.Parse(t, CellAffinity.BLOB).BLOB;
            return Source;

        }

        public static Cell Position(Cell Source, Cell Pattern, int StartAt)
        {

            if (StartAt < 0)
                StartAt = 0;

            if (StartAt > Source.DataCost)
                return new Cell(Source.AFFINITY);

            if (Source.AFFINITY == CellAffinity.STRING)
            {
                return new Cell(Source.STRING.IndexOf(Pattern.valueSTRING, StartAt));
            }

            if (Source.AFFINITY == CellAffinity.BLOB)
            {

                byte[] data = Source.BLOB;
                byte[] pattern = Pattern.valueBLOB;
                bool match = false;
                for (int i = StartAt; i < data.Length; i++)
                {

                    match = true;
                    for (int j = 0; j < pattern.Length; j++)
                    {
                        if (i + j >= data.Length)
                        {
                            match = false;
                            break;
                        }
                        if (data[i + j] != pattern[j])
                        {
                            match = false;
                            break;
                        }

                    }

                    if (match)
                    {
                        return new Cell(i);
                    }

                }

            }

            return new Cell(Source.AFFINITY);

        }

        #endregion

        #region Debug

        /// <summary>
        /// A string representing: Affinity : IsNull : Value
        /// </summary>
        internal string Chime
        {
            get
            {
                return this.Affinity.ToString() + " : " + this.IsNull.ToString() + " : " + this.ToString();
            }
        }

        /// <summary>
        /// A full data dump of the data dump
        /// </summary>
        internal string Decompile
        {
            get
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendLine(this.Affinity + " : " + this.IsNull);
                sb.AppendLine("BOOL: " + this.BOOL);
                sb.AppendLine("INT: " + this.INT);
                sb.AppendLine("DOUBLE: " + this.DOUBLE);
                sb.AppendLine("DATE: " + this.DATE_TIME);
                sb.AppendLine("STRING: " + this.STRING);
                sb.AppendLine("BLOB: " + BitConverter.ToString(this.BLOB));
                return sb.ToString();
            }
        }

        /// <summary>
        /// Returns the size of the data in memory
        /// </summary>
        internal int MemCost
        {

            get
            {

                if (this.AFFINITY == CellAffinity.DATE_TIME || this.AFFINITY == CellAffinity.DOUBLE || this.AFFINITY == CellAffinity.INT || this.AFFINITY == CellAffinity.BOOL)
                {
                    return 16;
                }
                else if (this.AFFINITY == CellAffinity.STRING)
                {
                    return (this.NULL == 1 ? 0 : this.STRING.Length * 2) + 20 + 16;
                }
                else
                {
                    return (this.NULL == 1 ? 0 : this.BLOB.Length) + 16 + 4;
                }

            }

        }

        /// <summary>
        /// Returns the size of the cell in bytes, including the null byte and affinity
        /// </summary>
        internal int DiskCost
        {

            get
            {
                if (this.NULL == 1)
                    return 2;

                if (this.AFFINITY == CellAffinity.DATE_TIME || this.AFFINITY == CellAffinity.DOUBLE || this.AFFINITY == CellAffinity.INT)
                {
                    return 10;
                }
                else if (this.AFFINITY == CellAffinity.BOOL)
                {
                    return 3;
                }
                else if (this.AFFINITY == CellAffinity.STRING)
                {
                    return this.STRING.Length * 2 + 4;
                }
                else
                {
                    return this.BLOB.Length + 4;
                }

            }

        }

        /// <summary>
        /// Returns the size of just the data
        /// </summary>
        internal int DataCost
        {

            get
            {
                
                if (this.AFFINITY == CellAffinity.DATE_TIME || this.AFFINITY == CellAffinity.DOUBLE || this.AFFINITY == CellAffinity.INT)
                {
                    return 8;
                }
                else if (this.AFFINITY == CellAffinity.BOOL)
                {
                    return 1;
                }
                else if (this.AFFINITY == CellAffinity.STRING)
                {
                    return this.STRING.Length;
                }
                else
                {
                    return this.BLOB.Length;
                }

            }

        }

        #endregion

    }



}
