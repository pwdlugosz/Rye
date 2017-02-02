using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data
{


    /// <summary>
    /// Describes both a set of references to fields in a table and how to sort a table
    /// </summary>
    public sealed class Key
    {

        private List<Cell> _data;

        /// <summary>
        /// Default constructor
        /// </summary>
        public Key()
        {
            this._data = new List<Cell>();
        }

        /// <summary>
        /// Used internally to build a key from a record
        /// </summary>
        /// <param name="R">Serialized key</param>
        internal Key(Record R)
            : this()
        {
            for (int i = 0; i < R.Count; i++)
            {
                this.Add(R[i].INT_A, (KeyAffinity)R[i].INT_B);
            }
        }

        /// <summary>
        /// Builds a key filled with a set of integers; all affinities will be set to ascending
        /// </summary>
        /// <param name="Indexes">The set of field references</param>
        public Key(params int[] Indexes)
            : this()
        {
            foreach (int i in Indexes)
            {
                this.Add(i);
            }
        }

        /// <summary>
        /// The total number of values in the key
        /// </summary>
        public int Count
        {
            get
            {
                return this._data.Count;
            }
        }

        /// <summary>
        /// Field index
        /// </summary>
        /// <param name="Index">Index of the key field to be pulled</param>
        /// <returns>A field offset</returns>
        public int this[int Index]
        {
            get
            {
                return this._data[Index].INT_A;
            }
        }

        /// <summary>
        /// Gets the key affinity
        /// </summary>
        /// <param name="Index">Index of the key</param>
        /// <returns>KeyAffinity</returns>
        public KeyAffinity Affinity(int Index)
        {
            return (KeyAffinity)this._data[Index].INT_B;
        }

        public void SetAffinity(int Index, KeyAffinity Affinity)
        {
            this._data[Index] = new Cell(this._data[Index].INT_A, (int)Affinity);
        }

        /// <summary>
        /// Appends the key
        /// </summary>
        /// <param name="Index">Field offset</param>
        /// <param name="Affinity">Desired key affinity</param>
        public void Add(int Index, KeyAffinity Affinity)
        {
            this._data.Add(new Cell(Index, (int)Affinity));
        }

        /// <summary>
        /// Appends the key, assuming an Ascending affinity
        /// </summary>
        /// <param name="Index">Field offset</param>
        public void Add(int Index)
        {
            this.Add(Index, KeyAffinity.Ascending);
        }

        /// <summary>
        /// Yields a comma deliminated string of the form 'FieldOffset Affinity'
        /// </summary>
        /// <returns>Key string</returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < this.Count; i++)
            {
                sb.Append(this[i].ToString() + " " + this.Affinity(i));
                if (i != this.Count - 1) sb.Append(",");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Serialized the key to a record
        /// </summary>
        /// <returns>Record representing the key</returns>
        public Record ToRecord()
        {
            return new Record(this._data.ToArray());
        }

        /// <summary>
        /// Converts the key to an array of longs
        /// </summary>
        /// <returns>An array of 64 bit integers</returns>
        public long[] ToIntArray()
        {
            return this._data.ConvertAll((t) => { return t.valueINT; }).ToArray();
        }

        public int MemCost
        {
            get { return this.Count * 16 + 4; }
        }

        public int DiskCost
        {
            get
            {
                // Don't count the key length as a disk cost since it's also stored in the header //
                return this.Count * 8;
            }
        }

        public int DataCost
        {
            get { return this.Count * 8; }
        }

        public byte[] Bash()
        {

            byte[] b = new byte[8 + this.Count * 8];
            Cell c = new Cell(this.Count, 0);
            b[0] = c.B0;
            b[1] = c.B1;
            b[2] = c.B2;
            b[3] = c.B3;

            // Write main data //
            for (int i = 0; i < this.Count; i++)
            {
                c = new Cell(this[i], (int)this.Affinity(i));
                b[4 + i * 8] = c.B0;
                b[5 + i * 8] = c.B1;
                b[6 + i * 8] = c.B2;
                b[7 + i * 8] = c.B3;
                b[8 + i * 8] = c.B4;
                b[9 + i * 8] = c.B5;
                b[10 + i * 8] = c.B6;
                b[11 + i * 8] = c.B7;
            }

            return b;

        }
        
        /// <summary>
        /// Compares two keys only on their field offsets, not their affinities
        /// </summary>
        /// <param name="K1">The left key</param>
        /// <param name="K2">The right key</param>
        /// <returns>Boolean indicating if both keys are the same</returns>
        public static bool EqualsWeak(Key K1, Key K2)
        {
            int n = Math.Min(K1.Count, K2.Count);
            int o = Math.Max(K1.Count, K2.Count);
            if (n == 0 && o != 0) return false;
            for (int i = 0; i < n; i++)
                if (K1[i] != K2[i]) return false;
            return true;
        }

        /// <summary>
        /// Compares two keys only on their field offsets, and their affinities
        /// </summary>
        /// <param name="K1">The left key</param>
        /// <param name="K2">The right key</param>
        /// <returns>Boolean indicating if both keys are the same</returns>
        public static bool EqualsStrong(Key K1, Key K2)
        {
            int n = Math.Min(K1.Count, K2.Count);
            int o = Math.Max(K1.Count, K2.Count);
            if (n == 0 && o != 0) 
                return false;
            for (int i = 0; i < n; i++)
                if (K1[i] != K2[i] || K1.Affinity(i) != K2.Affinity(i)) 
                    return false;
            return true;
        }

        /// <summary>
        /// Compares two keys only on their field offsets, not their affinities; if one is smaller than the other, then it ignores the values in the larger key past the smaller's max index
        /// </summary>
        /// <param name="K1">The left key</param>
        /// <param name="K2">The right key</param>
        /// <returns>Boolean indicating if both keys are the same</returns>
        public static bool SubsetWeak(Key K1, Key K2)
        {
            int n = Math.Min(K1.Count, K2.Count);
            for (int i = 0; i < n; i++)
                if (K1[i] != K2[i]) return false;
            return true;
        }

        /// <summary>
        /// Compares two keys only on their field offsets, and their affinities; if one is smaller than the other, then it ignores the values in the larger key past the smaller's max index
        /// </summary>
        /// <param name="K1">The left key</param>
        /// <param name="K2">The right key</param>
        /// <returns>Boolean indicating if both keys are the same</returns>
        public static bool SubsetStrong(Key K1, Key K2)
        {
            int n = Math.Min(K1.Count, K2.Count);
            for (int i = 0; i < n; i++)
                if (K1[i] != K2[i] || K1.Affinity(i) != K2.Affinity(i)) return false;
            return true;
        }

        /// <summary>
        /// Parses a key from a string
        /// </summary>
        /// <param name="Text">Text to parse</param>
        /// <returns>Key parsed from the string</returns>
        public static Key Parse(string Text)
        {

            string[] s = Text.Split(',');
            int i = 0;
            KeyAffinity ka = KeyAffinity.Ascending;
            Key k = new Key();

            foreach (string r in s)
            {

                string[] t = r.Trim().Split(' ');

                if (s.Length == 2)
                    ka = ParseAffinity(s[1]);
                else
                    ka = KeyAffinity.Ascending;

                i = int.Parse(t[0]);

                k.Add(i, ka);

            }

            return k;

        }

        /// <summary>
        /// Creates a key with a certain number of ascending elements
        /// </summary>
        /// <param name="StartAt">The starting index of the key</param>
        /// <param name="Count">The upper bound non-inclusive</param>
        /// <returns>Key</returns>
        public static Key Build(int StartAt, int Count)
        {
            Key k = new Key();
            for (int i = StartAt; i < StartAt + Count; i++)
            {
                k.Add(i);
            }
            return k;
        }

        /// <summary>
        /// Creates a key with a certain number of ascending elements
        /// </summary>
        /// <param name="Count">The upper bound non-inclusive</param>
        /// <returns>Key</returns>
        public static Key Build(int Count)
        {
            return Build(0, Count);
        }

        /// <summary>
        /// Parses a key affinity
        /// </summary>
        /// <param name="Text">Text to parse</param>
        /// <returns>Key affinity</returns>
        internal static KeyAffinity ParseAffinity(string Text)
        {
            string t = Text.ToUpper().Trim();
            switch (t)
            {
                case "A":
                case "ASC":
                case "ASCENDING":
                    return KeyAffinity.Ascending;
                case "D":
                case "DESC":
                case "DESCENDING":
                    return KeyAffinity.Descending;
                default:
                    return KeyAffinity.Ascending;
            }
        }

        /// <summary>
        /// Removes all duplicates fom a key
        /// </summary>
        /// <param name="K">The key to de-dup</param>
        /// <returns>A new key instance that does not contain duplicates</returns>
        public static Key ToDistinct(Key K)
        {
            Key k = new Key();
            k._data = K._data.Distinct().ToList();
            return k;
        }

        public static Key Read(byte[] Buffer, int Location)
        {

            int cnt = BitConverter.ToInt32(Buffer, Location);
            Key k = new Key();
            for (int i = 0; i < cnt; i++)
            {
                Cell c = new Cell(0);
                c.B0 = Buffer[Location + 4 + i * 8];
                c.B1 = Buffer[Location + 4 + i * 8 + 1];
                c.B2 = Buffer[Location + 4 + i * 8 + 2];
                c.B3 = Buffer[Location + 4 + i * 8 + 3];
                c.B4 = Buffer[Location + 4 + i * 8 + 4];
                c.B5 = Buffer[Location + 4 + i * 8 + 5];
                c.B6 = Buffer[Location + 4 + i * 8 + 6];
                c.B7 = Buffer[Location + 4 + i * 8 + 7];
                k.Add(c.INT_A, (KeyAffinity)c.INT_B);
            }

            return k;

        }

    }

    public static class KeyComparer
    {

        public static bool IsStrongSubset(Key A, Key B)
        {

            if (B.Count > A.Count)
                return false;

            for (int i = 0; i < A.Count; i++)
            {
                if (!(A[i] == B[i] && A.Affinity(i) == B.Affinity(i)))
                {
                    return false;
                }
            }

            return true;

        }

        public static bool IsWeakSubset(Key A, Key B)
        {

            if (B.Count > A.Count)
                return false;

            for (int i = 0; i < A.Count; i++)
            {
                if (!(A[i] == B[i]))
                {
                    return false;
                }
            }

            return true;

        }

        public static bool IsStrongUnion(Key A, Key B)
        {

            for (int i = 0; i < Math.Min(A.Count, B.Count); i++)
            {
                if (!(A[i] == B[i] && A.Affinity(i) == B.Affinity(i)))
                {
                    return false;
                }
            }

            return true;

        }

        public static bool IsWeakUnion(Key A, Key B)
        {

            for (int i = 0; i < Math.Min(A.Count, B.Count); i++)
            {
                if (!(A[i] == B[i]))
                {
                    return false;
                }
            }

            return true;

        }

        public static bool StrongEquals(Key A, Key B)
        {

            if (B.Count != A.Count)
                return false;

            for (int i = 0; i < A.Count; i++)
            {
                if (!(A[i] == B[i] && A.Affinity(i) == B.Affinity(i)))
                {
                    return false;
                }
            }

            return true;

        }

        public static bool WeakEquals(Key A, Key B)
        {

            if (B.Count != A.Count)
                return false;

            for (int i = 0; i < A.Count; i++)
            {
                if (!(A[i] == B[i]))
                {
                    return false;
                }
            }

            return true;

        }


    }

}
