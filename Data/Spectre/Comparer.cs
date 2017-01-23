using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data.Spectre
{


    // IComparer<T>     :   Compare(A, B)
    // IComparable<T>   :   CompareTo(A)

    // Predicates //
    public interface IRecordSeeker : IComparable<Record>, IEquatable<Record>
    {

        long ETicks { get; }

        long CTicks { get; }

    }

    public interface IRecordMatcher : IComparer<Record>, IEqualityComparer<Record>
    {

        /// <summary>
        /// Checks if a record is (inclusively) between two other records
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Lower"></param>
        /// <param name="Upper"></param>
        /// <returns></returns>
        int Between(Record Element, Record Lower, Record Upper);

        /// <summary>
        /// Checks if a record is (exclusively) between two other records
        /// </summary>
        /// <param name="Key"></param>
        /// <param name="Lower"></param>
        /// <param name="Upper"></param>
        /// <returns></returns>
        int BetweenX(Record Element, Record Lower, Record Upper);

        /// <summary>
        /// The number of equality checks
        /// </summary>
        long ETicks { get; }

        /// <summary>
        /// The number of comparison checks (including between)
        /// </summary>
        long CTicks { get; }

    }

    public class RecordSeeker : IRecordSeeker
    {

        private Record _Value;
        private Key _Key;
        private long _eTicks = 0;
        private long _cTicks = 0;

        public RecordSeeker(Key Key, Record Value)
        {
            this._Key = Key;
            this._Value = Value;
        }

        public int CompareTo(Record Value)
        {
            this._cTicks++;
            return Record.Compare(this._Value, Value);
        }

        public bool Equals(Record Value)
        {
            this._eTicks++;
            return Record.Equals(this._Value, this._Key, Value, this._Key);
        }

        public long ETicks
        {
            get { return this._eTicks; }
        }

        public long CTicks
        {
            get { return this._cTicks; }
        }

    }

    public class RecordMatcher : IRecordMatcher
    {

        private Key _LeftKey;
        private Key _RightKey;
        private long _eTicks = 0;
        private long _cTicks = 0;

        public RecordMatcher(Key LKey, Key RKey)
        {
            this._LeftKey = LKey;
            this._RightKey = RKey;
        }

        public RecordMatcher(Key Key)
            : this(Key, Key)
        {
        }

        public int Compare(Record A, Record B)
        {
            this._cTicks++;
            return Record.Compare(A, this._LeftKey, B, this._RightKey);
        }

        public int Between(Record Element, Record Lower, Record Upper)
        {

            int l = this.Compare(Element, Lower); // if negative, element < lower
            int u = this.Compare(Element, Upper); // if positive, element > upper

            // if l < 0, element is lower and we need to return -1
            // if u > 0, element is higher and we need to return +1
            // otherwise element is between lower and upper (may be equal to lower and/or upper)
            if (l < 0)
                return -1;
            else if (u > 0)
                return 1;
            else
                return 0;

        }

        public int BetweenX(Record Element, Record Lower, Record Upper)
        {

            int l = this.Compare(Element, Lower); // if negative, element < lower
            int u = this.Compare(Element, Upper); // if positive, element > upper

            // if l < 0, element is lower and we need to return -1
            // if u > 0, element is higher and we need to return +1
            // otherwise element is between lower and upper (may be equal to lower and/or upper)
            if (l <= 0)
                return -1;
            else if (u >= 0)
                return 1;
            else
                return 0;

        }

        public bool Equals(Record A, Record B)
        {
            this._eTicks++;
            return Record.Equals(A, this._LeftKey, B, this._RightKey);
        }

        public int GetHashCode(Record A)
        {
            return A.GetHashCode(this._RightKey);
        }

        public long ETicks
        {
            get { return this._eTicks; }
        }

        public long CTicks
        {
            get { return this._cTicks; }
        }

        public Key LeftKey
        {
            get { return this._LeftKey; }
        }

        public Key RightKey
        {
            get { return this._RightKey; }
        }

    }


}
