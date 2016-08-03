using System;
using System.Collections.Generic;
using Rye.Expressions;

namespace Rye.Data
{

    public class RecordComparer : IEqualityComparer<Record>, IComparer<Record>
    {

        protected long _clicks = 0;
        
        public RecordComparer()
        {
        }

        public long Clicks
        {
            get { return this._clicks; }
        }

        public virtual int Compare(Record R1, Record R2)
        {

            this._clicks++;

            if (R1 == null && R2 == null)
                return 0;
            else if (R1 == null)
                return -1;
            else if (R2 == null)
                return 1;

            if (R1.Count != R2.Count)
                throw new ArgumentException(string.Format("Records have differnt lengths: {0} {1}", R1.Count, R2.Count));
            int idx = 0;
            for (int i = 0; i < R1.Count; i++)
            {

                idx = Cell.Compare(R1[i], R2[i]);
                if (idx != 0)
                    return idx;

            }
            return 0;
        
        }

        public virtual bool Equals(Record R1, Record R2)
        {

            this._clicks++;

            if (R1 == null && R2 == null)
                return true;
            else if (R1 == null || R2 == null)
                return false;

            if (R1.Count != R2.Count)
                throw new ArgumentException(string.Format("Records have differnt lengths: {0} {1}", R1.Count, R2.Count));
            
            for (int i = 0; i < R1.Count; i++)
            {

                if (R1[i] != R2[i])
                    return false;

            }
            return true;

        }

        public virtual int GetHashCode(Record R)
        {
            return R.GetHashCode();
        }

    }

    public sealed class KeyedRecordComparer : RecordComparer, IEqualityComparer<Record>, IComparer<Record>
    {

        private Key _k1;
        private Key _k2;
        
        public KeyedRecordComparer(Key K1, Key K2)
        {
            if (K1.Count != K2.Count)
                throw new ArgumentException(string.Format("Both keys passed must have the same size: {0} {1}", K1.Count, K2.Count));
            this._k1 = K1;
            this._k2 = K2;
        }

        public KeyedRecordComparer(Key K)
            : this(K, K)
        {
        }

        public Key LeftKey
        {
            get { return this._k1; }
        }

        public Key RightKey
        {
            get { return this._k2; }
        }

        public bool IsEmpty
        {
            get { return this._k1.Count == 0; }
        }

        public override int Compare(Record R1, Record R2)
        {

            this._clicks++;

            int idx = 0;
            for (int i = 0; i < this._k1.Count; i++)
            {

                idx = Cell.Compare(R1[this._k1[i]], R2[this._k2[i]]);
                if (idx != 0)
                    return (this._k1.Affinity(i) == KeyAffinity.Ascending ? idx : -idx);

            }
            return 0;
        
        }

        public override bool Equals(Record R1, Record R2)
        {

            this._clicks++;

            for (int i = 0; i < this._k1.Count; i++)
            {

                if (R1[this._k1[i]] != R2[this._k1[i]])
                    return false;

            }
            return true;

        }

        public override int GetHashCode(Record R)
        {
            return Record.Split(R, this._k1).GetHashCode();
        }

        public KeyedRecordComparer Reverse()
        {
            return new KeyedRecordComparer(this._k2, this._k1);
        }

    }

    public sealed class ExpressionRecordComparer : RecordComparer, IEqualityComparer<Record>, IComparer<Record>
    {

        private ExpressionCollection _e1;
        private ExpressionCollection _e2;
        private Register _r1;
        private Register _r2;

        public ExpressionRecordComparer(ExpressionCollection E1, Register R1, ExpressionCollection E2, Register R2)
        {

            // Check if volatile //
            if (E1.IsVolatile)
                throw new ArgumentException("The first expression passed is invalid because it contains volatile elements");
            if (E2.IsVolatile)
                throw new ArgumentException("The second expression passed is invalid because it contains volatile elements");

            this._e1 = E1;
            this._e2 = E2;
            this._r1 = R1;
            this._r2 = R2;
        }

        public ExpressionRecordComparer(ExpressionCollection E, Register R)
            : this(E, R, E, R)
        {
        }

        public override int Compare(Record R1, Record R2)
        {

            this._r1.Value = R1;
            Record x1 = this._e1.Evaluate();
            this._r2.Value = R2;
            Record x2 = this._e2.Evaluate();
            
            return base.Compare(x1, x2);

        }

        public override bool Equals(Record R1, Record R2)
        {

            this._r1.Value = R1;
            Record x1 = this._e1.Evaluate();
            this._r2.Value = R2;
            Record x2 = this._e2.Evaluate();

            return base.Equals(x1, x2);

        }

    }

    public sealed class ExpressionSortComparer : RecordComparer, IEqualityComparer<Record>, IComparer<Record>
    {

        private ExpressionCollection _e;
        private Register _r;
        private Key _k;
        
        public ExpressionSortComparer(ExpressionCollection E, Register R, Key K)
        {

            // Check if volatile //
            if (E.IsVolatile)
                throw new ArgumentException("The first expression passed is invalid because it contains volatile elements");
            if (E.Count != K.Count)
                throw new ArgumentException("The key passed and the expression collection passed have different sizes");
            this._e = E;
            this._r = R;
            this._k = K;

        }

        public override int Compare(Record R1, Record R2)
        {

            this._r.Value = R1;
            Record x1 = this._e.Evaluate();
            this._r.Value = R2;
            Record x2 = this._e.Evaluate();

            this._clicks++;

            int idx = 0;
            for (int i = 0; i < this._k.Count; i++)
            {

                idx = Cell.Compare(x1[this._k[i]], x2[this._k[i]]);
                if (idx != 0)
                    return (this._k.Affinity(i) == KeyAffinity.Ascending ? idx : -idx);

            }
            return 0;

        }

        public override bool Equals(Record R1, Record R2)
        {

            this._r.Value = R1;
            Record x1 = this._e.Evaluate();
            this._r.Value = R2;
            Record x2 = this._e.Evaluate();

            return base.Equals(x1, x2);

        }

    }

}
