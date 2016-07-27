using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data
{
    
    public sealed class RecordComparer : IEqualityComparer<Record>, IComparer<Record>
    {

        private Key _k1;
        private Key _k2;
        private bool _keyed = false;
        private long _clicks = 0;

        public RecordComparer(Key K1, Key K2)
        {
            if (K1.Count != K2.Count)
                throw new ArgumentException(string.Format("Both keys passed must have the same size: {0} {1}", K1.Count, K2.Count));
            this._k1 = K1;
            this._k2 = K2;
            this._keyed = true;
        }

        public RecordComparer()
        {
            this._keyed = false;
        }

        public long Clicks
        {
            get
            {
                return this._clicks;
            }
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

        public int Compare(Record R1, Record R2)
        {

            this._clicks++;

            if (this._keyed == false)
            {

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
            else
            {

                int idx = 0;
                for (int i = 0; i < this._k1.Count; i++)
                {

                    idx = Cell.Compare(R1[this._k1[i]], R2[this._k2[i]]);
                    if (idx != 0)
                        return idx;

                }
                return 0;

            }
        
        }

        public bool Equals(Record R1, Record R2)
        {

            this._clicks++;

            if (this._keyed == false)
            {

                if (R1.Count != R2.Count)
                    throw new ArgumentException(string.Format("Records have differnt lengths: {0} {1}", R1.Count, R2.Count));
                bool b = true;
                for (int i = 0; i < R1.Count; i++)
                {

                    if (R1[i] != R2[i])
                        return false;

                }
                return true;

            }
            else
            {

                for (int i = 0; i < this._k1.Count; i++)
                {

                    if (R1[this._k1[i]] != R2[this._k1[i]])
                        return false;

                }
                return true;

            }

        }

        public int GetHashCode(Record R)
        {
            return R.GetHashCode();
        }

        public RecordComparer Reverse()
        {
            if (this._keyed)
                return new RecordComparer(this._k2, this._k2);
            return new RecordComparer();
        }

    }

}
