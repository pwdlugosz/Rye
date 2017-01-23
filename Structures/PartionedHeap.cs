using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Structures
{

    public class PartitionedHeap<T>
    {

        private const string DOT = ".";
        
        protected Dictionary<string, int> _RefSet;
        protected List<T> _Heap;
        protected List<bool> _IsReadOnly;

        public PartitionedHeap()
        {
            _RefSet = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            _Heap = new List<T>();
            _IsReadOnly = new List<bool>();
        }

        // Properties //
        public int Count
        {
            get { return this._RefSet.Count; }
        }

        public T this[string NameSpace, string Name]
        {

            get
            {
                string t = NameSpace + DOT + Name;
                return this._Heap[this._RefSet[t]];
            }

            set
            {
                string t = NameSpace + DOT + Name;
                this._Heap[this._RefSet[t]] = value;
            }

        }

        public T this[int Pointer]
        {

            get
            {
                return this._Heap[Pointer];
            }

            set
            {
                this._Heap[Pointer] = value;
            }

        }

        // Methods //
        public bool Exists(string NameSpace, string Name)
        {
            string t = NameSpace + DOT + Name;
            return this._RefSet.ContainsKey(t);
        }

        public int GetPointer(string NameSpace, string Name)
        {
            string t = NameSpace + DOT + Name;
            return this._RefSet[t];
        }

        public void Allocate(string NameSpace, string Name, T Value)
        {
            string t = NameSpace + DOT + Name;
            if (this.Exists(NameSpace, Name))
                throw new Exception(string.Format("Cannot allocate '{0}', an allocation with that name already exists", t));
            this._RefSet.Add(t, this._Heap.Count);
            this._Heap.Add(Value);
        }

        public void Deallocate(string NameSpace, string Name)
        {

            string t = NameSpace + DOT + Name;
            
            if (this.Exists(NameSpace, Name))
            {
                int ptr = this.GetPointer(NameSpace, Name);
                this._RefSet.Remove(t);
                this[ptr] = default(T);
            }

        }

        public void Reallocate(string NameSpace, string Name, T Value)
        {
            this.Deallocate(NameSpace, Name);
            this.Allocate(NameSpace, Name, Value);
        }

        public void Vacum()
        {

            List<T> NewHeap = new List<T>();

            int NewPointer = 0;

            foreach (KeyValuePair<string, int> kv in this._RefSet)
            {

                // Accumulate a Value to the new heap //
                NewHeap.Add(this._Heap[kv.Value]);

                // Reset the pointer //
                this._RefSet[kv.Key] = NewPointer;

                // Increment the pointer //
                NewPointer++;

            }

            // Point the new heap //
            this._Heap = NewHeap;

        }

        public string NameSpace(int Pointer)
        {
            return this._RefSet.Keys.ToArray()[Pointer];
        }

        public string Name(int Pointer)
        {
            return this._RefSet.Keys.ToArray()[Pointer];
        }

        public Dictionary<string, T> Entries
        {
            get
            {
                Dictionary<string, T> values = new Dictionary<string, T>();
                foreach (KeyValuePair<string, int> kv in this._RefSet)
                    values.Add(kv.Key, this[kv.Value]);
                return values;
            }
        }

        public List<T> Values
        {
            get { return this._Heap; }
        }
        
    }


}
