using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Structures
{


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

        public int Count
        {
            get { return this._Cache.Count; }
        }

        public bool IsEmpty
        {
            get { return this._Cache.Count == 0; }
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

        public void Remove(T Value)
        {
            this._Cache.Remove(Value);
        }

        public IEnumerable<T> ToCache
        {
            get
            {
                if (this.State == QuackState.FIFO)
                    return new Queue<T>(this._Cache);
                return new Stack<T>(this._Cache);
            }
        }

    }


}
