using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Structures;

namespace Rye.Expressions
{

    public sealed class ExpressionHeapRef : Expression
    {

        private Heap<Cell> _Heap;
        private int _Pointer;
        private CellAffinity _ReturnType;

        public ExpressionHeapRef(Expression Parent, Heap<Cell> Heap, int DirectRef, CellAffinity ReturnType)
            : base(Parent, ExpressionAffinity.Heap)
        {
            this._Pointer = DirectRef;
            this._Heap = Heap;
            this._ReturnType = ReturnType;
            this._name = Heap.Name(DirectRef);
        }

        public ExpressionHeapRef(Expression Parent, Heap<Cell> Heap, int DirectRef)
            : this(Parent, Heap, DirectRef, Heap[DirectRef].AFFINITY)
        {
        }

        public ExpressionHeapRef(Expression Parent, Heap<Cell> Heap, string Name)
            : this(Parent, Heap, Heap.GetPointer(Name), Heap[Name].Affinity)
        {
        }

        public override Cell Evaluate()
        {
            return this._Heap[this._Pointer];
        }

        public override CellAffinity ReturnAffinity()
        {
            return this._ReturnType;
        }

        public override string ToString()
        {
            return this.Affinity.ToString() + " : " + this._Heap[this._Pointer].ToString();
        }

        public override int GetHashCode()
        {
            return this._Heap[this._Pointer].GetHashCode() ^ Expression.HashCode(this._Cache);
        }

        public override string Unparse(Schema S)
        {
            return this._Heap[this._Pointer].ToString();
        }

        public override Expression CloneOfMe()
        {
            return new ExpressionHeapRef(this.ParentNode, this._Heap, this._Pointer);
        }

        public override int DataSize()
        {
            return this._Heap[this._Pointer].DataCost;
        }

        public int Pointer
        {
            get { return this._Pointer; }
        }

    }

}
