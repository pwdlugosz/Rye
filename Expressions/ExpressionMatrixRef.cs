using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Structures;

namespace Rye.Expressions
{

    public sealed class ExpressionMatrixRef : Expression
    {

        private Expression _RowIndex;
        private Expression _ColIndex;
        private Heap<CellMatrix> _Heap;
        private int _Pointer;

        public ExpressionMatrixRef(Expression Parent, Expression Row, Expression Col, Heap<CellMatrix> Matrixes, int Ref)
            : base(Parent, ExpressionAffinity.Matrix)
        {
            this._RowIndex = Row;
            this._ColIndex = Col;
            this._Heap = Matrixes;
            this._Pointer = Ref;
            this._Cache.Add(Row);
            this._Cache.Add(Col);
        }

        public override Cell Evaluate()
        {
            CellMatrix m = this._Heap[this._Pointer];
            return m[(int)this._RowIndex.Evaluate().INT, (int)this._ColIndex.Evaluate().INT];
        }

        public override string Unparse(Schema S)
        {
            return string.Format("Matrix[{0},{1}]", this._RowIndex.Unparse(S), this._ColIndex.Unparse(S));
        }

        public override Expression CloneOfMe()
        {

            Expression row = this._RowIndex.CloneOfMe();
            Expression col = this._ColIndex.CloneOfMe();

            return new ExpressionMatrixRef(this.ParentNode, row, col, this._Heap, this._Pointer);

        }

        public override string ToString()
        {
            return "MATRIX";
        }

        public override CellAffinity ReturnAffinity()
        {
            return this._Heap[this._Pointer].Affinity;
        }
        
        public void ForceHeap(Heap<CellMatrix> NewHeap)
        {

            if (NewHeap.Identifier.ToUpper() != this._Heap.Identifier.ToUpper())
                return;

            // Check to see if we need to change the heap ref //
            string name = this._Heap.Name(this._Pointer);
            int NewPtr = NewHeap.GetPointer(name);

            if (NewPtr == -1)
                throw new ArgumentException(string.Format("The new heap doesn't contain {0}", name));

            this._Pointer = NewPtr;
            this._Heap = NewHeap;

        }

        public override int DataSize()
        {
            return 0;//this._Heap.Matricies[this._Pointer].;
        }

        public Heap<CellMatrix> InnerHeap
        {
            get { return this._Heap; }
        }

    }

}
