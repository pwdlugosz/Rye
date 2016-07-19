using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Structures;

namespace Rye.Expressions
{

    public sealed class ExpressionArrayDynamicRef : Expression
    {

        private Expression _RowIndex;
        private Expression _ColIndex;
        private MemoryStructure _Heap;
        private int _DirectRef;

        public ExpressionArrayDynamicRef(Expression Parent, Expression Row, Expression Col, MemoryStructure Heap, int DirectRef)
            : base(Parent, ExpressionAffinity.Matrix)
        {
            this._RowIndex = Row;
            this._ColIndex = Col;
            this._DirectRef = DirectRef;
            this._Heap = Heap;
        }

        public ExpressionArrayDynamicRef(Expression Parent, Expression Row, Expression Col, MemoryStructure Heap, string Name)
            : this(Parent, Row, Col, Heap, Heap.Matricies.GetPointer(Name))
        { 
        }

        public override Cell Evaluate()
        {
            return this._Heap.Matricies[this._DirectRef][(int)this._RowIndex.Evaluate().INT, (int)this._ColIndex.Evaluate().INT];
        }

        public override string Unparse(Schema S)
        {
            return string.Format("Matrix[{0},{1}]", this._RowIndex.Unparse(S), this._ColIndex.Unparse(S));
        }

        public override Expression CloneOfMe()
        {
            return new ExpressionArrayDynamicRef(this.ParentNode, this._RowIndex, this._ColIndex, this._Heap, this._DirectRef);
        }

        public override string ToString()
        {
            return "MATRIX";
        }

        public override CellAffinity ReturnAffinity()
        {
            return this._Heap.Matricies[this._DirectRef].Affinity;
        }

        public override int DataSize()
        {
            return 0;//this._Heap.Matricies[this._DirectRef].;
        }

        public MemoryStructure Heap
        {
            get { return this._Heap; }
            set { this._Heap = value; }
        }

    }

}
