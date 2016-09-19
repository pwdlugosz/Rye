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
        private CellMatrix _Matrix;
        private int _DirectRef;

        public ExpressionArrayDynamicRef(Expression Parent, Expression Row, Expression Col, CellMatrix M)
            : base(Parent, ExpressionAffinity.Matrix)
        {
            this._RowIndex = Row;
            this._ColIndex = Col;
            this._Matrix = M;
        }

        public override Cell Evaluate()
        {
            return this._Matrix[(int)this._RowIndex.Evaluate().INT, (int)this._ColIndex.Evaluate().INT];
        }

        public override string Unparse(Schema S)
        {
            return string.Format("Matrix[{0},{1}]", this._RowIndex.Unparse(S), this._ColIndex.Unparse(S));
        }

        public override Expression CloneOfMe()
        {
            return new ExpressionArrayDynamicRef(this.ParentNode, this._RowIndex, this._ColIndex, this._Matrix);
        }

        public override string ToString()
        {
            return "MATRIX";
        }

        public override CellAffinity ReturnAffinity()
        {
            return this._Matrix.Affinity;
        }

        public override int DataSize()
        {
            return 0;//this._Heap.Matricies[this._DirectRef].;
        }


    }

}
