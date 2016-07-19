using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Structures;

namespace Rye.MatrixExpressions
{

    public sealed class MatrixExpressionHeap : MatrixExpression
    {

        private int _ref;
        private Heap<CellMatrix> _heap;

        public MatrixExpressionHeap(MatrixExpression Parent, Heap<CellMatrix> Mem, int Ref)
            : base(Parent)
        {
            this._ref = Ref;
            this._heap = Mem;
        }

        public override CellMatrix Evaluate()
        {
            return this._heap[this._ref];
        }

        public override CellAffinity ReturnAffinity()
        {
            return this._heap[this._ref].Affinity;
        }

        public override MatrixExpression CloneOfMe()
        {
            return new MatrixExpressionHeap(this.ParentNode, this._heap, this._ref);
        }

    }

}
