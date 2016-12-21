using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.MatrixExpressions;
using Rye.Structures;

namespace Rye.Methods
{

    class MethodMatrixAssign : Method
    {

        private MatrixExpression _expression;
        private int _ref;
        private Heap<CellMatrix> _mat;

        public MethodMatrixAssign(Method Parent, Heap<CellMatrix> Heap, int Ref, MatrixExpression Expression)
            : base(Parent)
        {
            this._expression = Expression;
            this._ref = Ref;
            this._mat = Heap;
        }

        public Heap<CellMatrix> InnerHeap
        {
            get { return this._mat; }
            set { this._mat = value; }
        }

        public override void Invoke()
        {
            this._mat[this._ref] = this._expression.Evaluate();
        }

        public override string Message()
        {
            return string.Format("Matrix assignment '{0}'", this._mat.Name(this._ref));
        }

        public override Method CloneOfMe()
        {
            return new MethodMatrixAssign(this.Parent, this._mat, this._ref, this._expression.CloneOfMe());
        }

    }

}
