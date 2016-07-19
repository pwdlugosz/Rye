using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.MatrixExpressions
{

    public sealed class MatrixExpressionMatrixMultiply : MatrixExpression
    {

        public MatrixExpressionMatrixMultiply(MatrixExpression Parent)
            : base(Parent)
        {
        }

        public override CellMatrix Evaluate()
        {
            return this[0].Evaluate() ^ this[1].Evaluate();
        }

        public override CellAffinity ReturnAffinity()
        {
            return this[0].ReturnAffinity();
        }

        public override MatrixExpression CloneOfMe()
        {
            MatrixExpression node = new MatrixExpressionMatrixMultiply(this.ParentNode);
            foreach (MatrixExpression m in this._Cache)
                node.AddChildNode(m.CloneOfMe());
            return node;
        }

    }
    
}
