using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.MatrixExpressions
{

    public sealed class MatrixExpressionTranspose : MatrixExpression
    {

        public MatrixExpressionTranspose(MatrixExpression Parent)
            : base(Parent)
        {
        }

        public override CellMatrix Evaluate()
        {
            return this[0].Evaluate().Transposition;
        }

        public override CellAffinity ReturnAffinity()
        {
            return this[0].ReturnAffinity();
        }

        public override MatrixExpression CloneOfMe()
        {
            MatrixExpression node = new MatrixExpressionTranspose(this.ParentNode);
            foreach (MatrixExpression m in this._Cache)
                node.AddChildNode(m.CloneOfMe());
            return node;
        }

    }

}
