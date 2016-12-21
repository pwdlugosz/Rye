using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.MatrixExpressions
{

    public sealed class MatrixExpressionCheckDivideScalar : MatrixExpression
    {

        private int _Association = 0; // 0 == left (A * B[]), 1 == right (B[] * A)
        private Expression _expression;

        public MatrixExpressionCheckDivideScalar(MatrixExpression Parent, Expression Expression, int Association)
            : base(Parent)
        {
            this._Association = Association;
            this._expression = Expression;
        }

        public override CellMatrix Evaluate()
        {
            if (this._Association == 0)
                return CellMatrix.CheckDivide(this._expression.Evaluate(), this[0].Evaluate());
            else
                return CellMatrix.CheckDivide(this[0].Evaluate(), this._expression.Evaluate());
        }

        public override CellAffinity ReturnAffinity()
        {
            if (this._Association == 0)
                return this._expression.ReturnAffinity();
            else
                return this[0].ReturnAffinity();
        }

        public override MatrixExpression CloneOfMe()
        {
            MatrixExpression node = new MatrixExpressionCheckDivideScalar(this.ParentNode, this._expression.CloneOfMe(), this._Association);
            foreach (MatrixExpression m in this._Cache)
                node.AddChildNode(m.CloneOfMe());
            return node;
        }

        public override List<Expression> InnerExpressions()
        {
            return new List<Expression>() { this._expression };
        }

    }

}
