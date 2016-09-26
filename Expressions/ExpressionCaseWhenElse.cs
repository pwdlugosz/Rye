using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Expressions
{

    public sealed class ExpressionCaseWhenElse : Expression
    {

        private CellAffinity _ReturnAffinity;
        private int _CheckCount = 0;

        public ExpressionCaseWhenElse(Expression Parent, CellAffinity ReturnAffinity)
            :base(Parent, ExpressionAffinity.Result)
        {

            this._ReturnAffinity = ReturnAffinity;

        }

        public override Cell Evaluate()
        {

            if (this._CheckCount == 0)
                this._CheckCount = this._Cache.Count;

            if (this._CheckCount < 3 || this._CheckCount % 2 == 0)
                throw new Interpreter.RyeCompileException("CASE expects an odd number of arguments, with the number of arguments greater than 3");

            for (int i = 0; i < this._CheckCount - 1; i += 2)
            {

                if (this._Cache[i].Evaluate().valueBOOL)
                {
                    return this._Cache[i + 1].Evaluate();
                }

            }

            return this._Cache.Last().Evaluate();

        }

        public override CellAffinity ReturnAffinity()
        {
            return this._ReturnAffinity;
        }

        public override Expression CloneOfMe()
        {

            Expression e = new ExpressionCaseWhenElse(this.ParentNode, this._ReturnAffinity);
            foreach (Expression s in this._Cache)
                e.AddChildNode(e.CloneOfMe());

            return e;

        }

        public override string Unparse(Schema S)
        {

            if (this._CheckCount == 0)
                this._CheckCount = this._Cache.Count;

            if (this._CheckCount < 3 || this._CheckCount % 2 == 0)
                throw new Interpreter.RyeCompileException("CASE expects an odd number of arguments, with the number of arguments greater than 3");

            StringBuilder sb = new StringBuilder();

            sb.AppendLine("CASE ");

            for (int i = 0; i < this._CheckCount / 2; i += 2)
            {

                sb.AppendLine(string.Format("WHEN {0} THEN {1} ", this._Cache[i].Unparse(S), this._Cache[i + 1].Unparse(S)));

            }

            sb.AppendLine(string.Format("ELSE {0}", this._Cache.Last().Unparse(S)));
            sb.AppendLine("END");

            return sb.ToString();

        }

        public void SetCellAffinity(CellAffinity Affinity)
        {
            this._ReturnAffinity = Affinity;
        }

    }

}
