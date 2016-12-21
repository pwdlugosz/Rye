using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Expressions
{

    public sealed class ExpressionResult : Expression
    {

        private CellFunction _Func;

        public ExpressionResult(Expression Parent, CellFunction Function)
            : base(Parent, ExpressionAffinity.Result)
        {
            this._Func = Function;
            this._name = null;
        }

        public void SetParameterCount(int Count)
        {
            this._Func.ParamCount = Count;
        }

        public CellFunction InnerFunction
        {
            get { return this._Func; }
        }

        public override Cell Evaluate()
        {
            return _Func.Evaluate(this.EvaluateChildren());
        }

        public override CellAffinity ReturnAffinity()
        {
            CellAffinity[] c = this.ReturnAffinityChildren();
            return _Func.ReturnAffinity(c);
        }

        public override string ToString()
        {
            return this._Func.NameSig;
        }

        public override int GetHashCode()
        {
            return this._Func.GetHashCode() ^ Expression.HashCode(this._Cache);
        }

        public override string Unparse(Schema S)
        {

            List<string> text = new List<string>();
            foreach (Expression ln in this.Children)
                text.Add(ln.Unparse(S));
            return this._Func.Unparse(text.ToArray());

        }

        public override Expression CloneOfMe()
        {
            ExpressionResult Dolly = new ExpressionResult(this.ParentNode, this._Func);
            foreach (Expression n in this._Cache)
            {
                Expression clone = n.CloneOfMe();
                clone.ParentNode = Dolly;
                Dolly.AddChildNode(clone);
            }
            return Dolly;
        }

        public override bool IsVolatile
        {
            get
            {
                return this._Func.IsVolatile;
            }
        }

        public override int DataSize()
        {

            List<int> sizes = new List<int>();
            foreach (Expression e in this._Cache)
                sizes.Add(e.DataSize());

            int size = this._Func.ReturnSize(this.ReturnAffinity(), sizes.ToArray());

            return size;
        }

    }

}
