using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Expressions;
using Rye.MatrixExpressions;
using Rye.Data;

namespace Rye.Methods
{

    public sealed class MethodPrintE : Method
    {

        private ExpressionCollection _val;
        private Session _Enriro;

        public MethodPrintE(Method Parent, Session Enviro, ExpressionCollection Val)
            : base(Parent)
        {
            this._val = Val;
            this._Enriro = Enviro;
        }

        public override void Invoke()
        {
            this._Enriro.IO.WriteLine(this._val.Evaluate().ToString());
        }

        public override Method CloneOfMe()
        {
            return new MethodPrintE(this._Parent, this._Enriro, this._val.CloneOfMe());
        }

        public override List<Expression> InnerExpressions()
        {
            return this._val.Nodes.ToList();
        }

    }

    public sealed class MethodPrintM : Method
    {

        private MatrixExpression _val;
        private Session _Enriro;

        public MethodPrintM(Method Parent, Session Enviro, MatrixExpression Val)
            : base(Parent)
        {
            this._val = Val;
            this._Enriro = Enviro;
        }

        public override void Invoke()
        {
            this._Enriro.IO.WriteLine(this._val.Evaluate().ToString());
        }

        public override Method CloneOfMe()
        {
            return new MethodPrintM(this._Parent, this._Enriro, this._val.CloneOfMe());
        }

        public override List<Expression> InnerExpressions()
        {
            return this._val.InnerExpressions();
        }

    }

    public sealed class MethodPrintL : Method
    {

        private Lambda _val;
        private Session _Enriro;

        public MethodPrintL(Method Parent, Session Enviro, Lambda Val)
            : base(Parent)
        {
            this._val = Val;
            this._Enriro = Enviro;
        }

        public override void Invoke()
        {
            this._Enriro.IO.WriteLine(this._val.ToString());
        }

        public override Method CloneOfMe()
        {
            return new MethodPrintL(this._Parent, this._Enriro, this._val);
        }

    }

}
