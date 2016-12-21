using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Methods
{

    /// <summary>
    /// While
    /// </summary>
    public sealed class MethodWhile : Method
    {

        private Expression _control;

        public MethodWhile(Method Parent, Expression ControlFlow)
            : base(Parent)
        {
            this._control = ControlFlow;
        }

        public override void BeginInvoke()
        {
            base.BeginInvoke();
            this.BeginInvokeChildren();
        }

        public override void EndInvoke()
        {
            base.EndInvoke();
            this.EndInvokeChildren();
        }

        public override void Invoke()
        {

            while(this._control.Evaluate().BOOL == true)
            {

                // Invoke children //
                foreach (Method node in this._Children)
                {

                    // Invoke //
                    node.Invoke();

                    // Check for the raise state == 1 or 2//
                    if (node.Raise == 1 || node.Raise == 2)
                        return;

                }

            }

        }

        public override string Message()
        {

            StringBuilder sb = new StringBuilder();
            sb.AppendLine("While-Loop");
            for (int i = 0; i < this._Children.Count; i++)
            {
                sb.AppendLine('\t' + this._Children[i].Message());
            }
            return sb.ToString();

        }

        public override Method CloneOfMe()
        {
            Method node = new MethodWhile(this.Parent, this._control.CloneOfMe());
            foreach (Method t in this._Children)
                node.AddChild(t.CloneOfMe());
            return node;
        }

        public override List<Expression> InnerExpressions()
        {
            return new List<Expression>() { this._control };
        }

    }

}
