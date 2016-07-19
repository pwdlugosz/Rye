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
    /// Represents set of actions not in a tree
    /// </summary>
    public sealed class MethodBeginEnd : Method
    {

        public MethodBeginEnd(Method Parent)
            : base(Parent)
        {
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
            this.InvokeChildren();
        }

        public override string Message()
        {

            StringBuilder sb = new StringBuilder();
            sb.AppendLine("Skip-End");
            for (int i = 0; i < this._Children.Count; i++)
            {

                if (i != this._Children.Count - 1)
                    sb.AppendLine('\t' + this._Children[i].Message());
                else
                    sb.Append('\t' + this._Children[i].Message());

            }
            return sb.ToString();

        }

        public override Method CloneOfMe()
        {
            MethodBeginEnd node = new MethodBeginEnd(this.Parent);
            foreach (Method t in this._Children)
                node.AddChild(t.CloneOfMe());
            return node;
        }

    }

}
