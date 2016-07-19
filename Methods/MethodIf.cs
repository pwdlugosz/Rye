﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Methods
{

    /// <summary>
    /// If node
    /// </summary>
    public sealed class MethodIf : Method
    {

        private Filter _Condition;

        public MethodIf(Method Parent, Filter Condition)
            : base(Parent)
        {
            this._Condition = Condition;
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

            if (this._Condition.Render())
            {
                this._Children[0].Invoke();
            }
            else if (this._Children.Count >= 2)
            {
                this._Children[1].Invoke();
            }

        }

        public override string Message()
        {

            StringBuilder sb = new StringBuilder();
            sb.AppendLine("If");
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
            MethodIf node = new MethodIf(this.Parent, this._Condition);
            foreach (Method t in this._Children)
                node.AddChild(t.CloneOfMe());
            return node;
        }

    }

}