using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Structures;

namespace Rye.Methods
{

    /// <summary>
    /// For loop class
    /// </summary>
    public sealed class MethodFor : Method
    {

        private Cell _Current;
        private Expression _Begin;
        private Expression _End;
        private Expression _Step;
        private int _ptrControll;
        private Heap<Cell> _Heap;

        public MethodFor(Method Parent, Expression Begin, Expression End, Expression Step, Heap<Cell> Heap, int CellPointer)
            : base(Parent)
        {

            if (Begin.IsVolatile)
                throw new ArgumentException("The 'Begin' variable cannot be volatible in a for-loop");
            if (End.IsVolatile)
                throw new ArgumentException("The 'End' variable cannot be volatile in a for-loop");
            
            this._Begin = Begin;
            this._End = End;
            this._Heap = Heap;
            this._Step = Step;
            this._ptrControll = CellPointer;

        }

        public Heap<Cell> InnerHeap
        {
            get { return this._Heap; }
            set { this._Heap = value; }
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

            Cell Begin = this._Begin.Evaluate();
            Cell End = this._End.Evaluate();
            Cell Step = this._Step.Evaluate();
            for (this._Current = Begin; this._Current <= End; this._Current += Step)
            {

                // Assign the controll variable //
                this._Heap[this._ptrControll] = this._Current;
                
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
            sb.AppendLine(string.Format("For Loop: {0} to {1}", this._Begin.Evaluate(), this._End.Evaluate()));
            for (int i = 0; i < this._Children.Count; i++)
            {
                sb.AppendLine('\t' + this._Children[i].Message());
            }
            return sb.ToString();

        }

        public override Method CloneOfMe()
        {
            MethodFor node = new MethodFor(this.Parent, this._Begin.CloneOfMe(), this._End.CloneOfMe(), this._Step.CloneOfMe(), this._Heap, this._ptrControll);
            foreach (Method t in this._Children)
                node.AddChild(t.CloneOfMe());
            return node;
        }

        public override List<Expression> InnerExpressions()
        {
            return new List<Expression>() { this._Begin, this._End, this._Step };
        }

    }

}
