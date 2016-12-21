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

    public sealed class MethodMatrixAllAssign : Method
    {

        private Expression _Node;
        private Heap<CellMatrix> _MHeap;
        private int _AssignID; // 0 == assign, 1 == increment, 2 == decrement, 3 == auto increment, 4 == auto decrement
        private int _Ref = -1;

        public MethodMatrixAllAssign(Method Parent, Heap<CellMatrix> Heap, int Ref, Expression Node, int AssignID)
            : base(Parent)
        {

            this._MHeap = Heap;
            this._Ref = Ref;
            this._Node = Node;
            this._AssignID = AssignID;

        }

        public Heap<CellMatrix> InnerHeap
        {
            get { return this._MHeap; }
            set { this._MHeap = value; }
        }

        public override void Invoke()
        {

            Cell c = (this._Node == null ? Cell.OneValue(this._MHeap[this._Ref].Affinity) : this._Node.Evaluate());
            switch (this._AssignID)
            {
                case 0:
                    this._MHeap[this._Ref] = new CellMatrix(this._MHeap[this._Ref].RowCount, this._MHeap[this._Ref].ColumnCount, c);
                    break;
                case 1:
                case 3:
                    this._MHeap[this._Ref] += c;
                    break;
                case 2:
                case 4:
                    this._MHeap[this._Ref] -= c;
                    break;
            }

        }

        public override string Message()
        {
            return string.Format("Matrix all units assignment '{0}'", this._MHeap.Name(this._Ref));
        }

        public override Method CloneOfMe()
        {
            return new MethodMatrixAllAssign(this.Parent, this._MHeap, this._Ref, this._Node.CloneOfMe(), this._AssignID);
        }

        public override List<Expression> InnerExpressions()
        {
            return new List<Expression>() { this._Node };
        }

    }

}
