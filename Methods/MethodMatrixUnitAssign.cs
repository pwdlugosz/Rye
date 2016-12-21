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
    /// Assign ID: 0 == assign, 1 == increment, 2 == decrement, 3 == auto increment, 4 == auto decrement
    /// </summary>
    public sealed class MethodMatrixUnitAssign : Method
    {

        private Expression _Node;
        private Heap<CellMatrix> _MHeap;
        private Expression _RowID;
        private Expression _ColID;
        private int _AssignID; // 0 == assign, 1 == increment, 2 == decrement, 3 == auto increment, 4 == auto decrement
        private int _Ref = -1;

        public MethodMatrixUnitAssign(Method Parent, Heap<CellMatrix> Heap, int Ref, Expression Node, Expression RowID, Expression ColumnID, int AssignID)
            : base(Parent)
        {

            if (RowID.IsVolatile)
                throw new ArgumentException("The row index variable cannot be volatile");
            if (ColumnID.IsVolatile)
                throw new ArgumentException("The column index variable cannot be volatile");

            this._MHeap = Heap;
            this._Ref = Ref;
            this._Node = Node;
            this._AssignID = AssignID;
            this._RowID = RowID;
            this._ColID = ColumnID;

        }

        public Heap<CellMatrix> InnerHeap
        {
            get { return this._MHeap; }
            set { this._MHeap = value; }
        }

        public override void Invoke()
        {

            int r = (int)this._RowID.Evaluate().INT;
            int c = (int)this._ColID.Evaluate().INT;

            switch (this._AssignID)
            {
                case 0:
                    this._MHeap[this._Ref][r, c] = this._Node.Evaluate();
                    break;
                case 1:
                    this._MHeap[this._Ref][r, c] += this._Node.Evaluate();
                    break;
                case 2:
                    this._MHeap[this._Ref][r, c] -= this._Node.Evaluate();
                    break;
                case 3:
                    this._MHeap[this._Ref][r, c]++;
                    break;
                case 4:
                    this._MHeap[this._Ref][r, c]--;
                    break;
            }

        }

        public override string Message()
        {
            return string.Format("Matrix unit assignment '{0}'", this._MHeap.Name(this._Ref));
        }

        public override Method CloneOfMe()
        {
            return new MethodMatrixUnitAssign(this.Parent, this._MHeap, this._Ref, this._Node.CloneOfMe(), this._RowID.CloneOfMe(), this._ColID.CloneOfMe(), this._AssignID);
        }
        
        public override List<Expression> InnerExpressions()
        {
            return new List<Expression>() { this._Node, this._RowID, this._ColID };
        }

    }

}
