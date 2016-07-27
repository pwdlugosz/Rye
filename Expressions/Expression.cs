using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Structures;

namespace Rye.Expressions
{

    public abstract class Expression
    {

        private Expression _ParentNode;
        private ExpressionAffinity _Affinity;
        protected List<Expression> _Cache;
        protected Guid _UID;
        protected string _name;

        public Expression(Expression Parent, ExpressionAffinity Affinity)
        {
            this._ParentNode = Parent;
            this._Affinity = Affinity;
            this._Cache = new List<Expression>();
            this._UID = Guid.NewGuid();
            this._name = null;
        }

        public Expression ParentNode
        {
            get { return _ParentNode; }
            set { this._ParentNode = value; }
        }

        public ExpressionAffinity Affinity
        {
            get { return _Affinity; }
        }

        public bool IsMaster
        {
            get { return _ParentNode == null; }
        }

        public bool IsTerminal
        {
            get { return this.Children.Count == 0; }
        }

        public bool IsResult
        {
            get { return this._Affinity == ExpressionAffinity.Result; }
        }

        public Guid NodeID
        {
            get { return this._UID; }
        }

        public bool IsQuasiTerminal
        {
            get
            {
                if (this.IsTerminal) return false;
                return this.Children.TrueForAll((n) => { return n.IsTerminal; });
            }
        }

        public string Name
        {
            get { return this._name; }
            set { this._name = value; }
        }

        public Expression this[int IndexOf]
        {
            get { return this._Cache[IndexOf]; }
        }

        // Methods //
        public void AddChildNode(Expression Node)
        {
            Node.ParentNode = this;
            this._Cache.Add(Node);
        }

        public void AddChildren(params Expression[] Nodes)
        {
            foreach (Expression n in Nodes)
                this.AddChildNode(n);
        }

        public List<Expression> Children
        {
            get { return _Cache; }
        }

        public Cell[] EvaluateChildren()
        {
            List<Cell> c = new List<Cell>();
            foreach (Expression x in _Cache)
                c.Add(x.Evaluate());
            return c.ToArray();
        }

        public CellAffinity[] ReturnAffinityChildren()
        {
            List<CellAffinity> c = new List<CellAffinity>();
            foreach (Expression x in _Cache)
                c.Add(x.ReturnAffinity());
            return c.ToArray();
        }

        public int[] ReturnSizeChildren()
        {

            List<int> c = new List<int>();
            foreach (Expression x in _Cache)
                c.Add(x.DataSize());
            return c.ToArray();

        }

        public void Deallocate()
        {
            if (this.IsMaster) return;
            this.ParentNode.Children.Remove(this);
        }

        public void Deallocate(Expression Node)
        {
            if (this.IsTerminal) return;
            this._Cache.Remove(Node);
        }

        // Abstracts //
        public abstract string Unparse(Schema S);

        public abstract Expression CloneOfMe();

        public abstract Cell Evaluate();

        public abstract CellAffinity ReturnAffinity();

        // Virtuals //
        public virtual int DataSize()
        {

            int max = int.MinValue;
            foreach (Expression n in this._Cache)
            {
                max = Math.Max(n.DataSize(), max);
            }
            return max;

        }

        public virtual bool IsVolatile
        {
            get { return false; }
        }

        // Register Handles //
        public virtual List<Register> GetMemoryRegisters()
        {

            List<Register> r = new List<Register>();
            foreach (Expression e in this._Cache)
            {
                r.AddRange(e.GetMemoryRegisters());
            }
            return r;
       
        }

        public virtual void AssignMemoryRegister(Register OldMemoryRegister, Register NewMemoryRegister)
        {

            // Don't procede if we have not child cache elements
            if (this._Cache.Count == 0)
                return;

            // Do a recursive seek to update the register //
            foreach (Expression e in this._Cache)
            {
                e.AssignMemoryRegister(OldMemoryRegister, NewMemoryRegister);
            }

        }

        // Statics //
        public static Expression BuildParent(Expression L, CellFunction F)
        {
            Expression n = new ExpressionResult(null, F);
            n.AddChildNode(L);
            return n;
        }

        public static int HashCode(List<Expression> Cache)
        {
            int h = int.MaxValue;
            foreach (Expression q in Cache)
                h = h ^ q.GetHashCode();
            return h;
        }

        // Opperators //
        public static Expression operator +(Expression Left, Expression Right)
        {
            ExpressionResult t = new ExpressionResult(null, new CellBinPlus());
            t.AddChildren(Left, Right);
            return t;
        }

        public static Expression operator -(Expression Left, Expression Right)
        {
            ExpressionResult t = new ExpressionResult(null, new CellBinMinus());
            t.AddChildren(Left, Right);
            return t;
        }

        public static Expression operator *(Expression Left, Expression Right)
        {
            ExpressionResult t = new ExpressionResult(null, new CellBinMult());
            t.AddChildren(Left, Right);
            return t;
        }

        public static Expression operator /(Expression Left, Expression Right)
        {
            ExpressionResult t = new ExpressionResult(null, new CellBinDiv());
            t.AddChildren(Left, Right);
            return t;
        }

        public static Expression operator %(Expression Left, Expression Right)
        {
            ExpressionResult t = new ExpressionResult(null, new CellBinMod());
            t.AddChildren(Left, Right);
            return t;
        }

        public static Expression operator ^(Expression Left, Expression Right)
        {
            ExpressionResult t = new ExpressionResult(null, new CellFuncFVPower());
            t.AddChildren(Left, Right);
            return t;
        }


    }

}
