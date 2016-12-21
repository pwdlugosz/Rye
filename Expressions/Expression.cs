using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Structures;

namespace Rye.Expressions
{

    public abstract class Expression : IRegisterExtractor
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
        public virtual Heap<Register> GetMemoryRegisters()
        {

            Heap<Register> bag = new Heap<Register>();
            foreach (Expression e in this._Cache)
            {
                bag.Import(e.GetMemoryRegisters());
            }
            return bag;
       
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

        public static int DecompileToFieldRef(Expression E)
        {
            
            if (E == null)
                return -1;
            if (E is ExpressionFieldRef)
                return (E as ExpressionFieldRef).Index;
            return -1;

        }

        // Clone Support //
        public static void AssignRegister(Expression E, Register R)
        {

            // Check if Value is a field ref //
            if (E is ExpressionFieldRef)
                (E as ExpressionFieldRef).ForceMemoryRegister(R);

            // Leave if there are no child nodes //
            if (E.Children.Count == 0)
                return;

            // Otherwise, go back through all child nodes and recursively call this method //
            foreach (Expression x in E.Children)
            {
                Expression.AssignRegister(x, R);
            }

        }

        public static void AssignRegister(Expression E, Heap<Register> R)
        {

            foreach (Register r in R.Values)
            {
                Expression.AssignRegister(E, r);
            }

        }

        public static void AssignCellHeap(Expression E, Heap<Cell> H)
        {

            // Check if Value is a heap ref //
            if (E is ExpressionHeapRef)
                (E as ExpressionHeapRef).ForceHeap(H);

            // Leave if there are no child nodes //
            if (E.Children.Count == 0)
                return;

            // Otherwise, go back through all child nodes and recursively call this method //
            foreach (Expression x in E.Children)
            {
                Expression.AssignCellHeap(x, H);
            }

        }

        public static void AssignMatrixHeap(Expression E, Heap<CellMatrix> H)
        {

            // Check if Value is a heap ref //
            if (E is ExpressionMatrixRef)
                (E as ExpressionMatrixRef).ForceHeap(H);

            // Leave if there are no child nodes //
            if (E.Children.Count == 0)
                return;

            // Otherwise, go back through all child nodes and recursively call this method //
            foreach (Expression x in E.Children)
            {
                Expression.AssignMatrixHeap(x, H);
            }

        }

        public static void ForceAssignRegister(Expression E, Register R)
        {

            if (E is ExpressionFieldRef)
            {
                (E as ExpressionFieldRef).MemoryRegister = R;
                return;
            }

            if (E.IsTerminal)
                return;

            foreach (Expression e in E.Children)
            {
                Expression.ForceAssignRegister(E, R);
            }

        }


    }

}
