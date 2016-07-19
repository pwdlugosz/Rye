using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.MatrixExpressions
{

    public abstract class MatrixExpression
    {

        private MatrixExpression _ParentNode;
        protected List<MatrixExpression> _Cache;

        public MatrixExpression(MatrixExpression Parent)
        {
            this._ParentNode = Parent;
            this._Cache = new List<MatrixExpression>();
        }

        public MatrixExpression ParentNode
        {
            get { return _ParentNode; }
            set { this._ParentNode = value; }
        }

        public bool IsMaster
        {
            get { return _ParentNode == null; }
        }

        public bool IsTerminal
        {
            get { return this.Children.Count == 0; }
        }

        public bool IsQuasiTerminal
        {
            get
            {
                if (this.IsTerminal) return false;
                return this.Children.TrueForAll((n) => { return n.IsTerminal; });
            }
        }

        public MatrixExpression this[int IndexOf]
        {
            get { return this._Cache[IndexOf]; }
        }

        // Methods //
        public void AddChildNode(MatrixExpression Node)
        {
            Node.ParentNode = this;
            this._Cache.Add(Node);
        }

        public void AddChildren(params MatrixExpression[] Nodes)
        {
            foreach (MatrixExpression n in Nodes)
                this.AddChildNode(n);
        }

        public List<MatrixExpression> Children
        {
            get { return _Cache; }
        }

        public CellMatrix[] EvaluateChildren()
        {
            List<CellMatrix> c = new List<CellMatrix>();
            foreach (MatrixExpression x in _Cache)
                c.Add(x.Evaluate());
            return c.ToArray();
        }

        public CellAffinity[] ReturnAffinityChildren()
        {
            List<CellAffinity> c = new List<CellAffinity>();
            foreach (MatrixExpression x in _Cache)
                c.Add(x.ReturnAffinity());
            return c.ToArray();
        }

        public void Deallocate()
        {
            if (this.IsMaster) return;
            this.ParentNode.Children.Remove(this);
        }

        // Abstracts //
        public abstract CellMatrix Evaluate();

        public abstract CellAffinity ReturnAffinity();

        public abstract MatrixExpression CloneOfMe();

    }

}
