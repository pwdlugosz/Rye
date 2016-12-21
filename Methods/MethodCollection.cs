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
    /// Represents a tree of TNodes //
    /// </summary>
    public class MethodCollection
    {

        protected List<Method> _tree;
        protected List<int> _ReturnRefs;

        public MethodCollection()
        {
            this._tree = new List<Method>();
            this._ReturnRefs = new List<int>();
        }

        public int Count
        {
            get { return this._tree.Count; }
        }

        public List<Method> Nodes
        {
            get { return this._tree; }
        }

        public bool CheckBreak
        {
            get
            {
                for (int i = 0; i < this.Count; i++)
                {
                    if (this._tree[i].Raise == 2)
                        return true;
                }
                return false;
            }
        }

        public void Add(Method Node)
        {
            this._tree.Add(Node);
            if (Node is MethodAppendTo)
            {
                this._ReturnRefs.Add(this._tree.Count - 1);
            }
        }

        public void Invoke()
        {
            foreach (Method n in this._tree)
                n.Invoke();
        }

        public void BeginInvoke()
        {
            foreach (Method n in this._tree)
                n.BeginInvoke();
        }

        public void EndInvoke()
        {
            foreach (Method n in this._tree)
                n.EndInvoke();
        }

        public void InvokeChildren()
        {
            foreach (Method n in this._tree)
                n.InvokeChildren();
        }

        public void BeginInvokeChildren()
        {
            foreach (Method n in this._tree)
                n.BeginInvokeChildren();
        }

        public void EndInvokeChildren()
        {
            foreach (Method n in this._tree)
                n.EndInvokeChildren();
        }

        public void InvokeAll()
        {
            this.BeginInvoke();
            this.Invoke();
            this.EndInvoke();
        }

        public void InvokeChildrenAll()
        {
            this.BeginInvokeChildren();
            this.InvokeChildren();
            this.EndInvokeChildren();
        }

        public MethodCollection CloneOfMe()
        {

            MethodCollection nodes = new MethodCollection();
            foreach (Method t in this._tree)
            {
                nodes.Add(t.CloneOfMe());
            }
            return nodes;

        }


    }

}
