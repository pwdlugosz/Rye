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
    /// Base class for all actions; unlike expressions, actions do not return anything
    /// </summary>
    public abstract class Method
    {

        protected Method _Parent;
        protected List<Method> _Children;
        protected int _RaiseElement = 0; // 0 == normal, 1 == break loop, 2 == break current read
        protected Register _reg;

        public Method(Method Parent)
        {
            this._Parent = Parent;
            this._Children = new List<Method>();
            this._reg = null;
        }

        // Properties //
        public Method Parent
        {
            get { return this._Parent; }
        }

        public List<Method> Children
        {
            get { return this._Children; }
        }

        public int Raise
        {
            get { return this._RaiseElement; }
            protected set { this._RaiseElement = value; }
        }

        public bool IsMaster
        {
            get { return this._Parent == null; }
        }

        public bool IsTerminal
        {
            get { return this._Children.Count == 0; }
        }

        public bool IsLonely
        {
            get { return this.IsMaster && this.IsTerminal; }
        }

        public bool MustBeLonely
        {
            get;
            protected set;
        }

        public virtual bool CanBeAsync
        {

            get
            {

                bool t = true;
                foreach (Method m in this._Children)
                    t = t & m.CanBeAsync;
                return t;

            }

        }

        // Methods //
        public abstract Method CloneOfMe();

        /// <summary>
        /// Performs an action
        /// </summary>
        public abstract void Invoke();

        /// <summary>
        /// Called once before the first invoke
        /// </summary>
        public virtual void BeginInvoke()
        {

        }

        /// <summary>
        /// Called once after the last invoke
        /// </summary>
        public virtual void EndInvoke()
        {
        }

        /// <summary>
        /// Invokes all children
        /// </summary>
        public virtual void InvokeChildren()
        {
            foreach (Method n in this._Children)
                n.Invoke();
        }

        /// <summary>
        /// BeginInvokes all children
        /// </summary>
        public virtual void BeginInvokeChildren()
        {
            foreach (Method n in this._Children)
                n.BeginInvoke();
        }

        /// <summary>
        /// EndInvokes all children
        /// </summary>
        public virtual void EndInvokeChildren()
        {
            foreach (Method n in this._Children)
                n.EndInvoke();
        }

        /// <summary>
        /// Adds a child node
        /// </summary>
        /// <param name="Node">A single child node to add to the current node</param>
        public void AddChild(Method Node)
        {
            if (Node.MustBeLonely)
                throw new Exception("This node cannot be an element of a tree");

            Node._Parent = this;
            this._Children.Add(Node);
        }

        /// <summary>
        /// Adds one or more children nodes
        /// </summary>
        /// <param name="Nodes">The collection of nodes to add</param>
        public void AddChildren(params Method[] Nodes)
        {
            foreach (Method n in Nodes)
                this.AddChild(n);
        }

        /// <summary>
        /// Raises an element into the parent node; this is really use for exiting loops are read commands
        /// </summary>
        /// <param name="RaiseElement">An integer raise code</param>
        protected void RaiseUp(int RaiseElement)
        {
            this._RaiseElement = RaiseElement;
            if (this._Parent != null)
                this.Parent.RaiseUp(RaiseElement);
        }

        /// <summary>
        /// Returns a message to the user
        /// </summary>
        /// <returns>A string message</returns>
        public virtual string Message()
        {
            return "Method";
        }

        /// <summary>
        /// Gets all expressions used by this method
        /// </summary>
        /// <returns></returns>
        public virtual List<Expression> InnerExpressions()
        {

            List<Expression> val = new List<Expression>();
            foreach (Method x in this._Children)
            {
                val.AddRange(x.InnerExpressions());
            }
            return val;

        }

        /// <summary>
        /// Using the original method, adds cloned children to a designated clone
        /// </summary>
        /// <param name="Orignal">The prime method</param>
        /// <param name="Clone">The clone</param>
        public static void AppendClonedChildren(Method Orignal, Method Clone)
        {

            foreach (Method m in Orignal.Children)
            {
                Clone.AddChild(m.CloneOfMe());
            }

        }

    }

}
