﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Expressions
{

    public sealed class Lambda
    {

        private Expression _Expression;
        private string _Name;
        private List<string> _Pointers;

        // Constructor //
        public Lambda(string Name, Expression Expression, List<string> Parameters)
        {
            
            this._Expression = Expression;
            this._Name = Name;
            this._Pointers = Parameters;
            this.Count = Parameters.Count;
            
        }

        public Lambda(string Name, Expression Expression)
            : this(Name, Expression, Analytics.AllPointersRefs(Expression).Distinct().ToList())
        {
        }

        public Lambda(string Name, Expression Expression, string Parameter)
            : this(Name, Expression, new List<string>() { Parameter })
        {
        }

        // Properties //
        public Expression InnerNode
        {
            get { return this._Expression; }
        }

        public string Name
        {
            get { return this._Name; }
        }

        public List<string> Pointers
        {
            get { return this._Pointers; }
        }

        public int Count
        {
            get;
            private set;
        }

        // Methods //
        public Expression Bind(List<Expression> Bindings)
        {

            Expression node = this._Expression.CloneOfMe();
            List<ExpressionPointer> refs = Analytics.AllPointers(node);

            Dictionary<string, int> idx = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            int i = 0;
            foreach (string s in this._Pointers)
            {
                idx.Add(s, i);
                i++;
            }

            foreach (ExpressionPointer n in refs)
            {
                int node_ref = idx[n.Name];
                Expression t = Bindings[node_ref];
                Analytics.ReplaceNode(n, t);
            }

            return node; 

        }

        public Lambda Gradient(string Name, string PointerName)
        {

            Expression node = ExpressionGradient.Gradient(this._Expression, PointerName);
            return new Lambda(Name, node, this._Pointers);

        }

        public Expression Gradient(string PointerName)
        {
            return ExpressionGradient.Gradient(this._Expression, PointerName);
        }

        public ExpressionCollection PartialGradients()
        {

            ExpressionCollection nodes = new ExpressionCollection();
            foreach (string ptr in this._Pointers)
                nodes.Add(this.Gradient(ptr), ptr);
            return nodes;

        }

        public bool IsDifferntiable(string PointerName)
        {

            try
            {
                Lambda dx = Gradient("test", PointerName);
                return true;
            }
            catch
            {
                return false;
            }

        }

        public string Unparse()
        {
            return this._Expression.Unparse(null);
        }

        public string FormatString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(this.Name);
            sb.Append("(");
            for (int i = 0; i < this._Pointers.Count; i++)
            {
                sb.Append(this._Pointers[i]);
                if (i != this.Count - 1) sb.Append(",");
            }
            sb.Append(") AS ");
            sb.Append(this._Expression.Unparse(null));
            return sb.ToString();
        }

        public Lambda CloneOfMe()
        {

            return new Lambda(this._Name, this._Expression.CloneOfMe(), this._Pointers);

        }

    }

}
