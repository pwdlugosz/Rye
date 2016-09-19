using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Structures;

namespace Rye.Expressions
{

    public class Filter
    {

        private Expression _Node;
        private bool _Default = false;

        public Filter(Expression Node)
        {


            if (Node.ReturnAffinity() != CellAffinity.BOOL)
            {
                throw new Exception(string.Format("Node passed does not return boolean : {0} : {1}", Node.ReturnAffinity(), Node.Unparse(null)));
            }
            this._Node = Node;
        }

        internal bool Default
        {
            get { return _Default; }
        }

        public Expression Node
        {
            get { return this._Node; }
        }

        public bool Render()
        {
            return this._Node.Evaluate().valueBOOL;
        }

        public Filter NOT
        {
            get
            {
                Expression node = this._Node.CloneOfMe();
                Expression t = new ExpressionResult(node.ParentNode, new CellUniNot());
                t.AddChildNode(node);
                return new Filter(t);
            }
        }

        public string UnParse(Schema Columns)
        {
            return this._Node.Unparse(Columns);
        }

        public Filter CloneOfMe()
        {
            return new Filter(this._Node.CloneOfMe());
        }

        public static Filter TrueForAll
        {
            get 
            {
                Filter p = new Filter(new ExpressionValue(null, new Cell(true)));
                p._Default = true;
                return p; 
            }
        }

        public static Filter FalseForAll
        {
            get { return new Filter(new ExpressionValue(null, new Cell(false))); }
        }

    }

    
    public static class FilterFactory
    {

        public static Filter Equals(Expression Left, Expression Right)
        {
            ExpressionResult node = new ExpressionResult(null, new CellBoolEQ());
            node.AddChildNode(Left);
            node.AddChildNode(Right);
            return new Filter(node);
        }

        public static Filter NotEquals(Expression Left, Expression Right)
        {
            ExpressionResult node = new ExpressionResult(null, new CellBoolNEQ());
            node.AddChildNode(Left);
            node.AddChildNode(Right);
            return new Filter(node);
        }

        public static Filter IsNull(Expression N)
        {
            ExpressionResult node = new ExpressionResult(null, new CellFuncFKIsNull());
            node.AddChildNode(N);
            return new Filter(node);
        }

        public static Filter IsNotNull(Expression N)
        {
            ExpressionResult node = new ExpressionResult(null, new CellFuncFKIsNotNull());
            node.AddChildNode(N);
            return new Filter(node);
        }

        public static Filter LessThan(Expression Left, Expression Right)
        {
            ExpressionResult node = new ExpressionResult(null, new CellBoolLT());
            node.AddChildNode(Left);
            node.AddChildNode(Right);
            return new Filter(node);
        }

        public static Filter GreaterThan(Expression Left, Expression Right)
        {
            ExpressionResult node = new ExpressionResult(null, new CellBoolGT());
            node.AddChildNode(Left);
            node.AddChildNode(Right);
            return new Filter(node);
        }

        public static Filter LessThanOrEquals(Expression Left, Expression Right)
        {
            ExpressionResult node = new ExpressionResult(null, new CellBoolLTE());
            node.AddChildNode(Left);
            node.AddChildNode(Right);
            return new Filter(node);
        }

        public static Filter GreaterThanOrEquals(Expression Left, Expression Right)
        {
            ExpressionResult node = new ExpressionResult(null, new CellBoolGTE());
            node.AddChildNode(Left);
            node.AddChildNode(Right);
            return new Filter(node);
        }

        public static Filter Between(Expression Compare, Expression Lower, Expression Upper)
        {
            Expression LT = LessThan(Compare, Upper).Node;
            Expression GT = GreaterThan(Compare, Lower).Node;
            return And(LT, GT);
        }
        
        public static Filter BetweenIN(Expression Compare, Expression Lower, Expression Upper)
        {
            Expression LT = LessThanOrEquals(Compare, Upper).Node;
            Expression GT = GreaterThanOrEquals(Compare, Lower).Node;
            return And(LT, GT);
        }

        public static Filter And(params Expression[] Nodes)
        {
            ExpressionResult and = new ExpressionResult(null, new AndMany());
            and.AddChildren(Nodes);
            return new Filter(and);
        }

        public static Filter Or(params Expression[] Nodes)
        {
            ExpressionResult or = new ExpressionResult(null, new AndMany());
            or.AddChildren(Nodes);
            return new Filter(or);
        }

    }
    
    
}
