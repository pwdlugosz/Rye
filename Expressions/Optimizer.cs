using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Libraries;

namespace Rye.Expressions
{

    public sealed class Optimizer
    {

        private int _Ticks = 0;
        private int _Tocks = 0;
        private int _Cycles = 0;

        // Compact all //
        public Expression Compact(Expression Node)
        {

            // Clone the current node //
            //Expression t = Node.CloneOfMe();

            this._Tocks = 1;
            while (this._Tocks != 0)
            {

                // Reset the tock variables //
                this._Tocks = 0;

                // Compact the leaf node; note that we may need to do this again //
                Node = CompactUnit(Node);

                // Accumulate the ticks //
                this._Ticks += this._Tocks;

                // Accumulate the cycles //
                this._Cycles++;

            }

            // return the compacted node //
            return Node;

        }

        public int TotalCompacts
        {
            get { return this._Ticks; }
        }

        public int Cycles
        {
            get { return this._Cycles; }
        }

        private Expression CompactUnit(Expression Node)
        {

            for (int i = 0; i < Node.Children.Count; i++)
                Node.Children[i] = CompactUnit(Node.Children[i]);

            return CompactSingle(Node);

        }

        private Expression CompactSingle(Expression Node)
        {

            // The order we do these is optimized to reduce the number of tock loops //
            Node = CompactPower(Node);
            Node = CompactMultDivMod(Node);
            Node = CompactAddSub(Node);
            Node = CompactUni(Node);
            Node = CompactCancleOut(Node);
            Node = CompactStaticArguments(Node);

            return Node;

        }

        // A - A -> 0
        // A / A -> 1
        private Expression CompactCancleOut(Expression Node)
        {

            if (Node.Affinity != ExpressionAffinity.Result)
                return Node;

            ExpressionResult x = (Node as ExpressionResult);
            string name = x.InnerFunction.NameSig;

            // Check that the node is either - or / //
            if (name != BaseLibrary.OP_SUB && name != BaseLibrary.OP_DIV && name != BaseLibrary.OP_DIV2)
                return Node;

            // Build an equality checker //
            IEqualityComparer<Expression> lne = new ExpressionComparer();

            // Check if A == B //
            if (!lne.Equals(Node.Children[0], Node.Children[1]))
                return Node;

            // Check for A - A -> 0 //
            if (name == BaseLibrary.OP_SUB)
                return new ExpressionValue(Node.ParentNode, Cell.ZeroValue(CellAffinity.DOUBLE));

            // Check for A - A -> 0 //
            if (name == BaseLibrary.OP_DIV || name == BaseLibrary.OP_DIV2)
                return new ExpressionValue(Node.ParentNode, Cell.OneValue(CellAffinity.DOUBLE));

            return Node;

        }

        // -(-A) -> A
        // !(!A) -> A
        // -c -> -c where c is a constant
        // !c -> !c where c is a constant
        private Expression CompactUni(Expression Node)
        {

            if (Node.Affinity != ExpressionAffinity.Result)
                return Node;

            ExpressionResult x = (Node as ExpressionResult);
            string name = x.InnerFunction.NameSig;

            // Check that the node is either -A, +A, !A //
            if (name != BaseLibrary.UNI_MINUS && name != BaseLibrary.UNI_PLUS && name != BaseLibrary.UNI_NOT)
                return Node;

            // Check for the child being a constant //
            if (Node.Children[0].Affinity == ExpressionAffinity.Value)
            {
                Cell c = (Node.Children[0] as ExpressionValue).InnerValue;
                if (name == BaseLibrary.UNI_MINUS)
                    c = -c;
                if (name == BaseLibrary.UNI_NOT)
                    c = !c;
                return new ExpressionValue(Node.ParentNode, c);
            }

            // Check that A = F(B) //
            if (Node.Children[0].Affinity != ExpressionAffinity.Result)
                return Node;

            // Get the name of the function of the child node //
            string sub_name = (Node.Children[0] as ExpressionResult).InnerFunction.NameSig;

            // Check for -(-A) //
            if (name == BaseLibrary.UNI_MINUS && sub_name == BaseLibrary.UNI_MINUS)
                return Node.Children[0].Children[0];

            // Check for !(!A) //
            if (name == BaseLibrary.UNI_NOT && sub_name == BaseLibrary.UNI_NOT)
                return Node.Children[0].Children[0];

            return Node;

        }

        // A + 0 or 0 + A or A - 0 -> A
        // 0 - A -> -A 
        // A + -B -> A - B
        private Expression CompactAddSub(Expression Node)
        {

            if (Node.Affinity != ExpressionAffinity.Result)
                return Node;

            ExpressionResult x = (Node as ExpressionResult);
            string name = x.InnerFunction.NameSig;

            if (name != BaseLibrary.OP_ADD && name != BaseLibrary.OP_SUB)
                return Node;

            // Look for A + 0 or A - 0 -> A //
            if (IsStaticZero(Node.Children[1]))
            {
                this._Tocks++;
                return Node.Children[0];
            }

            // Look for 0 + A -> A //
            if (IsStaticZero(Node.Children[0]) && name == BaseLibrary.OP_ADD)
            {
                this._Tocks++;
                return Node.Children[1];
            }

            // Look for 0 - A -> -A //
            if (IsStaticZero(Node.Children[0]) && name == BaseLibrary.OP_SUB)
            {
                this._Tocks++;
                Expression t = new ExpressionResult(Node.ParentNode, new CellUniMinus());
                t.AddChildNode(Node.Children[1]);
                return t;
            }

            // Look for A + -B -> A - B //
            if (IsUniNegative(Node.Children[1]) && name == BaseLibrary.OP_ADD)
            {
                this._Tocks++;
                Expression t = new ExpressionResult(Node.ParentNode, new CellBinMinus());
                t.AddChildNode(Node.Children[0]);
                t.AddChildNode(Node.Children[1].Children[0]);
                return t;
            }

            // Look for -A + B -> B - A //
            if (IsUniNegative(Node.Children[0]) && name == BaseLibrary.OP_ADD)
            {
                this._Tocks++;
                Expression t = new ExpressionResult(Node.ParentNode, new CellBinMinus());
                t.AddChildNode(Node.Children[1]);
                t.AddChildNode(Node.Children[0].Children[0]);
                return t;
            }

            // Look for A - -B -> A + B //
            if (IsUniNegative(Node.Children[1]) && name == BaseLibrary.OP_SUB)
            {
                this._Tocks++;
                Expression t = new ExpressionResult(Node.ParentNode, new CellBinPlus());
                t.AddChildNode(Node.Children[0]);
                t.AddChildNode(Node.Children[1].Children[0]);
                return t;
            }

            return Node;

        }

        // A * 1 or 1 * A or A / 1 or A /? 1 or A % 1 -> A 
        // A * -1 or -1 * A or A / -1 or A /? -1 or A % -1 -> -A 
        // A * 0, 0 * A, 0 / A, 0 /? A, A /? 0, 0 % A -> 0 
        // A / 0, A % 0 -> null
        private Expression CompactMultDivMod(Expression Node)
        {

            if (Node.Affinity != ExpressionAffinity.Result)
                return Node;

            ExpressionResult x = (Node as ExpressionResult);
            string name = x.InnerFunction.NameSig;

            if (name != BaseLibrary.OP_MUL
                && name != BaseLibrary.OP_DIV
                && name != BaseLibrary.OP_DIV2
                && name != BaseLibrary.OP_MOD)
                return Node;

            // A * 1 or A / 1 or A /? 1 or A % 1 //
            if (IsStaticOne(Node.Children[1]))
            {
                this._Tocks++;
                return Node.Children[0];
            }

            // 1 * A //
            if (IsStaticOne(Node.Children[0]) && name == BaseLibrary.OP_MUL)
            {
                this._Tocks++;
                return Node.Children[1];
            }

            // A * -1 or A / -1 or A /? -1 or A % -1 //
            if (IsStaticMinusOne(Node.Children[1]))
            {
                this._Tocks++;
                Expression t = new ExpressionResult(Node.ParentNode, new CellUniMinus());
                t.AddChildNode(Node.Children[0]);
                return t;
            }

            // -1 * A //
            if (IsStaticMinusOne(Node.Children[0]) && name == BaseLibrary.OP_MUL)
            {
                this._Tocks++;
                Expression t = new ExpressionResult(Node.ParentNode, new CellUniMinus());
                t.AddChildNode(Node.Children[1]);
                return t;
            }

            // Look 0 * A, 0 / A, 0 /? A, 0 % A //
            if (IsStaticZero(Node.Children[0]))
            {
                this._Tocks++;
                return new ExpressionValue(Node.ParentNode, new Cell(0.00));
            }

            // A * 0, A /? 0 //
            if (IsStaticZero(Node.Children[1]) && (name == BaseLibrary.OP_MUL || name == BaseLibrary.OP_DIV2))
            {
                this._Tocks++;
                return new ExpressionValue(Node.ParentNode, new Cell(0.00));
            }

            // A / 0, A % 0 //
            if (IsStaticZero(Node.Children[1]) && (name == BaseLibrary.OP_DIV || name == BaseLibrary.OP_MOD))
            {
                this._Tocks++;
                return new ExpressionValue(Node.ParentNode, Cell.NULL_DOUBLE);
            }

            return Node;

        }

        // 1 * 2 + 3 -> 5
        private Expression CompactStaticArguments(Expression Node)
        {

            if (ChildrenAreAllStatic(Node))
            {
                this._Tocks++;
                return new ExpressionValue(Node.ParentNode, Node.Evaluate());
            }

            return Node;

        }

        // power(A,1) -> A
        // power(A,0) -> 1
        private Expression CompactPower(Expression Node)
        {

            if (Node.Affinity != ExpressionAffinity.Result)
                return Node;

            ExpressionResult x = (Node as ExpressionResult);
            string name = x.InnerFunction.NameSig;

            if (name != BaseLibrary.FUNC_POWER)
                return Node;

            // Check the second argument of power(A,B) looking for B == 1 //
            if (IsStaticOne(Node.Children[1]))
                return Node.Children[0];

            // Check the second argumnet of power(A,B) looging for B == 0, if so return static 1.000, even power(0,0) = 1.000 //
            if (IsStaticZero(Node.Children[1]))
                return new ExpressionValue(Node.ParentNode, new Cell(1.000));

            return Node;

        }

        // Helpers //
        public static bool IsStaticZero(Expression Node)
        {
            if (Node.Affinity == ExpressionAffinity.Value)
                return (Node as ExpressionValue).InnerValue == Cell.ZeroValue(Node.ReturnAffinity());
            return false;
        }

        public static bool IsStaticOne(Expression Node)
        {
            if (Node.Affinity == ExpressionAffinity.Value)
                return (Node as ExpressionValue).InnerValue == Cell.OneValue(Node.ReturnAffinity());
            return false;
        }

        public static bool IsStaticMinusOne(Expression Node)
        {
            if (Node.Affinity == ExpressionAffinity.Value)
                return (Node as ExpressionValue).InnerValue == -Cell.OneValue(Node.ReturnAffinity());
            if (Node.Affinity == ExpressionAffinity.Result)
            {
                ExpressionResult x = (Node as ExpressionResult);
                if (x.InnerFunction.NameSig == BaseLibrary.UNI_MINUS && IsStaticOne(x.Children[0]))
                    return true;
            }
            return false;
        }

        public static bool IsUniNegative(Expression Node)
        {
            if (Node.Affinity == ExpressionAffinity.Result)
            {
                ExpressionResult x = (Node as ExpressionResult);
                return x.InnerFunction.NameSig == BaseLibrary.UNI_MINUS;
            }
            return false;
        }

        public static bool ChildrenAreAllStatic(Expression Node)
        {

            if (Node.IsTerminal)
                return false;

            foreach (Expression n in Node.Children)
            {
                if (n.Affinity != ExpressionAffinity.Value)
                    return false;
            }
            return true;

        }

        // Opperands //
        public static Expression CompactNode(Expression Node)
        {
            Optimizer lnc = new Optimizer();
            return lnc.Compact(Node);
        }

        public static ExpressionCollection CompactTree(ExpressionCollection Tree)
        {

            ExpressionCollection t = new ExpressionCollection();

            foreach (Expression n in Tree.Nodes)
                t.Add(CompactNode(n));

            return t;

        }

        /// <summary>
        /// Binds a leaf node to another node
        /// </summary>
        /// <param name="MainNode">The node containing a pointer node that will be bound</param>
        /// <param name="ParameterNode">The node that will be bound to the MainNode</param>
        /// <param name="PointerNodeName">The name of the pointer the ParameterNode will be replacing</param>
        /// <returns></returns>
        public static Expression Bind(Expression MainNode, Expression ParameterNode, string PointerNodeName)
        {

            // Clone the current node //
            Expression t = MainNode.CloneOfMe();

            // Decompile t //
            List<ExpressionPointer> refs = Analytics.AllPointers(t);

            // Replace the pointer node with the parameter node //
            foreach (ExpressionPointer x in refs)
            {
                if (x.PointerName == PointerNodeName)
                    Analytics.ReplaceNode(x, ParameterNode);
            }

            return t;

        }

    }


}
