using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Expressions
{

    public static class Analytics
    {

        // These next two tell us if the node is just a simple field or static value //
        public static bool IsFlatFieldNode(Expression Node)
        {
            if (Node.Affinity == ExpressionAffinity.Field && Node.IsTerminal) return true;
            return false;
        }

        public static bool IsFlatValueNode(Expression Node)
        {
            if (Node.Affinity == ExpressionAffinity.Field && Node.IsTerminal) return true;
            return false;
        }

        // These tell us if the nodes contain certain types //
        public static bool ContainsFieldNodes(Expression Node)
        {

            if (Node.Affinity == ExpressionAffinity.Field) return true;
            if (Node.Affinity == ExpressionAffinity.Value) return false;

            bool b = false;
            foreach (Expression n in Node.Children)
            {
                b = b || ContainsFieldNodes(n);
                if (b == true) return true;
            }

            return false;

        }

        public static bool ContainsValueNodes(Expression Node)
        {

            if (Node.Affinity == ExpressionAffinity.Field) return false;
            if (Node.Affinity == ExpressionAffinity.Value) return true;

            bool b = false;
            foreach (Expression n in Node.Children)
            {
                b = b || ContainsValueNodes(n);
                if (b == true) return true;
            }

            return false;

        }

        public static bool ContainsResultNodes(Expression Node)
        {

            if (Node.Affinity != ExpressionAffinity.Result) return false;
            
            foreach (Expression n in Node.Children)
                if (n.IsResult) return true;

            return false;

        }

        public static bool ContainsResultNode(Expression Node, string Signiture)
        {
            return AllResultNodes(Node, Signiture).Count == 0;
        }

        public static bool ContainsResultNode(Expression Node, CellFunction Func)
        {
            return ContainsResultNode(Node, Func.NameSig);
        }

        public static int FieldNodeCount(Expression Node)
        {

            if (Node.Affinity == ExpressionAffinity.Field) return 1;
            if (Node.Affinity == ExpressionAffinity.Value) return 0;

            int Counter = 0;
            foreach (Expression n in Node.Children)
                Counter += FieldNodeCount(n);

            return Counter;

        }

        public static int ValueNodeCount(Expression Node)
        {

            if (Node.Affinity == ExpressionAffinity.Field) return 0;
            if (Node.Affinity == ExpressionAffinity.Value) return 1;

            int Counter = 0;
            foreach (Expression n in Node.Children)
                Counter += ValueNodeCount(n);

            return Counter;

        }

        public static int ResultNodeCount(Expression Node)
        {

            if (Node.Affinity == ExpressionAffinity.Field) return 0;
            if (Node.Affinity == ExpressionAffinity.Value) return 0;

            int Counter = 1;
            foreach (Expression n in Node.Children)
                Counter += ResultNodeCount(n);

            return Counter;

        }

        // Decompiling Methods //
        public static List<Expression> Decompile(Expression Node)
        {

            // Create the master list //
            List<Expression> master = new List<Expression>();

            // Accumulate the master node //
            //master.Accumulate(Node);

            // Recursive method //
            DecompileHelper(Node, master);

            // Return //
            return master;

        }

        private static void DecompileHelper(Expression Node, List<Expression> AllNodes)
        {

            // Accumulate all the child nodes //
            AllNodes.Add(Node);
            
            // Go through each child node and call this function //
            foreach (Expression n in Node.Children)
                DecompileHelper(n, AllNodes);

        }

        public static List<Expression> AllNodes(Expression Node, ExpressionAffinity Affinity)
        {

            // All nodes that are fields //
            return Decompile(Node).Where((x) => { return x.Affinity == Affinity; }).ToList();

        }

        public static List<T> Convert<T>(List<Expression> Nodes) where T : Expression
        {
            List<T> nodes = Nodes.ConvertAll<T>((x) => { return x as T; });
            return nodes;
        }

        public static List<ExpressionPointer> AllPointers(Expression Node)
        {
            List<Expression> all_pointers = AllNodes(Node, ExpressionAffinity.Pointer);
            List<ExpressionPointer> converted_pointers = Convert<ExpressionPointer>(all_pointers);
            return converted_pointers;
        }

        public static List<string> AllPointersRefs(Expression Node)
        {
            return AllPointers(Node).ConvertAll<string>((node) => { return node.PointerName; });
        }

        public static List<ExpressionResult> AllResultNodes(Expression Node, string Name)
        {

            List<Expression> all_nodes = AllNodes(Node, ExpressionAffinity.Result);
            return Convert<ExpressionResult>(all_nodes).Where((x) => { return StringComparer.OrdinalIgnoreCase.Compare(x.InnerFunction.NameSig, Name) == 0;}).ToList();

        }

        public static Key AllFields(Expression Node)
        {

            List<Expression> all_nodes = AllNodes(Node, ExpressionAffinity.Result);
            List<ExpressionFieldRef> fields = Convert<ExpressionFieldRef>(all_nodes);
            Key k = new Key();
            foreach (ExpressionFieldRef n in fields)
                k.Add(n.Index);
            return k;

        }

        public static ExpressionCollection AllFields(Expression Node, Schema Columns, string Alias)
        {
            Key k = AllFields(Node);
            return ExpressionCollection.Render(Columns, Alias, k);
        }

        public static List<Cell> AllCellValues(Expression Node)
        {

            List<Expression> all_nodes = AllNodes(Node, ExpressionAffinity.Result);
            List<ExpressionValue> Values = Convert<ExpressionValue>(all_nodes);
            
            List<Cell> data = new List<Cell>();
            foreach (ExpressionValue n in Values)
                data.Add(n.InnerValue);
            return data;

        }

        public static List<string> AllCellFunctionNames(Expression Node)
        {

            List<Expression> all_nodes = AllNodes(Node, ExpressionAffinity.Result);
            List<ExpressionResult> fields = Convert<ExpressionResult>(all_nodes);

            List<string> data = new List<string>();
            foreach (ExpressionResult n in fields)
                data.Add(n.InnerFunction.NameSig);
            return data;

        }

        public static List<int> AllFieldRefs(Expression Node)
        {

            List<Expression> nodes = Analytics.Decompile(Node);
            List<int> refs = new List<int>();
            foreach (Expression n in nodes)
            {

                if (n is ExpressionFieldRef)
                    refs.Add((n as ExpressionFieldRef).Index);

            }
            return refs;

        }

        // Accumulate, remove, replace //
        public static void ReplaceNode(Expression OriginalNode, Expression ReplaceWithNode)
        {

            /*
             * Basic premise is to replace a node in the tree with another node:
             * -- If the node has children, need to point the kids to the new parent node
             * -- If the node is a child, need to point its parent to the new node
             * 
             */

            // Point Children to the new node //
            if (!OriginalNode.IsTerminal)
            {
                foreach (Expression n in OriginalNode.Children)
                    n.ParentNode = ReplaceWithNode;
            }

            // Point parent to new node //
            if (!OriginalNode.IsMaster)
            {
                for (int i = 0; i < OriginalNode.ParentNode.Children.Count; i++)
                {
                    if (OriginalNode.ParentNode.Children[i] == OriginalNode)
                        OriginalNode.ParentNode.Children[i] = ReplaceWithNode;
                }
            }


        }

        public static void AddNode(ExpressionResult NewParentNode, params Expression[] Nodes)
        {
            foreach (Expression n in Nodes)
                NewParentNode.AddChildNode(n);
        }

        public static void Disconnect(Expression Node)
        {
            if (Node.IsMaster) return;
            Node.ParentNode.Children.Remove(Node);
        }

        // Generational Methods //
        /// <summary>
        /// Traverses the tree to get the master parent node (parent node that has no parents)
        /// </summary>
        /// <param name="Node"></param>
        /// <returns></returns>
        public static Expression EveNode(Expression Node)
        {

            while (!Node.IsMaster)
                Node = Node.ParentNode;
            return Node;

        }

        /// <summary>
        /// Gets all nodes (including the one passed) that share the same parent node as the node passed
        /// </summary>
        /// <param name="Node"></param>
        /// <returns></returns>
        public static List<Expression> SiblingNodes(Expression Node)
        {

            if (Node.IsMaster || Node.Affinity != ExpressionAffinity.Result) return null;

            return new List<Expression>(Node.ParentNode.Children);

        }

        /// <summary>
        /// Provides all the terminal nodes for a given node
        /// </summary>
        /// <param name="Node"></param>
        /// <returns></returns>
        public static List<Expression> AllTerminalNodes(Expression Node)
        {
            return Decompile(Node).Where((n) => { return n.IsTerminal; }).ToList();
        }

        /// <summary>
        /// Returns any node that has a terminal child node
        /// </summary>
        /// <param name="Node"></param>
        /// <returns></returns>
        public static List<Expression> AllParentTerminalNodes(Expression Node)
        {
            List<Expression> nodes = new List<Expression>();
            if (Node == null) return nodes;
            if (Node.IsTerminal) return nodes;
            foreach(Expression n in AllTerminalNodes(Node))
            {
                if (!nodes.Contains(n.ParentNode))
                    nodes.Add(n);
            }
            return nodes;
        }

        /// <summary>
        /// Checks if 'Node' is a direct or distant child of eve
        /// </summary>
        /// <param name="Node"></param>
        /// <param name="Eve"></param>
        /// <returns></returns>
        public static bool IsDecendent(Expression Node, Expression Eve)
        {
            return Decompile(Eve).Contains(Node, new ExpressionComparer());
        }

        public static bool ContainsPointerRef(Expression Node, string Pointer)
        {

            if (Node.Affinity == ExpressionAffinity.Pointer)
                return (Node as ExpressionPointer).PointerName == Pointer;

            if (Node.Affinity != ExpressionAffinity.Result)
                return false;

            foreach (Expression n in Node.Children)
            {
                if (ContainsPointerRef(n, Pointer))
                    return true;
            }

            return false;

        }

        // Printing methods //
        private static string Tree(Expression N, StringBuilder SB, int Level)
        {

            SB.Append(new string(' ', Level * 2));
            SB.Append(Level);
            SB.Append(" : ");
            SB.Append(N.ToString());
            SB.Append(" : ");
            if (N.Affinity == ExpressionAffinity.Pointer)
                SB.Append("<Pointer>");
            else
                SB.Append(N.ReturnAffinity().ToString());
            SB.AppendLine();
            foreach (Expression x in N.Children)
                Tree(x, SB, Level + 1);

            return SB.ToString();

        }

        public static string Tree(Expression N)
        {
            int l = 0;
            StringBuilder sb = new StringBuilder();
            return Tree(N, sb, l);
        }

        private static string TreeLite(Expression N, StringBuilder SB, int Level)
        {

            SB.Append(new string(' ', Level * 2));
            SB.Append(Level);
            SB.Append(" : ");
            SB.Append(N.ToString());
            SB.AppendLine();
            foreach (Expression x in N.Children)
                TreeLite(x, SB, Level + 1);

            return SB.ToString();

        }

        public static string TreeLite(Expression N)
        {
            int l = 0;
            StringBuilder sb = new StringBuilder();
            return TreeLite(N, sb, l);
        }

        // 'Is' Methods //
        public static bool IsEqNode(Expression Node)
        {
            if (!Node.IsResult) return false;
            if ((Node as ExpressionResult).InnerFunction.NameSig == SystemFunctionLibrary.LookUp("==").NameSig) return true;
            return false;
        }

        public static bool IsAndNode(Expression Node)
        {
            if (!Node.IsResult) return false;
            if ((Node as ExpressionResult).InnerFunction.NameSig == SystemFunctionLibrary.LookUp("and").NameSig) return true;
            return false;
        }

        public static bool IsOrXorIfCaseNode(Expression Node)
        {

            if (!Node.IsResult) return false;
            string t = (Node as ExpressionResult).InnerFunction.NameSig;
            string[] s = { SystemFunctionLibrary.LookUp("or").NameSig, SystemFunctionLibrary.LookUp("xor").NameSig, SystemFunctionLibrary.LookUp("if").NameSig };
            return s.Contains(t);

        }

        public static bool IsBinaryBoolean(Expression Node)
        {

            if (Node.Children.Count != 2)
                return false;

            return Node.Children[0].ReturnAffinity() == CellAffinity.BOOL && Node.Children[1].ReturnAffinity() == CellAffinity.BOOL;

        }

        public static bool IsBinaryBooleanAnd(Expression Node)
        {
            return IsAndNode(Node) && IsBinaryBoolean(Node);
        }

        public static bool IsAndTree(Expression Node)
        {

            // If the node does not have a boolean return value //
            if (Node.ReturnAffinity() != CellAffinity.BOOL)
                return false;

            // Get all the binary boolean nodes //
            List<Expression> nodes = Analytics.Decompile(Node).Where((b) => { return Analytics.IsBinaryBoolean(b); }).ToList();

            // Get all the non-AND nodes //
            int NonAndCount = nodes.Where((b) => { return !Analytics.IsAndNode(b); }).Count();

            // If the count is not zero, then we cannot decompile //
            return NonAndCount == 0;

        }

        public static bool IsFieldToFieldEqaulity(Expression Node)
        {

            // Check if result //
            if (!Node.IsResult) 
                return false;
            
            // Convert to result, then check //
            ExpressionResult r = Node as ExpressionResult;

            // Check that the node is 'EE' //
            if (!IsEqNode(Node)) 
                return false;

            // Check that there are only two arguements //
            if (Node.Children.Count != 2) 
                return false;
            
            // Check that both kids are field nodes //
            if (Node.Children[0].Affinity == ExpressionAffinity.Result
                && Node.Children[1].Affinity == ExpressionAffinity.Result) 
                return true;
            
            
            return false;


        }

        // Datamining support //
        public static ExpressionCollection Split(Expression Node, string FunctionName)
        {

            // Build Tree //
            ExpressionCollection tree = new ExpressionCollection();
            if (Node.Affinity != ExpressionAffinity.Result || Node.ToString() != FunctionName)
            {
                tree.Add(Node);
                return tree;
            }

            Stack<Expression> nodes = new Stack<Expression>();
            nodes.Push(Node);

            while (nodes.Count != 0)
            {

                Expression t = nodes.Pop();

                if (t.ToString() == FunctionName)
                {
                    foreach (Expression u in t.Children)
                        nodes.Push(u);
                }
                else
                {
                    tree.Add(t);
                }

            }

            return tree;

        }

        public static Expression BindAllPointersToStatic(Expression Node, Cell StaticValue)
        {

            // Create a clone //
            Expression t = Node.CloneOfMe();

            // Check if the clone is just a pointer //
            if (Node.Affinity == ExpressionAffinity.Pointer)
                return new ExpressionValue(Node.ParentNode, StaticValue);

            // If the node has no children, return //
            if (Node.IsTerminal)
                return t;

            // If a more complex tree ... //
            List<ExpressionPointer> refs = Analytics.AllPointers(t);

            // Walk the decompiled tree and remove all pointers //
            foreach (ExpressionPointer n in refs)
            {
                Analytics.ReplaceNode(n, new ExpressionValue(n.ParentNode, StaticValue));
            }
            return t;

        }

        public static ExpressionCollection BindAllPointersToStatic(ExpressionCollection Node, Cell StaticValue)
        {

            ExpressionCollection q = new ExpressionCollection();
            for (int i = 0; i < Node.Count; i++)
            {
                q.Add(BindAllPointersToStatic(Node[i], StaticValue));
            }

            return q;

        }


    }

    
   
}
