using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Expressions
{

    internal static class ExpressionGradient
    {

        internal static bool Compact = true;
        private static bool Debug = false;

        /// <summary>
        /// Calculates the gradient (first derivative) of a node with respect to a parameter node passed (pointer node).
        /// This method calls ExpressionOptimizer.CompactNode if the class level static variable 'Compact' is true (by default it is set to true).
        /// The gradient calculation leaves a lot of un-needed expressions that could be cancled out.
        /// </summary>
        /// <param name="Node">The node to calculate the gradient over</param>
        /// <param name="X">The parameter we are differentiating with respect to</param>
        /// <returns>A node representing a gradient</returns>
        internal static Expression Gradient(Expression Node, ExpressionPointer X)
        {

            // The node is a pointer node //
            if (Node.Affinity == ExpressionAffinity.Pointer)
            {
                if ((Node as ExpressionPointer).PointerName == X.PointerName)
                    return new ExpressionValue(Node.ParentNode, Cell.OneValue(X.ReturnAffinity()));
                else
                    return new ExpressionValue(Node.ParentNode, Cell.ZeroValue(X.ReturnAffinity()));
            }

            // The node is not a function node //
            if (Node.Affinity != ExpressionAffinity.Result)
                return new ExpressionValue(Node.ParentNode, Cell.ZeroValue(Node.ReturnAffinity()));

            // Check if the node, which we now know is a function, has X as a decendant //
            if (!Analytics.IsDecendent(X, Node))
                return new ExpressionValue(Node.ParentNode, Cell.ZeroValue(X.ReturnAffinity()));

            // Otherwise we have to do work :( //

            // Get the name signiture //
            string name_sig = (Node as ExpressionResult).InnerFunction.NameSig;

            // Go through each differentiable function //
            Expression t = null;
            switch (name_sig)
            {

                case SystemFunctionLibrary.UNI_PLUS:
                    t = GradientOfUniPlus(Node, X);
                    break;
                case SystemFunctionLibrary.UNI_MINUS:
                    t = GradientOfUniMinus(Node, X);
                    break;

                case SystemFunctionLibrary.OP_ADD:
                    t = GradientOfAdd(Node, X);
                    break;
                case SystemFunctionLibrary.OP_SUB:
                    t = GradientOfSubtract(Node, X);
                    break;
                case SystemFunctionLibrary.OP_MUL:
                    t = GradientOfMultiply(Node, X);
                    break;
                case SystemFunctionLibrary.OP_DIV:
                    t = GradientOfDivide(Node, X);
                    break;

                case SystemFunctionLibrary.FUNC_LOG:
                    t = GradientOfLog(Node, X);
                    break;
                case SystemFunctionLibrary.FUNC_EXP:
                    t = GradientOfExp(Node, X);
                    break;
                case SystemFunctionLibrary.FUNC_POWER:
                    t = GradientOfPowerLower(Node, X);
                    break;

                case SystemFunctionLibrary.FUNC_SIN:
                    t = GradientOfSin(Node, X);
                    break;
                case SystemFunctionLibrary.FUNC_COS:
                    t = GradientOfCos(Node, X);
                    break;
                case SystemFunctionLibrary.FUNC_TAN:
                    t = GradientOfTan(Node, X);
                    break;

                case SystemFunctionLibrary.FUNC_SINH:
                    t = GradientOfSinh(Node, X);
                    break;
                case SystemFunctionLibrary.FUNC_COSH:
                    t = GradientOfCosh(Node, X);
                    break;
                case SystemFunctionLibrary.FUNC_TANH:
                    t = GradientOfTanh(Node, X);
                    break;

                case SystemFunctionLibrary.FUNC_LOGIT:
                    t = GradientOfLogit(Node, X);
                    break;

                case SystemFunctionLibrary.FUNC_NDIST:
                    t = GradientOfNDIST(Node, X);
                    break;
                
                default:
                    throw new Exception(string.Format("Function is not differentiable : {0}", name_sig));
            }

            if (Compact)
                t = Optimizer.CompactNode(t);

            return t;

        }

        /// <summary>
        /// Calculates the gradient (first derivative) of a node with respect to a parameter node passed (pointer node).
        /// This method calls ExpressionCompacter.CompactNode if the class level static variable 'Compact' is true (by default it is set to true).
        /// The gradient calculation leaves a lot of un-needed expressions that could be cancled out.
        /// </summary>
        /// <param name="Node">The node to calculate the gradient over</param>
        /// <param name="PointerRef">The parameter name we are differentiating with respect to</param>
        /// <returns>A node representing a gradient</returns>
        internal static Expression Gradient(Expression Node, string PointerRef)
        {
            return Gradient(Node, new ExpressionPointer(null, PointerRef, CellAffinity.DOUBLE, 8));
        }

        /// <summary>
        /// Calculates the gradient (first derivative) of a node with respect to a parameter node passed (pointer node).
        /// This method calls ExpressionCompacter.CompactNode if the class level static variable 'Compact' is true (by default it is set to true).
        /// The gradient calculation leaves a lot of un-needed expressions that could be cancled out.
        /// </summary>
        /// <param name="Tree">The node to calculate the gradient over</param>
        /// <param name="PointerRef">The parameter names we are differentiating with respect to; must have the same number of elements as the Tree parameter</param>
        /// <returns>A tree representing all gradients</returns>
        internal static ExpressionCollection Gradient(ExpressionCollection Tree, string[] PointerRefs)
        {

            if (Tree.Count != PointerRefs.Length)
                throw new Exception(string.Format("Tree and pointers have different counts {0} : {1}", Tree.Count, PointerRefs.Length));

            ExpressionCollection tree = new ExpressionCollection();

            for (int i = 0; i < Tree.Count; i++)
                tree.Add(Gradient(Tree[i], PointerRefs[i]), PointerRefs[i]);

            return tree;

        }

        /// <summary>
        /// Calculates the gradients (first derivative) of a node with respect to all parameters present.
        /// This method calls ExpressionCompacter.CompactNode if the class level static variable 'Compact' is true (by default it is set to true).
        /// The gradient calculation leaves a lot of un-needed expressions that could be cancled out.
        /// </summary>
        /// <param name="Node">The node to calculate the gradient over</param>
        /// <returns>A node representing a gradient</returns>
        internal static ExpressionCollection Gradient(Expression Node)
        {

            string[] variables = Analytics.AllPointersRefs(Node).Distinct().ToArray();
            ExpressionCollection tree = new ExpressionCollection();
            foreach (string n in variables)
                tree.Add(Gradient(Node, n), n);

            return tree;

        }

        // F(X) = -G(X), F'(X) = -G'(X)
        private static Expression GradientOfUniMinus(Expression Node, ExpressionPointer X)
        {
            ExpressionResult t = new ExpressionResult(Node.ParentNode, new CellUniMinus());
            t.AddChildNode(Gradient(Node.Children[0], X));
            return t;
        }

        // F(X) = +G(X), F'(X) = G'(X)
        private static Expression GradientOfUniPlus(Expression Node, ExpressionPointer X)
        {
            return Gradient(Node.Children[0], X);
        }

        // F(X) = G(X) + H(X), F'(X) = G'(X) + H'(X)
        private static Expression GradientOfAdd(Expression Node, ExpressionPointer X)
        {
            ExpressionResult t = new ExpressionResult(Node.ParentNode, new CellBinPlus());
            t.AddChildNode(Gradient(Node.Children[0], X));
            t.AddChildNode(Gradient(Node.Children[1], X));
            return t;
        }

        // F(X) = G(X) - H(X), F'(X) = G'(X) - H'(X)
        private static Expression GradientOfSubtract(Expression Node, ExpressionPointer X)
        {
            ExpressionResult t = new ExpressionResult(Node.ParentNode, new CellBinMinus());
            t.AddChildNode(Gradient(Node.Children[0], X));
            t.AddChildNode(Gradient(Node.Children[1], X));
            return t;
        }

        // F(X) = G(X) * H(X), F'(X) = G'(X) * H(X) + G(X) * H'(X)
        private static Expression GradientOfMultiply(Expression Node, ExpressionPointer X)
        {

            // G'(X) * H(X) //
            ExpressionResult t = new ExpressionResult(Node.ParentNode, new CellBinMult());
            t.AddChildNode(Gradient(Node.Children[0], X));
            t.AddChildNode(Node.Children[1]);

            // G(X) * H'(X) //
            ExpressionResult u = new ExpressionResult(Node.ParentNode, new CellBinMult());
            u.AddChildNode(Node.Children[0]);
            u.AddChildNode(Gradient(Node.Children[1], X));

            // Final Node //
            ExpressionResult v = new ExpressionResult(Node.ParentNode, new CellBinPlus());
            v.AddChildNode(t);
            v.AddChildNode(u);

            return v;
        }

        // F(X) = G(X) / H(X), F'(X) = (G'(X) * H(X) - G(X) * H'(X)) / (H(X) * H(X))
        private static Expression GradientOfDivide(Expression Node, ExpressionPointer X)
        {

            // Need to handle the case where H is not a function of X //
            if (!Analytics.ContainsPointerRef(Node[1], X.PointerName))
            {
                Expression a = Gradient(Node[0], X);
                return Builder.Divide(a, Node[1]);
            }

            // G'(X) * H(X) //
            ExpressionResult t = new ExpressionResult(Node.ParentNode, new CellBinMult());
            t.AddChildNode(Gradient(Node.Children[0], X));
            t.AddChildNode(Node.Children[1]);

            // G(X) * H'(X) //
            ExpressionResult u = new ExpressionResult(Node.ParentNode, new CellBinMult());
            u.AddChildNode(Node.Children[0]);
            u.AddChildNode(Gradient(Node.Children[1], X));

            // G'(X) * H(X) - G(X) * H'(X) //
            ExpressionResult v = new ExpressionResult(Node.ParentNode, new CellBinMinus());
            v.AddChildNode(t);
            v.AddChildNode(u);

            // H(X) * H(X) //
            ExpressionResult w = new ExpressionResult(Node.ParentNode, new CellFuncFVPower());
            w.AddChildNode(Node.Children[1]);
            w.AddChildNode(new ExpressionValue(null, new Cell(2.00)));

            // Final Node //
            ExpressionResult x = new ExpressionResult(Node.ParentNode, new CellBinDiv());
            x.AddChildNode(v);
            x.AddChildNode(w);

            return x;
        }

        // F(X) = exp(G(X)), F'(X) = exp(G(X)) * G'(X), or simplified F(X) * G'(X) 
        private static Expression GradientOfExp(Expression Node, ExpressionPointer X)
        {

            // F(X) * G'(X) //
            ExpressionResult t = new ExpressionResult(Node.ParentNode, new CellBinMult());
            t.AddChildNode(Node.CloneOfMe());
            t.AddChildNode(Gradient(Node.Children[0], X));

            return t;
        }

        // F(X) = log(G(X)), F'(X) = 1 / G(X) * G'(X)
        private static Expression GradientOfLog(Expression Node, ExpressionPointer X)
        {

            // 1 / G(X) //
            ExpressionResult t = new ExpressionResult(Node.ParentNode, new CellFuncFVPower());
            t.AddChildNode(Node.Children[0]);
            t.AddChildNode(new ExpressionValue(null, new Cell(-1.00)));

            // 1 / G(X) //
            ExpressionResult u = new ExpressionResult(Node.ParentNode, new CellBinMult());
            u.AddChildNode(t);
            u.AddChildNode(Gradient(t.Children[0], X));

            return u;

        }

        // F(X) = sin(G(X)), F'(X) = cos(G(X)) * G'(X)
        private static Expression GradientOfSin(Expression Node, ExpressionPointer X)
        {

            // cos(G(X)) //
            ExpressionResult t = new ExpressionResult(Node.ParentNode, new CellFuncFVCos());
            t.AddChildNode(Node.Children[0]);

            // 1 / G(X) //
            ExpressionResult u = new ExpressionResult(Node.ParentNode, new CellBinMult());
            u.AddChildNode(t);
            u.AddChildNode(Gradient(t.Children[0], X));

            return u;

        }

        // F(X) = cos(G(X)), F'(X) = -sin(G(X)) * G'(X)
        private static Expression GradientOfCos(Expression Node, ExpressionPointer X)
        {

            // sin(G(X)) //
            ExpressionResult t = new ExpressionResult(Node.ParentNode, new CellFuncFVSin());
            t.AddChildNode(Node.Children[0]);

            // -sin(G(X)) //
            ExpressionResult u = new ExpressionResult(t, new CellUniMinus());
            u.AddChildNode(t);

            // -sin(G(X)) * G'(X) //
            ExpressionResult v = new ExpressionResult(Node.ParentNode, new CellBinMult());
            v.AddChildNode(u);
            v.AddChildNode(Gradient(t.Children[0], X));

            return v;

        }

        // F(X) = tan(X), F'(X) = Power(cos(x) , -2.00)
        private static Expression GradientOfTan(Expression Node, ExpressionPointer X)
        {

            // cos(G(X)) //
            ExpressionResult t = new ExpressionResult(null, new CellFuncFVCos());
            t.AddChildNode(Node.Children[0]);

            // power(cos(G(x)),2) //
            ExpressionResult u = new ExpressionResult(t, new CellFuncFVPower());
            u.AddChildNode(t);
            u.AddChildNode(new ExpressionValue(null, new Cell(-2.00)));

            // power(cos(G(x)),2) * G'(X) //
            ExpressionResult v = new ExpressionResult(Node.ParentNode, new CellBinMult());
            v.AddChildNode(u);
            v.AddChildNode(Gradient(t.Children[0], X));

            return u;

        }

        // F(X) = sinh(G(X)), F'(X) = cosh(G(X)) * G'(X)
        private static Expression GradientOfSinh(Expression Node, ExpressionPointer X)
        {

            // cosh(G(X)) //
            ExpressionResult t = new ExpressionResult(Node.ParentNode, new CellFuncFVCosh());
            t.AddChildNode(Node.Children[0]);

            // 1 / G(X) //
            ExpressionResult u = new ExpressionResult(Node.ParentNode, new CellBinMult());
            u.AddChildNode(t);
            u.AddChildNode(Gradient(Node.Children[0], X));

            return u;

        }

        // F(X) = cosh(G(X)), F'(X) = sinh(G(X)) * G'(X)
        private static Expression GradientOfCosh(Expression Node, ExpressionPointer X)
        {

            // sinh(G(X)) //
            ExpressionResult t = new ExpressionResult(Node.ParentNode, new CellFuncFVSinh());
            t.AddChildNode(Node.Children[0]);

            // sinh(G(X)) * G'(X) //
            ExpressionResult u = new ExpressionResult(Node.ParentNode, new CellBinMult());
            u.AddChildNode(t);
            u.AddChildNode(Gradient(Node.Children[0], X));

            return u;

        }

        // F(X) = tanh(X), F'(X) = Power(cosh(x) , -2.00)
        private static Expression GradientOfTanh(Expression Node, ExpressionPointer X)
        {

            // cosh(G(X)) //
            ExpressionResult t = new ExpressionResult(null, new CellFuncFVCosh());
            t.AddChildNode(Node.Children[0]);

            // power(cosh(G(x)),2) //
            ExpressionResult u = new ExpressionResult(t, new CellFuncFVPower());
            u.AddChildNode(t);
            u.AddChildNode(new ExpressionValue(null, new Cell(-2.00)));

            // power(cosh(G(x)),2) * G'(X) //
            ExpressionResult v = new ExpressionResult(Node.ParentNode, new CellBinMult());
            v.AddChildNode(u);
            v.AddChildNode(Gradient(Node.Children[0], X));

            return u;

        }

        // F(X) = power(G(X), N), F'(X) = power(G(X), N - 1) * G'(X) * N, or G'(X) if N == 1, N must not be a decendant of X
        private static Expression GradientOfPowerLower(Expression Node, ExpressionPointer X)
        {

            // Throw an exception if X is decendant of N, in otherwords F(X) = Power(G(X), H(X))
            if (Analytics.IsDecendent(X, Node.Children[1]))
                throw new Exception(string.Format("Cannot differentiate the power function with the form: Power(G(X), H(X)); H(X) cannot have a relation to X"));

            // Build 'N-1' //
            Expression n_minus_one = new ExpressionResult(Node.ParentNode, new CellBinMinus());
            n_minus_one.AddChildNode(Node.Children[1]);
            n_minus_one.AddChildNode(new ExpressionValue(null, new Cell(1.00)));

            // Get Power(G(X), N-1) //
            Expression power_gx_n_minus_one = new ExpressionResult(Node.ParentNode, new CellFuncFVPower());
            power_gx_n_minus_one.AddChildNode(Node.Children[0]);
            power_gx_n_minus_one.AddChildNode(n_minus_one);

            // Get Power(G(X), N-1) * G'(X) //
            Expression t = new ExpressionResult(Node.ParentNode, new CellBinMult());
            t.AddChildNode(power_gx_n_minus_one);
            t.AddChildNode(Gradient(Node.Children[0], X));

            // Get Power(G(X), N-1) * G'(X) * N //
            Expression u = new ExpressionResult(Node.ParentNode, new CellBinMult());
            u.AddChildNode(t);
            u.AddChildNode(Node.Children[1]);

            return u;

        }

        // F(X) = power(Y, G(X)), F'(X) = LOG(Y) * power(Y, G(X)) * G'(X)
        private static Expression GradientOfPowerUpper(Expression Node, ExpressionPointer X)
        {

            // Throw an exception if X is decendant of Y, in otherwords F(X) = Power(G(X), H(X))
            if (Analytics.IsDecendent(X, Node.Children[1]))
                throw new Exception(string.Format("Cannot differentiate the power function with the form: Power(G(X), H(X)); G(X) cannot have a relation to X"));

            // LOG(Y) //
            Expression log_y = new ExpressionResult(null, new CellFuncFVLog());
            log_y.AddChildNode(Node.Children[0]);

            // Get Power(Y, G(X)) * LOG(Y) //
            Expression pow_f_dx = new ExpressionResult(Node.ParentNode, new CellBinMult());
            pow_f_dx.AddChildNode(Node.CloneOfMe());
            pow_f_dx.AddChildNode(log_y);

            // Get Power(G(X), N-1) * G'(X) //
            Expression t = new ExpressionResult(Node.ParentNode, new CellBinMult());
            t.AddChildNode(pow_f_dx);
            t.AddChildNode(Gradient(Node.Children[1], X));

            return t;

        }

        // F(X) = 1 / (1 + exp(-X)) = logit(X), F'(X) = logit(X) * (1 - logit(X))
        private static Expression GradientOfLogit(Expression Node, ExpressionPointer X)
        {

            // Logit, create two to avoid incest in the node tree //
            Expression t = X.CloneOfMe();
            Expression u = X.CloneOfMe();
            Expression v = Builder.Value(1D);

            return t * (v - u);

        }

        // F(X) = NDIST, F'(X) = 0.398942 * EXP(-0.50 * X * X) 
        private static Expression GradientOfNDIST(Expression Node, ExpressionPointer X)
        {

            // Build -0.5 * X * X //
            Expression t = Builder.Value(-0.5) * Node[0] * Node[0]; // - 0.5 * X^2
            Expression u = (new ExpressionResult(t, new CellFuncFVExp())); // exp(-0.5 * X^2)
            u.AddChildNode(t);
            Expression v = u * Builder.Value(0.398942); // 1/sqrt(2pi)
            Expression w = v * Gradient(Node[0], X); // X' * exp(-0.5 * X^2) / sqrt(2 * pi)
            
            return v;

        }

    }


}
