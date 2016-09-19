using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.MatrixExpressions;
using Rye.Expressions;
using Rye.Structures;
using Rye.Data;

namespace Rye.Interpreter
{

    public sealed class MatrixExpressionVisitor : RyeParserBaseVisitor<MatrixExpression>
    {

        private ExpressionVisitor _exp;
        private MatrixExpression _parent;
        private Session _Session;

        public MatrixExpressionVisitor(ExpressionVisitor ExpVisitor, Session Home)
            :base()
        {
            this._exp = ExpVisitor;
            this._Session = Home;
        }

        public MatrixExpression MasterNode
        {
            get { return this._parent; }
        }
        
        // Overrides //
        public override MatrixExpression VisitMatrixMinus(RyeParser.MatrixMinusContext context)
        {
            MatrixExpression m = new MatrixExpressionMinus(this._parent);
            m.AddChildNode(this.Visit(context.matrix_expression()));
            this._parent = m;
            return m;
        }

        public override MatrixExpression VisitMatrixInvert(RyeParser.MatrixInvertContext context)
        {
            MatrixExpression m = new MatrixExpressionInverse(this._parent);
            m.AddChildNode(this.Visit(context.matrix_expression()));
            this._parent = m;
            return m;
        }

        public override MatrixExpression VisitMatrixTranspose(RyeParser.MatrixTransposeContext context)
        {
            MatrixExpression m = new MatrixExpressionTranspose(this._parent);
            m.AddChildNode(this.Visit(context.matrix_expression()));
            this._parent = m;
            return m;
        }

        public override MatrixExpression VisitMatrixTrueMul(RyeParser.MatrixTrueMulContext context)
        {
            MatrixExpression m = new MatrixExpressionMatrixMultiply(this._parent);
            m.AddChildNode(this.Visit(context.matrix_expression()[0]));
            m.AddChildNode(this.Visit(context.matrix_expression()[1]));
            this._parent = m;
            return m;
        }

        public override MatrixExpression VisitMatrixMulDiv(RyeParser.MatrixMulDivContext context)
        {

            MatrixExpression m;
            if (context.op.Type == RyeParser.MUL)
                m = new MatrixExpressionMultiply(this._parent);
            else if (context.op.Type == RyeParser.DIV)
                m = new MatrixExpressionDivide(this._parent);
            else
                m = new MatrixExpressionCheckDivide(this._parent);

            m.AddChildNode(this.Visit(context.matrix_expression()[0]));
            m.AddChildNode(this.Visit(context.matrix_expression()[1]));
            this._parent = m;
            return m;

        }

        public override MatrixExpression VisitMatrixMulDivLeft(RyeParser.MatrixMulDivLeftContext context)
        {

            Expression node = this._exp.ToNode(context.expression());
            MatrixExpression m;
            // Third parameter here: 0 == scalar is on left side (A + B[]), 1 == scalar is on right side (A[] + B)
            if (context.op.Type == RyeParser.MUL)
                m = new MatrixExpressionMultiplyScalar(this._parent, node, 0);
            else if (context.op.Type == RyeParser.DIV)
                m = new MatrixExpressionDivideScalar(this._parent, node, 0);
            else
                m = new MatrixExpressionCheckDivideScalar(this._parent, node, 0);

            m.AddChildNode(this.Visit(context.matrix_expression()));
            this._parent = m;
            return m;

        }

        public override MatrixExpression VisitMatrixMulDivRight(RyeParser.MatrixMulDivRightContext context)
        {

            Expression node = this._exp.ToNode(context.expression());
            MatrixExpression m;
            // Third parameter here: 0 == scalar is on left side (A + B[]), 1 == scalar is on right side (A[] + B)
            if (context.op.Type == RyeParser.MUL)
                m = new MatrixExpressionMultiplyScalar(this._parent, node, 1);
            else if (context.op.Type == RyeParser.DIV)
                m = new MatrixExpressionDivideScalar(this._parent, node, 1);
            else
                m = new MatrixExpressionCheckDivideScalar(this._parent, node, 1);

            m.AddChildNode(this.Visit(context.matrix_expression()));
            this._parent = m;
            return m;

        }

        public override MatrixExpression VisitMatrixAddSub(RyeParser.MatrixAddSubContext context)
        {

            MatrixExpression m;
            if (context.op.Type == RyeParser.PLUS)
                m = new MatrixExpressionAdd(this._parent);
            else
                m = new MatrixExpressionSubtract(this._parent);

            m.AddChildNode(this.Visit(context.matrix_expression()[0]));
            m.AddChildNode(this.Visit(context.matrix_expression()[1]));
            this._parent = m;
            return m;

        }

        public override MatrixExpression VisitMatrixAddSubLeft(RyeParser.MatrixAddSubLeftContext context)
        {

            Expression node = this._exp.ToNode(context.expression());
            MatrixExpression m;
            // Third parameter here: 0 == scalar is on left side (A + B[]), 1 == scalar is on right side (A[] + B)
            if (context.op.Type == RyeParser.PLUS)
                m = new MatrixExpressionAddScalar(this._parent, node, 0);
            else
                m = new MatrixExpressionSubtractScalar(this._parent, node, 0);

            m.AddChildNode(this.Visit(context.matrix_expression()));
            this._parent = m;
            return m;

        }

        public override MatrixExpression VisitMatrixAddSubRight(RyeParser.MatrixAddSubRightContext context)
        {

            Expression node = this._exp.ToNode(context.expression());
            MatrixExpression m;
            // Third parameter here: 0 == scalar is on left side (A + B[]), 1 == scalar is on right side (A[] + B)
            if (context.op.Type == RyeParser.PLUS)
                m = new MatrixExpressionAddScalar(this._parent, node, 1);
            else
                m = new MatrixExpressionSubtractScalar(this._parent, node, 1);

            m.AddChildNode(this.Visit(context.matrix_expression()));
            this._parent = m;
            return m;

        }

        public override MatrixExpression VisitMatrixLookup(RyeParser.MatrixLookupContext context)
        {

            // Naked Lookup //
            if (context.generic_name().IDENTIFIER().Length == 1)
            {

                string vname = context.generic_name().IDENTIFIER()[0].GetText();
                if (this._exp.SecondaryMatrixes.Exists(vname))
                {
                    return new MatrixExpressionHeap(this._parent, this._exp.SecondaryMatrixes, this._exp.SecondaryMatrixes.GetPointer(vname));
                }
                else
                {
                    throw new RyeCompileException("Matrix '{0}' does not exist in '{1}'", vname, this._exp.SecondaryName);
                }

            }

            // Explicit lookup //
            string sname = context.generic_name().IDENTIFIER()[0].GetText();
            string mname = context.generic_name().IDENTIFIER()[1].GetText();

            if (string.Equals(this._exp.SecondaryName, sname, StringComparison.OrdinalIgnoreCase))
            {

                if (this._exp.SecondaryMatrixes.Exists(mname))
                {
                    return new MatrixExpressionHeap(this._parent, this._exp.SecondaryMatrixes, this._exp.SecondaryMatrixes.GetPointer(mname));
                }
                else
                {
                    throw new RyeCompileException("Matrix '{0}' does not exist in '{1}'", mname, this._exp.SecondaryName);
                }

            }
            else if (this._Session.IsGlobal(sname))
            {

                if (this._Session.MatrixExists(mname))
                {
                    return new MatrixExpressionHeap(this._parent, this._Session.Matrixes, this._Session.Matrixes.GetPointer(mname));
                }
                else
                {
                    throw new RyeCompileException("Matrix '{0}' does not exist in '{1}'", mname, sname);
                }

            }
            else
            {

                throw new RyeCompileException("Structure '{0}' does not exist", sname);

            }

        }

        public override MatrixExpression VisitMatrixLiteral(RyeParser.MatrixLiteralContext context)
        {

            int Cols = context.matrix_literal().vector_literal().Length;
            int Rows = context.matrix_literal().vector_literal()[0].expression().Length;
            CellAffinity affinity = _exp.ToNode(context.matrix_literal().vector_literal()[0].expression()[0]).ReturnAffinity();
            CellMatrix matrix = new CellMatrix(Rows, Cols, affinity);

            for (int i = 0; i < Rows; i++)
            {
                for (int j = 0; j < Cols; j++)
                {
                    matrix[i, j] = this._exp.ToNode(context.matrix_literal().vector_literal()[j].expression()[i]).Evaluate();
                }
            }

            return new MatrixExpressionLiteral(this._parent, matrix);

        }

        public override MatrixExpression VisitMatrixIdent(RyeParser.MatrixIdentContext context)
        {

            int rank = (int)this._exp.ToNode(context.expression()).Evaluate().INT;
            CellAffinity type = CompilerHelper.GetAffinity(context.type());

            return new MatrixExpressionIdentity(this.MasterNode, rank, type);

        }

        public override MatrixExpression VisitMatrixParen(RyeParser.MatrixParenContext context)
        {
            return this.Visit(context.matrix_expression());
        }

        public MatrixExpression ToMatrix(RyeParser.Matrix_expressionContext context)
        {
            return this.Visit(context);
        }



    }

}
