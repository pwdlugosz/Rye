using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Structures;
using Rye.Data;
using Rye.Expressions;
using Rye.MatrixExpressions;
using Rye.Methods;
using Rye.Libraries;

namespace Rye.Interpreter
{

    public sealed class MethodVisitor : RyeParserBaseVisitor<Method>
    {

        private Heap<RecordWriter> _OpenStreams;
        private ExpressionVisitor _exp;
        private MatrixExpressionVisitor _mat;
        private Method _master;
        private Session _Session;
        private bool _IsAsync = false;
        
        public MethodVisitor(ExpressionVisitor ExpVisitor, MatrixExpressionVisitor MatVisitor, Session Enviro, bool IsAsync)
            : base()
        {
            this._exp = ExpVisitor;
            this._mat = MatVisitor;
            this._OpenStreams = new Heap<RecordWriter>();
            this._Session = Enviro;
            this._IsAsync = IsAsync;
        }

        // Actions //
        public override Method VisitActAssign(RyeParser.ActAssignContext context)
        {

            Expression node = this._exp.ToNode(context.expression());
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<Cell> h = this._exp.GetScalarHeap(context.generic_name());
            Method t = new MethodAssignScalar(this._master, h, h.GetPointer(name), node, 0);
            this._master = t;
            return t;

        }

        public override Method VisitActInc(RyeParser.ActIncContext context)
        {

            Expression node = this._exp.ToNode(context.expression());
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<Cell> h = this._exp.GetScalarHeap(context.generic_name());
            Method t = new MethodAssignScalar(this._master, h, h.GetPointer(name), node, 1);
            this._master = t;
            return t;

        }

        public override Method VisitActDec(RyeParser.ActDecContext context)
        {

            Expression node = this._exp.ToNode(context.expression());
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<Cell> h = this._exp.GetScalarHeap(context.generic_name());
            Method t = new MethodAssignScalar(this._master, h, h.GetPointer(name), node, 2);
            this._master = t;
            return t;

        }

        public override Method VisitActAutoInc(RyeParser.ActAutoIncContext context)
        {

            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<Cell> h = this._exp.GetScalarHeap(context.generic_name());
            Method t = new MethodAssignScalar(this._master, h, h.GetPointer(name), new ExpressionValue(null, Cell.NULL_INT), 3);
            this._master = t;
            return t;

        }

        public override Method VisitActAutoDec(RyeParser.ActAutoDecContext context)
        {

            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<Cell> h = this._exp.GetScalarHeap(context.generic_name());
            Method t = new MethodAssignScalar(this._master, h, h.GetPointer(name), new ExpressionValue(null, Cell.NULL_INT), 4);
            this._master = t;
            return t;

        }

        public override Method VisitActAppend(RyeParser.ActAppendContext context)
        {

            // Get the ExpressionSet //
            ExpressionCollection nodes = new ExpressionCollection();
            this._exp.AppendSet(nodes, context.append_method().expression_or_wildcard_set());

            TabularData data = CompilerHelper.RenderData(this._Session, nodes, context);
            
            Method node;
            if (data.Header.Affinity == HeaderType.Table)
            {
                node = new MethodAppendToAsync(this._master, data as Table, nodes);
            }
            else
            {
                node = new MethodAppendToAsync(this._master, data as Extent, nodes);
            }

            // Look for the sort statement //
            if (context.append_method().K_SORT() != null && !this._IsAsync)
            {
                Method xsort = this.RenderSortStatement(context.append_method(), data);
                node.AddChild(xsort);
            }
            else if (context.append_method().K_SORT() != null)
            {
                this._Session.IO.WriteLine("Warning: cannot process the 'SORT' statement in the 'APPEND' clause in async mode");
            }

            // Look for the dump statement //
            if (context.append_method().K_DUMP() != null && !this._IsAsync)
            {
                Method xdump = this.RenderDumpStatement(context.append_method(), data);
                node.AddChild(xdump);
            }
            else if (context.append_method().K_DUMP() != null)
            {
                this._Session.IO.WriteLine("Warning: cannot process the 'DUMP' statement in the 'APPEND' clause in async mode");
            }

            this._master = node;

            return node;

        }

        public override Method VisitActIf(RyeParser.ActIfContext context)
        {

            Filter condition = this._exp.ToPredicate(context.expression());
            Method if_node = new MethodIf(this._master, condition);
            if_node.AddChild(this.Visit(context.method()[0]));
            if (context.K_ELSE() != null)
                if_node.AddChild(this.Visit(context.method()[1]));

            this._master = if_node;

            return if_node;

        }

        public override Method VisitActFor(RyeParser.ActForContext context)
        {

            // Get the variable name, which is located on one of the heaps //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<Cell> h = this._exp.GetScalarHeap(context.generic_name());
            
            // Determine the begin and end values //
            Expression beg = this._exp.ToNode(context.expression()[0]);
            Expression end = this._exp.ToNode(context.expression()[1]);
            Expression step = (context.K_BY() != null ? this._exp.ToNode(context.expression()[2]) : new ExpressionValue(null, Cell.OneValue(beg.ReturnAffinity())));

            // Create the parent node //
            Method t = new MethodFor(this._master, beg, end, step, h, h.GetPointer(name));

            // Get the sub - action //
            Method sub_action = this.ToNode(context.method());

            // Assign the sub-action to t //
            t.AddChild(sub_action);

            // Assign the master node to the node we just built //
            this._master = t;

            return t;

        }

        public override Method VisitActBeginEnd(RyeParser.ActBeginEndContext context)
        {

            Method t = new MethodDo(this._master);
            foreach (RyeParser.MethodContext x in context.method())
            {
                t.AddChild(this.Visit(x));
            }

            this._master = t;

            return t;

        }

        public override Method VisitActEscapeFor(RyeParser.ActEscapeForContext context)
        {
            return new MethodEscapeLoop(this._master);
        }

        public override Method VisitActEscapeRead(RyeParser.ActEscapeReadContext context)
        {
            return new MethodEscapeRead(this._master);
        }

        public override Method VisitActWhile(RyeParser.ActWhileContext context)
        {

            // Get the controll structure //
            Expression control = this._exp.ToNode(context.expression());

            // Create the parent node //
            Method t = new MethodWhile(this._master, control);

            // Get the sub - action //
            Method sub_action = this.ToNode(context.method());

            // Assign the sub-action to t //
            t.AddChild(sub_action);

            // Assign the master node to the node we just built //
            this._master = t;

            return t;

        }

        public override Method VisitActPrint(RyeParser.ActPrintContext context)
        {

            ExpressionCollection vars = new ExpressionCollection();
            this._exp.AppendSet(vars, context.expression_or_wildcard_set());
            MethodPrintE t = new MethodPrintE(this._master, this._Session, vars);
            return t;

        }

        public override Method VisitActPrintMat(RyeParser.ActPrintMatContext context)
        {

            MatrixExpression mat = this._mat.ToMatrix(context.matrix_expression());
            MethodPrintM t = new MethodPrintM(this._master, this._Session, mat); 
            return t;

        }

        public override Method VisitActPrintLambda(RyeParser.ActPrintLambdaContext context)
        {

            string libname = context.IDENTIFIER()[0].GetText();
            string lname = context.IDENTIFIER()[1].GetText();

            if (!this._Session.LambdaExists(lname))
                throw new RyeCompileException("Structure '{0}' does not exist", libname);

            Lambda l = this._Session.GetLambda(lname);
            MethodPrintL t = new MethodPrintL(this._master, this._Session, l);
            return t;

        }

        public override Method VisitActExec(RyeParser.ActExecContext context)
        {

            Heap<Expression> parameters = new Heap<Expression>();
            string script = this._exp.ToNode(context.exec_method().expression()).Evaluate().valueSTRING;
            bool NoPrint = (context.exec_method().K_NO_PRINT() != null);

            foreach (RyeParser.Exec_unitContext ctx in context.exec_method().exec_unit())
            {
                parameters.Allocate(ctx.PARAMETER().GetText(), this._exp.ToNode(ctx.expression()));
            }

            return new MethodExecScript(this._master, this._Session, script, parameters, NoPrint);

        }

        // Assign a matrix to another matrix //
        public override Method VisitActMatAssign(RyeParser.ActMatAssignContext context)
        {

            // Get the name //
            string name = context.matrix_name().generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.matrix_name().generic_name());
            MatrixExpression mat = this._mat.ToMatrix(context.matrix_expression());
            
            // Build a node //
            return new MethodMatrixAssign(this._master, heap, heap.GetPointer(name), mat);

        }

        // Assign an element of a matrix to an expression, using two dimensions //
        public override Method VisitMUnit2DAssign(RyeParser.MUnit2DAssignContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            Expression row = this._exp.ToNode(context.expression()[0]);
            Expression col = this._exp.ToNode(context.expression()[1]);
            Expression exp = this._exp.ToNode(context.expression()[2]);

            return new MethodMatrixUnitAssign(this._master, heap, idx, exp, row, col, 0);

        }

        public override Method VisitMUnit2DInc(RyeParser.MUnit2DIncContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            Expression row = this._exp.ToNode(context.expression()[0]);
            Expression col = this._exp.ToNode(context.expression()[1]);
            Expression exp = this._exp.ToNode(context.expression()[2]);

            return new MethodMatrixUnitAssign(this._master, heap, idx, exp, row, col, 1);


        }

        public override Method VisitMUnit2DDec(RyeParser.MUnit2DDecContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            Expression row = this._exp.ToNode(context.expression()[0]);
            Expression col = this._exp.ToNode(context.expression()[1]);
            Expression exp = this._exp.ToNode(context.expression()[2]);

            return new MethodMatrixUnitAssign(this._master, heap, idx, exp, row, col, 2);


        }

        public override Method VisitMUnit2DAutoInc(RyeParser.MUnit2DAutoIncContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            Expression row = this._exp.ToNode(context.expression()[0]);
            Expression col = this._exp.ToNode(context.expression()[1]);
            Expression exp = this._exp.ToNode(context.expression()[2]);

            return new MethodMatrixUnitAssign(this._master, heap, idx, exp, row, col, 3);


        }

        public override Method VisitMUnit2DAutoDec(RyeParser.MUnit2DAutoDecContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            Expression row = this._exp.ToNode(context.expression()[0]);
            Expression col = this._exp.ToNode(context.expression()[1]);
            Expression exp = this._exp.ToNode(context.expression()[2]);

            return new MethodMatrixUnitAssign(this._master, heap, idx, exp, row, col, 4);


        }

        // Assign an element of a matrix to an expression, using one dimension //
        public override Method VisitMUnit1DAssign(RyeParser.MUnit1DAssignContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            Expression row = this._exp.ToNode(context.expression()[0]);
            Expression col = new ExpressionValue(null, new Cell(0));
            Expression exp = this._exp.ToNode(context.expression()[1]);

            return new MethodMatrixUnitAssign(this._master, heap, idx, exp, row, col, 0);

        }

        public override Method VisitMUnit1DInc(RyeParser.MUnit1DIncContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            Expression row = this._exp.ToNode(context.expression()[0]);
            Expression col = new ExpressionValue(null, new Cell(0));
            Expression exp = this._exp.ToNode(context.expression()[1]);

            return new MethodMatrixUnitAssign(this._master, heap, idx, exp, row, col, 1);

        }

        public override Method VisitMUnit1DDec(RyeParser.MUnit1DDecContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            Expression row = this._exp.ToNode(context.expression()[0]);
            Expression col = new ExpressionValue(null, new Cell(0));
            Expression exp = this._exp.ToNode(context.expression()[1]);

            return new MethodMatrixUnitAssign(this._master, heap, idx, exp, row, col, 2);

        }

        public override Method VisitMUnit1DAutoInc(RyeParser.MUnit1DAutoIncContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            Expression row = this._exp.ToNode(context.expression());
            Expression col = new ExpressionValue(null, new Cell(0));
            
            return new MethodMatrixUnitAssign(this._master, heap, idx, null, row, col, 3);

        }

        public override Method VisitMUnit1DAutoDec(RyeParser.MUnit1DAutoDecContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            Expression row = this._exp.ToNode(context.expression());
            Expression col = new ExpressionValue(null, new Cell(0));

            return new MethodMatrixUnitAssign(this._master, heap, idx, null, row, col, 4);

        }

        // Assign All //
        public override Method VisitMAllAssign(RyeParser.MAllAssignContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            Expression val = this._exp.ToNode(context.expression());

            return new MethodMatrixAllAssign(this._master, heap, idx, val, 0);

        }

        public override Method VisitMAllInc(RyeParser.MAllIncContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            Expression val = this._exp.ToNode(context.expression());

            return new MethodMatrixAllAssign(this._master, heap, idx, val, 1);

        }

        public override Method VisitMAllDec(RyeParser.MAllDecContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            Expression val = this._exp.ToNode(context.expression());

            return new MethodMatrixAllAssign(this._master, heap, idx, val, 2);

        }

        public override Method VisitMAllAutoInc(RyeParser.MAllAutoIncContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            return new MethodMatrixAllAssign(this._master, heap, idx, null, 3);

        }

        public override Method VisitMAllAutoDec(RyeParser.MAllAutoDecContext context)
        {

            // Get the name //
            string name = context.generic_name().IDENTIFIER().Last().GetText();
            Heap<CellMatrix> heap = this._exp.GetMatrixHeap(context.generic_name());
            int idx = heap.GetPointer(name);

            return new MethodMatrixAllAssign(this._master, heap, idx, null, 4);

        }
        
        // Append helpers //
        public Method RenderDumpStatement(RyeParser.Append_methodContext context, TabularData Data)
        {

            if (context.K_DUMP() == null)
                return MethodDump.Empty;

            string path = this._exp.ToNode(context.expression()[0]).Evaluate().valueSTRING;
            char delim = this._exp.ToNode(context.expression()[1]).Evaluate().valueSTRING.First();
            return new MethodDump(this._master, Data, path, delim, this._Session);

        }

        public Method RenderSortStatement(RyeParser.Append_methodContext context, TabularData Data)
        {

            if (context.K_SORT() == null)
                return MethodSort.Empty;

            // Build the sort key //
            Key k = new Key();
            ExpressionCollection cols = new ExpressionCollection();
            ExpressionVisitor exp = new ExpressionVisitor(this._Session);
            Register r = new Register("T", Data.Columns);
            exp.AddRegister("T", r);

            int idx = 0;
            foreach (RyeParser.Sort_unitContext ctx in context.sort_unit())
            {

                Expression e = exp.ToNode(ctx.expression());
                cols.Add(e);
                KeyAffinity ka = KeyAffinity.Ascending;
                if (ctx.K_DESC() != null)
                    ka = KeyAffinity.Descending;
                k.Add(idx, ka);

                idx++;

            }

            return new MethodSort(this._master, Data, cols, r, k);

        }

        // Structures //
        public override Method VisitStructure_method_strict(RyeParser.Structure_method_strictContext context)
        {
            
            // Get the name of the structure and the method //
            string lib_name = context.IDENTIFIER()[0].GetText();
            string method_name = context.IDENTIFIER()[1].GetText();

            // Check to see if the library exists //
            if (!this._Session.LibraryExists(lib_name))
            {
                throw new ArgumentException(string.Format("Library '{0}' does not exist", lib_name));
            }
            // Now check that the method exists in the structure //
            Library lib = this._Session.GetLibrary(lib_name);
            if (!lib.MethodExists(method_name))
            {
                throw new ArgumentException(string.Format("Method '{0}' does not exist for '{1}'", method_name, lib_name));
            }

            // Render a visitor //
            ExpressionVisitor exp = new ExpressionVisitor(this._Session);
            exp.SetSecondary(this._exp.SecondaryName, this._exp.SecondaryScalars, this._exp.SecondaryMatrixes);
            exp.ImportRegisters(this._exp);

            // Create a parameter collection //
            ParameterCollection parameters = new ParameterCollection();

            // Search through the parameters for all table references //
            foreach (RyeParser.Method_param_namedContext ctx in context.method_param_named())
            {

                // If the scalar is not null //
                if (ctx.method_param().K_TABLE() != null)
                {

                    // Get the database, table and alias //
                    string db = ctx.method_param().table_name().IDENTIFIER()[0].GetText();
                    string name = ctx.method_param().table_name().IDENTIFIER()[1].GetText();
                    string alias = (ctx.method_param().IDENTIFIER() == null ? name : ctx.method_param().IDENTIFIER().GetText());

                    // Get the table //
                    TabularData data = this._Session.GetTabularData(db, name);

                    // Add to the expression visitor //
                    exp.AddRegister(alias, new Register(alias, data.Columns));

                    // Add the table to the //
                    parameters.Add(ctx.IDENTIFIER().GetText(), data);

                }

            }

            // Now that we're this far, build a matrix visitor //
            MatrixExpressionVisitor mat = new MatrixExpressionVisitor(exp, this._Session);
            
            // Now go through and parse the expressions and matricies //
            foreach (RyeParser.Method_param_namedContext ctx in context.method_param_named())
            {

                // Parse matricies //
                if (ctx.method_param().matrix_expression() != null)
                {
                    parameters.Add(ctx.IDENTIFIER().GetText(), mat.ToMatrix(ctx.method_param().matrix_expression()));
                }
                // Parse expressions //
                else if (ctx.method_param().expression() != null)
                {
                    parameters.Add(ctx.IDENTIFIER().GetText(), exp.ToNode(ctx.method_param().expression()));
                }
                // Parse expressions //
                else if (ctx.method_param().LCURL() != null && ctx.method_param().LCURL() != null)
                {
                    parameters.Add(ctx.IDENTIFIER().GetText(), exp.ToNodes(ctx.method_param().expression_or_wildcard_set()));
                }
                // Check for tables //
                else if (ctx.method_param().K_TABLE() != null)
                {
                    // Do nothing, we already caught the table 
                }
                // Check for NIL, which is is not allowed in this context //
                else if (ctx.method_param().DOT() != null)
                {
                    throw new ArgumentException(string.Format("Nil parameters are not allowed in strictly names method invocations"));
                }
                // Otherwise we found an error //
                else
                {
                    throw new ArgumentException(string.Format("Parameter is invalid: {0}", ctx.method_param().GetText()));
                }
                
            }

            // Now that the parameter collection is ready, check against the signiture //
            ParameterCollectionSigniture sig = lib.GetMethodSigniture(method_name);
            sig.Check(parameters);

            // If we made it this far... we must have passed the checking steps and can render the method node //
            Method node = lib.GetMethod(this._master, method_name, parameters);
            this._master = node;

            return node;

        }

        public override Method VisitStructure_method_weak(RyeParser.Structure_method_weakContext context)
        {

            // Get the name of the structure and the method //
            string lib_name = context.IDENTIFIER()[0].GetText();
            string method_name = context.IDENTIFIER()[1].GetText();

            // Check to see if the structure exists //
            if (!this._Session.LibraryExists(lib_name))
            {
                throw new ArgumentException(string.Format("Library '{0}' does not exist", lib_name));
            }
            // Now check that the method exists in the structure //
            Library lib = this._Session.GetLibrary(lib_name);
            if (!lib.MethodExists(method_name))
            {
                throw new ArgumentException(string.Format("Method '{0}' does not exist for '{1}'", method_name, lib_name));
            }

            // Lookup up the method //
            ParameterCollectionSigniture sig = lib.GetMethodSigniture(method_name);

            // First, check that the parameter counts match, otherwise throw an exception and dont bother trying to parse any further //
            if (sig.Count != context.method_param().Length)
            {
                throw new ArgumentException(string.Format("The parameter passed ({0}) doesn't match what was expected ({1})", context.method_param().Length, sig.Count));
            }

            // Render a visitor //
            ExpressionVisitor exp = new ExpressionVisitor(this._Session);
            exp.SetSecondary(this._exp.SecondaryName, this._exp.SecondaryScalars, this._exp.SecondaryMatrixes);
            Queue<TabularData> TableQueue = new Queue<TabularData>();
            Queue<string> NameQueue = new Queue<string>();

            // Search through the parameters for all table references //
            foreach (RyeParser.Method_paramContext ctx in context.method_param())
            {

                // If the scalar is not null //
                if (ctx.K_TABLE() != null)
                {

                    // Get the database, table and alias //
                    string db = ctx.table_name().IDENTIFIER()[0].GetText();
                    string name = ctx.table_name().IDENTIFIER()[1].GetText();
                    string alias = (ctx.IDENTIFIER() == null ? name : ctx.IDENTIFIER().GetText());

                    // Get the table //
                    TabularData data = this._Session.GetTabularData(db, name);

                    // Add to the expression visitor //
                    exp.AddRegister(alias, new Register(alias, data.Columns));

                    // Add the table to the queue //
                    TableQueue.Enqueue(data);
                    NameQueue.Enqueue(alias);

                }

            }

            // Now that we're this far, build a matrix visitor //
            MatrixExpressionVisitor mat = new MatrixExpressionVisitor(exp, this._Session);
            
            // Now go through create the parameter collection //
            int i = 0;
            ParameterCollection parameters = new ParameterCollection();
            foreach (RyeParser.Method_paramContext ctx in context.method_param())
            {

                // Parse matricies //
                if (ctx.matrix_expression() != null && sig.ParameterAffinity(i) == ParameterAffinity.Matrix)
                {
                    parameters.Add(sig.Name(i), mat.ToMatrix(ctx.matrix_expression()));
                }
                // Parse expressions //
                else if (ctx.expression() != null && sig.ParameterAffinity(i) == ParameterAffinity.Expression)
                {
                    parameters.Add(sig.Name(i), exp.ToNode(ctx.expression()));
                }
                // Parse expressions //
                else if (ctx.expression_or_wildcard_set() != null && sig.ParameterAffinity(i) == ParameterAffinity.ExpressionVector)
                {
                    parameters.Add(sig.Name(i), exp.ToNodes(ctx.expression_or_wildcard_set()));
                }
                // Check if a table //
                else if (ctx.K_TABLE() != null && sig.ParameterAffinity(i) == ParameterAffinity.Table)
                {
                    parameters.Add(NameQueue.Dequeue(), TableQueue.Dequeue());
                }
                // Nil parameter and it can accept nil values //
                else if (ctx.DOT() != null && sig.CanBeNull(i))
                {
                    parameters.AddNull(sig.Name(i), sig.ParameterAffinity(i));
                }
                // The parameter must be invalid //
                else
                {
                    throw new ArgumentException(string.Format("Paramter passed is invalid either due to a mis-matched affinity or being null when it cannot be: {0}", sig.Name(i)));
                }

            }

            // Note that we shouldn't have to check the signiture since we were checking along the way

            // If we made it this far... we must have passed the checking steps and can render the method node //
            Method node = lib.GetMethod(this._master, method_name, parameters);
            this._master = node;

            return node;

        }

        // Finalization //
        public Method ToNode(RyeParser.MethodContext context)
        {
            this._master = null;
            return this.Visit(context);
        }

    }

}
