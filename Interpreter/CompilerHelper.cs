using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Structures;
using Rye.Libraries;

namespace Rye.Interpreter
{

    public static class CompilerHelper
    {

        public static CellAffinity GetAffinity(RyeParser.TypeContext context)
        {
            string t = context.GetText().Split('.').First();
            return CellAffinityHelper.Parse(t);
        }

        public static int GetSize(RyeParser.TypeContext context)
        {

            CellAffinity t = GetAffinity(context);

            // double, int, date are 8 bytes //
            if (t == CellAffinity.INT || t == CellAffinity.DOUBLE || t == CellAffinity.DATE_TIME)
                return 8;

            // Bools are 1 byte //
            if (t == CellAffinity.BOOL)
                return 1;

            // Variable length with a predefined size //
            if (context.LITERAL_INT() != null)
            {
                int size = int.Parse(context.LITERAL_INT().GetText());
                return Math.Min(size, Cell.MAX_STRING_LENGTH);
            }

            // Default the sizes //
            if (t == CellAffinity.STRING)
                return Schema.DEFAULT_STRING_SIZE;
            return Schema.DEFAULT_BLOB_SIZE;


        }

        public static Filter GetWhere(ExpressionVisitor Visitor, RyeParser.Where_clauseContext context)
        {

            if (context == null)
                return Filter.TrueForAll;

            return new Filter(Visitor.ToNode(context.expression()));

        }

        public static int GetThreadCount(RyeParser.Thread_clauseContext context)
        {
            
            if (context == null)
                return 1;

            if (context.K_MAX() != null)
                return Environment.ProcessorCount;

            return int.Parse(context.LITERAL_INT().GetText());

        }

        public static Cell GetHint(Session Enviro, RyeParser.Base_clauseContext context)
        {

            if (context.K_HINT() == null)
                return Cell.NULL_INT;

            return new ExpressionVisitor(Enviro).ToNode(context.expression()).Evaluate();

        }

        public static Cell GetHint(Session Enviro, RyeParser.Command_joinContext context)
        {

            if (context.K_HINT() == null)
                return Cell.NULL_INT;

            return new ExpressionVisitor(Enviro).ToNode(context.expression()).Evaluate();

        }

        // Get the data //
        public static TabularData CallData(Session Enviro, RyeParser.Table_nameContext context)
        {

            // Get the name //
            string db_name = context.IDENTIFIER()[0].GetText();
            string t_name = context.IDENTIFIER()[1].GetText();

            if (Enviro.TableExists(db_name, t_name))
            {
                return Enviro.GetTable(db_name, t_name);
            }
            else if (Enviro.IsGlobal(db_name) && Enviro.ExtentExists(t_name))
            {
                return Enviro.GetExtent(t_name);
            }

            throw new RyeCompileException("'{0}.{1}' does not exist as either a structure or a disk connection", db_name, t_name);

        }

        public static TabularData RenderData(Session Enviro, ExpressionCollection Nodes, RyeParser.ActAppendContext context)
        {

            // Get the Page Size //
            long size = CompilerHelper.RenderPageSize(Enviro, context.append_method().page_size());
                
            // Check if we need to create the table or just open it //
            if (context.append_method().K_NEW() == null)
            {

                TabularData t = CallData(Enviro, context.append_method().table_name());
                if (t.Columns.Count != Nodes.Columns.Count)
                    throw new RyeCompileException("Attempting to insert {0} columns into {1}", Nodes.Columns.Count, t.Columns.Count);
                
                return t;
            }

            // Get the table name //
            string db_name = context.append_method().table_name().IDENTIFIER()[0].GetText();
            string t_name = context.append_method().table_name().IDENTIFIER()[1].GetText();

            // Check if disk based //
            if (Enviro.ConnectionExists(db_name))
            {
                return Enviro.CreateTable(db_name, t_name, Nodes.Columns, (int)size);
            }
            // Check if memory based //
            else if (Enviro.IsGlobal(db_name))
            {
                return Enviro.CreateExtent(t_name, Nodes.Columns, (int)size);
            }

            throw new RyeCompileException("'{0}.{1}' does not exist as either a structure or a disk connection", db_name, t_name);

        }

        public static TabularData RenderData(Session Enviro, ExpressionCollection Nodes, RyeParser.Append_methodContext context)
        {

            // Get the Page Size //
            long size = CompilerHelper.RenderPageSize(Enviro, context.page_size());

            // Check if we need to create the table or just open it //
            if (context.K_NEW() == null)
            {
                TabularData t = CallData(Enviro, context.table_name());
                if (t.Columns.Count != Nodes.Columns.Count)
                    throw new RyeCompileException("Attempting to insert {0} columns into {1}", Nodes.Columns.Count, t.Columns.Count);

                return t;
            }

            // Get the table name //
            string db_name = context.table_name().IDENTIFIER()[0].GetText();
            string t_name = context.table_name().IDENTIFIER()[1].GetText();

            // Check if disk based //
            if (Enviro.ConnectionExists(db_name))
            {
                return Enviro.CreateTable(db_name, t_name, Nodes.Columns, (int)size);
            }
            // Check if memory based //
            else if (Enviro.IsGlobal(db_name))
            {
                return Enviro.CreateExtent(t_name, Nodes.Columns, (int)size);
            }

            throw new RyeCompileException("'{0}.{1}' does not exist as either a structure or a disk connection", db_name, t_name);

        }

        // Declarations //
        public static string GetParentName(RyeParser.Generic_nameContext context, string DefaultName)
        {
            if (context.IDENTIFIER().Length == 2)
                return context.IDENTIFIER()[0].GetText();
            return DefaultName;
        }

        public static string GetVariableName(RyeParser.Generic_nameContext context)
        {
            if (context.IDENTIFIER().Length == 2)
                return context.IDENTIFIER()[1].GetText();
            return context.IDENTIFIER()[0].GetText();
        }
        
        public static void AppendScalar(Session Enviro, Heap<Cell> Heap, ExpressionVisitor Visitor, RyeParser.Unit_declare_scalarContext context)
        {

            string vname = context.IDENTIFIER().GetText();
            CellAffinity t = GetAffinity(context.type());
            int size = GetSize(context.type());

            Cell c = new Cell(t);

            if (context.expression() != null)
            {
                c = Visitor.Visit(context.expression()).Evaluate();
            }

            Heap.Reallocate(vname, c);

        }

        public static void AppendMatrix(Session Enviro, Heap<CellMatrix> Heap, ExpressionVisitor Visitor, RyeParser.Unit_declare_matrixContext context)
        {

            string vname = context.IDENTIFIER().GetText();
            CellAffinity t = GetAffinity(context.type());
            int size = GetSize(context.type());

            MatrixExpressionVisitor mat = new MatrixExpressionVisitor(Visitor, Enviro);
 
            int row = (context.expression().Length >= 1 ? (int)Visitor.Visit(context.expression()[0]).Evaluate().INT : 1);
            int col = (context.expression().Length == 2 ? (int)Visitor.Visit(context.expression()[1]).Evaluate().INT : 1);

            CellMatrix m = (context.matrix_expression() == null ? new CellMatrix(row, col, t) : mat.ToMatrix(context.matrix_expression()).Evaluate());

            Heap.Reallocate(vname, m);

        }

        public static void AppendLambda(Session Enviro, RyeParser.Unit_declare_lambdaContext context)
        {

            if (context.K_GRADIENT() == null)
            {
                CompilerHelper.AppendLambdaBuild(Enviro, context);
            }
            else
            {
                CompilerHelper.AppendLambdaGradient(Enviro, context);
            }

        }

        private static void AppendLambdaBuild(Session Enviro, RyeParser.Unit_declare_lambdaContext context)
        {

            // Get the name of lambda //
            string vname = context.lambda_name()[0].IDENTIFIER().GetText();

            // create an expression visitor //
            ExpressionVisitor exp = new ExpressionVisitor(Enviro);
            
            // add all needed pointers to the visitor //
            List<string> pointers = new List<string>();
            foreach (Antlr4.Runtime.Tree.ITerminalNode t in context.IDENTIFIER())
            {
                exp.AddPointer(t.GetText(), CellAffinity.INT);
                pointers.Add(t.GetText());
            }

            // Now, lets render the lambda ... //
            Expression mu = exp.ToNode(context.expression());

            // Create the lambda //
            Lambda l = new Lambda(vname, mu, pointers);

            // Add the lambda to our heap //
            Enviro.SetLambda(vname, l);

        }

        private static void AppendLambdaGradient(Session Enviro,  RyeParser.Unit_declare_lambdaContext context)
        {

            // Get the name of lambda we are creating //
            string sname = context.lambda_name()[0].IDENTIFIER().GetText();
            
            // Get the name of the lambda that already exists //
            string sname_fx = context.lambda_name()[1].IDENTIFIER().GetText();
            
            // Look for the original lambda, f(x) //
            Lambda l = Enviro.GetLambda(sname_fx);

            // Get the x variable, to calculate f'(x) //
            string x = context.IDENTIFIER()[0].GetText();

            // Calculate the gradient //
            Lambda f_prime = l.Gradient(sname, x);

            // Allocate //
            Enviro.SetLambda(sname, f_prime);
            
        }

        // Append Helpers //
        public static Methods.MethodDump RenderDumpMethod(Session Enviro, RyeParser.Append_methodContext context, TabularData Data)
        {

            if (context.K_DUMP() == null)
                return Methods.MethodDump.Empty;

            string path = Enviro.BaseVisitor.Visit(context.expression()[0]).Evaluate().valueSTRING;
            char delim = Enviro.BaseVisitor.Visit(context.expression()[1]).Evaluate().valueSTRING.First();
            return new Methods.MethodDump(null, Data, path, delim, Enviro);

        }

        public static Methods.MethodSort RenderSortMethod(Session Enviro, RyeParser.Append_methodContext context, TabularData Data)
        {

            if (context.K_SORT() == null)
                return Methods.MethodSort.Empty;

            // Build the sort key //
            Key k = new Key();
            ExpressionCollection cols = new ExpressionCollection();
            ExpressionVisitor exp = new ExpressionVisitor(Enviro);
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

            return new Methods.MethodSort(null, Data, cols, r, k);


        }

        // Page Size //
        public static long RenderPageSize(Session Enviro, RyeParser.Page_sizeContext context)
        {

            // Return the default page size //
            if (context == null)
                return Enviro.DefualtPageSize;

            // Get the literal integer //
            long page_size = long.Parse(context.LITERAL_INT().GetText());

            // Look for MB //
            if (context.SUNIT_MB() != null)
                return page_size * 1024 * 1024;

            // Look for KB //
            if (context.SUNIT_KB() != null)
                return page_size * 1024;

            // Otherwise, assume it in bytes //
            return page_size;

        }

        public static string UnParsePageSize(long PageSize)
        {

            if (PageSize >= 1024 * 1024)
                return Math.Round(((double)PageSize) / (1024D * 1024D), 1).ToString() + "MB";

            if (PageSize >= 1024)
                return Math.Round(((double)PageSize) / (1024D), 1).ToString() + "KB";

            return PageSize.ToString() + "B";

        }

    }

}
