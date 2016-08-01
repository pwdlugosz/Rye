using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Structures;

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

        // Get the data //
        public static DataSet CallData(Workspace Enviro, RyeParser.Table_nameContext context)
        {

            // Get the name //
            string db_name = context.IDENTIFIER()[0].GetText();
            string t_name = context.IDENTIFIER()[1].GetText();

            if (Enviro.Connections.Exists(db_name))
            {
                return Enviro.GetTable(db_name, t_name);
            }
            else if (Enviro.Structures.Exists(db_name))
            {
                if (Enviro.Structures[db_name].Extents.Exists(t_name))
                {
                    return Enviro.Structures[db_name].Extents[t_name];
                }
                else
                {
                    throw new RyeParseException("Extent '{0}' does not exist in '{1}'", t_name, db_name);
                }
            }

            throw new RyeParseException("'{0}' does not exist as an in memory structure or a disk connection", db_name);

        }

        public static DataSet RenderData(Workspace Enviro, ExpressionCollection Nodes, RyeParser.ActAppendContext context)
        {

            // Check if we need to create the table or just open it //
            if (context.append_method().K_NEW() == null)
            {
                DataSet t = CallData(Enviro, context.append_method().table_name());
                if (t.Columns.Count != Nodes.Columns.Count)
                    throw new RyeCompileException("Attempting to insert {0} columns into {1}", Nodes.Columns.Count, t.Columns.Count);
                
                return t;
            }

            // Get the table name //
            string db_name = context.append_method().table_name().IDENTIFIER()[0].GetText();
            string t_name = context.append_method().table_name().IDENTIFIER()[1].GetText();

            // Check if disk based //
            if (Enviro.Connections.Exists(db_name))
            {
                return new Table(Enviro.Connections[db_name], t_name, Nodes.Columns);
            }
            // Check if memory based //
            else if (Enviro.Structures.Exists(db_name))
            {
                Extent e = new Extent(Nodes.Columns);
                e.Header.Name = t_name;
                Enviro.Structures[db_name].Extents.Reallocate(t_name, e);
                return e;
            }
            else
            {
                throw new RyeCompileException("'{0}' does not exist as either a structure or a disk connection");
            }

        }

        public static DataSet RenderData(Workspace Enviro, ExpressionCollection Nodes, RyeParser.Append_methodContext context)
        {

            // Check if we need to create the table or just open it //
            if (context.K_NEW() == null)
            {
                DataSet t = CallData(Enviro, context.table_name());
                if (t.Columns.Count != Nodes.Columns.Count)
                    throw new RyeCompileException("Attempting to insert {0} columns into {1}", Nodes.Columns.Count, t.Columns.Count);

                return t;
            }

            // Get the table name //
            string db_name = context.table_name().IDENTIFIER()[0].GetText();
            string t_name = context.table_name().IDENTIFIER()[1].GetText();

            // Check if disk based //
            if (Enviro.Connections.Exists(db_name))
            {
                return new Table(Enviro.Connections[db_name], t_name, Nodes.Columns);
            }
            // Check if memory based //
            else if (Enviro.Structures.Exists(db_name))
            {
                Extent e = new Extent(Nodes.Columns);
                e.Header.Name = t_name;
                Enviro.Structures[db_name].Extents.Reallocate(t_name, e);
                return e;
            }
            else
            {
                throw new RyeCompileException("'{0}' does not exist as either a structure or a disk connection");
            }

        }

        // Declarations //
        public static void AppendScalar(Workspace Enviro, MemoryStructure Heap, RyeParser.Unit_declare_scalarContext context)
        {

            string sname = context.IDENTIFIER()[0].GetText();
            string vname = context.IDENTIFIER()[1].GetText();
            CellAffinity t = GetAffinity(context.type());
            int size = GetSize(context.type());

            Cell c = new Cell(t);

            if (context.expression() != null)
            {
                ExpressionVisitor exp = new ExpressionVisitor(Enviro);
                exp.AddStructure(sname, Heap);
                c = exp.Visit(context.expression()).Evaluate();
            }

            Heap.Scalars.Reallocate(vname, c);

        }

        public static void AppendMatrix(Workspace Enviro, MemoryStructure Heap, RyeParser.Unit_declare_matrixContext context)
        {

            string sname = context.IDENTIFIER()[0].GetText();
            string vname = context.IDENTIFIER()[1].GetText();
            CellAffinity t = GetAffinity(context.type());
            int size = GetSize(context.type());

            ExpressionVisitor exp = new ExpressionVisitor(Enviro);
            MatrixExpressionVisitor mat = new MatrixExpressionVisitor(exp);
            exp.AddStructure(sname, Heap);
            int row = (context.expression().Length >= 1 ? (int)exp.Visit(context.expression()[0]).Evaluate().INT : 1);
            int col = (context.expression().Length == 2 ? (int)exp.Visit(context.expression()[1]).Evaluate().INT : 1);

            CellMatrix m = (context.matrix_expression() == null ? new CellMatrix(row, col, t) : mat.ToMatrix(context.matrix_expression()).Evaluate());

            Heap.Matricies.Reallocate(vname, m);

        }

        public static void AppendLambda(Workspace Enviro, MemoryStructure Heap, RyeParser.Unit_declare_lambdaContext context)
        {

            if (context.K_GRADIENT() == null)
            {
                CompilerHelper.AppendLambdaBuild(Enviro, Heap, context);
            }
            else
            {
                CompilerHelper.AppendLambdaGradient(Enviro, Heap, context);
            }

        }

        private static void AppendLambdaBuild(Workspace Enviro, MemoryStructure Heap, RyeParser.Unit_declare_lambdaContext context)
        {

            // Get the name of lambda //
            string sname = context.lambda_name()[0].IDENTIFIER()[0].GetText();
            string vname = context.lambda_name()[0].IDENTIFIER()[1].GetText();

            // Check the caller //
            if (StringComparer.OrdinalIgnoreCase.Compare(sname, Heap.Name) != 0)
                throw new ArgumentException(string.Format("Caller structure '{0}' differs from assigned structure '{1}'", Heap.Name, sname));

            // create an expression visitor //
            ExpressionVisitor exp = new ExpressionVisitor(Enviro);
            exp.AddStructure(Heap.Name, Heap);

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
            Heap.Lambda.Reallocate(vname, l);

        }

        private static void AppendLambdaGradient(Workspace Enviro, MemoryStructure Heap, RyeParser.Unit_declare_lambdaContext context)
        {

            // Get the name of lambda //
            string sname = context.lambda_name()[0].IDENTIFIER()[0].GetText();
            string vname = context.lambda_name()[0].IDENTIFIER()[1].GetText();

            // Get the name of the current lambda //
            string sname_fx = context.lambda_name()[1].IDENTIFIER()[0].GetText();
            string vname_fx = context.lambda_name()[1].IDENTIFIER()[1].GetText();

            // Check the caller //
            if (StringComparer.OrdinalIgnoreCase.Compare(sname, Heap.Name) != 0)
                throw new ArgumentException(string.Format("Caller structure '{0}' differs from assigned structure '{1}'", Heap.Name, sname));

            // Look for the original lambda, f(x) //
            Lambda l = null;
            if (Heap.Lambda.Exists(vname_fx) && string.Compare(sname_fx, Heap.Name, true) == 0)
            {
                l = Heap.Lambda[vname_fx];
            }
            else if (Enviro.Structures.Exists(sname_fx))
            {

                if (Enviro.Structures[sname_fx].Lambda.Exists(vname_fx))
                {
                    l = Enviro.Structures[sname_fx].Lambda[vname_fx];
                }
                else
                {
                    throw new ArgumentException(string.Format("Cannot find lambda '{0}'", vname_fx));
                }

            }
            else
            {
                throw new ArgumentException(string.Format("Cannot find lambda '{0}'", vname_fx));
            }

            // Get the x variable, to calculate f'(x) //
            string x = context.IDENTIFIER()[0].GetText();

            // Calculate the gradient //
            Lambda f_prime = l.Gradient(sname, x);

            // Allocate //
            Heap.Lambda.Reallocate(vname, f_prime);
            
        }

    }

}
