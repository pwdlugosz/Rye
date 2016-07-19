using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using Rye.Data;
using Rye.Expressions;
using Rye.Aggregates;
using Rye.Methods;
using Rye.MatrixExpressions;
using Rye.Query;
using Rye.Structures;

namespace Rye.Interpreter
{

    public sealed class CommandVisitor : RyeParserBaseVisitor<int>
    {

        private Workspace _enviro;

        public CommandVisitor(Workspace Enviro)
            : base()
        {
            this._enviro = Enviro;
        }

        public override int VisitCommand_method(RyeParser.Command_methodContext context)
        {

            ExpressionVisitor exp_vis = new ExpressionVisitor(this._enviro);
            MatrixExpressionVisitor mat_vis = new MatrixExpressionVisitor(exp_vis, this._enviro);
            MethodVisitor met_vis = new MethodVisitor(exp_vis, mat_vis, this._enviro);
            Method m = met_vis.Visit(context.method());
            m.BeginInvoke();
            m.Invoke();
            m.EndInvoke();

            return 1;

        }

        public override int VisitCommand_connect(RyeParser.Command_connectContext context)
        {

            ExpressionVisitor exp = new ExpressionVisitor(this._enviro);
            foreach (RyeParser.Connect_unitContext ctx in context.connect_unit())
            {

                string alias = ctx.IDENTIFIER().GetText();
                string path = exp.Visit(ctx.expression()).Evaluate().valueSTRING;
                if (!System.IO.Directory.Exists(path))
                {
                    throw new RyeCompileException("Directory does not exist: \n\t{0}", path);
                }
                this._enviro.Connections.Reallocate(alias, path);

            }

            return 1;

        }

        public override int VisitCommand_disconnect(RyeParser.Command_disconnectContext context)
        {

            foreach (Antlr4.Runtime.IToken t in context.IDENTIFIER())
            {
                this._enviro.Connections.Deallocate(t.Text);
            }
            return 1;

        }

        public override int VisitCommand_declare(RyeParser.Command_declareContext context)
        {

            foreach (RyeParser.Unit_declare_scalarContext ctx1 in context.unit_declare_scalar())
            {
                
                string sname = ctx1.IDENTIFIER()[0].GetText();
                if (!this._enviro.Structures.Exists(sname))
                    throw new RyeCompileException("Structure '{0}' does not exist", sname);
                CompilerHelper.AppendScalar(this._enviro, this._enviro.Structures[sname], ctx1);
                
            }

            foreach (RyeParser.Unit_declare_matrixContext ctx2 in context.unit_declare_matrix())
            {

                string sname = ctx2.IDENTIFIER()[0].GetText();
                if (!this._enviro.Structures.Exists(sname))
                    throw new RyeCompileException("Structure '{0}' does not exist", sname);
                CompilerHelper.AppendMatrix(this._enviro, this._enviro.Structures[sname], ctx2);

            }

            return 1;

        }

        public override int VisitCommand_create(RyeParser.Command_createContext context)
        {
            
            // build the schema //
            Schema cols = new Schema();
            foreach (RyeParser.Create_unitContext ctx in context.create_unit())
            {

                string name = ctx.IDENTIFIER().GetText();
                CellAffinity type = CompilerHelper.GetAffinity(ctx.type());
                int size = CompilerHelper.GetSize(ctx.type());

                cols.Add(name, type, true, size);

            }

            // get the size //
            long extent_size = Extent.EstimateMaxRecords(cols);
            if (context.LITERAL_INT() != null)
                extent_size = long.Parse(context.LITERAL_INT().GetText());
            
            // Get the names //
            string sname = context.table_name().IDENTIFIER()[0].GetText();
            string tname = context.table_name().IDENTIFIER()[1].GetText();
            
            // Check we need to build this //
            if (this._enviro.Connections.Exists(sname))
            {
                Table t = new Table(this._enviro.Connections[sname], tname, cols, extent_size);
            }
            else if (this._enviro.Structures.Exists(sname))
            {
                Extent e = new Extent(cols, Header.NewMemoryOnlyExtentHeader(sname, cols.Count, extent_size));
                this._enviro.Structures[sname].Extents.Reallocate(tname, e);
            }
            else
            {
                throw new RyeCompileException("Structure or connection with alias '{0}' does not exist", sname);
            }

            return 1;

        }

        public override int VisitCommand_read(RyeParser.Command_readContext context)
        {
            
            // Get some high level data first, such as thread count, 'where' clause, and source data //
            DataSet data = CompilerHelper.CallData(this._enviro, context.base_clause().table_name());
            int threads = CompilerHelper.GetThreadCount(context.base_clause().thread_clause());
            List<SelectProcessNode> nodes = new List<SelectProcessNode>();
            string alias = (context.base_clause().IDENTIFIER() == null ? data.Header.Name : context.base_clause().IDENTIFIER().GetText());

            // Render each node //
            for (int i = 0; i < threads; i++)
            {

                // Create an expression visitor //
                ExpressionVisitor exp = new ExpressionVisitor(this._enviro);

                // Create the local heap //
                MemoryStructure local = new MemoryStructure("LOCAL");
                local.Scalars.Allocate(SelectProcessNode.ROW_ID, Cell.ZeroValue(CellAffinity.INT));
                local.Scalars.Allocate(SelectProcessNode.EXTENT_ID, Cell.ZeroValue(CellAffinity.INT));
                local.Scalars.Allocate(SelectProcessNode.KEY_CHANGE, Cell.ZeroValue(CellAffinity.BLOB));
                local.Scalars.Allocate(SelectProcessNode.IS_FIRST, Cell.FALSE);
                local.Scalars.Allocate(SelectProcessNode.IS_LAST, Cell.FALSE);

                // Create a register //
                Register reg = new Register(alias, data.Columns);

                // Accumulate the heap and register to the expression visitor //
                exp.AddRegister(alias, reg);
                exp.AddStructure("LOCAL", local);

                // Process the 'by' clause //
                ExpressionCollection by_clause = (context.by_clause() == null ? null : exp.ToNodes(context.by_clause().expression_or_wildcard_set()));

                // Create a matrix visitor //
                MatrixExpressionVisitor mat = new MatrixExpressionVisitor(exp, this._enviro);
                mat.AddStructure("LOCAL", local);
                
                // Create a method visitor //
                MethodVisitor met = new MethodVisitor(exp, mat, this._enviro);
                met.AddStructure("LOCAL", local);

                // Load the local heap //
                if (context.command_declare() != null)
                {

                    foreach (RyeParser.Unit_declare_scalarContext ctx in context.command_declare().unit_declare_scalar())
                    {
                        CompilerHelper.AppendScalar(this._enviro, local, ctx);
                    }
                    foreach (RyeParser.Unit_declare_matrixContext ctx in context.command_declare().unit_declare_matrix())
                    {
                        CompilerHelper.AppendMatrix(this._enviro, local, ctx);
                    }

                }
                // Get whatever where clause we need //
                Filter where = CompilerHelper.GetWhere(exp, context.base_clause().where_clause());

                // Create the mapping methods //
                MethodCollection map = new MethodCollection();
                foreach (RyeParser.MethodContext ctx in context.map_clause().method())
                {
                    map.Add(met.Visit(ctx));
                }

                // Build the reducing methods //
                MethodCollection reduce = new MethodCollection();
                if (context.reduce_clause() != null)
                {
                    foreach (RyeParser.MethodContext ctx in context.reduce_clause().method())
                    {
                        reduce.Add(met.Visit(ctx));
                    }
                }
                
                // Render the worker node //
                SelectProcessNode node = new SelectProcessNode(i, data.CreateVolume(i, threads), reg, local, map, reduce, where, by_clause);
                nodes.Add(node);

            }

            // Render the reducer //
            SelectProcessConsolidation reducer = new SelectProcessConsolidation();

            // Create a process //
            QueryProcess<SelectProcessNode> process = new QueryProcess<SelectProcessNode>(nodes, reducer);

            // Run the process //
            Stopwatch sw = Stopwatch.StartNew();
            if (this._enviro.AllowAsync && threads > 1)
            {
                process.ExecuteAsync();
            }
            else
            {
                process.Execute();
            }
            sw.Stop();

            // Close the output //
            this._enviro.IO.WriteHeader("Select");
            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            this._enviro.IO.WriteLine();

            return 1;

        }

        public override int VisitCommand_update(RyeParser.Command_updateContext context)
        {
            
            // Get the source data //
            DataSet data = CompilerHelper.CallData(this._enviro, context.base_clause().table_name());
            string alias = (context.base_clause().IDENTIFIER() != null ? context.base_clause().IDENTIFIER().GetText() : "T");
            int threads = CompilerHelper.GetThreadCount(context.base_clause().thread_clause());

            // Build the key //
            Key key = new Key();
            foreach (Antlr4.Runtime.Tree.ITerminalNode t in context.IDENTIFIER())
            {
                int idx = data.Columns.ColumnIndex(t.GetText());
                if (idx == -1)
                    throw new RyeCompileException("Column '{0}' does not exist in '{1}'", t.GetText(), alias);
                key.Add(idx);
            }

            // Create the update process nodes //
            List<UpdateProcessNode> nodes = new List<UpdateProcessNode>();
            for (int i = 0; i < threads; i++)
            {

                // Get the expression visitor //
                ExpressionVisitor exp = new ExpressionVisitor(this._enviro);
                Register mem = new Register(alias, data.Columns);
                exp.AddRegister(alias, mem);

                // Get the filter //
                Filter where = CompilerHelper.GetWhere(exp, context.base_clause().where_clause());

                // Build the expression collections //
                ExpressionCollection values = exp.ToNodes(context.expression()); // new ExpressionCollection();

                if (data.Header.IsMemoryOnly)
                {
                    nodes.Add(new UpdateProcessNode(0, null, data.CreateVolume(i, threads), key, values, where, mem));
                }
                else
                {
                    nodes.Add(new UpdateProcessNode(i, data as Table, data.CreateVolume(i, threads), key, values, where, mem));
                }

            }

            QueryProcess<UpdateProcessNode> tprocess = new QueryProcess<UpdateProcessNode>(nodes, new UpdateProcessConsolidation());
            Stopwatch sw = Stopwatch.StartNew();
            if (this._enviro.AllowAsync && threads > 1)
                tprocess.ExecuteAsync();
            else
                tprocess.Execute();
            sw.Stop();

            // Close the output //
            this._enviro.IO.WriteHeader("Update");
            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            this._enviro.IO.WriteLine();

            return 1;

        }

        public override int VisitCommand_delete(RyeParser.Command_deleteContext context)
        {

            // Get the source data //
            DataSet data = CompilerHelper.CallData(this._enviro, context.base_clause().table_name());
            string alias = (context.base_clause().IDENTIFIER() != null ? context.base_clause().IDENTIFIER().GetText() : "T");
            int threads = CompilerHelper.GetThreadCount(context.base_clause().thread_clause());

            // Create the update process nodes //
            List<DeleteProcessNode> nodes = new List<DeleteProcessNode>();
            for (int i = 0; i < threads; i++)
            {


                // Get the expression visitor //
                ExpressionVisitor exp = new ExpressionVisitor(this._enviro);
                Register mem = new Register(alias, data.Columns);
                exp.AddRegister(alias, mem);

                // Get the filter //
                Filter where = CompilerHelper.GetWhere(exp, context.base_clause().where_clause());

                if (data.Header.IsMemoryOnly)
                {
                    nodes.Add(new DeleteProcessNode(0, null, data.CreateVolume(i, threads), where, mem));
                }
                else
                {
                    nodes.Add(new DeleteProcessNode(i, data as Table, data.CreateVolume(i, threads), where, mem));
                }

            }

            // Build the consolidator //
            DeleteProcessConsolidation reducer = new DeleteProcessConsolidation();

            // Build the processor //
            QueryProcess<DeleteProcessNode> process = new QueryProcess<DeleteProcessNode>(nodes, reducer);

            // Run the process //
            Stopwatch sw = Stopwatch.StartNew();
            if (this._enviro.AllowAsync && threads > 1)
            {
                process.ExecuteAsync();
            }
            else
            {
                process.Execute();
            }
            sw.Stop();

            // Close the output //
            this._enviro.IO.WriteHeader("Delete");
            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            this._enviro.IO.WriteLine();

            return 1;
            
        }

        public override int VisitCommand_aggregate(RyeParser.Command_aggregateContext context)
        {

            // Get some high level data first, such as thread count, 'where' clause, and source data //
            DataSet data = CompilerHelper.CallData(this._enviro, context.base_clause().table_name());
            int threads = CompilerHelper.GetThreadCount(context.base_clause().thread_clause());
            List<AggregateProcessNode> nodes = new List<AggregateProcessNode>();
            string alias = (context.base_clause().IDENTIFIER() == null ? data.Header.Name : context.base_clause().IDENTIFIER().GetText());

            // Create all the aggregate process nodes //
            ExpressionCollection FinalKey = new ExpressionCollection();
            AggregateCollection FinalValue = new AggregateCollection();
            for (int i = 0; i < threads; i++)
            {

                // Construct the expression visitor and memory register used in the aggregation process //
                ExpressionVisitor in_exp = new ExpressionVisitor(this._enviro);
                Register in_mem = new Register(alias, data.Columns);
                in_exp.AddRegister(alias, in_mem);

                // Get the keys, the aggregates and the where clause //
                ExpressionCollection keys = in_exp.ToNodes(context.by_clause().expression_or_wildcard_set());
                AggregateCollection aggs = in_exp.ToReducers(context.over_clause().beta_reduction_list());
                Filter where = CompilerHelper.GetWhere(in_exp, context.base_clause().where_clause());

                AggregateProcessNode n = new AggregateProcessNode(i, data.CreateVolume(i, threads), keys, aggs, where, in_mem);
                nodes.Add(n);

                // Save the key/value //
                FinalKey = keys;
                FinalValue = aggs;

            }

            // Create the output expression visitor //
            Schema out_columns = Schema.Join(FinalKey.Columns, FinalValue.Columns);
            ExpressionVisitor out_exp = new ExpressionVisitor(this._enviro);
            Register out_mem = new Register("OUT", out_columns);
            out_exp.AddRegister("OUT", out_mem); // TODO, think of a better alias to use
            ExpressionCollection out_keys = out_exp.ToNodes(context.append_method().expression_or_wildcard_set());

            // Render the record writer that will be used to fill the output //
            DataSet out_data = CompilerHelper.RenderData(this._enviro, out_keys, context.append_method());
            RecordWriter out_writer = out_data.OpenWriter();

            // Create the consolidation process //
            AggregateConsolidationProcess reducer = new AggregateConsolidationProcess(FinalKey, FinalValue, out_writer, out_keys, out_mem);

            // Build the query process that will handle this //
            QueryProcess<AggregateProcessNode> process = new QueryProcess<AggregateProcessNode>(nodes, reducer);

            // Run the process //
            Stopwatch sw = Stopwatch.StartNew();
            if (this._enviro.AllowAsync && threads > 1)
            {
                process.ExecuteAsync();
            }
            else
            {
                process.Execute();
            }
            sw.Stop();

            // Close the output //
            this._enviro.IO.WriteHeader("Aggregate");
            this._enviro.IO.WriteLine("Actual Aggregate Cost: {0}", reducer.Clicks);
            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            this._enviro.IO.WriteLine();

            return 1;

        }

        public override int VisitCommand_sort(RyeParser.Command_sortContext context)
        {

            // Get the data //
            DataSet data = CompilerHelper.CallData(this._enviro, context.table_name());
            
            // Build the sort key //
            Key k = new Key();
            foreach (RyeParser.Sort_unitContext ctx in context.sort_unit())
            {

                int idx = data.Columns.ColumnIndex(ctx.IDENTIFIER().GetText());
                if (idx == -1)
                    throw new RyeCompileException("Field '{0}' does not exist in '{1}'", ctx.IDENTIFIER().GetText(), data.Name);
                KeyAffinity ka = KeyAffinity.Ascending;
                if (ctx.K_DESC() != null)
                    ka = KeyAffinity.Descending;
                k.Add(idx, ka);

            }

            Stopwatch sw = Stopwatch.StartNew();
            long cost = data.SortC(k);
            sw.Stop();
            double avg = (double)data.RecordCount / (double)data.ExtentCount;
            double nsum = data.ExtentCount * (data.ExtentCount - 1);


            this._enviro.IO.WriteHeader("Sort");
            this._enviro.IO.WriteLine("Rows: {0}", data.RecordCount);
            this._enviro.IO.WriteLine("Expected Cost: {0}", avg * Math.Log(avg) * data.ExtentCount + nsum * avg);
            this._enviro.IO.WriteLine("Actual Cost: {0}", cost);
            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            this._enviro.IO.WriteLine();

            return 1;

        }

        public override int VisitCommand_join(RyeParser.Command_joinContext context)
        {
            
            // Get each table //
            DataSet DLeft = CompilerHelper.CallData(this._enviro, context.table_name()[0]);
            DataSet DRight = CompilerHelper.CallData(this._enviro, context.table_name()[1]);

            // Get the aliases //
            string ALeft = context.IDENTIFIER()[0].GetText();
            string ARight = context.IDENTIFIER()[1].GetText();

            // Get the thread count //
            int Threads = CompilerHelper.GetThreadCount(context.thread_clause());
            Threads = Math.Min(Threads, (int)DLeft.ExtentCount);

            // Create an algorithm //
            JoinAlgorithm Engine = new NestedLoop();
            if (context.join_on_unit() != null)
                Engine = new SortMerge();

            // Get the join type //
            JoinType t = this.RenderJoinType(context.join_type());

            // Start rendering the nodes //
            List<JoinProcessNode> Nodes = new List<JoinProcessNode>();
            for (int i = 0; i < Threads; i++)
            {

                // Create a record comparer //
                RecordComparer rc = this.RenderJoinRecordComparer(DLeft.Columns, DRight.Columns, ALeft, ARight, context.join_on_unit());

                // Create out registers //
                Register MemLeft = new Register(ALeft, DLeft.Columns);
                Register MemRight = new Register(ARight, DRight.Columns);

                // Create the expression visitors we'll need //
                ExpressionVisitor exp = new ExpressionVisitor(this._enviro);
                exp.AddRegister(ALeft, MemLeft);
                exp.AddRegister(ARight, MemRight);

                // Render the filter //
                Filter F = CompilerHelper.GetWhere(exp, context.where_clause());

                // Get the expression we'll return //
                ExpressionCollection select = exp.ToNodes(context.append_method().expression_or_wildcard_set());

                // Get the output table //
                DataSet OutSet = CompilerHelper.RenderData(this._enviro, select, context.append_method());
                RecordWriter w = OutSet.OpenWriter();

                // Create the new join node //
                JoinProcessNode node = new JoinProcessNode(i, Engine, t, DLeft.CreateVolume(i, Threads), MemLeft, DRight.CreateVolume(), MemRight, rc, F, select, w);

                // Add the node to collection //
                Nodes.Add(node);

            }

            // Create the process //
            JoinConsolidation reducer = new JoinConsolidation();
            QueryProcess<JoinProcessNode> process = new QueryProcess<JoinProcessNode>(Nodes, reducer);

            // Run the process //
            Stopwatch sw = Stopwatch.StartNew();
            if (this._enviro.AllowAsync && Threads > 1)
            {
                process.ExecuteAsync();
            }
            else
            {
                process.Execute();
            }
            sw.Stop();

            this._enviro.IO.WriteHeader("Join");
            this._enviro.IO.WriteLine("Actual Join Cost: {0}", reducer.Clicks);
            this._enviro.IO.WriteLine("Join Type: {0} : {1}", Engine.BaseJoinAlgorithmType, t);
            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            this._enviro.IO.WriteLine();

            return 1;

        }

        public override int VisitCommand_debug(RyeParser.Command_debugContext context)
        {

            // Get the data //
            DataSet data = CompilerHelper.CallData(this._enviro, context.table_name());
           
            // Get the string //
            ExpressionVisitor exp = new ExpressionVisitor(this._enviro);
            string path = exp.ToNode(context.expression()).Evaluate().valueSTRING;

            // Dump //
            Kernel.TextDump(data, path,',');

            return 1;

        }

        private RecordComparer RenderJoinRecordComparer(Schema SLeft, Schema SRight, string ALeft, string ARight, RyeParser.Join_on_unitContext[] Predicates)
        {

            Key KLeft = new Key();
            Key KRight = new Key();

            for (int i = 0; i < Predicates.Length; i++ )
            {

                string AL = Predicates[i].IDENTIFIER()[0].GetText();
                string CL = Predicates[i].IDENTIFIER()[1].GetText();
                string AR = Predicates[i].IDENTIFIER()[2].GetText();
                string CR = Predicates[i].IDENTIFIER()[3].GetText();

                if (AL == ALeft && AR == ARight)
                {

                    int a = SLeft.ColumnIndex(CL);
                    int b = SRight.ColumnIndex(CR);
                    if (a == -1)
                        throw new RyeCompileException("Column '{0}' does not exist in '{1}'", CL, AL);
                    if (b == -1)
                        throw new RyeCompileException("Column '{0}' does not exist in '{1}'", CR, AR);
                    KLeft.Add(a);
                    KRight.Add(b);

                }
                else if (AL == ARight && AR == ALeft)
                {

                    int a = SLeft.ColumnIndex(CR);
                    int b = SRight.ColumnIndex(CL);
                    if (a == -1)
                        throw new RyeCompileException("Column '{0}' does not exist in '{1}'", CR, AR);
                    if (b == -1)
                        throw new RyeCompileException("Column '{0}' does not exist in '{1}'", CL, AL);
                    KLeft.Add(a);
                    KRight.Add(b);

                }
                else
                {
                    throw new RyeCompileException("Cannot comprehend {0} and/or {1} as table aliases", AL, AR);
                }

            }

            return new RecordComparer(KLeft, KRight);

        }

        private JoinType RenderJoinType(RyeParser.Join_typeContext context)
        {

            if (context == null)
                return JoinType.Inner;

            if (context.K_ANTI() == null)
            {
                if (context.K_INNER() != null)
                    return JoinType.Inner;
                if (context.K_LEFT() != null)
                    return JoinType.Left;
                if (context.K_RIGHT() != null)
                    return JoinType.Right;
                if (context.K_FULL() != null)
                    return JoinType.Full;
                if (context.K_CROSS() != null)
                    return JoinType.Cross;
            }
            else
            {
                if (context.K_INNER() != null)
                    return JoinType.AntiInner;
                if (context.K_LEFT() != null)
                    return JoinType.AntiLeft;
                if (context.K_RIGHT() != null)
                    return JoinType.AntiRight;
            }

            throw new RyeCompileException("Invalid join type '{0}'", context.GetText());

        }

    }

}
