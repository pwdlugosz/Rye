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

        private Session _enviro;

        public CommandVisitor(Session Enviro)
            : base()
        {
            this._enviro = Enviro;
        }

        // Action //
        public override int VisitCommand_method(RyeParser.Command_methodContext context)
        {

            // Notifiy //
            this._enviro.IO.WriteHeader("Action");

            ExpressionVisitor exp_vis = new ExpressionVisitor(this._enviro);
            MatrixExpressionVisitor mat_vis = new MatrixExpressionVisitor(exp_vis, this._enviro);
            MethodVisitor met_vis = new MethodVisitor(exp_vis, mat_vis, this._enviro);
            Method m = met_vis.Visit(context.method());
            Stopwatch sw = Stopwatch.StartNew();
            m.BeginInvoke();
            m.Invoke();
            m.EndInvoke();
            sw.Stop();

            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            //this._enviro.IO.WriteLine(m.Message());
            this._enviro.IO.WriteLine();


            return 1;

        }

        // Connect / Disconnect //
        public override int VisitCommand_connect(RyeParser.Command_connectContext context)
        {

            // Notifiy //
            this._enviro.IO.WriteHeader("Connect");
            ExpressionVisitor exp = new ExpressionVisitor(this._enviro);
            StringBuilder sb = new StringBuilder();
            foreach (RyeParser.Connect_unitContext ctx in context.connect_unit())
            {

                string alias = ctx.IDENTIFIER().GetText();
                string path = exp.Visit(ctx.expression()).Evaluate().valueSTRING;
                
                if (!System.IO.Directory.Exists(path))
                {
                    throw new RyeCompileException("Directory does not exist: \n\t{0}", path);
                }
                this._enviro.SetConnection(alias, path);
                sb.AppendLine(string.Format("\t{0} : {1}", alias, path));
            }
            
            this._enviro.IO.WriteLine(sb.ToString());
            this._enviro.IO.WriteLine();

            return 1;

        }

        public override int VisitCommand_disconnect(RyeParser.Command_disconnectContext context)
        {

            // Notifiy //
            this._enviro.IO.WriteHeader("Disconnect");
            
            StringBuilder sb = new StringBuilder();
            foreach (Antlr4.Runtime.Tree.ITerminalNode t in context.IDENTIFIER())
            {
                //this._enviro.Connections.Deallocate(t.GetText());
                sb.AppendLine("\t" + t.GetText());
            }

            this._enviro.IO.WriteLine(sb.ToString());
            this._enviro.IO.WriteLine();

            return 1;

        }

        // Declare //
        public override int VisitCommand_declare(RyeParser.Command_declareContext context)
        {

            // Notifiy //
            this._enviro.IO.WriteHeader("Declare");
            StringBuilder sb = new StringBuilder();

            // Create visitor //
            ExpressionVisitor exp = new ExpressionVisitor(this._enviro);

            // Render the scalar variables //
            foreach (RyeParser.Unit_declare_scalarContext ctx1 in context.unit_declare_scalar())
            {

                CompilerHelper.AppendScalar(this._enviro, this._enviro.Scalars, exp, ctx1);
                sb.AppendLine("\t" + ctx1.IDENTIFIER().GetText() + " AS " + ctx1.type().GetText());

            }

            // Render each matrix //
            foreach (RyeParser.Unit_declare_matrixContext ctx2 in context.unit_declare_matrix())
            {

                CompilerHelper.AppendMatrix(this._enviro, this._enviro.Matrixes, exp, ctx2);
                sb.AppendLine("\t" + ctx2.IDENTIFIER().GetText() + "[] AS " + ctx2.type().GetText());

            }

            // Render the lambdas //
            foreach (RyeParser.Unit_declare_lambdaContext ctx3 in context.unit_declare_lambda())
            {

                string sname = ctx3.lambda_name()[0].IDENTIFIER().GetText();
                CompilerHelper.AppendLambda(this._enviro, ctx3);
                sb.AppendLine("\t" + ctx3.lambda_name()[0].GetText());

            }

            this._enviro.IO.WriteLine(sb.ToString());
            this._enviro.IO.WriteLine();
            return 1;

        }

        // Create //
        public override int VisitCommand_create(RyeParser.Command_createContext context)
        {

            // Notifiy //
            this._enviro.IO.WriteHeader("Create");
            
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
            long page_size = Extent.DEFAULT_PAGE_SIZE;
            if (context.LITERAL_INT() != null)
                page_size = long.Parse(context.LITERAL_INT().GetText());
            
            // Get the names //
            string sname = context.table_name().IDENTIFIER()[0].GetText();
            string tname = context.table_name().IDENTIFIER()[1].GetText();
            
            // Check we need to build this //
            bool Table = false;
            if (this._enviro.ConnectionExists(sname))
            {
                Table t = this._enviro.CreateTable(sname, tname, cols, (int)page_size);
                Table = true;
            }
            else if (this._enviro.IsGlobal(sname))
            {
                Extent e = this._enviro.CreateExtent(tname, cols, (int)page_size);
            }
            else
            {
                throw new RyeCompileException("Structure or connection with alias '{0}' does not exist", sname);
            }

            this._enviro.IO.WriteLine("{0} table '{1}' created in '{2}'", (Table ? "Disk" : "Value"), tname, sname);
            this._enviro.IO.WriteLine();

            return 1;

        }

        // Select / Read //
        public override int VisitCommand_read(RyeParser.Command_readContext context)
        {

            // Notifiy //
            this._enviro.IO.WriteHeader("Select");
            
            // Get some high level data first, such as thread count, 'where' clause, and source data //
            TabularData data = CompilerHelper.CallData(this._enviro, context.base_clause().table_name());
            int threads = CompilerHelper.GetThreadCount(context.base_clause().thread_clause());
            List<SelectProcessNode> nodes = new List<SelectProcessNode>();
            string alias = (context.base_clause().IDENTIFIER() == null ? data.Header.Name : context.base_clause().IDENTIFIER().GetText());

            // Render each node //
            for (int i = 0; i < threads; i++)
            {

                // Create an expression visitor //
                ExpressionVisitor exp = new ExpressionVisitor(this._enviro);

                // Create the local heap //
                Heap<Cell> lscalars = new Heap<Cell>();
                Heap<CellMatrix> lmatrixes = new Heap<CellMatrix>();
                lscalars.Allocate(SelectProcessNode.ROW_ID, Cell.ZeroValue(CellAffinity.INT));
                lscalars.Allocate(SelectProcessNode.EXTENT_ID, Cell.ZeroValue(CellAffinity.INT));
                lscalars.Allocate(SelectProcessNode.KEY_CHANGE, Cell.FALSE);
                lscalars.Allocate(SelectProcessNode.IS_FIRST, Cell.FALSE);
                lscalars.Allocate(SelectProcessNode.IS_LAST, Cell.FALSE);

                // Create a register //
                Register reg = new Register(alias, data.Columns);

                // Accumulate the heap and register to the expression visitor //
                exp.AddRegister(alias, reg);
                exp.SetSecondary("LOCAL", lscalars, lmatrixes);

                // Process the 'by' clause //
                ExpressionCollection by_clause = (context.by_clause() == null ? null : exp.ToNodes(context.by_clause().expression_or_wildcard_set()));

                // Create a matrix visitor //
                MatrixExpressionVisitor mat = new MatrixExpressionVisitor(exp, this._enviro);
                
                // Create a method visitor //
                MethodVisitor met = new MethodVisitor(exp, mat, this._enviro);
                
                // Load the local heap //
                if (context.command_declare() != null)
                {

                    foreach (RyeParser.Unit_declare_scalarContext ctx in context.command_declare().unit_declare_scalar())
                    {
                        string sname = ctx.IDENTIFIER().GetText();
                        CompilerHelper.AppendScalar(this._enviro, lscalars, exp, ctx);
                    }
                    foreach (RyeParser.Unit_declare_matrixContext ctx in context.command_declare().unit_declare_matrix())
                    {
                        string sname = ctx.IDENTIFIER().GetText();
                        CompilerHelper.AppendMatrix(this._enviro, lmatrixes, exp, ctx);
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
                SelectProcessNode node = new SelectProcessNode(i, this._enviro, data.CreateVolume(i, threads), reg, lscalars, lmatrixes, map, reduce, where, by_clause);
                nodes.Add(node);

            }

            // Render the reducer //
            SelectProcessConsolidation reducer = new SelectProcessConsolidation(this._enviro);

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
            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            this._enviro.IO.WriteLine();

            return 1;

        }

        // Update //
        public override int VisitCommand_update(RyeParser.Command_updateContext context)
        {

            // Notifiy //
            this._enviro.IO.WriteHeader("Update");
            
            // Get the source data //
            TabularData data = CompilerHelper.CallData(this._enviro, context.base_clause().table_name());
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
                    nodes.Add(new UpdateProcessNode(0, this._enviro, null, data.CreateVolume(i, threads), key, values, where, mem));
                }
                else
                {
                    nodes.Add(new UpdateProcessNode(i, this._enviro, data as Table, data.CreateVolume(i, threads), key, values, where, mem));
                }

            }

            QueryProcess<UpdateProcessNode> tprocess = new QueryProcess<UpdateProcessNode>(nodes, new UpdateProcessConsolidation(this._enviro));
            Stopwatch sw = Stopwatch.StartNew();
            if (this._enviro.AllowAsync && threads > 1)
                tprocess.ExecuteAsync();
            else
                tprocess.Execute();
            sw.Stop();

            // Close the output //
            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            this._enviro.IO.WriteLine();

            return 1;

        }

        // Delete //
        public override int VisitCommand_delete(RyeParser.Command_deleteContext context)
        {

            // Notifiy //
            this._enviro.IO.WriteHeader("Delete");
            
            // Get the source data //
            TabularData data = CompilerHelper.CallData(this._enviro, context.base_clause().table_name());
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
                    nodes.Add(new DeleteProcessNode(0, this._enviro, null, data.CreateVolume(i, threads), where, mem));
                }
                else
                {
                    nodes.Add(new DeleteProcessNode(i, this._enviro, data as Table, data.CreateVolume(i, threads), where, mem));
                }

            }

            // Build the consolidator //
            DeleteProcessConsolidation reducer = new DeleteProcessConsolidation(this._enviro);

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
            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            this._enviro.IO.WriteLine();

            return 1;
            
        }

        // Aggregates //
        public override int VisitCommand_aggregate(RyeParser.Command_aggregateContext context)
        {

            /*
             * Check the hints:
             * HINT = 'SORT' => ordered aggregate
             * HINT = 'HASH' => has table aggregate
             * 
             */
            Cell hint = CompilerHelper.GetHint(this._enviro, context.base_clause());
            if (hint.valueSTRING.ToUpper() == "SORT")
            {
                return this.AggregateOrdered(context);
            }
            else if (hint.valueSTRING.ToUpper() == "HASH")
            {
                return this.AggregateHashTable(context);
            }

            // Otherwise, lets optimize //
            TabularData data = CompilerHelper.CallData(this._enviro, context.base_clause().table_name());
            int threads = CompilerHelper.GetThreadCount(context.base_clause().thread_clause());
            string alias = (context.base_clause().IDENTIFIER() == null ? data.Header.Name : context.base_clause().IDENTIFIER().GetText());

            // Construct the expression visitor and memory register //
            ExpressionVisitor in_exp = new ExpressionVisitor(this._enviro);
            Register in_mem = new Register(alias, data.Columns);
            in_exp.AddRegister(alias, in_mem);

            // Get the keys, the aggregates and the where clause //
            ExpressionCollection keys = in_exp.ToNodes(context.by_clause().expression_or_wildcard_set());
            
            // Check to see if the expression can be reduced to a key //
            Key k = ExpressionCollection.DecompileToKey(keys);

            // If the key is not the same length as the expression collection, then the collection must not be all field refs, so use a hash table //
            if (k.Count != keys.Count)
            {
                return this.AggregateHashTable(context);
            }

            // Check if the data set is sorted by the key, if it is, then use the ordered set algorithm //
            if (KeyComparer.IsWeakSubset(data.SortBy ?? new Key(), k))
            {
                return this.AggregateOrdered(context);
            }

            // Otherwise, use the hash table algorithm //
            return this.AggregateHashTable(context);

        }

        private int AggregateHashTable(RyeParser.Command_aggregateContext context)
        {

            // Notify //
            this._enviro.IO.WriteHeader("Aggregate - Hash ShartTable");

            // Get some high level data first, such as thread count, 'where' clause, and source data //
            TabularData data = CompilerHelper.CallData(this._enviro, context.base_clause().table_name());
            int threads = CompilerHelper.GetThreadCount(context.base_clause().thread_clause());
            List<AggregateHashTableProcessNode> nodes = new List<AggregateHashTableProcessNode>();
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

                AggregateHashTableProcessNode n = new AggregateHashTableProcessNode(i, this._enviro, data.CreateVolume(i, threads), keys, aggs, where, in_mem, null);
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

            // Create the sink table //
            Table sink = KeyValueSet.DataSink(this._enviro, FinalKey, FinalValue);
            foreach (AggregateHashTableProcessNode node in nodes)
            {
                node.Sink = sink;
            }

            // Render the record writer that will be used to fill the output //
            TabularData out_data = CompilerHelper.RenderData(this._enviro, out_keys, context.append_method());
            RecordWriter out_writer = out_data.OpenWriter();

            // Create the consolidation process //
            AggregateHashTableConsolidationProcess reducer = new AggregateHashTableConsolidationProcess(this._enviro, FinalKey, FinalValue, out_writer, out_keys, out_mem, sink);

            // Build the query process that will handle this //
            QueryProcess<AggregateHashTableProcessNode> process = new QueryProcess<AggregateHashTableProcessNode>(nodes, reducer);

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
            this._enviro.IO.WriteLine("Actual Aggregate Cost: {0}", reducer.Clicks);
            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            this._enviro.IO.WriteLine();

            return 1;

        }
        
        private int AggregateOrdered(RyeParser.Command_aggregateContext context)
        {

            // Notify //
            this._enviro.IO.WriteHeader("Aggregate - Order");

            // Get some high level data first, such as thread count, 'where' clause, and source data //
            TabularData data = CompilerHelper.CallData(this._enviro, context.base_clause().table_name());
            int threads = CompilerHelper.GetThreadCount(context.base_clause().thread_clause());
            List<AggregateOrderedProcessNode> nodes = new List<AggregateOrderedProcessNode>();
            string alias = (context.base_clause().IDENTIFIER() == null ? data.Header.Name : context.base_clause().IDENTIFIER().GetText());

            // Create all the aggregate process nodes //
            TabularData out_data = this.RenderAggregateDestination(context);
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

                // Create the output expression visitor //
                Schema out_columns = Schema.Join(keys.Columns, aggs.Columns);
                ExpressionVisitor out_exp = new ExpressionVisitor(this._enviro);
                Register out_mem = new Register("OUT", out_columns);
                out_exp.AddRegister("OUT", out_mem); // TODO, think of a better alias to use
                ExpressionCollection out_keys = out_exp.ToNodes(context.append_method().expression_or_wildcard_set());
                
                // Render the record writer that will be used to fill the output //
                RecordWriter out_writer = out_data.OpenWriter();

                // Render the node //
                AggregateOrderedProcessNode n = new AggregateOrderedProcessNode(i, this._enviro, data.CreateVolume(i, threads), keys, aggs, where, in_mem, out_keys, out_mem, out_writer);
                nodes.Add(n);

            }

            // Create the consolidation process //
            AggregateOrderedConsolidationProcess reducer = new AggregateOrderedConsolidationProcess(this._enviro);

            // Build the query process that will handle this //
            QueryProcess<AggregateOrderedProcessNode> process = new QueryProcess<AggregateOrderedProcessNode>(nodes, reducer);

            // Run the process //
            Stopwatch sw = Stopwatch.StartNew();

            // Sort the dataset //
            PreProcessor p = this.RenderAggregatePreProcessor(context, data, alias);
            process.AddPreProcessor(p);

            // Run the gruoper //
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
            if (p.Clicks == 0)
                this._enviro.IO.WriteLine("Aggregate operation optimized using naturally sorted data");
            this._enviro.IO.WriteLine("Actual Aggregate Cost: {0}", reducer.Clicks + p.Clicks);
            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            this._enviro.IO.WriteLine();

            return 1;

        }

        private TabularData RenderAggregateDestination(RyeParser.Command_aggregateContext context)
        {

            // Get some high level data first, such as thread count, 'where' clause, and source data //
            TabularData data = CompilerHelper.CallData(this._enviro, context.base_clause().table_name());
            string alias = (context.base_clause().IDENTIFIER() == null ? data.Header.Name : context.base_clause().IDENTIFIER().GetText());

            // Construct the expression visitor and memory register used in the aggregation process //
            ExpressionVisitor in_exp = new ExpressionVisitor(this._enviro);
            Register in_mem = new Register(alias, data.Columns);
            in_exp.AddRegister(alias, in_mem);

            // Get the keys, the aggregates and the where clause //
            ExpressionCollection keys = in_exp.ToNodes(context.by_clause().expression_or_wildcard_set());
            AggregateCollection aggs = in_exp.ToReducers(context.over_clause().beta_reduction_list());
           
            // Create the output expression visitor //
            Schema out_columns = Schema.Join(keys.Columns, aggs.Columns);
            ExpressionVisitor out_exp = new ExpressionVisitor(this._enviro);
            Register out_mem = new Register("OUT", out_columns);
            out_exp.AddRegister("OUT", out_mem); // TODO, think of a better alias to use
            ExpressionCollection out_keys = out_exp.ToNodes(context.append_method().expression_or_wildcard_set());

            // Render the record writer that will be used to fill the output //
            return CompilerHelper.RenderData(this._enviro, out_keys, context.append_method());

        }

        private PreProcessor RenderAggregatePreProcessor(RyeParser.Command_aggregateContext context, TabularData Data, string Alias)
        {

            // Construct the expression visitor and memory register used in the aggregation process //
            ExpressionVisitor in_exp = new ExpressionVisitor(this._enviro);
            Register in_mem = new Register(Alias, Data.Columns);
            in_exp.AddRegister(Alias, in_mem);

            // Generate the expression collection //
            ExpressionCollection keys = in_exp.ToNodes(context.by_clause().expression_or_wildcard_set());

            // Now try to decompile the keys //
            //Key k = ExpressionCollection.DecompileToKey(keys);
            //if (k.Count == keys.Count)
            //{
            //    return new SortPreProcessor(Data, k);
            //}

            return new SortPreProcessor(Data, keys, in_mem);

        }

        // Sort //
        public override int VisitCommand_sort(RyeParser.Command_sortContext context)
        {

            // Notify //
            this._enviro.IO.WriteHeader("Sort");
            
            // Get the data //
            TabularData data = CompilerHelper.CallData(this._enviro, context.table_name());
            string alias = (context.K_AS() == null ? data.Header.Name : context.IDENTIFIER().GetText());
            
            // Create a visitor / register //
            Register r = new Register(data.Header.Name, data.Columns);
            ExpressionVisitor exp = new ExpressionVisitor(this._enviro);
            exp.AddRegister(alias, r);

            // Build the sort key //
            Key k = new Key();
            ExpressionCollection cols = new ExpressionCollection();
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

            // Try to see if we can decomile to a raw key based sort, which should be faster //
            Key TryDecompile = ExpressionCollection.DecompileToKey(cols);
            bool CanOptimize = false;
            if (TryDecompile.Count == cols.Count)
            {

                for (int i = 0; i < TryDecompile.Count; i++)
                {
                    TryDecompile.SetAffinity(i, k.Affinity(i));
                }
                CanOptimize = true;

            }

            Stopwatch sw = Stopwatch.StartNew();
            long cost = 0;
            if (data.Header.Affinity == HeaderType.Extent && !CanOptimize)
            {
                 cost = SortMaster.Sort(data as Extent, cols, r, k);
            }
            else if (!CanOptimize && data.Header.Affinity == HeaderType.Table)
            {
                cost = SortMaster.Sort(data as Table, cols, r, k);
            }
            else if (CanOptimize && data.Header.Affinity == HeaderType.Extent)
            {
                this._enviro.IO.WriteLine("Sort optimized to to use field keys");
                cost = SortMaster.Sort(data as Extent, TryDecompile);
            }
            else if (CanOptimize && data.Header.Affinity == HeaderType.Table)
            {
                this._enviro.IO.WriteLine("Sort optimized to to use field keys");
                cost = SortMaster.Sort(data as Table, TryDecompile);
            }
            sw.Stop();
            double avg = (double)data.RecordCount / (double)data.ExtentCount;
            double nsum = data.ExtentCount * (data.ExtentCount - 1);

            this._enviro.IO.WriteLine("Rows: {0}", data.RecordCount);
            this._enviro.IO.WriteLine("Expected Cost: {0}", avg * Math.Log(avg, 2D) * data.ExtentCount + nsum * avg);
            this._enviro.IO.WriteLine("Actual Cost: {0}", cost);
            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            this._enviro.IO.WriteLine();

            return 1;

        }

        // Join //
        public override int VisitCommand_join(RyeParser.Command_joinContext context)
        {

            // Notify //
            this._enviro.IO.WriteHeader("Join");
                
            // Get each table //
            TabularData DLeft = CompilerHelper.CallData(this._enviro, context.table_name()[0]);
            TabularData DRight = CompilerHelper.CallData(this._enviro, context.table_name()[1]);

            // Get the aliases //
            string ALeft = context.IDENTIFIER()[0].GetText();
            string ARight = context.IDENTIFIER()[1].GetText();

            // Get the thread count //
            int Threads = CompilerHelper.GetThreadCount(context.thread_clause());
            Threads = Math.Min(Threads, (int)DLeft.ExtentCount);

            // Get the hint //
            Cell hint = CompilerHelper.GetHint(this._enviro, context);

            // Create an algorithm //
            JoinAlgorithm Engine = new SortMerge();
            if (context.join_predicate() == null || hint.valueSTRING.ToUpper() == "LOOP")
            {
                Engine = new NestedLoop();
            }

            // Get the join type //
            JoinType t = this.RenderJoinType(context.join_type());

            // Start rendering the nodes //
            List<JoinProcessNode> Nodes = new List<JoinProcessNode>();
            for (int i = 0; i < Threads; i++)
            {

                // Create a record comparer //
                KeyedRecordComparer rc = this.RenderJoinRecordComparer(DLeft.Columns, DRight.Columns, ALeft, ARight, context.join_predicate());

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
                TabularData OutSet = CompilerHelper.RenderData(this._enviro, select, context.append_method());
                RecordWriter w = OutSet.OpenWriter();

                // Create the new join node //
                JoinProcessNode node = new JoinProcessNode(i, this._enviro, Engine, t, DLeft.CreateVolume(i, Threads), MemLeft, DRight.CreateVolume(), MemRight, rc, F, select, w);

                // Add the node to collection //
                Nodes.Add(node);

            }

            // Create the process //
            JoinConsolidation reducer = new JoinConsolidation(this._enviro);
            QueryProcess<JoinProcessNode> process = new QueryProcess<JoinProcessNode>(Nodes, reducer);

            // Create a record comparer //
            if (Engine.BaseJoinAlgorithmType == JoinAlgorithmType.SortMerge)
            {

                KeyedRecordComparer comp = this.RenderJoinRecordComparer(DLeft.Columns, DRight.Columns, ALeft, ARight, context.join_predicate());

                if (KeyComparer.IsStrongSubset(DLeft.SortBy, comp.LeftKey))
                {
                    this._enviro.IO.WriteLine("Join optimized for '{0}' by using pre-sorted data", DLeft.Header.Name);
                }
                else
                {
                    process.AddPreProcessor(new SortPreProcessor(DLeft, comp.LeftKey));
                }

                if (KeyComparer.IsStrongSubset(DRight.SortBy, comp.RightKey))
                {
                    this._enviro.IO.WriteLine("Join optimized for '{0}' by using pre-sorted data", DRight.Header.Name);
                }
                else
                {
                    process.AddPreProcessor(new SortPreProcessor(DRight, comp.RightKey));
                }
                    
            }

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
            long TrueCost = reducer.ActualCost + process.PreProcessorClicks;

            this._enviro.IO.WriteLine("Join Cost: \n\tActual {0} \n\tEstimated {1}", TrueCost, Engine.Cost(DLeft, DRight, Threads, 1D, JoinImplementationType.Block_VxV));
            this._enviro.IO.WriteLine("IO Calls: {0}", reducer.IOCalls);
            this._enviro.IO.WriteLine("Join Type: {0} : {1}", Engine.BaseJoinAlgorithmType, t);
            this._enviro.IO.WriteLine("Runtime: {0}", sw.Elapsed);
            this._enviro.IO.WriteLine();

            return 1;

        }

        private KeyedRecordComparer RenderJoinRecordComparer(Schema SLeft, Schema SRight, string ALeft, string ARight, RyeParser.Join_predicateContext Predicates)
        {

            Key KLeft = new Key();
            Key KRight = new Key();

            if (Predicates == null)
            {
                return new KeyedRecordComparer(KLeft, KRight);
            }

            for (int i = 0; i < Predicates.join_on_unit().Length; i++)
            {

                string AL = Predicates.join_on_unit()[i].IDENTIFIER()[0].GetText();
                string CL = Predicates.join_on_unit()[i].IDENTIFIER()[1].GetText();
                string AR = Predicates.join_on_unit()[i].IDENTIFIER()[2].GetText();
                string CR = Predicates.join_on_unit()[i].IDENTIFIER()[3].GetText();

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

            return new KeyedRecordComparer(KLeft, KRight);

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
