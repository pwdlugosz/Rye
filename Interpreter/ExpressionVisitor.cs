using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using Rye.Expressions;
using Rye.Data;
using Rye.Structures;
using Rye.Aggregates;

namespace Rye.Interpreter
{

    public class ExpressionVisitor : RyeParserBaseVisitor<Expression>
    {

        const char SLIT1 = '\'';
        const char SLIT2 = '"';
        const char SLIT3 = '$';
        const char NEGATIVE = '~';
        const char DATE_SUFFIX_U = 'T';
        const char DATE_SUFFIX_L = 't';
        const char DOUBLE_SUFFIX_U = 'D';
        const char DOUBLE_SUFFIX_L = 'd';
        const string DEFAULT_ALIAS = "T";
        const string STRING_NEWLINE = "crlf";
        const string STRING_TAB = "tab";

        private Heap<MemoryStructure> _structs;
        private Heap<Register> _registers;
        private Heap<Schema> _columns;
        private Heap2<CellAffinity, int> _pointers;
        
        public ExpressionVisitor()
            : base()
        {
            this._structs = new Heap<MemoryStructure>();
            this._registers = new Heap<Register>();
            this._columns = new Heap<Schema>();
            this._pointers = new Heap2<CellAffinity, int>();
        }

        public ExpressionVisitor(Workspace Space)
            : this()
        {
            foreach (KeyValuePair<string, MemoryStructure> kv in Space.Structures.Entries)
            {
                this._structs.Allocate(kv.Key, kv.Value);
            }
        }

        public void AddStructure(string Alias, MemoryStructure Structure)
        {
            this._structs.Reallocate(Alias, Structure);
        }

        public void AddRegister(string Alias, Register MemoryLocation)
        {
            this._registers.Reallocate(Alias, MemoryLocation);
            this._columns.Reallocate(Alias, MemoryLocation.Columns);
        }

        public void AddPointer(string Alias, CellAffinity Type, int Size)
        {
            this._pointers.Reallocate(Alias, Type, Size);
        }

        public void AddPointer(string Alias, CellAffinity Type)
        {

            int size = 8;
            if (Type == CellAffinity.STRING)
                size = Schema.DEFAULT_STRING_SIZE;
            else if (Type == CellAffinity.BLOB)
                size = Schema.DEFAULT_BLOB_SIZE;
            this.AddPointer(Alias, Type, size);

        }

        public Expression MasterNode
        {
            get;
            private set;
        }

        // Overrides //
        public override Expression VisitPointer(RyeParser.PointerContext context)
        {
            string name = context.IDENTIFIER().GetText();
            CellAffinity type = CompilerHelper.GetAffinity(context.type());
            int size = CompilerHelper.GetSize(context.type()); // pointers can never come from a table
            return new ExpressionPointer(this.MasterNode, name, type, size);
        }

        public override Expression VisitUniary(RyeParser.UniaryContext context)
        {

            Expression t;
            Expression right = this.Visit(context.expression());

            if (context.op.Type == RyeParser.MINUS) // -A
                t = new ExpressionResult(this.MasterNode, new CellUniMinus());
            else if (context.op.Type == RyeParser.PLUS) // +A
                t = new ExpressionResult(this.MasterNode, new CellUniPlus());
            else // !A
                t = new ExpressionResult(this.MasterNode, new CellUniNot());

            // Accumulate the node //
            t.AddChildNode(right);

            this.MasterNode = t;

            return t;

        }

        public override Expression VisitPower(RyeParser.PowerContext context)
        {

            Expression t = new ExpressionResult(this.MasterNode, new CellFuncFVPower());
            Expression left = this.Visit(context.expression()[0]);
            Expression right = this.Visit(context.expression()[1]);
            t.AddChildren(left, right);
            this.MasterNode = t;
            return t;

        }

        public override Expression VisitMultDivMod(RyeParser.MultDivModContext context)
        {

            Expression t;
            Expression left = this.Visit(context.expression()[0]);
            Expression right = this.Visit(context.expression()[1]);

            if (context.op.Type == RyeParser.MUL) // left * right
                t = new ExpressionResult(this.MasterNode, new CellBinMult());
            else if (context.op.Type == RyeParser.DIV) // left / right
                t = new ExpressionResult(this.MasterNode, new CellBinDiv());
            else if (context.op.Type == RyeParser.DIV2) // left /? right
                t = new ExpressionResult(this.MasterNode, new CellBinDiv2());
            else // left % right
                t = new ExpressionResult(this.MasterNode, new CellBinMod());

            // Accumulate the node //
            t.AddChildren(left, right);

            this.MasterNode = t;

            return t;

        }

        public override Expression VisitAddSub(RyeParser.AddSubContext context)
        {

            Expression t;
            Expression left = this.Visit(context.expression()[0]);
            Expression right = this.Visit(context.expression()[1]);

            if (context.op.Type == RyeParser.PLUS) // left + right
                t = new ExpressionResult(this.MasterNode, new CellBinPlus());
            else // left - right
                t = new ExpressionResult(this.MasterNode, new CellBinMinus());

            // Accumulate the node //
            t.AddChildren(left, right);

            this.MasterNode = t;

            return t;

        }

        public override Expression VisitGreaterLesser(RyeParser.GreaterLesserContext context)
        {

            Expression t;
            Expression left = this.Visit(context.expression()[0]);
            Expression right = this.Visit(context.expression()[1]);

            if (context.op.Type == RyeParser.LTE) // left <= right
                t = new ExpressionResult(this.MasterNode, new CellBoolLTE());
            else if (context.op.Type == RyeParser.LT) // left < right
                t = new ExpressionResult(this.MasterNode, new CellBoolLT());
            else if (context.op.Type == RyeParser.GTE) // left >= right
                t = new ExpressionResult(this.MasterNode, new CellBoolGTE());
            else // left > right
                t = new ExpressionResult(this.MasterNode, new CellBoolGT());

            // Accumulate the node //
            t.AddChildren(left, right);

            this.MasterNode = t;

            return t;

        }

        public override Expression VisitEquality(RyeParser.EqualityContext context)
        {

            Expression t;
            Expression left = this.Visit(context.expression()[0]);
            Expression right = this.Visit(context.expression()[1]);

            if (context.op.Type == RyeParser.EQ) // left == right
                t = new ExpressionResult(this.MasterNode, new CellBoolEQ());
            else // left != right
                t = new ExpressionResult(this.MasterNode, new CellBoolNEQ());

            // Accumulate the node //
            t.AddChildren(left, right);

            this.MasterNode = t;

            return t;

        }

        public override Expression VisitIsNull(RyeParser.IsNullContext context)
        {

            Expression t = new ExpressionResult(this.MasterNode, new CellFuncFKIsNull());
            Expression left = this.Visit(context.expression());

            // Accumulate the node //
            t.AddChildren(left);

            this.MasterNode = t;

            return t;

        }

        public override Expression VisitLogicalAnd(RyeParser.LogicalAndContext context)
        {
            Expression t = new ExpressionResult(this.MasterNode, new CellFuncFVAND());
            Expression left = this.Visit(context.expression()[0]);
            Expression right = this.Visit(context.expression()[1]);

            // Accumulate the node //
            t.AddChildren(left, right);

            this.MasterNode = t;

            return t;
        }

        public override Expression VisitLogicalOr(RyeParser.LogicalOrContext context)
        {

            Expression t;
            Expression left = this.Visit(context.expression()[0]);
            Expression right = this.Visit(context.expression()[1]);

            if (context.op.Type == RyeParser.OR) // left OR right
                t = new ExpressionResult(this.MasterNode, new CellFuncFVOR());
            else // left XOR right
                t = new ExpressionResult(this.MasterNode, new CellFuncFVXOR());

            // Accumulate the node //
            t.AddChildren(left, right);

            this.MasterNode = t;

            return t;

        }

        public override Expression VisitCast(RyeParser.CastContext context)
        {

            Expression left = this.Visit(context.expression());
            CellAffinity affinity = CompilerHelper.GetAffinity(context.type());
            int size = CompilerHelper.GetSize(context.type());
            CellFunction f = new CellCast(affinity);
            f.SetSize(size);
            Expression t = new ExpressionResult(this.MasterNode, f);

            // Accumulate the node //
            t.AddChildren(left);
            
            this.MasterNode = t;

            return t;

        }

        public override Expression VisitIfNullOp(RyeParser.IfNullOpContext context)
        {

            Expression t = new ExpressionResult(this.MasterNode, new CellFuncFVIfNull());
            Expression left = this.Visit(context.expression()[0]);
            Expression right = this.Visit(context.expression()[1]);

            // Accumulate the node //
            t.AddChildren(left, right);

            this.MasterNode = t;

            return t;

        }

        public override Expression VisitIfOp(RyeParser.IfOpContext context)
        {

            Expression t = new ExpressionResult(this.MasterNode, new CellFuncIf());
            Expression check = this.Visit(context.expression()[0]);
            Expression iftrue = this.Visit(context.expression()[1]);
            Expression iffalse = (context.ELSE_OP() != null) ? this.Visit(context.expression()[2]) : new ExpressionValue(t, new Cell(iftrue.ReturnAffinity()));

            // Accumulate the node //
            t.AddChildren(check, iftrue, iffalse);

            this.MasterNode = t;

            return t;

        }

        //public override Expression VisitCaseOp(RyeParser.CaseOpContext context)
        //{

        //    List<Expression> when_nodes = new List<Expression>();
        //    List<Expression> then_nodes = new List<Expression>();
        //    Expression else_node = null;

        //    int when_then_count = context.K_WHEN().Length * 2;
        //    for (int i = 0; i < when_then_count; i += 2)
        //    {

        //        int when_idx = i;
        //        int then_idx = when_idx + 1;

        //        Expression when_node = this.Visit(context.expression()[when_idx]);
        //        Expression then_node = this.Visit(context.expression()[then_idx]);

        //        when_nodes.Add(when_node);
        //        then_nodes.Add(then_node);

        //    }

        //    // Check for the else //
        //    if (context.K_ELSE() != null)
        //        else_node = this.Visit(context.expression().Last());

        //    // Build the case statement //
        //    CellFuncCase func = new CellFuncCase(when_nodes, then_nodes, else_node);

        //    return new ExpressionResult(this.MasterNode, func);

        //}

        public override Expression VisitParens(RyeParser.ParensContext context)
        {
            return this.Visit(context.expression());
        }

        public override Expression VisitSystemFunction(RyeParser.SystemFunctionContext context)
        {

            // Get the function //
            string func_name = context.system_function().IDENTIFIER().GetText();

            // Lookup the function //
            if (!this._structs[GlobalStructure.DEFAULT_NAME].Functions.Exists(func_name))
                throw new RyeCompileException("Function '{0}' does not exist", func_name);
            CellFunction func_ref = this._structs[GlobalStructure.DEFAULT_NAME].Functions.RenderFunction(func_name);

            // Check the variable count //
            if (func_ref.ParamCount != -1 && func_ref.ParamCount != context.expression().Length)
                throw new RyeCompileException("Function '{0}' expects {1} parameters but was passed {2} parameters", func_name, func_ref.ParamCount, context.expression().Length);

            // Create the node //
            Expression t = new ExpressionResult(this.MasterNode, func_ref);
            
            // Get all the paramters //
            foreach (RyeParser.ExpressionContext ctx in context.expression())
            {
                Expression node = this.Visit(ctx);
                t.AddChildNode(node);
            }

            this.MasterNode = t;

            return t;

        }

        public override Expression VisitStructureFunction(RyeParser.StructureFunctionContext context)
        {

            // Get the function //
            string sname = context.structure_function().IDENTIFIER()[0].GetText();
            string func_name = context.structure_function().IDENTIFIER()[1].GetText();

            // Lookup the structure //
            if (!this._structs.Exists(sname))
                throw new RyeCompileException("Structure '{0}' does not exist", sname);
            MemoryStructure s = this._structs[sname];

            // Lookup the function //
            Expression t = null;
            if (s.Functions.Exists(func_name))
            {
                t = this.VisitSF_Native(context);
            }
            else if (s.Lambda.Exists(func_name))
            {
                t = this.VisitSF_Lambda(context);
            }
            else
            {
                throw new RyeCompileException("Function or lambda '{0}' does not exist in '{1}'", func_name, sname);
            }

            this.MasterNode = t;

            return t;

        }

        private Expression VisitSF_Native(RyeParser.StructureFunctionContext context)
        {

            // At this point, the compiler has verified the function and structure exist //

            // Get the function //
            string sname = context.structure_function().IDENTIFIER()[0].GetText();
            string func_name = context.structure_function().IDENTIFIER()[1].GetText();

            // Lookup the structure //
            MemoryStructure s = this._structs[sname];

            // Lookup the function //
            CellFunction func_ref = s.Functions.RenderFunction(func_name);

            // Check the variable count //
            if (func_ref.ParamCount != -1 && func_ref.ParamCount != context.expression().Length)
                throw new RyeCompileException("Function '{0}' expects {1} parameters but was passed {2} parameters", func_name, func_ref.ParamCount, context.expression().Length);

            // Create the node //
            Expression t = new ExpressionResult(this.MasterNode, func_ref);

            // Get all the paramters //
            foreach (RyeParser.ExpressionContext ctx in context.expression())
            {
                Expression node = this.Visit(ctx);
                t.AddChildNode(node);
            }

            return t;

        }

        private Expression VisitSF_Lambda(RyeParser.StructureFunctionContext context)
        {

            // At this point, the compiler has verified the lambda and structure exist //

            // Get the function //
            string sname = context.structure_function().IDENTIFIER()[0].GetText();
            string func_name = context.structure_function().IDENTIFIER()[1].GetText();

            // Lookup the structure //
            MemoryStructure s = this._structs[sname];

            // Lookup the function //
            Lambda fx = s.Lambda[func_name];
            
            // Check the variable count //
            if (fx.Count != context.expression().Length)
                throw new RyeCompileException("Lambda '{0}' expects {1} parameters but was passed {2} parameters", func_name, fx.Count, context.expression().Length);

            // Get all the paramters //
            List<Expression> ptrs = new List<Expression>();
            foreach (RyeParser.ExpressionContext ctx in context.expression())
            {
                Expression node = this.Visit(ctx);
                ptrs.Add(node);
            }

            // lambda -> expression //
            Expression t = fx.Bind(ptrs);
            t.ParentNode = this.MasterNode;

            return t;

        }

        public override Expression VisitMatrix2D(RyeParser.Matrix2DContext context)
        {

            // Get the matrix //
            string sname = context.IDENTIFIER()[0].GetText();
            string mname = context.IDENTIFIER()[1].GetText();

            Expression row = this.Visit(context.expression()[0]);
            Expression col = this.Visit(context.expression()[1]);

            if (this._structs.Exists(sname))
            {

                if (this._structs[sname].Matricies.Exists(mname))
                {
                    return new ExpressionArrayDynamicRef(this.MasterNode, row, col, this._structs[sname], mname);
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

        public override Expression VisitMatrix1D(RyeParser.Matrix1DContext context)
        {

            // Get the matrix //
            string sname = context.IDENTIFIER()[0].GetText();
            string mname = context.IDENTIFIER()[1].GetText();

            Expression row = this.Visit(context.expression());
            Expression col = new ExpressionValue(null, Cell.ZeroValue(CellAffinity.INT));

            if (this._structs.Exists(sname))
            {

                if (this._structs[sname].Matricies.Exists(mname))
                {
                    return new ExpressionArrayDynamicRef(this.MasterNode, row, col, this._structs[sname], mname);
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

        public override Expression VisitMatrix2DNaked(RyeParser.Matrix2DNakedContext context)
        {

            // Get the matrix //
            string mname = context.IDENTIFIER().GetText();

            Expression row = this.Visit(context.expression()[0]);
            Expression col = this.Visit(context.expression()[1]);

            foreach (MemoryStructure x in this._structs.Values)
            {

                if (x.Matricies.Exists(mname))
                {

                    return new ExpressionArrayDynamicRef(this.MasterNode, row, col, x, mname);

                }

            }

            throw new RyeCompileException("Can't find matrix '{0}'", mname);

        }

        public override Expression VisitMatrix1DNaked(RyeParser.Matrix1DNakedContext context)
        {

            // Get the matrix //
            string mname = context.IDENTIFIER().GetText();

            Expression row = this.Visit(context.expression());
            Expression col = new ExpressionValue(null, Cell.ZeroValue(CellAffinity.INT));

            foreach (MemoryStructure x in this._structs.Values)
            {

                if (x.Matricies.Exists(mname))
                {

                    return new ExpressionArrayDynamicRef(this.MasterNode, row, col, x, mname);

                }

            }

            throw new RyeCompileException("Can't find matrix '{0}'", mname);

        }

        public override Expression VisitCellLiteralBool(RyeParser.CellLiteralBoolContext context)
        {

            // TRUE (any case)
            // FALSE (any case)
            bool value = bool.Parse(context.LITERAL_BOOL().GetText());
            Cell c = new Cell(value);

            return new ExpressionValue(this.MasterNode, c);

        }

        public override Expression VisitCellLiteralInt(RyeParser.CellLiteralIntContext context)
        {

            // ~12345 //
            string t = context.LITERAL_INT().GetText();
            bool negative = false;
            if (t[0] == NEGATIVE)
            {
                t = t.Substring(1, t.Length - 1);
                negative = true;
            }
            long value = long.Parse(t);
            if (negative)
                value = -value;
            Cell c = new Cell(value);
            return new ExpressionValue(this.MasterNode, c);

        }

        public override Expression VisitCellLiteralDouble(RyeParser.CellLiteralDoubleContext context)
        {

            // ~12345.6789//
            string t = context.LITERAL_DOUBLE().GetText();
            if (t.Last() == DOUBLE_SUFFIX_U)
                t = t.Substring(0, t.Length - 1);
            if (t.Last() == DOUBLE_SUFFIX_L)
                t = t.Substring(0, t.Length - 1);
            bool negative = false;
            if (t[0] == NEGATIVE)
            {
                t = t.Substring(1, t.Length - 1);
                negative = true;
            }
            double value = double.Parse(t);
            if (negative)
                value = -value;
            Cell c = new Cell(value);
            return new ExpressionValue(this.MasterNode, c);

        }

        public override Expression VisitCellLiteralDate(RyeParser.CellLiteralDateContext context)
        {

            // '2015-01-01'T -> '2015-01-01' //
            string t = context.LITERAL_DATE().GetText();
            t = t.Substring(0, t.Length - 1);
            Cell c = Cell.DateParse(t);
            return new ExpressionValue(this.MasterNode, c);

        }

        public override Expression VisitCellLiteralString(RyeParser.CellLiteralStringContext context)
        {

            Cell c = new Cell(CleanString(context.LITERAL_STRING().GetText()));
            return new ExpressionValue(this.MasterNode, c);

        }

        public override Expression VisitCellLiteralBLOB(RyeParser.CellLiteralBLOBContext context)
        {

            Cell c = Cell.ByteParse(context.LITERAL_BLOB().GetText());
            return new ExpressionValue(this.MasterNode, c);

        }

        public override Expression VisitCellNull(RyeParser.CellNullContext context)
        {
            return new ExpressionValue(this.MasterNode, Cell.NULL_INT);
        }

        public override Expression VisitVariableNaked(RyeParser.VariableNakedContext context)
        {
            
            // Get the variable name //
            string vname = context.IDENTIFIER().GetText();

            // Check if its a pointer //
            if (this._pointers.Exists(vname))
            {
                return new ExpressionPointer(this.MasterNode, vname, this._pointers[vname].Item1, this._pointers[vname].Item2);
            }

            // go through every schema first looking for this variable //
            foreach (KeyValuePair<string, Schema> kv in this._columns.Entries)
            {

                if (kv.Value.Contains(vname))
                {
                    Expression e = new ExpressionFieldRef(this.MasterNode, kv.Value.ColumnIndex(vname), kv.Value.ColumnAffinity(vname), kv.Value.ColumnSize(vname), this._registers[kv.Key]);
                    return e;
                }

            }

            // go through every structure //
            foreach (KeyValuePair<string, MemoryStructure> kv in this._structs.Entries)
            {

                if (kv.Value.Scalars.Exists(vname))
                {
                    Expression e = new ExpressionHeapRef(this.MasterNode, kv.Value, kv.Value.Scalars.GetPointer(vname));
                    return e;
                }

            }

            throw new RyeCompileException("Cannot find '{0}'", vname);

        }

        public override Expression VisitSpecificVariable(RyeParser.SpecificVariableContext context)
        {

            string sname = context.IDENTIFIER()[0].GetText();
            string vname = context.IDENTIFIER()[1].GetText();

            if (this._columns.Exists(sname))
            {

                if (!this._columns[sname].Contains(vname) && !this._structs.Exists(sname))
                    throw new RyeCompileException("Variable '{0}' does not exist in '{1}'", vname, sname);
                Schema s = this._columns[sname];
                Register r = this._registers[sname];
                return new ExpressionFieldRef(this.MasterNode, s.ColumnIndex(vname), s.ColumnAffinity(vname), s.ColumnSize(vname), this._registers[sname]);

            }
            else if (this._structs.Exists(sname))
            {

                if (!this._structs[sname].Scalars.Exists(vname))
                    throw new RyeCompileException("Variable '{0}' does not exist in '{1}'", vname, sname);
                return new ExpressionHeapRef(this.MasterNode, this._structs[sname], this._structs[sname].Scalars.GetPointer(vname));

            }

            throw new RyeCompileException("Schema / structure '{0}' does not exist", sname);

        }

        // To methods //
        public Expression ToNode(RyeParser.ExpressionContext context)
        {
            this.MasterNode = null;
            return this.Visit(context);
        }

        public ExpressionCollection ToNodes(RyeParser.Expression_alias_listContext context)
        {

            ExpressionCollection fset = new ExpressionCollection();

            // Check for instances where we have no expressions //
            if (context == null)
                return fset;

            // Otherwise, parse each expression //
            foreach (RyeParser.Expression_aliasContext c in context.expression_alias())
            {

                Expression node = this.ToNode(c.expression());

                string alias = node.Name ?? "G" + fset.Count.ToString();

                if (c.K_AS() != null)
                    alias = c.IDENTIFIER().GetText();

                fset.Add(node, alias);

            }

            return fset;

        }

        public ExpressionCollection ToNodes(RyeParser.Expression_or_wildcard_setContext context)
        {

            ExpressionCollection e = new ExpressionCollection();
            this.AppendSet(e, context);
            return e;

        }

        public ExpressionCollection ToNodes(RyeParser.ExpressionContext[] context)
        {

            ExpressionCollection c = new ExpressionCollection();
            foreach (RyeParser.ExpressionContext ctx in context)
            {
                c.Add(this.ToNode(ctx));
            }
            return c;

        }

        public Filter ToPredicate(RyeParser.ExpressionContext context)
        {
            return new Filter(this.ToNode(context));
        }

        public Aggregate ToReduce(RyeParser.Beta_reductionContext context)
        {

            // Get the expressions //
            ExpressionCollection nodes = this.ToNodes(context.expression_alias_list());

            // Get the reduction ID //
            string RID = context.SET_REDUCTIONS().GetText().ToLower();

            // Get the reduction //
            Aggregate r;
            switch (RID)
            {

                case "avg":
                    if (nodes.Count == 2) r = CellReductions.Average(nodes[0], nodes[1]);
                    else r = CellReductions.Average(nodes[0]);
                    break;

                case "corr":
                    if (nodes.Count == 3) r = CellReductions.Correl(nodes[0], nodes[1], nodes[2]);
                    else r = CellReductions.Correl(nodes[0], nodes[1]);
                    break;

                case "count":
                    r = CellReductions.Count(nodes[0]);
                    break;

                case "count_all":
                    r = CellReductions.CountAll();
                    break;

                case "count_null":
                    r = CellReductions.CountNull(nodes[0]);
                    break;

                case "covar":
                    if (nodes.Count == 3) r = CellReductions.Covar(nodes[0], nodes[1], nodes[2]);
                    else r = CellReductions.Covar(nodes[0], nodes[1]);
                    break;

                case "freq":
                    if (nodes.Count == 2) r = CellReductions.Frequency(new Filter(nodes[1]), nodes[0]);
                    else r = CellReductions.Frequency(new Filter(nodes[0]));
                    break;

                case "intercept":
                    if (nodes.Count == 3) r = CellReductions.Intercept(nodes[0], nodes[1], nodes[2]);
                    else r = CellReductions.Intercept(nodes[0], nodes[1]);
                    break;

                case "max":
                    r = CellReductions.Max(nodes[0]);
                    break;

                case "min":
                    r = CellReductions.Min(nodes[0]);
                    break;

                case "slope":
                    if (nodes.Count == 3) r = CellReductions.Slope(nodes[0], nodes[1], nodes[2]);
                    else r = CellReductions.Slope(nodes[0], nodes[1]);
                    break;

                case "stdev":
                    if (nodes.Count == 2) r = CellReductions.Stdev(nodes[0], nodes[1]);
                    else r = CellReductions.Stdev(nodes[0]);
                    break;

                case "sum":
                    r = CellReductions.Sum(nodes[0]);
                    break;

                case "var":
                    if (nodes.Count == 2) r = CellReductions.Var(nodes[0], nodes[1]);
                    else r = CellReductions.Var(nodes[0]);
                    break;

                default:
                    throw new Exception(string.Format("Reducer with name '{0}' is invalid", RID));
            }

            // Check for a filter //
            if (context.where_clause() != null)
                r.BaseFilter = CompilerHelper.GetWhere(this, context.where_clause());

            // Return //
            return r;

        }

        public AggregateCollection ToReducers(RyeParser.Beta_reduction_listContext context)
        {

            AggregateCollection aggregates = new AggregateCollection();
            foreach (RyeParser.Beta_reductionContext ctx in context.beta_reduction())
            {
                Aggregate agg = this.ToReduce(ctx);
                string alias =
                    (ctx.IDENTIFIER() == null)
                    ? "R" + aggregates.Count.ToString()
                    : ctx.IDENTIFIER().GetText();
                aggregates.Add(this.ToReduce(ctx), alias);
            }
            return aggregates;

        }

        public void AppendSet(ExpressionCollection Expressions, RyeParser.Expression_or_wildcard_setContext context)
        {

            foreach (RyeParser.Expression_or_wildcardContext ctx in context.expression_or_wildcard())
            {

                // If this is a single expression //
                if (ctx.expression_alias() != null)
                {

                    // Render expression //
                    Expression exp = this.ToNode(ctx.expression_alias().expression());
                    
                    // Get the defaul alias //
                    string alias = (exp.Name ?? "F" + Expressions.Count.ToString());
                    
                    // If there is an alias //
                    if (ctx.expression_alias().IDENTIFIER() != null)
                        alias = ctx.expression_alias().IDENTIFIER().GetText();
                    
                    // Append the set //
                    Expressions.Add(exp, alias);

                }
                else
                {

                    // Get the caller name //
                    string sname = ctx.IDENTIFIER().GetText();

                    // If its a memory structure, something like LOCAL.* or GLOBAL.* //
                    if (this._structs.Exists(sname))
                    {

                        // This will naturally alias using the variable name //
                        foreach (KeyValuePair<string, Cell> kv in this._structs[sname].Scalars.Entries)
                        {
                            Expressions.Add(new ExpressionValue(null, kv.Value), kv.Key);
                        }

                    }
                    else if (this._columns.Exists(sname))
                    {

                        // This will alias using the column Name //
                        Schema cols = this._columns[sname];
                        Register reg = this._registers[sname];
                        for (int i = 0; i < cols.Count; i++)
                        {
                            Expressions.Add(new ExpressionFieldRef(null, i, cols.ColumnAffinity(i), cols.ColumnSize(i), reg), cols.ColumnName(i));
                        }

                    }
                    else
                    {
                        throw new RyeCompileException("'{0}' does not exist anywhere", sname);
                    }

                }

            }

        }

        // Others //
        public ExpressionVisitor CloneOfMe()
        {

            ExpressionVisitor exp = new ExpressionVisitor();
            for(int i = 0; i < this._structs.Count; i++)
            {
                exp.AddStructure(this._structs.Name(i), this._structs[i]);
            }

            for (int i = 0; i < this._registers.Count; i++)
            {
                exp.AddRegister(this._registers.Name(i), this._registers[i]);
            }

            return exp;

        }

        // Statics //
        public static string CleanString(string Value)
        {

            // Check for tab //
            if (Value.ToLower() == STRING_TAB)
                return "\t";

            // Check for newline //
            if (Value.ToLower() == STRING_NEWLINE)
                return "\n";

            // Check for lengths less than two //
            if (Value.Length < 2)
            {
                return Value.Replace("\\n","\n").Replace("\\t","\t");
            }

            // Handle 'ABC' to ABC //
            if (Value.First() == SLIT1 && Value.Last() == SLIT1)
            {
                Value = Value.Substring(1, Value.Length - 2);
                while (Value.Contains("''"))
                {
                    Value = Value.Replace("''", "'");
                }
            }

            // Handle "ABC" to ABC //
            if (Value.First() == SLIT2 && Value.Last() == SLIT2)
            {
                Value = Value.Substring(1, Value.Length - 2);
                while (Value.Contains("\"\""))
                {
                    Value = Value.Replace("\"\"", "\"");
                }
            }

            // Check for lengths less than four //
            if (Value.Length < 4)
            {
                return Value.Replace("\\n", "\n").Replace("\\t", "\t");
            }

            // Handle $$ABC$$ to ABC //
            int Len = Value.Length;
            if (Value[0] == SLIT3 && Value[1] == SLIT3 && Value[Len - 2] == SLIT3 && Value[Len - 1] == SLIT3)
            {
                Value = Value.Substring(2, Value.Length - 4);
                while (Value.Contains("$$$$"))
                {
                    Value = Value.Replace("$$$$", "$$");
                }
            }

            // Otherwise, return Value //
            return Value.Replace("\\n", "\n").Replace("\\t", "\t");

        }


    }

}
