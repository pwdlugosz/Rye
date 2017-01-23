using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.MatrixExpressions;
using Rye.Methods;
using Rye.Structures;

namespace Rye.Query
{

    /*

    public sealed class SelectProcessNode : QueryNode
    {

        public const string KEY_CHANGE = "KEY_CHANGE";
        public const string ROW_ID = "ROW_ID";
        public const string EXTENT_ID = "EXTENT_ID";
        public const string IS_FIRST = "IS_FIRST";
        public const string IS_LAST = "IS_LAST";

        private Volume _data;

        private Register _main;
        private Register _peek;
        private Heap<Cell> _lscalar;
        private Heap<CellMatrix> _lmatrix;
        private MethodCollection _map;
        private MethodCollection _reduce;

        private Filter _where;
        private ExpressionCollection _key1;
        private ExpressionCollection _key2;

        private int _rowIDptr = 0;
        private int _extentIDptr = 0;
        private int _kcptr = 0;
        private int _isFirstptr = 0;
        private int _isLastptr = 0;
        private bool _hasKC = false;

        public SelectProcessNode(int ThreadID, Session Session, Volume Data, Register Main, Heap<Cell> Scalars, Heap<CellMatrix> Matrixes, MethodCollection MapActions, MethodCollection ReduceActions, 
            Filter Where, ExpressionCollection KeyChange)
            : base(ThreadID, Session)
        {

            this._data = Data;

            this._main = Main;
            this._lscalar = Scalars;
            this._lmatrix = Matrixes;
            this._map = MapActions;
            this._reduce = ReduceActions;
            this._where = Where;

            // Handle the key change //
            if (KeyChange == null)
            {
                this._hasKC = false;
            }
            else
            {

                this._key1 = KeyChange;
                this._peek = new Register("PEEK", this._data.Columns);
                this._key2 = this._key1.CloneOfMe();
                this._key2.ForceMemoryRegister(this._peek);
                this._key2.ForceCellHeap(this._lscalar);
                this._key2.ForceCellMaxtrixHeap(this._lmatrix);
                this._hasKC = true;

            }

        }

        public override void BeginInvoke()
        {
            
            // Get the pointers //
            this._rowIDptr = this._lscalar.GetPointer(ROW_ID);
            this._extentIDptr = this._lscalar.GetPointer(EXTENT_ID);
            this._kcptr = this._lscalar.GetPointer(KEY_CHANGE);
            this._isFirstptr = this._lscalar.GetPointer(IS_FIRST);
            this._isLastptr = this._lscalar.GetPointer(IS_LAST);
            this._map.BeginInvoke();

        }

        public override void EndInvoke()
        {
            this._map.EndInvoke();
        }

        public MethodCollection Reducer
        {
            get
            {
                return this._reduce;
            }
        }

        public override void Invoke()
        {

            // Burn if no records //
            if (this._data.RecordCount == 0)
                return;

            // Open a stream to read data //
            RecordReader stream = this._data.OpenReader(this._main, this._where);

            // Open a record comparer for the key change //
            RecordComparer rc = new RecordComparer();

            // Create a bool trip flag for the first record //
            bool first = true;

            // Check if we are at the end of the stream //
            if (stream.EndOfData)
                return;

            // Set the initial register for the where clause //
            this._main.Value = stream.Read();

            // Traverse the stream //
            while (!stream.EndOfData)
            {

                // Set up the current values //
                this._main.Value = stream.ReadNext();

                // Conditionally set the lag Value //
                if (this._hasKC)
                {
                    if (!stream.EndOfData)
                        this._peek.Value = stream.Read();
                    else
                        this._peek.Value = this._peek.Columns.NullRecord;
                }

                // Set the key variables //
                this._lscalar[this._extentIDptr] = new Cell(stream.SetID);
                this._lscalar[this._rowIDptr] = new Cell(stream.Position);
                this._lscalar[this._isFirstptr] = (first ? Cell.TRUE : Cell.FALSE);
                this._lscalar[this._isLastptr] = (stream.EndOfData ? Cell.TRUE : Cell.FALSE);
                if (first) 
                    first = false;
                if (this._hasKC)
                {
                    bool Mem = !rc.Equals(this._key1.Evaluate(), this._key2.Evaluate());
                    this._lscalar[this._kcptr] = new Cell(Mem);
                }

                // Perform our actions //
                this._map.Invoke();

                // Check if we have a read break //
                if (this._map.CheckBreak)
                {
                    return;
                }

            }

        }

    }

    public sealed class SelectProcessConsolidation : QueryConsolidation<SelectProcessNode>
    {

        public SelectProcessConsolidation(Session Session)
            :base(Session)
        {
        }

        public override void Consolidate(List<SelectProcessNode> Nodes)
        {
            foreach (SelectProcessNode n in Nodes)
            {
                n.Reducer.BeginInvoke();
                n.Reducer.Invoke();
                n.Reducer.EndInvoke();
            }
        }

    }

    */

    /// <summary>
    /// Provides support for selecting data
    /// </summary>
    public sealed class SelectProcessNode : QueryNode
    {

        private Volume _data;

        private Register _main;
        private Register _peek;
        private Heap<Cell> _lscalar;
        private Heap<CellMatrix> _lmatrix;
        private MethodCollection _map;
        private MethodCollection _reduce;

        private Filter _where;
        private ExpressionCollection _key1;
        private ExpressionCollection _key2;

        private int _kcptr = 0;
        private int _rowIDptr = 1;
        private int _extentIDptr = 2;
        private int _isFirstptr = 3;
        private int _isLastptr = 4;

        public SelectProcessNode(int ThreadID, Session Session, Volume Data, Filter Where, 
            Register Main, Register Peek,
            Heap<Cell> Scalars, Heap<CellMatrix> Matrixes, 
            MethodCollection MapActions, MethodCollection ReduceActions,
            ExpressionCollection KeyChange1, ExpressionCollection KeyChange2)
            : base(ThreadID, Session)
        {

            this._data = Data;

            this._main = Main;
            this._peek = Peek;
            this._lscalar = Scalars;
            this._lmatrix = Matrixes;
            this._map = MapActions;
            this._reduce = ReduceActions;
            this._where = Where;

            this._key1 = KeyChange1;
            this._key2 = KeyChange2;

        }

        public override void BeginInvoke()
        {

            // Get the pointers //
            this._map.BeginInvoke();

        }

        public override void EndInvoke()
        {
            this._map.EndInvoke();
        }

        public MethodCollection Reducer
        {
            get
            {
                return this._reduce;
            }
        }

        public override void Invoke()
        {

            int x = 0;

            // Burn if no records //
            if (this._data.IsEmpty)
                return;

            // Open a stream to read data //
            RecordReader stream = this._data.OpenReader(this._main, this._where);

            // Open a record comparer for the key change //
            RecordComparer rc = new RecordComparer();

            // Create a bool trip flag for the first record //
            bool first = true;

            // Check if we are at the end of the stream //
            if (stream.EndOfData)
                return;

            // Set the initial register for the where clause //
            this._main.Value = stream.Read();

            // Traverse the stream //
            while (!stream.EndOfData)
            {

                // Set up the current values //
                this._main.Value = stream.ReadNext();

                // Conditionally set the lag Value //
                if (!stream.EndOfData)
                    this._peek.Value = stream.Read();
                else
                    this._peek.Value = this._peek.Columns.NullRecord;

                // Set the key variables //
                this._lscalar[this._extentIDptr] = new Cell(stream.SetID);
                this._lscalar[this._rowIDptr] = new Cell(stream.Position);
                this._lscalar[this._isFirstptr] = (first ? Cell.TRUE : Cell.FALSE);
                this._lscalar[this._isLastptr] = (stream.EndOfData ? Cell.TRUE : Cell.FALSE);
                if (first) first = false;

                // Handle Key Change //
                bool b = !rc.Equals(this._key1.Evaluate(), this._key2.Evaluate());
                this._lscalar[this._kcptr] = new Cell(b);

                // Perform our actions //
                this._map.Invoke();
                
                // Check if we have a read break //
                if (this._map.CheckBreak)
                {
                    return;
                }

                x++;

            }

        }

    }

    /// <summary>
    /// Provides support for consolidating select process nodes
    /// </summary>
    public sealed class SelectProcessConsolidation : QueryConsolidation<SelectProcessNode>
    {

        public SelectProcessConsolidation(Session Session)
            : base(Session)
        {
        }

        public override void Consolidate(List<SelectProcessNode> Nodes)
        {
            foreach (SelectProcessNode n in Nodes)
            {
                n.Reducer.BeginInvoke();
                n.Reducer.Invoke();
                n.Reducer.EndInvoke();
            }
        }

    }

    /// <summary>
    /// Given various inputs, this method will render a SELECT query process node and consolidator
    /// </summary>
    public class SelectModel : QueryModel
    {

        public const string DEFAULT_ALIAS = "T";
        public const string LOCAL_ALIAS = "LOCAL";
        public const string PEEK_ALIAS = "PEEK";

        public const string NAME_KEY_CHANGE = "KEY_CHANGE";
        public const string NAME_ROW_ID = "ROW_ID";
        public const string NAME_EXTENT_ID = "EXTENT_ID";
        public const string NAME_IS_FIRST = "IS_FIRST";
        public const string NAME_IS_LAST = "IS_LAST";

        // Can't be null section
        private TabularData _Source; // FROM;
        private string _SourceAlias = DEFAULT_ALIAS; 
        private MethodCollection _Main; // MAIN or MAP

        // Can be missing (null) //
        private Heap<Cell> _LocalS; // DECLARE
        private Heap<CellMatrix> _LocalM; // DECLARE
        private ExpressionCollection _By; // BY
        private MethodCollection _Reduce; // REDUCE
        private Filter _Where; // WHERE

        // Constructor //
        public SelectModel(Session Control)
            :base(Control)
        {

            // Fill the ancilary variables //
            this._Main = new MethodCollection();
            this._By = new ExpressionCollection();
            this._Reduce = new MethodCollection();
            this._Where = Filter.TrueForAll;

            // Create and load the local heap //
            this._LocalS = new Heap<Cell>();
            this._LocalS.Identifier = LOCAL_ALIAS;
            this._LocalS.Reallocate(NAME_KEY_CHANGE, Cell.FALSE);
            this._LocalS.Reallocate(NAME_ROW_ID, Cell.ZeroValue(CellAffinity.INT));
            this._LocalS.Reallocate(NAME_EXTENT_ID, Cell.ZeroValue(CellAffinity.INT));
            this._LocalS.Reallocate(NAME_IS_FIRST, Cell.FALSE);
            this._LocalS.Reallocate(NAME_IS_LAST, Cell.FALSE);
            this._LocalM = new Heap<CellMatrix>();
            this._LocalM.Identifier = LOCAL_ALIAS;

        }

        // Properties //
        public TabularData FROM
        {
            get { return this._Source; }
        }

        public Filter WHERE
        {
            get { return this._Where; }
        }

        public ExpressionCollection BY
        {
            get { return this._By; }
        }

        public MethodCollection MAIN
        {
            get { return this._Main; }
        }

        public MethodCollection REDUCE
        {
            get { return this._Reduce; }
        }

        public Heap<Cell> LOCALS
        {
            get { return this._LocalS; }
        }

        public Heap<CellMatrix> LOCALM
        {
            get { return this._LocalM; }
        }

        // Set / Add Methods //
        public void SetFROM(TabularData Data, string Alias)
        {
            this._Source = Data;
            this._SourceAlias = Alias;
        }

        public void SetWHERE(Filter Where)
        {
            this._Where = Where;
        }

        public void AddDECLARE(string Name, Cell InitialValue)
        {
            this._LocalS.Reallocate(Name, InitialValue);
        }

        public void AddDECLARE(string Name, CellAffinity Type)
        {
            this._LocalS.Reallocate(Name, new Cell(Type));
        }

        public void AddDECLARE(string Name, CellMatrix InitialValue)
        {
            this._LocalM.Reallocate(Name, InitialValue);
        }

        public void AddBY(Expression Key)
        {
            this._By.Add(Key);
        }

        public void AddBY(ExpressionCollection Keys)
        {
            if (Keys == null)
                return;
            this._By = ExpressionCollection.Union(this._By, Keys);
        }

        public void AddMAIN(Method Element)
        {
            this._Main.Add(Element);
        }

        public void AddMAIN(MethodCollection Elements)
        {

            if (Elements == null)
                return;

            foreach (Method m in Elements.Nodes)
            {
                this._Main.Add(m);
            }

        }

        public void AddREDUCE(Method Element)
        {

            if (Element == null)
                return;

            this._Reduce.Add(Element);

        }

        public void AddREDUCE(MethodCollection Elements)
        {

            if (Elements == null)
                return;

            foreach (Method m in Elements.Nodes)
            {
                this._Reduce.Add(m);
            }

        }

        // Create a single process node //
        public SelectProcessNode RenderNode(int ThreadID, int ThreadCount)
        {

            // Create the volume //
            Volume source = this._Source.CreateVolume(ThreadID, ThreadCount);

            // Create two registers //
            Register current = new Register(this._SourceAlias, source.Columns);
            Register peek = new Register(PEEK_ALIAS, source.Columns);

            // Create clones of our heaps //
            Heap<Cell> locals = CloneFactory.Clone(this._LocalS);
            Heap<CellMatrix> localm = CloneFactory.Clone(this._LocalM);
            
            // Create the memory envrioment //
            CloneFactory spiderweb = new CloneFactory();
            spiderweb.Append(current);
            spiderweb.Append(peek);
            spiderweb.Append(locals);
            spiderweb.Append(localm);
            spiderweb.Append(this._Session.Scalars); // Add in the global scalars
            spiderweb.Append(this._Session.Matrixes); // Add in the global matrixes

            // Create clones of all our inputs //
            MethodCollection main = spiderweb.Clone(this._Main);
            MethodCollection reduce = spiderweb.Clone(this._Reduce);
            ExpressionCollection key1 = spiderweb.Clone(this._By);
            ExpressionCollection key2 = spiderweb.Clone(this._By);
            Filter where = spiderweb.Clone(this._Where);

            // Need to link key2 -> peek //
            ExpressionCollection.ForceAssignRegister(key2, peek);

            // Return a node //
            return new SelectProcessNode(ThreadID, this._Session, source, where, current, peek, locals, localm, main, reduce, key1, key2);

        }

        public List<SelectProcessNode> RenderNodes(int ThreadCount)
        {

            List<SelectProcessNode> nodes = new List<SelectProcessNode>();

            for (int i = 0; i < ThreadCount; i++)
            {
                nodes.Add(this.RenderNode(i, ThreadCount));
            }

            return nodes;

        }

        // Execution //
        private void BuildCompileString()
        {

            //this._Message.Append("--- SELECT ------------------------------------\n");
            this._Message.Append(string.Format("From: {0}\n", this._Source.Header.Name));
            if (!this._Where.Default)
                this._Message.Append(string.Format("Where: {0}\n", this._Where.UnParse(this._Source.Columns)));
            if (this._By.Count != 0)
                this._Message.Append(string.Format("By: {0}\n", this._By.Unparse(this._Source.Columns)));
            if (this._LocalS.Count + this._LocalM.Count != 0)
                this._Message.Append(string.Format("Local: {0} scalar(s), {1} matrix(es)\n", this._LocalS.Count, this._LocalM.Count));
            this._Message.Append(string.Format("Main: {0} action(s)\n", this._Main.Count));
            if (this._Reduce.Count != 0)
                this._Message.Append(string.Format("Reduce: {0} action(s)\n", this._Reduce.Count));

        }

        public override void ExecuteAsynchronous()
        {

            // Set the thread count //
            this.ThreadCount = 1;

            // Build the process //
            List<SelectProcessNode> nodes = this.RenderNodes(this.ThreadCount);
            SelectProcessConsolidation reducer = new SelectProcessConsolidation(this._Session);
            QueryProcess<SelectProcessNode> process = new QueryProcess<SelectProcessNode>(nodes, reducer);

            // Compile strings //
            this.BuildCompileString();

            // Run the process //
            this._Timer = System.Diagnostics.Stopwatch.StartNew();
            process.Execute();
            this._Timer.Stop();

            // Append the run string //
            this._Message.Append(string.Format("Runtime: {0}, over {1} thread(s)\n\n", this._Timer.Elapsed, this.ThreadCount));

        }

        public override void ExecuteConcurrent(int ThreadCount)
        {

            // Set this thread count //
            this.ThreadCount = ThreadCount;

            // Build the process //
            List<SelectProcessNode> nodes = this.RenderNodes(this.ThreadCount);
            SelectProcessConsolidation reducer = new SelectProcessConsolidation(this._Session);
            QueryProcess<SelectProcessNode> process = new QueryProcess<SelectProcessNode>(nodes, reducer);

            // Compile strings //
            this.BuildCompileString();

            // Run the process //
            this._Timer = System.Diagnostics.Stopwatch.StartNew();
            process.ExecuteThreaded();
            this._Timer.Stop();

            // Append the run string //
            this._Message.Append(string.Format("Runtime: {0}, over {1} thread(s)\n\n", this._Timer.Elapsed, this.ThreadCount));

        }

    }


}
