using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Query
{

    /// <summary>
    /// Supports updating a data set via a single thread
    /// </summary>
    public sealed class UpdateProcessNode : QueryNode
    {

        private Table _Source;
        private Volume _Data;
        private Key _SetVariables;
        private ExpressionCollection _Expressions;
        private Filter _Where;
        private Register _Memory;
        private bool _PassBack = false;

        public UpdateProcessNode(int ThreadID, Session Session, Table Source, Volume Data, Key Fields, ExpressionCollection Values, Filter Where, Register MemoryLocation)
            : base(ThreadID, Session)
        {

            this._Source = Source;
            this._Data = Data;
            this._SetVariables = Fields;
            this._Expressions = Values;
            this._Where = Where;
            this._Memory = MemoryLocation;
            this._PassBack = !(this._Data == null);

        }

        public override void BeginInvoke()
        {
            base.BeginInvoke();
        }

        public override void EndInvoke()
        {
            base.EndInvoke();
        }

        public override void Invoke()
        {

            foreach (Extent e in this._Data.Extents)
            {

                this.UpdateUnit(e);
                if (this._PassBack)
                {
                    this._Source.SetExtent(e);
                }

            }

        }

        private void UpdateUnit(Extent e)
        {

            for (int i = 0; i < e.Count; i++)
            {

                this._Memory.Value = e[i];
                if (this._Where.Render())
                {

                    Record r = this._Expressions.Evaluate();
                    for (int j = 0; j < this._SetVariables.Count; j++) 
                    {
                        int idx = this._SetVariables[j];
                        e[i][idx] = r[j];
                    }

                }

            }

        }

    }

    /// <summary>
    /// Support for consolidating update nodes
    /// </summary>
    public sealed class UpdateProcessConsolidation : QueryConsolidation<UpdateProcessNode>
    {

        public UpdateProcessConsolidation(Session Session)
            : base(Session)
        {
        }

        public override void Consolidate(List<UpdateProcessNode> Nodes)
        {
            // do nothing
        }

    }

    /// <summary>
    /// Support for building update nodes
    /// </summary>
    public class UpdateModel : QueryModel
    {

        public const string DEFAULT_ALIAS = "T";
        
        // Can't be null section
        private TabularData _Source; // FROM;
        private string _SourceAlias = DEFAULT_ALIAS;
        private ExpressionCollection _Vals;
        private Key _Key;

        // Can be null //
        private Filter _Where;

        public UpdateModel(Session Session)
            :base(Session)
        {
            this._Vals = new ExpressionCollection();
            this._Key = new Key();
            this._Where = Filter.TrueForAll;
        }

        public void SetFROM(TabularData Value, string Alias)
        {
            this._Source = Value;
            this._SourceAlias = Alias;
        }

        public void AddSET(Expression Value, string Alias)
        {
            this._Vals.Add(Value, Alias);
        }

        public void AddSET(ExpressionCollection Value)
        {

            for (int i = 0; i < Value.Count; i++)
            {
                this._Vals.Add(Value[i], Value.Alias(i));
            }

        }

        public void AddKEY(int Value)
        {
            this._Key.Add(Value);
        }

        public void AddKEY(Key Value)
        {
            foreach (int i in Value.ToIntArray())
            {
                this._Key.Add(i);
            }
        }
        
        public void SetWHERE(Filter Where)
        {
            this._Where = Where;
        }

        // Node Rendering //
        public UpdateProcessNode RenderNode(int ThreadID, int ThreadCount)
        {

            // Create the volume //
            Volume source = this._Source.CreateVolume(ThreadID, ThreadCount);

            // Create two registers //
            Register current = new Register(this._SourceAlias, source.Columns);
            
            // Create the memory envrioment //
            CloneFactory spiderweb = new CloneFactory();
            spiderweb.Append(current);
            spiderweb.Append(this._Session.Scalars); // Add in the global scalars
            spiderweb.Append(this._Session.Matrixes); // Add in the global matrixes

            // Create clones of all our inputs //
            ExpressionCollection vals = spiderweb.Clone(this._Vals);
            Filter where = spiderweb.Clone(this._Where);

            // Return a node //
            return new UpdateProcessNode(ThreadID, this._Session, (this._Source is Table ? this._Source as Table : null), source, this._Key, vals, where, current);

        }

        public List<UpdateProcessNode> RenderNodes(int ThreadCount)
        {

            List<UpdateProcessNode> nodes = new List<UpdateProcessNode>();

            for (int i = 0; i < ThreadCount; i++)
            {
                nodes.Add(this.RenderNode(i, ThreadCount));
            }

            return nodes;

        }

        public void BuildCompileString()
        {

            //this._Message.Append("--- UPDATE ------------------------------------\n");
            this._Message.Append(string.Format("From: {0}\n", this._Source.Header.Name));
            if (!this._Where.Default)
                this._Message.Append(string.Format("Where: {0}\n", this._Where.UnParse(this._Source.Columns)));
            this._Message.Append(string.Format("Updating {0} field(s)", this._Key.Count));

        }

        public override void ExecuteConcurrent(int ThreadCount)
        {

            this.ThreadCount = ThreadCount;

            List<UpdateProcessNode> nodes = this.RenderNodes(this.ThreadCount);
            UpdateProcessConsolidation reducer = new UpdateProcessConsolidation(this._Session);
            QueryProcess<UpdateProcessNode> process = new QueryProcess<UpdateProcessNode>(nodes, reducer);

            this._Timer = System.Diagnostics.Stopwatch.StartNew();
            process.ExecuteThreaded();
            this._Timer.Stop();

        }

        public override void ExecuteAsynchronous()
        {

            this.ThreadCount = 1;

            List<UpdateProcessNode> nodes = this.RenderNodes(this.ThreadCount);
            UpdateProcessConsolidation reducer = new UpdateProcessConsolidation(this._Session);
            QueryProcess<UpdateProcessNode> process = new QueryProcess<UpdateProcessNode>(nodes, reducer);

            this._Timer = System.Diagnostics.Stopwatch.StartNew();
            process.Execute();
            this._Timer.Stop();

        }

    }


}
