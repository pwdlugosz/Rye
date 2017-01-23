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
    /// Support for deleting data via one thread
    /// </summary>
    public sealed class DeleteProcessNode : QueryNode
    {

        private Table _Source;
        private Volume _Data;
        private Filter _Where;
        private Register _Memory;
        private bool _PassBack = false;
        private long _Clicks;

        public DeleteProcessNode(int ThreadID, Session Session, Table Source, Volume Data, Filter Where, Register MemoryLocation)
            : base(ThreadID, Session)
        {

            this._Where = Where;
            this._Memory = MemoryLocation;
            this._Data = Data;
            this._Source = Source;
            this._PassBack = !(Source == null);

        }

        public long Clicks
        {
            get { return this._Clicks; }
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

                // Load a shell table with the records that are NOT
                Extent shell = e.EmptyClone();
                for (int i = 0; i < e.Count; i++)
                {
                    
                    this._Memory.Value = e[i];
                    if (!this._Where.Render())
                    {
                        shell.Add(e[i]);
                        this._Clicks++;
                    }

                }

                // Update the parent data with the extent //
                if (this._PassBack)
                {
                    this._Source.SetExtent(shell);
                }

            }

        }

    }

    /// <summary>
    /// Support for consolidating delete nodes
    /// </summary>
    public sealed class DeleteProcessConsolidation : QueryConsolidation<DeleteProcessNode>
    {

        private long _Clicks = 0;

        public DeleteProcessConsolidation(Session Session)
            : base(Session)
        {
        }

        public long Clicks
        {
            get { return this._Clicks; }
        }

        public override void Consolidate(List<DeleteProcessNode> Nodes)
        {

            foreach (DeleteProcessNode n in Nodes)
            {
                this._Clicks += n.Clicks;
            }

        }

    }

    /// <summary>
    /// Support for building delete nodes
    /// </summary>
    public class DeleteModel : QueryModel
    {

        public const string DEFAULT_ALIAS = "T";

        // Can't be null section
        private TabularData _Source; // FROM;
        private string _SourceAlias = DEFAULT_ALIAS;
        
        // Can be null //
        private Filter _Where;

        public DeleteModel(Session Session)
            :base(Session)
        {
            this._Where = Filter.TrueForAll;
        }

        public void SetFROM(TabularData Value, string Alias)
        {
            this._Source = Value;
            this._SourceAlias = Alias;
        }

        public void SetWHERE(Filter Where)
        {
            this._Where = Where;
        }

        // Node Rendering //
        public DeleteProcessNode RenderNode(int ThreadID, int ThreadCount)
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
            Filter where = spiderweb.Clone(this._Where);

            // Return a node //
            return new DeleteProcessNode(ThreadID, this._Session, (this._Source is Table ? this._Source as Table : null), source, where, current);

        }

        public List<DeleteProcessNode> RenderNodes(int ThreadCount)
        {

            List<DeleteProcessNode> nodes = new List<DeleteProcessNode>();

            for (int i = 0; i < ThreadCount; i++)
            {
                nodes.Add(this.RenderNode(i, ThreadCount));
            }

            return nodes;

        }

        public void BuildCompileString()
        {

            //this._Message.Append("--- DELETE ------------------------------------\n");
            this._Message.Append(string.Format("From: {0}\n", this._Source.Header.Name));
            if (!this._Where.Default)
                this._Message.Append(string.Format("Where: {0}\n", this._Where.UnParse(this._Source.Columns)));

        }

        public override void ExecuteConcurrent(int ThreadCount)
        {

            this.ThreadCount = ThreadCount;

            List<DeleteProcessNode> nodes = this.RenderNodes(this.ThreadCount);
            DeleteProcessConsolidation reducer = new DeleteProcessConsolidation(this._Session);
            QueryProcess<DeleteProcessNode> process = new QueryProcess<DeleteProcessNode>(nodes, reducer);

            this._Timer = System.Diagnostics.Stopwatch.StartNew();
            process.ExecuteThreaded();
            this._Timer.Stop();

        }

        public override void ExecuteAsynchronous()
        {

            this.ThreadCount = 1;

            List<DeleteProcessNode> nodes = this.RenderNodes(this.ThreadCount);
            DeleteProcessConsolidation reducer = new DeleteProcessConsolidation(this._Session);
            QueryProcess<DeleteProcessNode> process = new QueryProcess<DeleteProcessNode>(nodes, reducer);

            this._Timer = System.Diagnostics.Stopwatch.StartNew();
            process.Execute();
            this._Timer.Stop();

        }



    }

}
