using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Query
{

    public sealed class DeleteProcessNode : QueryNode
    {

        private Table _Source;
        private Volume _Data;
        private Filter _Where;
        private Register _Memory;
        private bool _PassBack = false;
        private long _Clicks;

        public DeleteProcessNode(int ThreadID, Table Source, Volume Data, Filter Where, Register MemoryLocation)
            : base(ThreadID)
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

    public sealed class DeleteProcessConsolidation : QueryConsolidation<DeleteProcessNode>
    {

        private long _Clicks = 0;

        public DeleteProcessConsolidation()
            : base()
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


}
