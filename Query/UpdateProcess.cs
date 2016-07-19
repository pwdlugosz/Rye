using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Query
{

    public sealed class UpdateProcessNode : QueryNode
    {

        private Table _Source;
        private Volume _Data;
        private Key _SetVariables;
        private ExpressionCollection _Expressions;
        private Filter _Where;
        private Register _Memory;
        private bool _PassBack = false;

        public UpdateProcessNode(int ThreadID, Table Source, Volume Data, Key Fields, ExpressionCollection Values, Filter Where, Register MemoryLocation)
            : base(ThreadID)
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

    public sealed class UpdateProcessConsolidation : QueryConsolidation<UpdateProcessNode>
    {

        public override void Consolidate(List<UpdateProcessNode> Nodes)
        {
            // do nothing
        }

    }

}
