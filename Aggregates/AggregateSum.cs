using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Aggregates
{

    public sealed class AggregateSum : Aggregate
    {

        private Expression _Map;

        public AggregateSum(Expression M, Filter F)
            : base(M.ReturnAffinity())
        {
            this._Map = M;
            this._F = F;
            this._Sig = 1;
        }

        public AggregateSum(Expression M)
            : this(M, Filter.TrueForAll)
        {
            this._Map = M;
        }

        public override List<int> FieldRefs
        {
            get
            {
                List<int> refs = new List<int>();
                refs.AddRange(Expressions.Analytics.AllFieldRefs(this._F.Node));
                refs.AddRange(Expressions.Analytics.AllFieldRefs(this._Map));
                return refs;
            }
        }

        public override Record Initialize()
        {
            //return Record.Stitch(Cell.ZeroValue(this.ReturnAffinity));
            return Record.Stitch(new Cell(this.ReturnAffinity));
        }

        public override void Accumulate(Record WorkData)
        {

            if (!this._F.Render()) return;

            Cell a = this._Map.Evaluate();
            Cell b = WorkData[0];
            
            if (!a.IsNull && !b.IsNull)
            {
                WorkData[0] = a + b;
            }
            else if (b.IsNull)
            {
                WorkData[0] = a;
            }
            
        }

        public override void Merge(Record WorkData, Record MergeIntoWorkData)
        {
            Cell a = WorkData[0];
            Cell b = MergeIntoWorkData[0];
            if (!a.IsNull && !b.IsNull)
            {
                MergeIntoWorkData[0] += WorkData[0];
            }
            else if (!a.IsNull && b.IsNull)
            {
                MergeIntoWorkData[0] = WorkData[0];
            }

        }

        public override Cell Evaluate(Record WorkData)
        {
            return WorkData[0];
        }

        public override Aggregate CloneOfMe()
        {
            return new AggregateSum(this._Map.CloneOfMe(), this._F.CloneOfMe());
        }

    }

}




