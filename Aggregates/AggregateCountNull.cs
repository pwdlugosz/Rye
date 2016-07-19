using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Aggregates
{

    public sealed class AggregateCountNull : AggregateCount
    {

        public AggregateCountNull(Expression M, Filter F)
            : base(M, F)
        {
        }

        public AggregateCountNull(Expression M)
            : base(M)
        {
        }

        public override void Accumulate(Record WorkData)
        {

            if (!this._F.Render()) return;

            Cell a = this._Map.Evaluate();
            Cell b = WorkData[0];
            if (a.IsNull && !b.IsNull)
            {
                WorkData[0]++;
            }
            else if (b.IsNull && a.IsNull)
            {
                WorkData[0] = Cell.OneValue(this.ReturnAffinity);
            }

        }

        public override Aggregate CloneOfMe()
        {
            return new AggregateCountNull(this._Map.CloneOfMe(), this._F.CloneOfMe());
        }

    }

}




