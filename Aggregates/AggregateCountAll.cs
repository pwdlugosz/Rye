using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Aggregates
{

    public sealed class AggregateCountAll : AggregateCount
    {

        public AggregateCountAll(Expression M, Filter F)
            : base(M, F)
        {
        }

        public AggregateCountAll(Expression M)
            : base(M)
        {
        }

        public override void Accumulate(Record WorkData)
        {

            if (!this._F.Render()) return;

            Cell a = this._Map.Evaluate();
            Cell b = WorkData[0];
            if (b.IsNull)
            {
                WorkData[0] = Cell.ZeroValue(this.ReturnAffinity);
            }
            WorkData[0]++;

        }

        public override Aggregate CloneOfMe()
        {
            return new AggregateCountAll(this._Map.CloneOfMe(), this._F.CloneOfMe());
        }

    }

}




