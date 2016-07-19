using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Aggregates
{

    public sealed class AggregateStdevP : AggregateStat
    {

        public AggregateStdevP(Expression X, Expression W, Filter F)
            : base(X, W, F)
        {
        }

        public AggregateStdevP(Expression X, Filter F)
            : base(X, F)
        {
        }

        public AggregateStdevP(Expression X, Expression W)
            :base(X, W)
        {
        }

        public AggregateStdevP(Expression X)
            : base(X)
        {
        }

        public override Cell Evaluate(Record WorkData)
        {
            if (WorkData[0].IsZero) return new Cell(this.ReturnAffinity);
            return Cell.Sqrt(WorkData[2] / WorkData[0] - Cell.Power(WorkData[1] / WorkData[0], new Cell((double)2)));
        }

        public override Aggregate CloneOfMe()
        {
            return new AggregateStdevP(this._MapX.CloneOfMe(), this._MapW.CloneOfMe(), this._F.CloneOfMe());
        }

    }


}
