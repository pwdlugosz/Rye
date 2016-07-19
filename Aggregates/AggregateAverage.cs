using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Aggregates
{

    public sealed class AggregateAverage : AggregateStat
    {

        public AggregateAverage(Expression X, Expression W, Filter F)
            : base(X, W, F)
        {
        }

        public AggregateAverage(Expression X, Filter F)
            : base(X, F)
        {
        }

        public AggregateAverage(Expression X, Expression W)
            :base(X, W)
        {
        }

        public AggregateAverage(Expression X)
            : base(X)
        {
        }

        public override Cell Evaluate(Record WorkData)
        {
            if (WorkData[0].IsZero) return new Cell(this.ReturnAffinity);
            return WorkData[1] / WorkData[0];
        }
        
        public override Aggregate CloneOfMe()
        {
            return new AggregateAverage(this._MapX.CloneOfMe(), this._MapW.CloneOfMe(), this._F.CloneOfMe());
        }

    }

}
