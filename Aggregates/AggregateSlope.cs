using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Aggregates
{

    public sealed class AggregateSlope : AggregateStatCo
    {

        public AggregateSlope(Expression X, Expression Y, Expression W, Filter F)
            : base(X, Y, W, F)
        {
        }

        public AggregateSlope(Expression X, Expression Y, Filter F)
            : base(X, Y, F)
        {
        }

        public AggregateSlope(Expression X, Expression Y, Expression W)
            : base(X, Y, W)
        {
        }

        public AggregateSlope(Expression X, Expression Y)
            : base(X, Y)
        {
        }

        public override Cell Evaluate(Record WorkData)
        {

            if (WorkData[0].IsZero == true) return new Cell(this.ReturnAffinity);
            Cell avgx = WorkData[1] / WorkData[0];
            Cell varx = WorkData[2] / WorkData[0] - avgx * avgx;
            Cell avgy = WorkData[3] / WorkData[0];
            Cell covxy = WorkData[5] / WorkData[0] - avgx * avgy;
            if (varx.IsZero) return new Cell(this.ReturnAffinity);
            return covxy / varx;

        }

        public override Aggregate CloneOfMe()
        {
            return new AggregateSlope(this._MapX.CloneOfMe(), this._MapY.CloneOfMe(), this._MapW.CloneOfMe(), this._F.CloneOfMe());
        }

    }

}
