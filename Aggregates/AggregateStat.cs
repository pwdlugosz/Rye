using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Aggregates
{

    // designed to support: average, varp, stdevp, vars, stdevs
    public abstract class AggregateStat : Aggregate
    {

        protected Expression _MapX;
        protected Expression _MapW;

        public AggregateStat(Expression X, Expression W, Filter F)
            : base(X.ReturnAffinity())
        {
            this._MapX = X;
            this._MapW = W;
            this._F = F;
            this._Sig = 3;
        }

        public AggregateStat(Expression X, Expression W)
            : this(X, W, FilterFactory.IsNotNull(X))
        {
        }

        public AggregateStat(Expression X)
            : this(X, new ExpressionValue(null, Cell.OneValue(X.ReturnAffinity())))
        {
        }

        public AggregateStat(Expression X, Filter F)
            : this(X, new ExpressionValue(null, Cell.OneValue(X.ReturnAffinity())), F)
        {
        }

        public override List<int> FieldRefs
        {

            get
            {
                List<int> refs = new List<int>();
                refs.AddRange(Expressions.Analytics.AllFieldRefs(this._F.Node));
                refs.AddRange(Expressions.Analytics.AllFieldRefs(this._MapX));
                refs.AddRange(Expressions.Analytics.AllFieldRefs(this._MapW));
                return refs;
            }

        }

        public override Record Initialize()
        {
            return Record.Stitch
            (
                Cell.ZeroValue(this._MapW.ReturnAffinity()), Cell.ZeroValue(this._MapX.ReturnAffinity()), Cell.ZeroValue(this._MapX.ReturnAffinity())
            );
        }

        public override void Accumulate(Record WorkData)
        {

            if (!this._F.Render()) return;

            Cell a = this._MapW.Evaluate();
            Cell b = WorkData[0];
            Cell c = this._MapX.Evaluate();
            Cell d = WorkData[1];
            Cell e = WorkData[2];

            if (!a.IsNull && !b.IsNull && !c.IsNull && !d.IsNull)
            {
                WorkData[0] += a;
                WorkData[1] += a * c;
                WorkData[2] += a * c * c;
            }
            else if (b.IsNull)
            {
                WorkData[0] = a;
                WorkData[1] = a * c;
                WorkData[2] = a * c * c;
            }

        }

        public override void Merge(Record WorkData, Record MergeIntoWorkData)
        {

            Cell a = WorkData[0];
            Cell b = MergeIntoWorkData[0];
            Cell c = WorkData[1];
            Cell d = MergeIntoWorkData[1];
            
            if (!a.IsNull && !b.IsNull && !c.IsNull && !d.IsNull)
            {
                MergeIntoWorkData[0] += WorkData[0];
                MergeIntoWorkData[1] += WorkData[1];
                MergeIntoWorkData[2] += WorkData[2];
            }
            else if (!a.IsNull && !c.IsNull && b.IsNull)
            {
                MergeIntoWorkData[0] = WorkData[0];
                MergeIntoWorkData[1] = WorkData[1];
                MergeIntoWorkData[2] = WorkData[2];
            }

        }

        public override int Size()
        {
            return this._MapX.DataSize();
        }

        public override List<Expression> InnerExpressions()
        {
            return new List<Expression>() { this._F.Node, this._MapX, this._MapW };
        }

    }

}




