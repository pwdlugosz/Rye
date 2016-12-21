using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Aggregates
{

    public sealed class AggregateFreq : Aggregate
    {

        private Expression _M;
        private Filter _G;

        public AggregateFreq(Expression M, Filter F, Filter G)
            : base(CellAffinity.DOUBLE, F)
        {
            this._M = M;
            this._G = G;
            this._Sig = 2;
        }

        public AggregateFreq(Expression M, Filter G)
            : this(M, Filter.TrueForAll, G)
        {
        }

        public AggregateFreq(Filter F, Filter G)
            : this(new ExpressionValue(null, Cell.OneValue(CellAffinity.DOUBLE)), F, G)
        {
        }

        public AggregateFreq(Filter G)
            : this(Filter.TrueForAll, G)
        {
        }

        public override List<int> FieldRefs
        {
            get
            {
                List<int> refs = new List<int>();
                refs.AddRange(Expressions.Analytics.AllFieldRefs(this._F.Node));
                refs.AddRange(Expressions.Analytics.AllFieldRefs(this._M));
                refs.AddRange(Expressions.Analytics.AllFieldRefs(this._G.Node));
                return refs;
            }
        }

        public override Record Initialize()
        {

            /*
             * 0: denominator accumulator
             * 1: numerator accumulator
             */
            return Record.Stitch
            (
                Cell.ZeroValue(CellAffinity.DOUBLE), Cell.ZeroValue(CellAffinity.DOUBLE)
            );
        }

        public override void Accumulate(Record WorkData)
        {

            if (!this._F.Render()) return;

            // denominator //
            WorkData[0]++;
            
            // numerator //
            if (this._G.Render())
                WorkData[1]++;

        }

        public override void Merge(Record WorkData, Record MergeIntoWorkData)
        {
            MergeIntoWorkData[0] += WorkData[0];
            MergeIntoWorkData[1] += WorkData[1];  
        }

        public override Cell Evaluate(Record WorkData)
        {
            // If 0 / 0 then null //
            if (WorkData[0] == Cell.ZeroValue(CellAffinity.DOUBLE))
                return new Cell(CellAffinity.DOUBLE);

            // return m / n //
            return WorkData[1] / WorkData[0];

        }

        public override Aggregate CloneOfMe()
        {
            return new AggregateFreq(this._M.CloneOfMe(), this._F.CloneOfMe(), this._G.CloneOfMe());
        }
        
        public override List<Expression> InnerExpressions()
        {
            return new List<Expression>() { this._F.Node, this._M, this._G.Node };
        }

    }

}
