using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Aggregates
{

    public sealed class AggregateMin : Aggregate
    {

        private Expression _Map;

        public AggregateMin(Expression M, Filter F)
            : base(M.ReturnAffinity())
        {
            this._Map = M;
            this._F = F;
            this._Sig = 1;
        }

        public AggregateMin(Expression M)
            : this(M, Filter.TrueForAll)
        {
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
            return Record.Stitch(new Cell(this.ReturnAffinity));
        }

        public override void Accumulate(Record WorkData)
        {

            if (!this._F.Render()) return;

            Cell c = this._Map.Evaluate();
            Cell d = WorkData[0];
            if (!c.IsNull && !d.IsNull) 
            {
                WorkData[0] = Cell.Min(c, d);
            }
            else if (d.IsNull)
            {
                WorkData[0] = c;
            }

        }

        public override void Merge(Record WorkData, Record MergeIntoWorkData)
        {

            Cell a = WorkData[0];
            Cell b = MergeIntoWorkData[0];
            if (!a.IsNull && !b.IsNull)
            {
                MergeIntoWorkData[0] = Cell.Min(a, b);
            }
            else if (!a.IsNull && b.IsNull)
            {
                MergeIntoWorkData[0] = a;
            }

        }

        public override Cell Evaluate(Record WorkData)
        {
            return WorkData[0];
        }

        public override Aggregate CloneOfMe()
        {
            return new AggregateMin(this._Map.CloneOfMe(), this._F.CloneOfMe());
        }

    }

    public sealed class AggregateMinOf : Aggregate
    {

        private Expression _Key;
        private Expression _Value;

        public AggregateMinOf(Expression K, Expression V, Filter F)
            : base(V.ReturnAffinity())
        {
            this._Key = K;
            this._Value = V;
            this._F = F;
            this._Sig = 2;
        }

        public AggregateMinOf(Expression K, Expression V)
            : this(K, V, Filter.TrueForAll)
        {
        }

        public override List<int> FieldRefs
        {
            get
            {
                List<int> refs = new List<int>();
                refs.AddRange(Expressions.Analytics.AllFieldRefs(this._F.Node));
                refs.AddRange(Expressions.Analytics.AllFieldRefs(this._Key));
                refs.AddRange(Expressions.Analytics.AllFieldRefs(this._Value));
                return refs;
            }
        }

        public override Record Initialize()
        {
            return Record.Stitch(new Cell(this._Key.ReturnAffinity()), new Cell(this._Value.ReturnAffinity()));
        }

        public override void Accumulate(Record WorkData)
        {

            if (!this._F.Render()) return;

            Cell c = this._Key.Evaluate();
            Cell e = this._Value.Evaluate();
            Cell d = WorkData[0];
            if (!c.IsNull && !d.IsNull)
            {
                if (c < d)
                {
                    WorkData[0] = c;
                    WorkData[1] = e;
                }
            }
            else if (d.IsNull)
            {
                WorkData[0] = c;
                WorkData[1] = e;
            }

        }

        public override void Merge(Record WorkData, Record MergeIntoWorkData)
        {

            Cell a = WorkData[0];
            Cell c = WorkData[1];
            Cell b = MergeIntoWorkData[0];
            if (!a.IsNull && !b.IsNull)
            {

                if (a < b)
                {
                    MergeIntoWorkData[0] = a;
                    MergeIntoWorkData[1] = c;
                }

            }
            else if (!a.IsNull && b.IsNull)
            {
                MergeIntoWorkData[0] = a;
                MergeIntoWorkData[1] = c;
            }

        }

        public override Cell Evaluate(Record WorkData)
        {
            return WorkData[1];
        }

        public override Aggregate CloneOfMe()
        {
            return new AggregateMinOf(this._Key.CloneOfMe(), this._Value.CloneOfMe(), this._F.CloneOfMe());
        }

    }

    public sealed class AggregateFirst : Aggregate
    {

        private Expression _Map;

        public AggregateFirst(Expression M, Filter F)
            : base(M.ReturnAffinity())
        {
            this._Map = M;
            this._F = F;
            this._Sig = 1;
        }

        public AggregateFirst(Expression M)
            : this(M, Filter.TrueForAll)
        {
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
            return Record.Stitch(new Cell(this.ReturnAffinity));
        }

        public override void Accumulate(Record WorkData)
        {

            if (!this._F.Render()) return;

            Cell c = this._Map.Evaluate();
            Cell d = WorkData[0];
            if (d.IsNull)
            {
                WorkData[0] = c;
            }

        }

        public override void Merge(Record WorkData, Record MergeIntoWorkData)
        {

            // Do nothing becuase the MergeIntoWorkData is what we want!!!

        }

        public override Cell Evaluate(Record WorkData)
        {
            return WorkData[0];
        }

        public override Aggregate CloneOfMe()
        {
            return new AggregateFirst(this._Map.CloneOfMe(), this._F.CloneOfMe());
        }

    }

}




