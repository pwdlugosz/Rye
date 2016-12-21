using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Aggregates
{

    public abstract class Aggregate
    {

        protected CellAffinity _ReturnAffinity;
        protected Filter _F;
        protected int _Sig = 1;

        public Aggregate(CellAffinity NewAffinity, Filter F)
        {
            this._ReturnAffinity = NewAffinity;
            this._F = F;
        }

        public Aggregate(CellAffinity NewAffinity)
            : this(NewAffinity, Filter.TrueForAll)
        {
        }

        public CellAffinity ReturnAffinity
        {
            get { return this._ReturnAffinity; }
        }

        public Filter BaseFilter
        {
            get { return this._F; }
            set { this._F = value; }
        }

        internal int Signiture
        {
            get { return this._Sig; }
        }

        public virtual List<int> FieldRefs
        {
            get 
            { 
                return Expressions.Analytics.AllFieldRefs(this._F.Node); 
            }
        }

        public abstract Record Initialize();

        public abstract void Accumulate(Record WorkData);

        public abstract void Merge(Record WorkData, Record MergeIntoWorkData);

        public abstract Cell Evaluate(Record WorkData);

        public virtual int Size()
        {
            return Schema.FixSize(this.ReturnAffinity, -1);
        }

        public abstract Aggregate CloneOfMe();

        public abstract List<Expression> InnerExpressions();
        
    }

}
