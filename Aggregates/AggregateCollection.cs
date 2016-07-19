using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Aggregates
{

    public sealed class AggregateCollection
    {

        private List<Aggregate> _cache;
        private List<string> _alias;
        
        // Constructor //
        public AggregateCollection()
        {
            this._cache = new List<Aggregate>();
            this._alias = new List<string>();
        }

        // Properties //
        public int Count
        {
            get { return this._cache.Count; }
        }

        public Schema Columns
        {
            get
            {
                Schema s = new Schema();
                for (int i = 0; i < this.Count; i++)
                {
                    s.Add(this._alias[i], this._cache[i].ReturnAffinity, true, this._cache[i].Size());
                }
                return s;
            }
        }

        public Schema GetInterimSchema
        {
            get
            {
                Schema s = new Schema();
                Record r = this.Initialize().ToRecord();
                for (int i = 0; i < r.Count; i++)
                {
                    s.Add("R" + i.ToString(), r[i].Affinity);
                }
                return s;
            }
        }

        public Aggregate this[int index]
        {
            get { return this._cache[index]; }
        }

        public string GetAlias(int Index)
        {
            return this._alias[Index];
        }

        public int[] Signiture
        {
            get
            {
                return this._cache.ConvertAll((r) => { return r.Signiture; }).ToArray();
            }
        }

        // Methods //
        public void Add(Aggregate R, string Alias)
        {
            this._cache.Add(R);
            this._alias.Add(Alias);
        }

        public void Add(Aggregate R)
        {
            this.Add(R, "R" + this._alias.Count.ToString());
        }

        public List<int> FieldRefs
        {
            get
            {
                List<int> field_refs = new List<int>();
                foreach (Aggregate a in this._cache)
                {
                    field_refs.AddRange(a.FieldRefs);
                }
                return field_refs.Distinct().ToList();
            }
        }

        public CompoundRecord Initialize()
        {
            
            CompoundRecord cr = new CompoundRecord(this.Count);
            for (int i = 0; i < this.Count; i++)
            {
                cr[i] = this._cache[i].Initialize();
            }
            return cr;

        }

        public void Accumulate(CompoundRecord WorkData)
        {
            for (int i = 0; i < this.Count; i++)
            {
                this._cache[i].Accumulate(WorkData[i]);
            }
        }

        public void Merge(CompoundRecord WorkData, CompoundRecord MergeIntoWorkData)
        {
            for (int i = 0; i < this.Count; i++)
            {
                this._cache[i].Merge(WorkData[i], MergeIntoWorkData[i]);
            }
        }

        public Record Evaluate(CompoundRecord WorkData)
        {
            List<Cell> c = new List<Cell>(this.Count);
            for (int i = 0; i < this.Count; i++)
            {
                c.Add(this._cache[i].Evaluate(WorkData[i]));
            }
            return new Record(c.ToArray());
        }

        public void Clear()
        {
            this._cache.Clear();
            this._alias.Clear();
        }

        public AggregateCollection CloneOfMe()
        {

            AggregateCollection agg = new AggregateCollection();
            for (int i = 0; i < this.Count; i++)
            {
                agg.Add(this._cache[i].CloneOfMe(), this._alias[i]);
            }
            return agg;

        }

    }

}
