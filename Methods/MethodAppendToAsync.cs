using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Methods
{
    
    public sealed class MethodAppendToAsync : Method
    {

        private Table _tParentData;
        private Extent _eParentData;
        private Extent _RecordCache;
        private ExpressionCollection _Fields;
        private bool _IsTable = false;

        public MethodAppendToAsync(Method Parent, Table UseParentData, ExpressionCollection UseFields)
            : base(Parent)
        {

            if (UseParentData.Columns.Count != UseFields.Columns.Count)
                throw new ArgumentException("Output table and fields passed are not compatible");

            this._tParentData = UseParentData;
            this._eParentData = null;
            this._RecordCache = new Extent(UseParentData.Columns);
            this._RecordCache.MaxRecords = UseParentData.MaxRecords;
            this._Fields = UseFields;
            this._IsTable = true;

        }

        public MethodAppendToAsync(Method Parent, Extent UseParentData, ExpressionCollection UseFields)
            : base(Parent)
        {

            if (UseParentData.Columns.GetHashCode() != UseFields.Columns.GetHashCode())
                throw new ArgumentException("Output table and fields passed are not compatible");

            this._tParentData = null;
            this._eParentData = UseParentData;
            this._RecordCache = new Extent(UseParentData.Columns);
            this._RecordCache.MaxRecords = UseParentData.MaxRecords;
            this._Fields = UseFields;
            this._IsTable = false;

        }

        // Base Implementations //
        public override void Invoke()
        {

            // Sink the cache to the parent table //
            if (this._RecordCache.IsFull && this._IsTable)
            {
                this._tParentData.AddExtent(this._RecordCache);
                this._RecordCache = new Extent(this._tParentData.Columns);
                this._RecordCache.MaxRecords = this._tParentData.MaxRecords;
            }
            else if (this._RecordCache.IsFull)
            {
                throw new IndexOutOfRangeException("Extent is full; cannot add any more records");
            }

            // Accumulate the record //
            this._RecordCache.Add(this._Fields.Evaluate());

        }

        public override void EndInvoke()
        {

            if (this._RecordCache.Count != 0 && this._IsTable)
            {

                this._tParentData.AddExtent(this._RecordCache);

            }
            else if (this._RecordCache.Count != 0)
            {

                foreach (Record r in this._RecordCache.Records)
                {
                    this._eParentData.Add(r);
                }

            }

        }

        public override Method CloneOfMe()
        {
            if (this._IsTable)
                return new MethodAppendToAsync(this.Parent, this._tParentData, this._Fields);
            else
                return new MethodAppendToAsync(this.Parent, this._eParentData, this._Fields);
        }

    }

}
