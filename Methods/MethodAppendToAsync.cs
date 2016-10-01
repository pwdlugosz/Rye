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
        private long _Writes = 0;

        public MethodAppendToAsync(Method Parent, Table UseParentData, ExpressionCollection UseFields)
            : base(Parent)
        {


            if (UseParentData.Columns.Count != UseFields.Columns.Count)
                throw new ArgumentException("Output table and fields passed are not compatible");

            this._tParentData = UseParentData;
            this._eParentData = null;
            this._RecordCache = new Extent(UseParentData.Columns);
            this._RecordCache.Header.PageSize = UseParentData.Header.PageSize;
            this._Fields = UseFields;
            this._IsTable = true;

        }

        public MethodAppendToAsync(Method Parent, Extent UseParentData, ExpressionCollection UseFields)
            : base(Parent)
        {

            if (UseParentData.Columns.Count != UseFields.Columns.Count)
                throw new ArgumentException("Output table and fields passed are not compatible");

            this._tParentData = null;
            this._eParentData = UseParentData;
            this._RecordCache = new Extent(UseParentData.Columns);
            this._RecordCache.Header.PageSize = UseParentData.Header.PageSize;
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
                this._RecordCache.Header.PageSize = this._tParentData.Header.PageSize;
            }
            else if (this._RecordCache.IsFull)
            {
                throw new IndexOutOfRangeException("Shard is full; cannot add any more records");
            }

            // Accumulate the record //
            this._RecordCache.Add(this._Fields.Evaluate());
            this._Writes++;

        }

        public override void EndInvoke()
        {

            // Append the data to the table //
            if (this._RecordCache.Count != 0 && this._IsTable)
            {

                this._tParentData.AddExtent(this._RecordCache);

            }
            // Otherwise push all records into the extent //
            else if (this._RecordCache.Count != 0)
            {

                foreach (Record r in this._RecordCache.Records)
                {
                    this._eParentData.Add(r);
                }

            }

            // Invoke the child nodes, which is usually the sort and export nodes //
            this.InvokeChildren();

        }

        public override Method CloneOfMe()
        {
            if (this._IsTable)
                return new MethodAppendToAsync(this.Parent, this._tParentData, this._Fields);
            else
                return new MethodAppendToAsync(this.Parent, this._eParentData, this._Fields);
        }

        public override string Message()
        {
            return string.Format("Append: {0}", this._Writes);
        }

        public static Method Optimize(Method Parent, Table UseParentData, ExpressionCollection UseFields)
        {

            if (UseParentData.Columns.GetHashCode() == UseFields.Columns.GetHashCode())
                return new MethodAppendToAsyncFast(Parent, UseParentData, UseFields);
            return new MethodAppendToAsync(Parent, UseParentData, UseFields);

        }

        public static Method Optimize(Method Parent, Extent UseParentData, ExpressionCollection UseFields)
        {

            if (UseParentData.Columns.GetHashCode() == UseFields.Columns.GetHashCode())
                return new MethodAppendToAsyncFast(Parent, UseParentData, UseFields);
            return new MethodAppendToAsync(Parent, UseParentData, UseFields);

        }

    }

    public sealed class MethodAppendToAsyncFast : Method
    {

        private Table _tParentData;
        private Extent _eParentData;
        private Extent _RecordCache;
        private ExpressionCollection _Fields;
        private bool _IsTable = false;
        private long _Writes = 0;

        private bool _IsFull;
        private int _CurrentCount = 0;
        private int _MaxCount = 0;

        public MethodAppendToAsyncFast(Method Parent, Table UseParentData, ExpressionCollection UseFields)
            : base(Parent)
        {


            if (UseParentData.Columns.Count != UseFields.Columns.Count)
                throw new ArgumentException("Output table and fields passed are not compatible");

            this._tParentData = UseParentData;
            this._eParentData = null;
            this._RecordCache = new Extent(UseParentData.Columns);
            this._RecordCache.Header.PageSize = UseParentData.Header.PageSize;
            this._Fields = UseFields;
            this._IsTable = true;

        }

        public MethodAppendToAsyncFast(Method Parent, Extent UseParentData, ExpressionCollection UseFields)
            : base(Parent)
        {

            if (UseParentData.Columns.Count != UseFields.Columns.Count)
                throw new ArgumentException("Output table and fields passed are not compatible");

            this._tParentData = null;
            this._eParentData = UseParentData;
            this._RecordCache = new Extent(UseParentData.Columns);
            this._RecordCache.Header.PageSize = UseParentData.Header.PageSize;
            this._Fields = UseFields;
            this._IsTable = false;

        }

        // Base Implementations //
        public override void BeginInvoke()
        {
            this._CurrentCount = this._RecordCache.Count;
            this._MaxCount = (int)this._RecordCache.MaxRecordEstimate;
            this._IsFull = this._CurrentCount >= this._MaxCount;
        }

        public override void Invoke()
        {

            // Check if full //
            this._IsFull = this._CurrentCount >= this._MaxCount;

            // Sink the cache to the parent table //
            if (this._IsFull && this._IsTable)
            {
                this._tParentData.AddExtent(this._RecordCache);
                this._RecordCache = new Extent(this._tParentData.Columns);
                this._RecordCache.Header.PageSize = this._tParentData.Header.PageSize;
                this._CurrentCount = 0;
                this._MaxCount = (int)this._RecordCache.MaxRecordEstimate;
            }
            else if (this._IsFull)
            {
                throw new IndexOutOfRangeException("Shard is full; cannot add any more records");
            }

            // Accumulate the record //
            this._RecordCache._Cache.Add(this._Fields.Evaluate());
            this._CurrentCount++;
            this._Writes++;

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

            // Invoke the child nodes, which is usually the sort and export nodes //
            this.InvokeChildren();

        }

        public override Method CloneOfMe()
        {
            if (this._IsTable)
                return new MethodAppendToAsyncFast(this.Parent, this._tParentData, this._Fields);
            else
                return new MethodAppendToAsyncFast(this.Parent, this._eParentData, this._Fields);
        }

        public override string Message()
        {
            return string.Format("Append: {0}", this._Writes);
        }

    }

}
