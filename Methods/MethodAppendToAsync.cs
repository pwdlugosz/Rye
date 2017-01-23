using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Methods
{
    
    public sealed class MethodAppendToAsync2 : Method
    {

        private Table _tParentData;
        private Extent _eParentData;
        private Extent _RecordCache;
        private ExpressionCollection _Fields;
        private bool _IsTable = false;
        private long _Writes = 0;

        public MethodAppendToAsync2(Method Parent, Table UseParentData, ExpressionCollection UseFields)
            : base(Parent)
        {

            if (UseParentData.Columns.Count != UseFields.Columns.Count)
                throw new ArgumentException("Output table and fields passed are not compatible");

            this._tParentData = UseParentData;
            this._eParentData = null;
            this._Fields = UseFields;
            this._IsTable = true;

        }

        public MethodAppendToAsync2(Method Parent, Extent UseParentData, ExpressionCollection UseFields)
            : base(Parent)
        {

            if (UseParentData.Columns.Count != UseFields.Columns.Count)
                throw new ArgumentException("Output table and fields passed are not compatible");

            this._tParentData = null;
            this._eParentData = UseParentData;
            this._Fields = UseFields;
            this._IsTable = false;

        }

        // Base Implementations //
        public override void BeginInvoke()
        {

            if (this._IsTable)
            {
                this._RecordCache = this._tParentData.NewShell();
                this._RecordCache.Header.PageSize = this._tParentData.Header.PageSize;
            }
            else
            {
                this._RecordCache = new Extent(this._eParentData.Columns);
                this._RecordCache.Header.PageSize = this._eParentData.Header.PageSize;
            }

        }

        public override void Invoke()
        {

            // Sink the cache to the parent table //
            if (this._RecordCache.IsFull && this._IsTable)
            {
                this._tParentData.AddExtent(this._RecordCache);
                this._RecordCache = this._tParentData.NewShell();
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

                foreach (Record r in this._RecordCache.Cache)
                {
                    this._eParentData.Add(r);
                }

            }

            // Invoke the child nodes, which is usually the sort and export nodes //
            this.InvokeChildren();
            
        }

        public override Method CloneOfMe()
        {

            Method x;
            if (this._IsTable)
                x = new MethodAppendToAsync2(this.Parent, this._tParentData, this._Fields.CloneOfMe());
            else
                x = new MethodAppendToAsync2(this.Parent, this._eParentData, this._Fields.CloneOfMe());

            Method.AppendClonedChildren(this, x);

            return x;

        }

        public override string Message()
        {
            return string.Format("Append: {0}", this._Writes);
        }

        /*
        public static Method Optimize(Method Parent, BaseTable UseParentData, ExpressionCollection UseFields)
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
        */

        public override List<Expression> InnerExpressions()
        {
            return this._Fields.Nodes.ToList();
        }

    }

    public sealed class MethodAppendToAsync : Method
    {

        private TabularData _Source;
        private IConcurrentWriteManager _Manager;
        private Extent _WorkData;
        private ExpressionCollection _Fields;
        private long _Writes = 0;

        public MethodAppendToAsync(Method Parent, TabularData UseParentData, ExpressionCollection UseFields)
            : base(Parent)
        {

            if (UseParentData.Columns.Count != UseFields.Columns.Count)
                throw new ArgumentException("Output table and fields passed are not compatible");

            this._Source = UseParentData;
            this._Manager = UseParentData.ConcurrentWriteManager;
            this._Fields = UseFields;
            this._WorkData = UseParentData.ConcurrentWriteManager.GetExtent();

        }

        public override void Invoke()
        {

            // Sink the cache to the parent data //
            if (this._WorkData.IsFull)
            {
                this._Manager.AddExtent(this._WorkData);
                this._WorkData = this._Manager.GetExtent();
            }

            // Accumulate the record //
            this._WorkData.Add(this._Fields.Evaluate());
            this._Writes++;

        }

        public override void EndInvoke()
        {

            // Append the data to the table //
            if (this._WorkData.Count > 0)
            {
                this._Manager.AddExtent(this._WorkData);
                this._Manager.Collapse();
            }
            
            // Invoke the child nodes, which is usually the sort and export nodes //
            this.InvokeChildren();

        }

        public override Method CloneOfMe()
        {

            Method x = new MethodAppendToAsync(this.Parent, this._Source, this._Fields.CloneOfMe());
            
            Method.AppendClonedChildren(this, x);

            return x;

        }

        public override string Message()
        {
            return string.Format("Append: {0}", this._Writes);
        }

        public override List<Expression> InnerExpressions()
        {
            return this._Fields.Nodes.ToList();
        }

    }


    /*
    public sealed class MethodAppendToAsyncFast : Method
    {

        private BaseTable _tParentData;
        private Extent _eParentData;
        private Extent _RecordCache;
        private ExpressionCollection _Fields;
        private bool _IsTable = false;
        private long _Writes = 0;

        private bool _IsFull;
        private int _CurrentCount = 0;
        private int _MaxCount = 0;

        public MethodAppendToAsyncFast(Method Parent, BaseTable UseParentData, ExpressionCollection UseFields)
            : base(Parent)
        {

            if (UseParentData.Columns.Count != UseFields.Columns.Count)
                throw new ArgumentException("Output table and fields passed are not compatible");

            this._tParentData = UseParentData;
            this._eParentData = null;
            this._RecordCache = this._tParentData.PopLastOrGrow();
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
                this._tParentData.SetExtent(this._RecordCache);
                this._RecordCache = this._tParentData.Grow();
                this._RecordCache.Header.PageSize = this._tParentData.Header.PageSize;
                this._CurrentCount = 0;
                this._MaxCount = (int)this._RecordCache.MaxRecordEstimate;
            }
            else if (this._IsFull)
            {
                throw new IndexOutOfRangeException("Shard is full; cannot add any more records");
            }

            // Accumulate the record //
            this._RecordCache._Data.Add(this._Fields.Evaluate());
            this._CurrentCount++;
            this._Writes++;
            
        }

        public override void EndInvoke()
        {

            if (this._RecordCache.Count != 0 && this._IsTable)
            {

                this._tParentData.SetExtent(this._RecordCache);

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
                return new MethodAppendToAsyncFast(this.Parent, this._tParentData, this._Fields.CloneOfMe());
            else
                return new MethodAppendToAsyncFast(this.Parent, this._eParentData, this._Fields.CloneOfMe());
        }

        public override string Message()
        {
            return string.Format("Append: {0}", this._Writes);
        }

        public override List<Expression> InnerExpressions()
        {
            return this._Fields.Nodes.ToList();
        }

    }
    */

}
