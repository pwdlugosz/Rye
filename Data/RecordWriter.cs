using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data
{
    
    public abstract class RecordWriter
    {

        public RecordWriter()
        {
        }

        public abstract void Insert(Record Data);

        public abstract void Close();

        public abstract Schema Columns { get;}

        public virtual void Insert(params Cell[] Values)
        {
            this.Insert(new Record(Values));
        }

        public virtual void Insert(RecordBuilder Builder)
        {
            this.Insert(Builder.ToRecord());
        }

        public virtual void BulkInsert(Extent Data)
        {
            foreach (Record r in Data.Cache)
                this.Insert(r);
        }

        public virtual bool IsChecked
        {
            get { return true; }
        }

    }

    public class ExtentWriter : RecordWriter
    {

        protected Extent _e;

        public ExtentWriter(Extent Data)
        {
            this._e = Data;
        }

        public override void Insert(Record Data)
        {
            this._e.Add(Data);
        }

        public override void Close()
        {
            
        }

        public override Schema Columns
        {
            get{ return this._e.Columns; }
        }

    }

    public sealed class UncheckedExtentWriter : ExtentWriter
    {

        public UncheckedExtentWriter(Extent Data)
            :base(Data)
        {
        }

        public override void Insert(Record Data)
        {
            this._e.UncheckedAdd(Data);
        }

        public override bool IsChecked
        {
            get
            {
                return false;
            }
        }

    }

    public class TableWriter : RecordWriter
    {

        protected Table _t;
        protected Extent _e;

        public TableWriter(Table Data)
        {
            this._t = Data;
            this._e = Data.NewShell();
        }

        public override void Insert(Record Data)
        {

            if (this._e.IsFull)
            {
                this._t.AddExtent(this._e);
                this._e = this._t.NewShell();
            }
            this._e.Add(Data);

        }

        public override void BulkInsert(Extent Data)
        {

            if (Data.Columns.GetHashCode() == this._t.Columns.GetHashCode())
                this._t.AddExtent(Data);
            else
                base.BulkInsert(Data);

        }

        public override void Close()
        {
            if (this._e.Count != 0)
            {
                this._t.AddExtent(this._e);
            }
            this._t.RequestFlushMe();
        }

        public override Schema Columns
        {
            get { return this._t.Columns; }
        }

    }

    public sealed class UncheckedTableWriter : TableWriter
    {

        public UncheckedTableWriter(Table Data)
            :base(Data)
        {
        }

        public override void Insert(Record Data)
        {
            if (this._e.IsFull)
            {
                this._t.AddExtent(this._e);
                this._e = this._t.NewShell();
            }
            this._e.UncheckedAdd(Data);
        }

        public override void BulkInsert(Extent Data)
        {
            if (Data.Columns.GetHashCode() == this._t.Columns.GetHashCode())
                this._t.AddExtent(Data);
            else
                base.BulkInsert(Data);
        }

        public override void Close()
        {
            if (this._e.Count != 0)
            {
                this._t.AddExtent(this._e);
            }
            this._t.RequestFlushMe();
        }

        public override Schema Columns
        {
            get { return this._t.Columns; }
        }

        public override bool IsChecked
        {
            get
            {
                return false;
            }
        }

    }

}
