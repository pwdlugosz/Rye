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

        public void Insert(params Cell[] Values)
        {
            Record r = new Record(Values);
            this.Insert(r);
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

    }

    public class TableWriter : RecordWriter
    {

        protected Table _t;
        protected Extent _e;

        public TableWriter(Table Data)
        {
            this._t = Data;
            this._e = Data.PopLastOrGrow();
        }

        public override void Insert(Record Data)
        {

            if (this._e.IsFull)
            {
                this._t.SetExtent(this._e);
                this._e = this._t.Grow();
            }
            this._e.Add(Data);

        }

        public override void Close()
        {
            this._t.SetExtent(this._e);
            Kernel.RequestFlushTable(this._t);
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
            this._e.UncheckedAdd(Data);
        }

    }

}
