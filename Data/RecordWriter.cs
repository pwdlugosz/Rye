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

    public sealed class ExtentWriter : RecordWriter
    {

        private Extent _e;

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

    public sealed class TableWriter : RecordWriter
    {

        private Table _t;
        private Extent _e;

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


}
