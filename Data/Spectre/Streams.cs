using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data.Spectre
{

    /// <summary>
    /// The base class for all record readers
    /// </summary>
    public abstract class ReadStream
    {

        /// <summary>
        /// True if the stream can advance
        /// </summary>
        public abstract bool CanAdvance
        {
            get;
        }

        /// <summary>
        /// True if the stream can revert
        /// </summary>
        public abstract bool CanRevert
        {
            get;
        }

        /// <summary>
        /// Gets the columns of the underlying data structure
        /// </summary>
        public abstract Schema Columns
        {
            get;
        }

        /// <summary>
        /// Reads a record without advancing the stream
        /// </summary>
        /// <returns></returns>
        public abstract Record Read();

        /// <summary>
        /// Reads a record and advances the stream
        /// </summary>
        /// <returns></returns>
        public abstract Record ReadNext();

        /// <summary>
        /// Advances the stream one unit forward
        /// </summary>
        public abstract void Advance();

        /// <summary>
        /// Advances the stream up to N units forward; this will stop advancing if the end of the stream is reached
        /// </summary>
        /// <param name="Itterations"></param>
        public abstract void Advance(int Itterations);

        /// <summary>
        /// Reverts the stream back one unit
        /// </summary>
        public abstract void Revert();

        /// <summary>
        /// Reverts the stream back up to N units; this will stop reverting if the begining of the stream is reached
        /// </summary>
        /// <param name="Itterations"></param>
        public abstract void Revert(int Itterations);

        /// <summary>
        /// The position on the page of the current record
        /// </summary>
        /// <returns></returns>
        public abstract int RecordID();

        /// <summary>
        /// The ID of the current page being read
        /// </summary>
        /// <returns></returns>
        public abstract int PageID();

        /// <summary>
        /// The position, in terms of records read, of the current stream
        /// </summary>
        /// <returns></returns>
        public abstract long Position();

        /// <summary>
        /// The current position of the stream expressed as a record key
        /// </summary>
        public RecordKey PositionKey
        {
            get { return new RecordKey(this.PageID(), this.RecordID()); }
        }

    }

    /// <summary>
    /// The base class for all record writers
    /// </summary>
    public abstract class WriteStream
    {

        /// <summary>
        /// Gets the columns of the underlying data structure
        /// </summary>
        public abstract Schema Columns
        {
            get;
        }

        /// <summary>
        /// Inserts a record into the stream
        /// </summary>
        /// <param name="Value"></param>
        public abstract void Insert(Record Value);

        /// <summary>
        /// Inserts many records into the stream
        /// </summary>
        /// <param name="Value"></param>
        public abstract void BulkInsert(IEnumerable<Record> Value);

        /// <summary>
        /// Gets the total number of writes this stream has made
        /// </summary>
        /// <returns></returns>
        public abstract long WriteCount();

        /// <summary>
        /// Closes the stream, releasing all resources; this calls the 'PreSerialize' method form the page table
        /// </summary>
        public abstract void Close();

    }

    /// <summary>
    /// A basic read stream
    /// </summary>
    public class VanillaReadStream : ReadStream
    {

        protected BaseTable _Parent;
        protected Page _CurrentPage;
        protected int _CurrentPageID = -1;
        protected int _CurrentRecordIndex = -1;
        protected RecordKey _Lower;
        protected RecordKey _Upper;
        protected int _Ticks = 0;

        public VanillaReadStream(BaseTable Data, RecordKey LKey, RecordKey UKey)
            : base()
        {

            this._Lower = LKey;
            this._Upper = UKey;
            this._Parent = Data;
            if (Data.PageCount == 0)
            {
                this._CurrentPage = null;
                this._CurrentPageID = -1;
                this._CurrentRecordIndex = -1;
            }
            else
            {
                this._CurrentPage = this._Parent.GetPage(this._Lower.PAGE_ID);
                this._CurrentPageID = this._CurrentPage.PageID;
                this._CurrentRecordIndex = this._Lower.ROW_ID;
            }

        }

        public VanillaReadStream(BaseTable Data)
            : this(Data, VanillaReadStream.OriginKey(Data), VanillaReadStream.TerminalKey(Data))
        {
        }

        public override bool CanAdvance
        {

            get
            {

                if (this._CurrentPage == null)
                    return false;
                else if (this._CurrentPageID == -1)
                    return false;
                return !(this._CurrentPageID == this._Upper.PAGE_ID && this._CurrentRecordIndex > this._Upper.ROW_ID);

            }

        }

        public override bool CanRevert
        {
            get
            {
                if (this._CurrentPage == null)
                    return false;
                else if (this._CurrentPageID == -1)
                    return false;
                return !(this._CurrentPageID == this._Lower.PAGE_ID && this._CurrentRecordIndex < this._Lower.ROW_ID);
            }
        }

        public override Schema Columns
        {
            get { return this._Parent.Columns; }
        }

        public override void Advance()
        {

            this._CurrentRecordIndex++;
            if (this._CurrentRecordIndex >= this._CurrentPage.Count)
            {
                this._CurrentRecordIndex = 0;
                this._CurrentPageID = this._CurrentPage.NextPageID;

                if (this._CurrentPageID != -1)
                    this._CurrentPage = this._Parent.GetPage(this._CurrentPageID);

            }

            this._Ticks++;

        }

        public override void Advance(int Itterations)
        {
            for (int i = 0; i < Itterations; i++)
                this.Advance();
        }

        public override void Revert()
        {



            this._CurrentRecordIndex--;
            if (this._CurrentRecordIndex < 0)
            {

                this._CurrentPageID = this._CurrentPage.LastPageID;
                if (this._CurrentPageID != -1)
                {
                    this._CurrentPage = this._Parent.GetPage(this._CurrentPageID);
                    this._CurrentRecordIndex = this._CurrentPage.Count - 1;
                }

            }

            this._Ticks--;

        }

        public override void Revert(int Itterations)
        {
            for (int i = 0; i < Itterations; i++)
                this.Revert();
        }

        public override Record Read()
        {
            return this._CurrentPage.Select(this._CurrentRecordIndex);
        }

        public override Record ReadNext()
        {
            Record r = this.Read();
            this.Advance();
            return r;
        }

        public override int PageID()
        {
            return this._CurrentPage.PageID;
        }

        public override int RecordID()
        {
            return this._CurrentRecordIndex;
        }

        public override long Position()
        {
            return this._Ticks;
        }

        public static RecordKey OriginKey(BaseTable Parent)
        {
            if (Parent.OriginPageID == -1)
                return RecordKey.RecordNotFound;
            return new RecordKey(Parent.Header.OriginPageID, 0);
        }

        public static RecordKey TerminalKey(BaseTable Parent)
        {
            if (Parent.TerminalPageID == -1)
                return RecordKey.RecordNotFound;
            return new RecordKey(Parent.TerminalPageID, Parent.TerminalPage.Count - 1);
        }

    }

    /// <summary>
    /// A vanilla write stream
    /// </summary>
    public class VanillaWriteStream : WriteStream
    {

        private BaseTable _Parent;
        private long _Ticks = 0;

        public VanillaWriteStream(BaseTable Data)
            : base()
        {
            this._Parent = Data;
        }

        public override Schema Columns
        {
            get { return this._Parent.Columns; }
        } 

        public override void Close()
        {
            // do nothing
        }

        public override void Insert(Record Value)
        {
            this._Parent.Insert(Value);
        }

        public override void BulkInsert(IEnumerable<Record> Value)
        {
            this._Parent.Insert(Value);
        }

        public override long WriteCount()
        {
            return this._Ticks;
        }

    }

    /// <summary>
    /// Reads the record keys from an index
    /// </summary>
    public class IndexKeyReadStream : VanillaReadStream
    {

        private IndexHeader _Header;

        public IndexKeyReadStream(IndexHeader Header, BaseTable Storage, RecordKey LKey, RecordKey RKey)
            : base(Storage, LKey, RKey)
        {
            this._Header = Header;
        }

        public IndexKeyReadStream(IndexHeader Header, BaseTable Storage)
            : this(Header, Storage, VanillaReadStream.OriginKey(Storage), VanillaReadStream.TerminalKey(Storage))
        {
        }

        public RecordKey ReadKey()
        {
            return new RecordKey(this.Read()[this._Header.PointerIndex]);
        }

        public RecordKey ReadNextKey()
        {
            return new RecordKey(this.ReadNext()[this._Header.PointerIndex]);
        }

    }

    /// <summary>
    /// Reads data from an index
    /// </summary>
    public class IndexDataReadStream : VanillaReadStream
    {

        private IndexHeader _Header;
        private BaseTable _Parent;

        /// <summary>
        /// Opens an indexed reader
        /// </summary>
        /// <param name="Header">The index header</param>
        /// <param name="Storage">The table that stores the index pages</param>
        /// <param name="Parent">The table that stores the data pages; may be the same object as 'Storage'</param>
        /// <param name="LKey">The lower bound key</param>
        /// <param name="RKey">The upper bound key</param>
        public IndexDataReadStream(IndexHeader Header, BaseTable Storage, BaseTable Parent, RecordKey LKey, RecordKey RKey)
            : base(Storage, LKey, RKey)
        {
            this._Header = Header;
            this._Parent = Parent;
        }

        /// <summary>
        /// Opens an indexed reader
        /// </summary>
        /// <param name="Header">The index header</param>
        /// <param name="Storage">The table that stores the index pages</param>
        /// <param name="Parent">The table that stores the data pages; may be the same object as 'Storage'</param>
        public IndexDataReadStream(IndexHeader Header, BaseTable Storage, BaseTable Parent)
            : this(Header, Storage, Parent, VanillaReadStream.OriginKey(Storage), VanillaReadStream.TerminalKey(Storage))
        {
        }

        public RecordKey ReadKey()
        {
            return new RecordKey(base.Read()[this._Header.PointerIndex]);
        }

        public RecordKey ReadNextKey()
        {
            return new RecordKey(base.ReadNext()[this._Header.PointerIndex]);
        }

        public override Record Read()
        {

            RecordKey x = new RecordKey(base.Read()[this._Header.PointerIndex]);
            return this._Parent.GetPage(x.PAGE_ID).Select(x.ROW_ID);

        }

        public override Record ReadNext()
        {

            RecordKey x = new RecordKey(base.ReadNext()[this._Header.PointerIndex]);
            return this._Parent.GetPage(x.PAGE_ID).Select(x.ROW_ID);

        }

    }



}
