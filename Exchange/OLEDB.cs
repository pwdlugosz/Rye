using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;
using Rye.Data;

namespace Rye.Exchange
{

    //public static class RyeOLEDBHelper
    //{

    //    public static Schema RenderSchema(System.Data.DataTable Source)
    //    {

    //        Schema s = new Schema();

    //        foreach (System.Data.DataRow r in Source.Rows)
    //        {

    //            string name = r[0].ToString();
    //            CellAffinity affinity = CellAffinityHelper.Render((Type)r[5]);
    //            int size = 8;
    //            if (affinity == CellAffinity.BLOB || affinity == CellAffinity.STRING)
    //                size = (int)r[3];

    //            s.Add(name, affinity, size);

    //        }

    //        return s;

    //    }

    //    public static bool[] RenderValidFields(System.Data.DataTable Source)
    //    {

    //        List<bool> InvalidFields = new List<bool>();

    //        foreach (System.Data.DataRow r in Source.Rows)
    //        {
    //            InvalidFields.Add(CellAffinityHelper.IsValidType((Type)r[5]));
    //        }

    //        return InvalidFields.ToArray();

    //    }

    //    public static long ConsumeReader(DbDataReader ReadStream, RecordWriter Consumer)
    //    {

    //        // Step One: Check the schemas //
    //        Schema input = RyeOLEDBHelper.RenderSchema(ReadStream.GetSchemaTable());
    //        bool[] valid = RyeOLEDBHelper.RenderValidFields(ReadStream.GetSchemaTable());
    //        if (input.Count != Consumer.Columns.Count)
    //            throw new ArgumentException(string.Format("DbDataReader has {0} columns but RecordWriter has {1}", input.Count, Consumer.Columns));

    //        // Step Two: handle un-checked writers //
    //        if (!Consumer.IsChecked && input.GetHashCode() != Consumer.Columns.GetHashCode())
    //            throw new ArgumentException(string.Format("Schemas are invalid; schemas must match if the consumer is an un-checked writer"));

    //        // Step Three: create a casting object to conver the records //
    //        RyeOLEDBRecordConverter caster = new RyeOLEDBRecordConverter(Consumer.Columns, valid);

    //        // Step Four: copy over the data //
    //        long Ticks = 0;
    //        while (ReadStream.Read())
    //        {

    //            // Get the record //
    //            Record r = caster.Render(ReadStream);

    //            // Append the record //
    //            Consumer.InsertKey(r);

    //            // Increment the tracker //
    //            Ticks++;

    //        }

    //        return Ticks;

    //    }

    //    public static Extent ConsumeReader(DbDataReader ReadStream, string Name, long MaxRecordCount)
    //    {

    //        // Get the columns //
    //        Schema input = RyeOLEDBHelper.RenderSchema(ReadStream.GetSchemaTable());
            
    //        // Get the valid fields //
    //        bool[] valid = RyeOLEDBHelper.RenderValidFields(ReadStream.GetSchemaTable());

    //        // Create the converter //
    //        RyeOLEDBRecordConverter caster = new RyeOLEDBRecordConverter(input, valid);

    //        // Create the extent //
    //        Extent e = new Extent(input, Header.NewMemoryOnlyExtentHeader(Name, input.Count, MaxRecordCount));
    //        RecordWriter w = e.OpenUncheckedWriter(input.GetHashCode());

    //        // Consume the data //
    //        while (ReadStream.Read())
    //        {

    //            // Get the record //
    //            Record r = caster.Render(ReadStream);

    //            // Append the record //
    //            w.InsertKey(r);

    //        }

    //        // Close //
    //        w.Close();

    //        return e;

    //    }

    //    public static BaseTable ConsumeReader(DbDataReader ReadStream, Kernel Driver, string Dir, string Name, long MaxRecordCount)
    //    {

    //        // Get the columns //
    //        Schema input = RyeOLEDBHelper.RenderSchema(ReadStream.GetSchemaTable());

    //        // Get the valid fields //
    //        bool[] valid = RyeOLEDBHelper.RenderValidFields(ReadStream.GetSchemaTable());

    //        // Create the converter //
    //        RyeOLEDBRecordConverter caster = new RyeOLEDBRecordConverter(input, valid);

    //        // Create the extent //
    //        BaseTable t = new BaseTable(Driver, Dir, Name, input, MaxRecordCount);
    //        RecordWriter w = t.OpenUncheckedWriter(input.GetHashCode());

    //        // Consume the data //
    //        while (ReadStream.Read())
    //        {

    //            // Get the record //
    //            Record r = caster.Render(ReadStream);

    //            // Append the record //
    //            w.InsertKey(r);

    //        }

    //        // Close //
    //        w.Close();

    //        return t;

    //    }

    //    public static long ConsumeTable(System.Data.DataTable T, RecordWriter Consumer)
    //    {
    //        return RyeOLEDBHelper.ConsumeReader(T.CreateDataReader(), Consumer);
    //    }

    //    public static Extent ConsumeTable(System.Data.DataTable T, string Name, long MaxRecordCount)
    //    {
    //        return RyeOLEDBHelper.ConsumeReader(T.CreateDataReader(), Name, MaxRecordCount);
    //    }

    //    public static BaseTable ConsumeTable(System.Data.DataTable T, Kernel Driver, string Dir, string Name, long MaxRecordCount)
    //    {
    //        return RyeOLEDBHelper.ConsumeReader(T.CreateDataReader(), Driver, Dir, Name, MaxRecordCount);
    //    }

    //}

    //public sealed class RyeOLEDBRecordConverter
    //{

    //    private Schema _BaseSchema;
    //    private bool[] _ValidFields;
        
    //    public RyeOLEDBRecordConverter(Schema Columns, bool[] ValidFields)
    //    {

    //        if (ValidFields.Length != Columns.Count)
    //            throw new ArgumentException("The invalid field bit array and the schema must have the same length");
    //        this._BaseSchema = Columns;
    //        this._ValidFields = ValidFields;
    //    }

    //    public RyeOLEDBRecordConverter(Schema Columns)
    //        : this(Columns, RyeOLEDBRecordConverter.Fields(Columns.Count, true))
    //    {
    //    }

    //    public Record Render(DbDataRecord Key)
    //    {

    //        if (Key.FieldCount != this._BaseSchema.Count)
    //            throw new ArgumentException(string.Format("Record passed has {0} elements but schema expects {1} elements", Key.FieldCount, this._BaseSchema.Count));

    //        RecordBuilder rb = new RecordBuilder();
    //        for (int i = 0; i < this._BaseSchema.Count; i++)
    //        {

    //            if (Key.IsDBNull(i) || Key[i] == null || !this._ValidFields[i])
    //            {
    //                rb.Add(new Cell(this._BaseSchema.ColumnAffinity(i)));
    //            }
    //            if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.BOOL)
    //            {
    //                rb.Add(Key.GetBoolean(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.INT)
    //            {
    //                rb.Add(Key.GetInt64(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.DOUBLE)
    //            {
    //                rb.Add(Key.GetDouble(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.DATE_TIME)
    //            {
    //                rb.Add(Key.GetDateTime(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.STRING)
    //            {
    //                rb.Add(Key.GetString(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.BLOB)
    //            {
    //                byte[] Mem = new byte[this._BaseSchema.ColumnSize(i)];
    //                Key.GetBytes(i, 0, Mem, 0, Mem.Length);
    //                rb.Add(Mem);
    //            }
    //            else
    //            {
    //                rb.Add(Cell.TryUnBoxInto(Key[i], this._BaseSchema.ColumnAffinity(i)));
    //            }

    //        }

    //        return rb.ToRecord();

    //    }

    //    public Record Render(DbDataReader Stream)
    //    {

    //        if (Stream.FieldCount != this._BaseSchema.Count)
    //            throw new ArgumentException(string.Format("Record passed has {0} elements but schema expects {1} elements", Stream.FieldCount, this._BaseSchema.Count));

    //        RecordBuilder rb = new RecordBuilder();
    //        for (int i = 0; i < this._BaseSchema.Count; i++)
    //        {

    //            if (Stream.IsDBNull(i) || Stream[i] == null || !this._ValidFields[i])
    //            {
    //                rb.Add(new Cell(this._BaseSchema.ColumnAffinity(i)));
    //            }
    //            if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.BOOL)
    //            {
    //                rb.Add(Stream.GetBoolean(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.INT)
    //            {
    //                rb.Add(Stream.GetInt64(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.DOUBLE)
    //            {
    //                rb.Add(Stream.GetDouble(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.DATE_TIME)
    //            {
    //                rb.Add(Stream.GetDateTime(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.STRING)
    //            {
    //                rb.Add(Stream.GetString(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.BLOB)
    //            {
    //                byte[] Mem = new byte[this._BaseSchema.ColumnSize(i)];
    //                Stream.GetBytes(i, 0, Mem, 0, Mem.Length);
    //                rb.Add(Mem);
    //            }
    //            else
    //            {
    //                rb.Add(Cell.TryUnBoxInto(Stream[i], this._BaseSchema.ColumnAffinity(i)));
    //            }

    //        }

    //        return rb.ToRecord();

    //    }

    //    public Record Render(System.Data.IDataRecord Key)
    //    {

    //        if (Key.FieldCount != this._BaseSchema.Count)
    //            throw new ArgumentException(string.Format("Record passed has {0} elements but schema expects {1} elements", Key.FieldCount, this._BaseSchema.Count));

    //        RecordBuilder rb = new RecordBuilder();
    //        for (int i = 0; i < this._BaseSchema.Count; i++)
    //        {

    //            if (Key.IsDBNull(i) || Key[i] == null || !this._ValidFields[i])
    //            {
    //                rb.Add(new Cell(this._BaseSchema.ColumnAffinity(i)));
    //            }
    //            if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.BOOL)
    //            {
    //                rb.Add(Key.GetBoolean(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.INT)
    //            {
    //                rb.Add(Key.GetInt64(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.DOUBLE)
    //            {
    //                rb.Add(Key.GetDouble(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.DATE_TIME)
    //            {
    //                rb.Add(Key.GetDateTime(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.STRING)
    //            {
    //                rb.Add(Key.GetString(i));
    //            }
    //            else if (this._BaseSchema.ColumnAffinity(i) == CellAffinity.BLOB)
    //            {
    //                byte[] Mem = new byte[this._BaseSchema.ColumnSize(i)];
    //                Key.GetBytes(i, 0, Mem, 0, Mem.Length);
    //                rb.Add(Mem);
    //            }
    //            else
    //            {
    //                rb.Add(Cell.TryUnBoxInto(Key[i], this._BaseSchema.ColumnAffinity(i)));
    //            }

    //        }

    //        return rb.ToRecord();

    //    }

    //    private static bool[] Fields(int Length, bool Value)
    //    {

    //        List<bool> v = new List<bool>();
    //        for (int i = 0; i < Length; i++)
    //        {
    //            v.Add(Value);
    //        }

    //        return v.ToArray();

    //    }

    //}



}
