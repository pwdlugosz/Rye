using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Exchange
{

    public static class ObjectExchange
    {

        // ShartTable / Shard Exchange //
        //public static Extent TableToExtent(BaseTable Data)
        //{

        //    return Data.PopFirstOrGrow();

        //}

        //public static BaseTable ExtentToTable(Kernel Driver, Extent Data, string Dir, string Name)
        //{

        //    BaseTable t = BaseTable.CreateShardedTable(Driver, Dir, Name, Data.Columns);
        //    RecordWriter w = t.OpenUncheckedWriter(Data.Columns.GetHashCode());
        //    foreach (Record r in Data.Records)
        //    {
        //        w.InsertKey(r);
        //    }
        //    w.Close();

        //    return t;

        //}

        // Shard / Matrix Exchange //
        
        public static void RazeMatrix(CellMatrix Element, RecordWriter Stream)
        {

            for (int i = 0; i < Element.RowCount; i++)
            {

                RecordBuilder rb = new RecordBuilder();
                for (int j = 0; j < Element.ColumnCount; j++)
                {
                    rb.Add(Element[i, j]);
                }
                Stream.Insert(rb.ToRecord());

            }

        }

        public static Extent RazeMatrix(CellMatrix Element)
        {

            Schema s = new Schema();
            for (int i = 0; i < Element.ColumnCount; i++)
                s.Add("F" + i.ToString(), Element.Affinity);

            Extent e = new Extent(s);
            ObjectExchange.RazeMatrix(Element, e.OpenWriter());
            return e;

        }

        public static CellMatrix RenderMatrix(Extent Data, CellAffinity Type)
        {

            CellMatrix m = new CellMatrix(Data.Count, Data.Columns.Count, Cell.ZeroValue(Type));

            for (int i = 0; i < Data.Count; i++)
            {

                for (int j = 0; j < Data.Columns.Count; j++)
                {
                    Cell c = Data[i][j];
                    m[i, j] = (c.AFFINITY == Type ? c : Cell.Cast(c, Type));
                }

            }

            return m;

        }

        // Data Set //
        public static System.Data.DataTable RenderDataTable(Extent Data)
        {

            // Build the table and schema //
            System.Data.DataTable t = new System.Data.DataTable(Data.Header.Name);
            for (int i = 0; i < Data.Columns.Count; i++)
            {

                CellAffinity ca = Data.Columns.ColumnAffinity(i);
                if (ca == CellAffinity.BOOL)
                    t.Columns.Add(Data.Columns.ColumnName(i), typeof(bool));
                else if (ca == CellAffinity.INT)
                    t.Columns.Add(Data.Columns.ColumnName(i), typeof(long));
                else if (ca == CellAffinity.DOUBLE)
                    t.Columns.Add(Data.Columns.ColumnName(i), typeof(double));
                else if (ca == CellAffinity.DATE_TIME)
                    t.Columns.Add(Data.Columns.ColumnName(i), typeof(DateTime));
                else if (ca == CellAffinity.STRING)
                    t.Columns.Add(Data.Columns.ColumnName(i), typeof(string));
                else if (ca == CellAffinity.BLOB)
                    t.Columns.Add(Data.Columns.ColumnName(i), typeof(byte[]));

            }

            // Load the data //
            
            for (int i = 0; i < Data.Count; i++)
            {
                
                System.Data.DataRow r = t.NewRow();
                for (int j = 0; j < Data.Columns.Count; j++)
                {

                    Cell c = Data[i][j];
                    if (c.AFFINITY == CellAffinity.BOOL)
                        r[j] = c.valueBOOL;
                    else if (c.AFFINITY == CellAffinity.INT)
                        r[j] = c.valueBOOL;
                    else if (c.AFFINITY == CellAffinity.DOUBLE)
                        r[j] = c.valueBOOL;
                    else if (c.AFFINITY == CellAffinity.DATE_TIME)
                        r[j] = c.valueBOOL;
                    else if (c.AFFINITY == CellAffinity.STRING)
                        r[j] = c.valueBOOL;
                    else if (c.AFFINITY == CellAffinity.BLOB)
                        r[j] = c.valueBOOL;

                }

                t.Rows.Add(r);

            }

            return t;

        }

        public static Extent RaizeDataTable(System.Data.DataTable Element, int RowOffset, int RowCount, int ColumnOffset, int ColumnCount)
        {

            
            RowCount = Math.Min(RowCount, Element.Rows.Count - RowOffset);
            ColumnCount = Math.Min(ColumnCount, Element.Columns.Count - ColumnOffset);
           
            Schema s = new Schema();
            bool[] IsNull = new bool[ColumnCount];
            for (int i = 0; i < Element.Columns.Count; i++)
            {

                Type t = Element.Columns[i].DataType;
                CellAffinity a = CellAffinity.INT;

                Console.WriteLine("TYPE: {0}", t);

                if (t == typeof(byte) || t == typeof(sbyte) || t == typeof(short) || t == typeof(ushort) || t == typeof(int) || t == typeof(uint) || t == typeof(long) || t == typeof(ulong))
                {
                    a = CellAffinity.INT;
                    IsNull[i] = false;
                    Console.WriteLine("INT");
                }
                else if (t == typeof(double) || t == typeof(float) || t == typeof(decimal))
                {
                    a = CellAffinity.DOUBLE;
                    IsNull[i] = false;
                    Console.WriteLine("DOUBLE");
                }
                else if (t == typeof(DateTime))
                {
                    a = CellAffinity.DATE_TIME;
                    IsNull[i] = false;
                    Console.WriteLine("DATE_TIME");
                }
                else if (t == typeof(bool))
                {
                    a = CellAffinity.BOOL;
                    IsNull[i] = false;
                    Console.WriteLine("BOOL");
                }
                else if (t == typeof(string))
                {
                    a = CellAffinity.STRING;
                    IsNull[i] = false;
                    Console.WriteLine("STRING");
                }
                else if (t == typeof(byte[]))
                {
                    a = CellAffinity.BLOB;
                    IsNull[i] = false;
                    Console.WriteLine("BLOB");
                }
                s.Add(Element.Columns[i].ColumnName, a);

            }

            // Create the dataset //
            Extent e = new Extent(s);

            // Load the data //
            for (int i = RowOffset; i < RowOffset + RowCount; i++)
            {
                
                System.Data.DataRow r = Element.Rows[i];
                RecordBuilder rb = new RecordBuilder();
                for (int j = 0; j < ColumnCount; j++)
                {

                    if (IsNull[j])
                    {
                        rb.Add(Cell.NULL_INT);
                    }
                    else
                    {
                        rb.Add(Cell.TryUnBoxInto(r[j + ColumnOffset], s.ColumnAffinity(j)));
                    }

                }

                e.Add(rb.ToRecord());
               
            }

            return e;


        }

        public static Extent RaizeDataTable(System.Data.DataTable Element)
        {
            return RaizeDataTable(Element, 0, Element.Rows.Count, 0, Element.Columns.Count);
        }

        public static void RaizeDataTable(System.Data.DataTable Element, RecordWriter Writer, int RowOffset, int RowCount, int ColumnOffset, int ColumnCount)
        {

            RowCount = Math.Min(RowCount, Element.Rows.Count - RowOffset);
            ColumnCount = Math.Min(ColumnCount, Element.Columns.Count - ColumnOffset);

            if (ColumnCount != Writer.Columns.Count)
                return;

            // Load the data //
            for (int i = RowOffset; i < RowOffset + RowCount; i++)
            {

                System.Data.DataRow r = Element.Rows[i];
                RecordBuilder rb = new RecordBuilder();
                for (int j = 0; j < ColumnCount; j++)
                {
                    rb.Add(Cell.TryUnBoxInto(r[j + ColumnOffset], Writer.Columns.ColumnAffinity(j)));
                }
                Writer.Insert(rb.ToRecord());

            }

        }

        public static void RaizeDataTable(System.Data.DataTable Element, RecordWriter Writer)
        {
            RaizeDataTable(Element, Writer, 0, Element.Rows.Count, 0, Element.Columns.Count);
        }

        // Parsing //
        public static void ParseTable(RecordWriter Writer, string Text, char[] RowDelim, char[] ColDelim, char Escape)
        {

            // Get the schema //
            Schema s = Writer.Columns;

            // Cycle through each record //
            foreach (string rec in Text.Split(RowDelim))
            {

                Record r = Helpers.Splitter.ToRecord(rec, s, ColDelim, Escape);
                Writer.Insert(r);

            }

        }

        public static void ParseTable(TabularData Data, string Text, char[] RowDelim, char[] ColDelim, char Escape)
        {

            RecordWriter w = Data.OpenWriter();
            ParseTable(w, Text, RowDelim, ColDelim, Escape);
            w.Close();

        }

    }

}
