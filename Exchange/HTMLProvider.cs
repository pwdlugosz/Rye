using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using HtmlAgilityPack;

namespace Rye.Exchange
{

    public static class HTMLProvider
    {

        public static void WriteToStream(HtmlDocument HTML, string TableTag, string RowTag, string ColumnTag, RecordWriter OutStream)
        {

            // Build the support variables //
            int i = 0;
            HtmlNode node = HTML.DocumentNode.SelectSingleNode(TableTag);

            // Cycle through all rows //
            foreach (HtmlNode row in node.SelectNodes(RowTag))
            {

                // Create a record builder //
                RecordBuilder rb = new RecordBuilder();
                i = 0;

                // Cycle through each column //
                IEnumerable<HtmlNode> records = row.SelectNodes(ColumnTag);
                if (records != null)
                {

                    foreach (HtmlNode unit in records)
                    {

                        // Parse the cell //
                        Cell x = CellParser.TryParse(unit.InnerText, OutStream.Columns.ColumnAffinity(i));
                        rb.Add(x);
                        i++;

                    }

                    // Load it into the table //
                    OutStream.Insert(rb.ToRecord());

                }

            }

        }

        public static void WriteToStream(string FilePath, string Format, RecordWriter OutStream)
        {

            HtmlDocument doc = new HtmlDocument();
            doc.Load(FilePath);

            string[] Tags = Format.Split(';');
            if (Tags.Length != 3)
                throw new ArgumentException("The Format variable must look like 'TableTag;RowTag;ColumTag'");

            WriteToStream(doc, Tags[0], Tags[1], Tags[2], OutStream);

        }

        public static Extent ToExtent(HtmlDocument HTML, string TableTag, string RowTag, string ColumnTag, Schema Columns)
        {

            // Build the extent //
            Extent e = new Extent(Columns);

            // Build the support variables //
            int i = 0;
            HtmlNode node = HTML.DocumentNode.SelectSingleNode(TableTag);

            // Cycle through all rows //
            foreach (HtmlNode row in node.SelectNodes(RowTag))
            {

                // Create a record builder //
                RecordBuilder rb = new RecordBuilder();
                i = 0;

                // Cycle through each column //
                IEnumerable<HtmlNode> records = row.SelectNodes(ColumnTag);
                if (records != null)
                {

                    foreach (HtmlNode unit in records)
                    {

                        // Parse the cell //
                        Cell x = Cell.TryParse(unit.InnerText, Columns.ColumnAffinity(i));
                        rb.Add(x);
                        i++;

                    }

                    // Load it into the table //
                    e.Add(rb.ToRecord());

                }

            }

            return e;

        }

        public static Extent ToExtent(string FilePath, string Format, Schema Columns)
        {

            HtmlDocument doc = new HtmlDocument();
            doc.Load(FilePath);
            
            string[] Tags = Format.Split(';');
            if (Tags.Length != 3)
                throw new ArgumentException("The Format variable must look like 'TableTag;RowTag;ColumTag'");

            return ToExtent(doc, Tags[0], Tags[1], Tags[2], Columns);
        
        }

        public static string ToString(HtmlDocument HTML, string Tag)
        {
            try
            {
                return HTML.DocumentNode.SelectSingleNode(Tag).InnerText;
            }
            catch
            {
                return null;
            }
        }

        public static string ToString(string FilePath, string Tag)
        {
            
            HtmlDocument doc = new HtmlDocument();
            doc.Load(FilePath);
            return ToString(doc, Tag);

        }


    }

}
