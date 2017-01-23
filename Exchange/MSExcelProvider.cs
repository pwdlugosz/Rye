using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using xl = Microsoft.Office.Interop.Excel;
using fxl = Excel;
using System.IO;
using System.Data;

namespace Rye.Exchange
{

    public sealed class MSExcelProvider
    {

        private xl.Application _Application;
        private bool _IsLaunched = false;
        
        public MSExcelProvider()
        {
            //this._Application = new xl.Application();
        }

        public bool BookIsOpen(string Workbook)
        {

            foreach (xl.Workbook wb in this._Application.Workbooks)
            {
                if (wb.Name == Workbook)
                    return true;
            }
            return false;

        }

        public bool SheetExists(string Workbook, string Sheetname)
        {

            foreach (xl.Worksheet ws in this._Application.Workbooks[Workbook].Worksheets)
            {
                if (ws.Name == Sheetname)
                    return true;
            }
            return false;
        }
        
        public void OpenWorkbook(string Path)
        {
            this._Application.Workbooks.Open(Path);
        }

        public void CreateWorkbook(string Path)
        {
            
            // Check if the file exists, if so, then delete //
            if (System.IO.File.Exists(Path))
            {
                System.IO.File.Delete(Path);
            }

            // Create the excel file //
            xl.Workbook wb = this._Application.Workbooks.Add(Type.Missing);
            
            /* Figure out the format:
             * xlsx - xlOpenXMLWorkbook 
             * xlsb - xlExcel12 
             * xlsm - xlOpenXMLWorkbookMacroEnabled
             * xls - Excel8
             * csv - xlCSV 
             * 
             */
            string format = (new System.IO.FileInfo(Path).Extension).ToLower().Replace(".","");
            xl.XlFileFormat xformat = xl.XlFileFormat.xlOpenXMLWorkbook;
            switch (format)
            {
                case "xlsx":
                    xformat = xl.XlFileFormat.xlOpenXMLWorkbook;
                    break;
                case "xlsm":
                    xformat = xl.XlFileFormat.xlOpenXMLWorkbookMacroEnabled;
                    break;
                case "xlsb":
                    xformat = xl.XlFileFormat.xlExcel12;
                    break;
                case "xls":
                    xformat = xl.XlFileFormat.xlExcel8;
                    break;
                case "csv":
                    xformat = xl.XlFileFormat.xlCSV;
                    break;
            }

            // Save the file //
            wb.SaveAs(Path, xformat);

            // Release the COM object //
            System.Runtime.InteropServices.Marshal.ReleaseComObject(wb);
            wb = null;

        }

        public void CloseWorkbook(string Alias)
        {
            this._Application.Workbooks[Alias].Close(false);
        }

        public void SaveWorkbook(string Alias)
        {
            this._Application.Workbooks[Alias].Save();
        }

        public void SaveCloseWorkbook(string Alias)
        {
            this._Application.Workbooks[Alias].Close(true);
        }

        public void ImportRange(string Workbook, string SheetName, string Address, RecordWriter OutputStream)
        {

            xl.Range tablix = this._Application.Workbooks[Workbook].Sheets[SheetName].Range[Address];
            int RowCount = tablix.Rows.Count;
            int ColCount = tablix.Columns.Count;

            if (OutputStream.Columns.Count != ColCount)
                throw new ArgumentException(string.Format("The range passed '{0}' for sheet '{1}' has {2} columns, but the input schema expects {3}", Address, SheetName, ColCount, OutputStream.Columns.Count));

            for (int row = 0; row < RowCount; row++)
            {

                RecordBuilder rb = new RecordBuilder();
                for (int col = 0; col < ColCount; col++)
                {
                    //Console.WriteLine(tablix[row + 1, col + 1].Value.GetType());
                    Cell c = Cell.UnBoxInto(tablix[row + 1, col + 1].Value, OutputStream.Columns.ColumnAffinity(col));
                    rb.Add(c);

                }

                OutputStream.Insert(rb.ToRecord());

            }

            System.Runtime.InteropServices.Marshal.ReleaseComObject(tablix);
            tablix = null;
            
        }

        public void ExportRange(string Workbook, string SheetName, string Address, TabularData Data)
        {

            xl.Range tablix = this._Application.Workbooks[Workbook].Sheets[SheetName].Range[Address];
            int row = 0;

            for (int col = 0; col < Data.Columns.Count; col++)
            {
                tablix[row + 1, col + 1].Value = Data.Columns.ColumnName(col);
            }
            row++;

            RecordReader ReadStream = Data.CreateVolume().OpenReader(new Expressions.Register(Data.Header.Name, Data.Columns));
            while(!ReadStream.EndOfData)
            {

                Record r = ReadStream.ReadNext();
                for (int col = 0; col < r.Count; col++)
                {
                    tablix[row + 1, col + 1].Value = r[col].valueSTRING;
                }

                row++;

            }

            System.Runtime.InteropServices.Marshal.ReleaseComObject(tablix);
            tablix = null;

        }

        public void CopyPaste(string FromBook, string FromSheet, string FromAddress, string ToBook, string ToSheet, string ToAddress)
        {

            this._Application.Workbooks[FromBook].Sheets[FromSheet].Range[FromAddress].Copy(Type.Missing);
            this._Application.Workbooks[ToBook].Sheets[ToSheet].Range[ToAddress].PasteSpecial(xl.XlPasteType.xlPasteAll);

        }

        public void CopyPasteValues(string FromBook, string FromSheet, string FromAddress, string ToBook, string ToSheet, string ToAddress)
        {

            this._Application.Workbooks[FromBook].Sheets[FromSheet].Range[FromAddress].Copy(Type.Missing);
            this._Application.Workbooks[ToBook].Sheets[ToSheet].Range[ToAddress].PasteSpecial(xl.XlPasteType.xlPasteValues);

        }

        public void Teleport(string FromBook, string FromSheet, string FromAddress, string ToBook, string ToSheet, string ToAddress)
        {
            this._Application.Workbooks[ToBook].Sheets[ToSheet].Range[ToAddress].PasteSpecial(xl.XlPasteType.xlPasteValues).Value2 =
                this._Application.Workbooks[FromBook].Sheets[FromSheet].Range[FromAddress].Copy(Type.Missing).Value2;
        }

        public void CopySheet(string FromBook, string FromSheet, string ToBook, string ToSheet)
        {

            this._Application.Workbooks[FromBook].Sheets[FromSheet].Copy(Type.Missing, this._Application.Workbooks[ToBook].Sheets[ToSheet]);

        }

        public void DeleteSheet(string FromBook, string FromSheet)
        {

            try
            {
                this._Application.Workbooks[FromBook].Worksheets[FromSheet].Delete();
            }
            catch
            {
            }

        }

        public void RunMacro(string MacroName)
        {
            this._Application.Run(MacroName);
        }

        public void SetValue(string Workbook, string SheetName, string Address, Cell Value)
        {
            this._Application.Workbooks[Workbook].Sheets[SheetName].Range[Address].Value2 = Value.valueSTRING;
        }

        public Cell GetValue(string Workbook, string SheetName, string Address)
        {
            return new Cell(this._Application.Workbooks[Workbook].Sheets[SheetName].Range(Address).Value2.ToString());
        }

        public void SetFormula(string Workbook, string SheetName, string Address, string Formula)
        {

            this._Application.Workbooks[Workbook].Worksheets[SheetName].Range[Address].Formula = Formula;

        }

        public string GetFormula(string Workbook, string SheetName, string Address)
        {
            return this._Application.Workbooks[Workbook].Worksheets[SheetName].Range[Address].Formula;
        }

        // -------------------------- Launch / Shutdown -------------------------- //
        public void Launch()
        {

            if (this._IsLaunched)
                return;
            this._Application = new xl.Application();
            this._IsLaunched = true;

        }
        
        public void ShutDown()
        {

            if (!this._IsLaunched)
                return;
            this._Application.Workbooks.Close();
            this._Application.Quit();
            while (0 != System.Runtime.InteropServices.Marshal.ReleaseComObject(this._Application) ){ };
            this._Application = null;
            GC.Collect();

        }

    }

    public sealed class MSExcelFast
    {

        public struct Address
        {

            private int _X1, _X2;
            private int _Y1, _Y2;

            public Address(string Text)
            {
                this._X1 = 0; this._X2 = 0; this._Y1 = 0; this._Y2 = 0;
                this.Initialize(Text);
            }

            private int ColumnOffset(string Element)
            {

                Element = Element.ToUpper();
                int Offset = 0;
                for (int i = 0; i < Element.Length; i++)
                {
                    Offset += ((int)Element[i] - 65 + 1);
                }

                return Offset - 1;

            }

            private void Initialize(string Text)
            {

                StringBuilder sb1 = new StringBuilder();
                StringBuilder sb2 = new StringBuilder();
                StringBuilder sb3 = new StringBuilder();
                StringBuilder sb4 = new StringBuilder();
                int state = 1;

                foreach (char c in Text)
                {

                    if (char.IsLetter(c))
                    {

                        if (state == 1)
                        {
                            sb1.Append(c);
                        }
                        else if (state == 2)
                        {
                            state = 3;
                            sb3.Append(c);
                        }
                        else if (state == 3)
                        {
                            sb3.Append(c);
                        }
                        else
                        {
                            throw new FormatException(string.Format("String is not in the correct format {0}", Text));
                        }

                    }
                    else if (char.IsNumber(c))
                    {

                        if (state == 1)
                        {
                            state = 2;
                            sb2.Append(c);
                        }
                        else if (state == 2)
                        {
                            sb2.Append(c);
                        }
                        else if (state == 3)
                        {
                            state = 4;
                            sb4.Append(c);
                        }
                        else if (state == 4)
                        {
                            sb4.Append(c);
                        }
                        else
                        {
                            throw new FormatException(string.Format("String is not in the correct format {0}", Text));
                        }

                    }

                }

                // Render the values //
                this._X1 = int.Parse(sb2.ToString()) - 1;
                this._X2 = int.Parse(sb4.ToString()) - 1;
                this._Y1 = this.ColumnOffset(sb1.ToString());
                this._Y2 = this.ColumnOffset(sb3.ToString());

            }

            public int RowRangeBegin
            {
                get { return this._X1; }
            }

            public int RowRangeEnd
            {
                get { return this._X2; }
            }

            public int RowSpan
            {
                get { return this._X2 - this._X1 + 1; }
            }

            public int ColRangeBegin
            {
                get { return this._Y1; }
            }

            public int ColRangeEnd
            {
                get { return this._Y2; }
            }

            public int ColSpan
            {
                get { return this._Y2 - this._Y1 + 1; }
            }

            public override string ToString()
            {
                return string.Format("{0}.{1} OriginalNode {2}.{3}", this._X1, this._Y1, this._X2, this._Y2);
            }

        }

        public static void ImportExcelFast(RecordWriter Writer, string Path, string Sheet, string Range)
        {

            using (System.IO.Stream s = File.OpenRead(Path))
            {

                
                // Get the extension and format //
                string format = Path.Split('.').Last().ToLower();
                fxl.IExcelDataReader stream = (format == "xls" || format == "xlsb" ? fxl.ExcelReaderFactory.CreateBinaryReader(s) : fxl.ExcelReaderFactory.CreateOpenXmlReader(s));

                // Render a dataset //
                DataSet sets = stream.AsDataSet();
                DataTable t = sets.Tables[Sheet];
                Address point = new Address(Range);

                // Write data //
                Exchange.ObjectExchange.RaizeDataTable(t, Writer, point.RowRangeBegin, point.RowSpan, point.ColRangeBegin, point.ColSpan);
                
            }

        }

    }

}
