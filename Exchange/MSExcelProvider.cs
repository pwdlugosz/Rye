using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using xl = Microsoft.Office.Interop.Excel;

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

}
