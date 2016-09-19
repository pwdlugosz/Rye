using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;
using Rye.Data;
using Rye.Expressions;
using Rye.Structures;
using Rye.Methods;
using xl = Excel;
using System.Data.OleDb;

namespace Rye.Structures
{

    //public sealed class MSOffice
    //{

    //    public struct Address
    //    {

    //        private int _X1, _X2;
    //        private int _Y1, _Y2;

    //        public Address(string Text)
    //        {
    //            this._X1 = 0; this._X2 = 0; this._Y1 = 0; this._Y2 = 0;
    //            this.Initialize(Text);
    //        }

    //        private int ColumnOffset(string Element)
    //        {

    //            Element = Element.ToUpper();
    //            int Offset = 0;
    //            for (int i = 0; i < Element.Length; i++)
    //            {
    //                Offset += ((int)Element[i] - 65 + 1);
    //            }

    //            return Offset - 1;

    //        }

    //        private void Initialize(string Text)
    //        {

    //            StringBuilder sb1 = new StringBuilder();
    //            StringBuilder sb2 = new StringBuilder();
    //            StringBuilder sb3 = new StringBuilder();
    //            StringBuilder sb4 = new StringBuilder();
    //            int state = 1;

    //            foreach (char c in Text)
    //            {

    //                if (char.IsLetter(c))
    //                {
                    
    //                    if (state == 1)
    //                    {
    //                        sb1.Append(c);
    //                    }
    //                    else if (state == 2)
    //                    {
    //                        state = 3;
    //                        sb3.Append(c);
    //                    }
    //                    else if (state == 3)
    //                    {
    //                        sb3.Append(c);
    //                    }
    //                    else
    //                    {
    //                        throw new FormatException(string.Format("String is not in the correct format {0}", Text));
    //                    }
                    
    //                }
    //                else if (char.IsNumber(c))
    //                {

    //                    if (state == 1)
    //                    {
    //                        state = 2;
    //                        sb2.Append(c);
    //                    }
    //                    else if (state == 2)
    //                    {
    //                        sb2.Append(c);
    //                    }
    //                    else if (state == 3)
    //                    {
    //                        state = 4;
    //                        sb4.Append(c);
    //                    }
    //                    else if (state == 4)
    //                    {
    //                        sb4.Append(c);
    //                    }
    //                    else
    //                    {
    //                        throw new FormatException(string.Format("String is not in the correct format {0}", Text));
    //                    }

    //                }

    //            }

    //            // Render the values //
    //            this._X1 = int.Parse(sb2.ToString()) - 1;
    //            this._X2 = int.Parse(sb4.ToString()) - 1;
    //            this._Y1 = this.ColumnOffset(sb1.ToString());
    //            this._Y2 = this.ColumnOffset(sb3.ToString());

    //        }

    //        public int RowRangeBegin
    //        {
    //            get { return this._X1; }
    //        }

    //        public int RowRangeEnd
    //        {
    //            get { return this._X2; }
    //        }

    //        public int RowSpan
    //        {
    //            get { return this._X2 - this._X1 + 1; }
    //        }

    //        public int ColRangeBegin
    //        {
    //            get { return this._Y1; }
    //        }

    //        public int ColRangeEnd
    //        {
    //            get { return this._Y2; }
    //        }

    //        public int ColSpan
    //        {
    //            get { return this._Y2 - this._Y1 + 1; }
    //        }

    //        public override string ToString()
    //        {
    //            return string.Format("{0}.{1} x {2}.{3}", this._X1, this._Y1, this._X2, this._Y2);
    //        }

    //    }

    //    public static long ImportExcelFast(RecordWriter Writer, string Key, string Sheet, string Range)
    //    {

    //        long l = 0;
    //        using (System.IO.Stream s = File.OpenRead(Key))
    //        {


    //            string format = Key.Split('.').Last().ToLower();
    //            xl.IExcelDataReader stream = (format == "xls" || format == "xlsb" ? xl.ExcelReaderFactory.CreateBinaryReader(s) : xl.ExcelReaderFactory.CreateOpenXmlReader(s));
    //            DataSet sets = stream.AsDataSet();
    //            DataTable t = sets.Tables[Sheet];
    //            Address point= new Address(Range);

    //            Shard e = Exchange.ObjectExchange.RaizeDataTable(t, point.RowRangeBegin, point.RowSpan, point.ColRangeBegin, point.ColSpan);
    //            l = e.Count;
    //            Writer.BulkInsert(e);

    //        }

    //        return l;

    //    }


    //}

    //public sealed class MSOfficeStructure : MemoryStructure
    //{

    //    public const string STRUCT_NAME = "MSOFFICE";

    //    private Exchange.MSExcelProvider _xl;

    //    public MSOfficeStructure()
    //        : base(STRUCT_NAME)
    //    {

    //        this._xl = new Exchange.MSExcelProvider();
        
    //    }

    //    public override void Burn()
    //    {
    //        this._xl.ShutDown();
    //    }

    //    private class MSOfficeFunctionLibrary : FunctionLibrary
    //    {

    //        public const string XL_IS_OPEN = "XL_IS_OPEN";
    //        public const string XL_SHEET_EXISTS = "XL_SHEET_EXISTS";
    //        public const string XL_GET_CELL = "XL_GET_CELL";
    //        public const string XL_GET_FORMULA = "XL_GET_FORMULA";

    //        private static string[] _FunctionNames = new string[]
    //        {
    //            XL_IS_OPEN,
    //            XL_SHEET_EXISTS,
    //            XL_GET_CELL,
    //            XL_GET_FORMULA
    //        };

    //        private Exchange.MSExcelProvider _xl;

    //        public MSOfficeFunctionLibrary(Exchange.MSExcelProvider Provider)
    //            : base()
    //        {
    //            this._xl = Provider;
    //        }

    //        public override CellFunction RenderFunction(string Name)
    //        {

    //            switch (Name)
    //            {
    //                case MSOfficeFunctionLibrary.XL_GET_CELL: return LambdaXLGetCell();
    //                case MSOfficeFunctionLibrary.XL_GET_FORMULA: return LambdaXLGetFormula();
    //                case MSOfficeFunctionLibrary.XL_IS_OPEN: return LambdaXLIsOpen();
    //                case MSOfficeFunctionLibrary.XL_SHEET_EXISTS: return LambdaXLGetCell();
    //            }

    //            throw new ArgumentException(string.Format("Function '{0}' does not exist", Name));

    //        }

    //        public override string[] Names
    //        {
    //            get { return MSOfficeFunctionLibrary._FunctionNames; }
    //        }

    //        private CellFunction LambdaXLGetCell()
    //        {

    //            Func<Cell[], Cell> lambda = (x) =>
    //            {
    //                return this._xl.GetValue(x[0].valueSTRING, x[1].valueSTRING, x[2].valueSTRING);
    //            };
    //            return new CellFunctionFixedShell(XL_GET_CELL, 3, CellAffinity.STRING, lambda);

    //        }

    //        private CellFunction LambdaXLGetFormula()
    //        {

    //            Func<Cell[], Cell> lambda = (x) =>
    //            {
    //                return new Cell(this._xl.GetFormula(x[0].valueSTRING, x[1].valueSTRING, x[2].valueSTRING));
    //            };
    //            return new CellFunctionFixedShell(XL_GET_FORMULA, 3, CellAffinity.STRING, lambda);

    //        }

    //        private CellFunction LambdaXLIsOpen()
    //        {

    //            Func<Cell[], Cell> lambda = (x) =>
    //            {
    //                return new Cell(this._xl.BookIsOpen(x[0].valueSTRING));
    //            };
    //            return new CellFunctionFixedShell(XL_IS_OPEN, 1, CellAffinity.STRING, lambda);

    //        }

    //        private CellFunction LambdaXLSheetExists()
    //        {

    //            Func<Cell[], Cell> lambda = (x) =>
    //            {
    //                return new Cell(this._xl.SheetExists(x[0].valueSTRING, x[1].valueSTRING));
    //            };
    //            return new CellFunctionFixedShell(XL_SHEET_EXISTS, 2, CellAffinity.STRING, lambda);

    //        }


    //    }

    //    private sealed class MSOfficeProcedureLibrary : MethodLibrary
    //    {

    //        public const string XL_OPEN_WORKBOOK = "XL_OPEN_WORKBOOK";
    //        public const string XL_CLOSE_WORKBOOK = "XL_CLOSE_WORKBOOK";
    //        public const string XL_SAVE_WORKBOOK = "XL_SAVE_WORKBOOK";
    //        public const string XL_SAVE_CLOSE_WORKBOOK = "XL_SAVE_CLOSE_WORKBOOK";
            
    //        private static string[] _BaseNames = new string[]
    //        {
    //            XL_OPEN_WORKBOOK,
    //            XL_CLOSE_WORKBOOK,
    //            XL_SAVE_WORKBOOK,
    //            XL_SAVE_CLOSE_WORKBOOK,
    //        };

    //        private Heap2<string, string> _CompressedSig;

    //        private Exchange.MSExcelProvider _xl;

    //        /*
    //         * Name | Description | Affinity | Can Be Null?
    //         * 
    //         * Affinity can be:
    //         * E: expression
    //         * V: vector
    //         * M: matrix expression
    //         * T: table (or extent)
    //         * 
    //         */

    //        public MSOfficeProcedureLibrary(FileStructure Caller)
    //            : base(Caller)
    //        {

    //            this._Caller = Caller;
    //            this._CompressedSig = new Heap2<string, string>();

    //            this._CompressedSig.Allocate(MSOfficeProcedureLibrary.XL_OPEN_WORKBOOK, "Opens a workbook", "PATH|The path of the Excel file|E|false");
    //            this._CompressedSig.Allocate(MSOfficeProcedureLibrary.XL_CLOSE_WORKBOOK, "Closes a workbook without saving it", "ALIAS|The application workbook alias|E|false");
    //            this._CompressedSig.Allocate(MSOfficeProcedureLibrary.XL_SAVE_WORKBOOK, "Saves a workbook", "ALIAS|The application workbook alias|E|false");
    //            this._CompressedSig.Allocate(MSOfficeProcedureLibrary.XL_SAVE_CLOSE_WORKBOOK, "Closes and saved a workbook", "ALIAS|The application workbook alias|E|false");
                
    //        }

    //        public override Method RenderMethod(Method Parent, string Name, ParameterCollection Parameters)
    //        {

    //            switch (Name.ToUpper())
    //            {

    //                case MSOfficeProcedureLibrary.XL_OPEN_WORKBOOK:
    //                    return this.KappaXLOpenWorkbook(Parent, Parameters);
    //                case MSOfficeProcedureLibrary.XL_CLOSE_WORKBOOK:
    //                    return this.KappaXLCloseWorkbook(Parent, Parameters);
    //                case MSOfficeProcedureLibrary.XL_SAVE_WORKBOOK:
    //                    return this.KappaXLSaveWorkbook(Parent, Parameters);
    //                case MSOfficeProcedureLibrary.XL_SAVE_CLOSE_WORKBOOK:
    //                    return this.KappaXLSaveCloseWorkbook(Parent, Parameters);

    //            }
    //            throw new ArgumentException(string.Format("Method '{0}' does not exist in '{1}'", Name, this._Caller.Name));

    //        }

    //        public override ParameterCollectionSigniture RenderSigniture(string Name)
    //        {

    //            if (this._CompressedSig.Exists(Name))
    //                return ParameterCollectionSigniture.Parse(Name, this._CompressedSig[Name].Item1, this._CompressedSig[Name].Item2);
    //            throw new ArgumentException(string.Format("Method '{0}' does not exist in '{1}'", Name, this._Caller.Name));

    //        }

    //        public override string[] Names
    //        {
    //            get { return MSOfficeProcedureLibrary._BaseNames; }
    //        }

    //        // Method support //
    //        private DynamicStructureMethod KappaXLOpenWorkbook(Method Parent, ParameterCollection Parameters)
    //        {

    //            Action<ParameterCollection> kappa = (x) =>
    //            {

    //                string path = x.Expressions["PATH"].Evaluate().valueSTRING;
    //                this._xl.OpenWorkbook(path);
                
    //            };

    //            return new DynamicStructureMethod(Parent, this._Caller, MSOfficeProcedureLibrary.XL_OPEN_WORKBOOK, Parameters, false, kappa);

    //        }

    //        private DynamicStructureMethod KappaXLCloseWorkbook(Method Parent, ParameterCollection Parameters)
    //        {

    //            Action<ParameterCollection> kappa = (x) =>
    //            {

    //                string alias = x.Expressions["ALIAS"].Evaluate().valueSTRING;
    //                this._xl.OpenWorkbook(alias);

    //            };

    //            return new DynamicStructureMethod(Parent, this._Caller, MSOfficeProcedureLibrary.XL_CLOSE_WORKBOOK, Parameters, false, kappa);

    //        }

    //        private DynamicStructureMethod KappaXLSaveWorkbook(Method Parent, ParameterCollection Parameters)
    //        {

    //            Action<ParameterCollection> kappa = (x) =>
    //            {

    //                string alias = x.Expressions["ALIAS"].Evaluate().valueSTRING;
    //                this._xl.SaveWorkbook(alias);

    //            };

    //            return new DynamicStructureMethod(Parent, this._Caller, MSOfficeProcedureLibrary.XL_SAVE_WORKBOOK, Parameters, false, kappa);

    //        }

    //        private DynamicStructureMethod KappaXLSaveCloseWorkbook(Method Parent, ParameterCollection Parameters)
    //        {

    //            Action<ParameterCollection> kappa = (x) =>
    //            {

    //                string alias = x.Expressions["ALIAS"].Evaluate().valueSTRING;
    //                this._xl.SaveCloseWorkbook(alias);

    //            };

    //            return new DynamicStructureMethod(Parent, this._Caller, MSOfficeProcedureLibrary.XL_SAVE_CLOSE_WORKBOOK, Parameters, false, kappa);

    //        }

    //    }

    //}

}
