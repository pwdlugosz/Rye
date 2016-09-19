using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Methods;
using Rye.Structures;

namespace Rye.Libraries
{

    public sealed class MSOfficeLibrary : MethodLibrary
    {

        public const string STRUCT_NAME = "MSOFFICE";

        public const string XL_LAUNCH = "XL_LAUNCH";
        public const string XL_SHUTDOWN = "XL_SHUTDOWN";
        public const string XL_OPEN = "XL_OPEN";
        public const string XL_CLOSE = "XL_CLOSE";
        public const string XL_SAVE = "XL_SAVE";
        public const string XL_SAVE_CLOSE = "XL_SAVE_CLOSE";
        public const string XL_CREATE = "XL_CREATE";

        public const string XL_IMPORT = "XL_IMPORT";
        public const string XL_EXPORT = "XL_EXPORT";
        public const string XL_COPY_PASTE = "XL_COPY_PASTE";
        public const string XL_COPY_PASTE_VALUES = "XL_COPY_PASTE_VALUES";
        public const string XL_TELEPORT = "XL_TELEPORT";
        public const string XL_COPY_SHEET = "XL_COPY_SHEET";
        public const string XL_DELETE_SHEET = "XL_DELETE_SHEET";
        public const string XL_RUN_MACRO = "XL_RUN_MACRO";
        public const string XL_SET_VALUE = "XL_SET_VALUE";
        public const string XL_SET_FORMULA = "XL_SET_FORMULA";

        private string[] _Names = new string[]
        {
            XL_LAUNCH,
            XL_SHUTDOWN,
            XL_OPEN,
            XL_CLOSE,
            XL_SAVE,
            XL_SAVE_CLOSE,
            XL_CREATE,
            XL_IMPORT,
            XL_EXPORT,
            XL_COPY_PASTE,
            XL_COPY_PASTE_VALUES,
            XL_TELEPORT,
            XL_COPY_SHEET,
            XL_DELETE_SHEET,
            XL_RUN_MACRO,
            XL_SET_VALUE,
            XL_SET_FORMULA,
        };

        private Exchange.MSExcelProvider _xl;
        private Heap2<string, string> _CompressedSig;

        public MSOfficeLibrary(Session Session)
            : base(Session)
        {

            this._xl = new Exchange.MSExcelProvider(); // Doesnt actually launch the application

            this._CompressedSig = new Heap2<string, string>();
            this.LibName = STRUCT_NAME;
            this.Build();

        }

        private void Build()
        {

            this._CompressedSig.Allocate(XL_LAUNCH, "Launches an Excel application", ParameterCollectionSigniture.ZERO_PARAMETER);
            this._CompressedSig.Allocate(XL_SHUTDOWN, "Shuts down an Excel application", ParameterCollectionSigniture.ZERO_PARAMETER);
            this._CompressedSig.Allocate(XL_OPEN, "Opens an Excel workbook", "PATH|The path to the Excel file|E|false");
            this._CompressedSig.Allocate(XL_CREATE, "Creates an Excel workbook", "PATH|The path to the Excel file|E|false");
            this._CompressedSig.Allocate(XL_CLOSE, "Closes an Excel workbook", "BOOK|The workbook alias name|E|false");
            this._CompressedSig.Allocate(XL_SAVE, "Saves an Excel workbook", "BOOK|The workbook alias name|E|false");
            this._CompressedSig.Allocate(XL_SAVE_CLOSE, "Closes and saves an Excel workbook", "BOOK|The workbook alias name|E|false");

            this._CompressedSig.Allocate(XL_IMPORT, "Imports a range into a table", "BOOK|The workbook alias name|E|false;SHEET|The worksheet name|E|false;RANGE|The workbook range|E|false;DATA|The table to import into|T|false");
            this._CompressedSig.Allocate(XL_EXPORT, "Exports a table into a workbook", "BOOK|The workbook alias name|E|false;SHEET|The worksheet name|E|false;RANGE|The workbook range|E|false;DATA|The table to export|T|false");
            this._CompressedSig.Allocate(XL_COPY_PASTE, "Copies one book/sheet/range into another", "FROM_BOOK|The source workbook|E|false;FROM_SHEET|The source worksheet|E|false;FROM_RANGE|The source range|E|false;TO_BOOK|The destination workbook|E|false;TO_SHEET|The destination worksheet|E|false;TO_RANGE|The destination range|E|false");
            this._CompressedSig.Allocate(XL_COPY_PASTE_VALUES, "Copies the values from one book/sheet/range to another", "FROM_BOOK|The source workbook|E|false;FROM_SHEET|The source worksheet|E|false;FROM_RANGE|The source range|E|false;TO_BOOK|The destination workbook|E|false;TO_SHEET|The destination worksheet|E|false;TO_RANGE|The destination range|E|false");
            this._CompressedSig.Allocate(XL_TELEPORT, "Does a strict range set from one book/sheet/range to another", "FROM_BOOK|The source workbook|E|false;FROM_SHEET|The source worksheet|E|false;FROM_RANGE|The source range|E|false;TO_BOOK|The destination workbook|E|false;TO_SHEET|The destination worksheet|E|false;TO_RANGE|The destination range|E|false");
            this._CompressedSig.Allocate(XL_COPY_SHEET, "Copies one book/sheet to another", "FROM_BOOK|The source workbook|E|false;FROM_SHEET|The source worksheet|E|false;TO_BOOK|The destination workbook|E|false;TO_SHEET|The destination worksheet|E|false");
            this._CompressedSig.Allocate(XL_DELETE_SHEET, "Deletes a sheet form a workbook", "BOOK|The workbook alias name|E|false;SHEET|The worksheet name|E|false");
            this._CompressedSig.Allocate(XL_RUN_MACRO, "Runs a macro", "NAME|The macro name|E|false");
            this._CompressedSig.Allocate(XL_SET_VALUE, "Set a cell/range to a specific value", "BOOK|The workbook alias name|E|false;SHEET|The worksheet name|E|false;RANGE|The workbook range|E|false;VALUE|The value to set|E|false");
            this._CompressedSig.Allocate(XL_SET_FORMULA, "Sets a formula", "BOOK|The workbook alias name|E|false;SHEET|The worksheet name|E|false;RANGE|The workbook range|E|false;FORMULA|The formula to set|E|false");

        }

        /*
            Import range
            Export range
            CopyPaste
            CopyPasteValues
            Teleport
            CopySheet
            DeleteSheet
            RunMacro
            SetValue
            SetFormula
        */

        // Overrides //
        public override string[] Names
        {
            get 
            { 
                return this._Names; 
            }
        }

        public override Method RenderMethod(Method Parent, string Name, ParameterCollection Parameters)
        {

            switch (Name.ToUpper())
            {

                case XL_LAUNCH:
                    return this.Method_XL_Launch(Parent, Parameters);
                case XL_SHUTDOWN:
                    return this.Method_XL_Shutdown(Parent, Parameters);
                case XL_CLOSE:
                    return this.Method_XL_Close(Parent, Parameters);
                case XL_OPEN:
                    return this.Method_XL_Open(Parent, Parameters);
                case XL_SAVE:
                    return this.Method_XL_Save(Parent, Parameters);
                case XL_SAVE_CLOSE:
                    return this.Method_XL_SaveClose(Parent, Parameters);
                case XL_CREATE:
                    return this.Method_XL_Create(Parent, Parameters);

                case XL_IMPORT:
                    return this.Method_XL_Import(Parent, Parameters);
                case XL_EXPORT:
                    return this.Method_XL_Export(Parent, Parameters);
                case XL_COPY_PASTE:
                    return this.Method_XL_CopyPaste(Parent, Parameters);
                case XL_COPY_PASTE_VALUES:
                    return this.Method_XL_CopyPasteValues(Parent, Parameters);
                case XL_TELEPORT:
                    return this.Method_XL_Teleport(Parent, Parameters);
                case XL_COPY_SHEET:
                    return this.Method_XL_CopySheet(Parent, Parameters);
                case XL_DELETE_SHEET:
                    return this.Method_XL_DeleteSheet(Parent, Parameters);
                case XL_RUN_MACRO:
                    return this.Method_XL_RunMacro(Parent, Parameters);
                case XL_SET_VALUE:
                    return this.Method_XL_SetValue(Parent, Parameters);
                case XL_SET_FORMULA:
                    return this.Method_XL_SetFormula(Parent, Parameters);


            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override ParameterCollectionSigniture RenderSigniture(string Name)
        {

            if (this._CompressedSig.Exists(Name))
                return ParameterCollectionSigniture.Parse(Name, this._CompressedSig[Name].Item1, this._CompressedSig[Name].Item2);
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        // Private callers //
        private Method Method_XL_Launch(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                this._xl.Launch();

            };
            return new LibraryMethod(Parent, XL_LAUNCH, Parameters, false, kappa);

        }

        private Method Method_XL_Shutdown(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                this._xl.ShutDown();

            };
            return new LibraryMethod(Parent, XL_SHUTDOWN, Parameters, false, kappa);

        }

        private Method Method_XL_Open(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string path = x.Expressions["PATH"].Evaluate().valueSTRING;
                this._xl.OpenWorkbook(path);

            };
            return new LibraryMethod(Parent, XL_SHUTDOWN, Parameters, false, kappa);

        }

        private Method Method_XL_Close(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string book = x.Expressions["BOOK"].Evaluate().valueSTRING;
                this._xl.CloseWorkbook(book);

            };
            return new LibraryMethod(Parent, XL_CLOSE, Parameters, false, kappa);

        }

        private Method Method_XL_Save(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string book = x.Expressions["BOOK"].Evaluate().valueSTRING;
                this._xl.SaveWorkbook(book);

            };
            return new LibraryMethod(Parent, XL_SAVE, Parameters, false, kappa);

        }

        private Method Method_XL_SaveClose(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string book = x.Expressions["BOOK"].Evaluate().valueSTRING;
                this._xl.SaveCloseWorkbook(book);

            };
            return new LibraryMethod(Parent, XL_SAVE_CLOSE, Parameters, false, kappa);

        }

        private Method Method_XL_Create(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string path = x.Expressions["PATH"].Evaluate().valueSTRING;
                this._xl.CreateWorkbook(path);

            };
            return new LibraryMethod(Parent, XL_CREATE, Parameters, false, kappa);

        }


        private Method Method_XL_Import(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData data = x.Tables["DATA"];
                string Book = x.Expressions["BOOK"].Evaluate().valueSTRING;
                string Sheet = x.Expressions["SHEET"].Evaluate().valueSTRING;
                string Range = x.Expressions["RANGE"].Evaluate().valueSTRING;
                RecordWriter stream = data.OpenWriter();
                this._xl.ImportRange(Book, Sheet, Range, stream);
                stream.Close();

            };
            return new LibraryMethod(Parent, XL_IMPORT, Parameters, false, kappa);

        }

        private Method Method_XL_Export(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData data = x.Tables["DATA"];
                string Book = x.Expressions["BOOK"].Evaluate().valueSTRING;
                string Sheet = x.Expressions["SHEET"].Evaluate().valueSTRING;
                string Range = x.Expressions["RANGE"].Evaluate().valueSTRING;
                this._xl.ExportRange(Book, Sheet, Range, data);
                
            };
            return new LibraryMethod(Parent, XL_EXPORT, Parameters, false, kappa);

        }

        private Method Method_XL_CopyPaste(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string FromBook = x.Expressions["FROM_BOOK"].Evaluate().valueSTRING;
                string FromSheet = x.Expressions["FROM_SHEET"].Evaluate().valueSTRING;
                string FromRange = x.Expressions["FROM_RANGE"].Evaluate().valueSTRING;
                string ToBook = x.Expressions["TO_BOOK"].Evaluate().valueSTRING;
                string ToSheet = x.Expressions["TO_SHEET"].Evaluate().valueSTRING;
                string ToRange = x.Expressions["TO_RANGE"].Evaluate().valueSTRING;

                this._xl.CopyPaste(FromBook, FromSheet, FromRange, ToBook, ToRange, ToSheet);

            };
            return new LibraryMethod(Parent, XL_COPY_PASTE, Parameters, false, kappa);

        }

        private Method Method_XL_CopyPasteValues(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string FromBook = x.Expressions["FROM_BOOK"].Evaluate().valueSTRING;
                string FromSheet = x.Expressions["FROM_SHEET"].Evaluate().valueSTRING;
                string FromRange = x.Expressions["FROM_RANGE"].Evaluate().valueSTRING;
                string ToBook = x.Expressions["TO_BOOK"].Evaluate().valueSTRING;
                string ToSheet = x.Expressions["TO_SHEET"].Evaluate().valueSTRING;
                string ToRange = x.Expressions["TO_RANGE"].Evaluate().valueSTRING;

                this._xl.CopyPasteValues(FromBook, FromSheet, FromRange, ToBook, ToRange, ToSheet);

            };
            return new LibraryMethod(Parent, XL_COPY_PASTE_VALUES, Parameters, false, kappa);

        }

        private Method Method_XL_Teleport(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string FromBook = x.Expressions["FROM_BOOK"].Evaluate().valueSTRING;
                string FromSheet = x.Expressions["FROM_SHEET"].Evaluate().valueSTRING;
                string FromRange = x.Expressions["FROM_RANGE"].Evaluate().valueSTRING;
                string ToBook = x.Expressions["TO_BOOK"].Evaluate().valueSTRING;
                string ToSheet = x.Expressions["TO_SHEET"].Evaluate().valueSTRING;
                string ToRange = x.Expressions["TO_RANGE"].Evaluate().valueSTRING;

                this._xl.Teleport(FromBook, FromSheet, FromRange, ToBook, ToRange, ToSheet);

            };
            return new LibraryMethod(Parent, XL_TELEPORT, Parameters, false, kappa);

        }

        private Method Method_XL_CopySheet(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string FromBook = x.Expressions["FROM_BOOK"].Evaluate().valueSTRING;
                string FromSheet = x.Expressions["FROM_SHEET"].Evaluate().valueSTRING;
                string ToBook = x.Expressions["TO_BOOK"].Evaluate().valueSTRING;
                string ToSheet = x.Expressions["TO_SHEET"].Evaluate().valueSTRING;

                this._xl.CopySheet(FromBook, FromSheet, ToBook, ToSheet);

            };
            return new LibraryMethod(Parent, XL_COPY_SHEET, Parameters, false, kappa);

        }

        private Method Method_XL_DeleteSheet(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string Book = x.Expressions["BOOK"].Evaluate().valueSTRING;
                string Sheet = x.Expressions["SHEET"].Evaluate().valueSTRING;
                
                this._xl.DeleteSheet(Book, Sheet);

            };
            return new LibraryMethod(Parent, XL_DELETE_SHEET, Parameters, false, kappa);

        }

        private Method Method_XL_RunMacro(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string Name = x.Expressions["NAME"].Evaluate().valueSTRING;
                
                this._xl.RunMacro(Name);

            };
            return new LibraryMethod(Parent, XL_RUN_MACRO, Parameters, false, kappa);

        }

        private Method Method_XL_SetValue(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string Book = x.Expressions["BOOK"].Evaluate().valueSTRING;
                string Sheet = x.Expressions["SHEET"].Evaluate().valueSTRING;
                string Range = x.Expressions["RANGE"].Evaluate().valueSTRING;
                Cell Value = x.Expressions["VALUE"].Evaluate();

                this._xl.SetValue(Book, Sheet, Range, Value);

            };
            return new LibraryMethod(Parent, XL_SET_VALUE, Parameters, false, kappa);

        }

        private Method Method_XL_SetFormula(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string Book = x.Expressions["BOOK"].Evaluate().valueSTRING;
                string Sheet = x.Expressions["SHEET"].Evaluate().valueSTRING;
                string Range = x.Expressions["RANGE"].Evaluate().valueSTRING;
                string Formula = x.Expressions["FORMULA"].Evaluate().valueSTRING;

                this._xl.SetFormula(Book, Sheet, Range, Formula);

            };
            return new LibraryMethod(Parent, XL_SET_FORMULA, Parameters, false, kappa);

        }


    }

}
