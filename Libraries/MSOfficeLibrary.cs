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

    // MSOffice Library //
    public sealed class MSOfficeLibrary : Library
    {

        public const string M_XL_LAUNCH = "XL_LAUNCH";
        public const string M_XL_SHUTDOWN = "XL_SHUTDOWN";
        public const string M_XL_OPEN = "XL_OPEN";
        public const string M_XL_CLOSE = "XL_CLOSE";
        public const string M_XL_SAVE = "XL_SAVE";
        public const string M_XL_SAVE_CLOSE = "XL_SAVE_CLOSE";
        public const string M_XL_CREATE = "XL_CREATE";

        public const string M_XL_IMPORT = "XL_IMPORT";
        public const string M_XL_IMPORT_FAST = "XL_IMPORT_FAST";
        public const string M_XL_EXPORT = "XL_EXPORT";
        public const string M_XL_COPY_PASTE = "XL_COPY_PASTE";
        public const string M_XL_COPY_PASTE_VALUES = "XL_COPY_PASTE_VALUES";
        public const string M_XL_TELEPORT = "XL_TELEPORT";
        public const string M_XL_COPY_SHEET = "XL_COPY_SHEET";
        public const string M_XL_DELETE_SHEET = "XL_DELETE_SHEET";
        public const string M_XL_RUN_MACRO = "XL_RUN_MACRO";
        public const string M_XL_SET_VALUE = "XL_SET_VALUE";
        public const string M_XL_SET_FORMULA = "XL_SET_FORMULA";

        private string[] _MethodNames = new string[]
        {
            M_XL_LAUNCH,
            M_XL_SHUTDOWN,
            M_XL_OPEN,
            M_XL_CLOSE,
            M_XL_SAVE,
            M_XL_SAVE_CLOSE,
            M_XL_CREATE,
            M_XL_IMPORT,
            M_XL_EXPORT,
            M_XL_COPY_PASTE,
            M_XL_COPY_PASTE_VALUES,
            M_XL_TELEPORT,
            M_XL_COPY_SHEET,
            M_XL_DELETE_SHEET,
            M_XL_RUN_MACRO,
            M_XL_SET_VALUE,
            M_XL_SET_FORMULA,
            M_XL_IMPORT_FAST,
        };

        private string[] _FunctionNames = new string[] { "" };

        private Exchange.MSExcelProvider _xl;

        public MSOfficeLibrary(Session Session)
            : base(Session, "MSOFFICE")
        {
            this._xl = new Exchange.MSExcelProvider(); // Doesnt actually launch the application
        }

        public override Method GetMethod(Method Parent, string Name, ParameterCollection Parameters)
        {

            switch (Name.ToUpper())
            {

                case M_XL_LAUNCH:
                    return this.Method_XL_Launch(Parent, Parameters);
                case M_XL_SHUTDOWN:
                    return this.Method_XL_Shutdown(Parent, Parameters);
                case M_XL_CLOSE:
                    return this.Method_XL_Close(Parent, Parameters);
                case M_XL_OPEN:
                    return this.Method_XL_Open(Parent, Parameters);
                case M_XL_SAVE:
                    return this.Method_XL_Save(Parent, Parameters);
                case M_XL_SAVE_CLOSE:
                    return this.Method_XL_SaveClose(Parent, Parameters);
                case M_XL_CREATE:
                    return this.Method_XL_Create(Parent, Parameters);

                case M_XL_IMPORT:
                    return this.Method_XL_Import(Parent, Parameters);

                case M_XL_IMPORT_FAST:
                    return this.Method_XL_ImportFast(Parent, Parameters);

                case M_XL_EXPORT:
                    return this.Method_XL_Export(Parent, Parameters);
                case M_XL_COPY_PASTE:
                    return this.Method_XL_CopyPaste(Parent, Parameters);
                case M_XL_COPY_PASTE_VALUES:
                    return this.Method_XL_CopyPasteValues(Parent, Parameters);
                case M_XL_TELEPORT:
                    return this.Method_XL_Teleport(Parent, Parameters);
                case M_XL_COPY_SHEET:
                    return this.Method_XL_CopySheet(Parent, Parameters);
                case M_XL_DELETE_SHEET:
                    return this.Method_XL_DeleteSheet(Parent, Parameters);
                case M_XL_RUN_MACRO:
                    return this.Method_XL_RunMacro(Parent, Parameters);
                case M_XL_SET_VALUE:
                    return this.Method_XL_SetValue(Parent, Parameters);
                case M_XL_SET_FORMULA:
                    return this.Method_XL_SetFormula(Parent, Parameters);


            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override ParameterCollectionSigniture GetMethodSigniture(string Name)
        {

            switch (Name.ToUpper())
            {

                case M_XL_LAUNCH:
                    return ParameterCollectionSigniture.Parse(M_XL_LAUNCH, "Launches an Excel application", ParameterCollectionSigniture.ZERO_PARAMETER);
                case M_XL_SHUTDOWN:
                    return ParameterCollectionSigniture.Parse(M_XL_SHUTDOWN, "Shuts down an Excel application", ParameterCollectionSigniture.ZERO_PARAMETER);
                case M_XL_CLOSE:
                    return ParameterCollectionSigniture.Parse(M_XL_CLOSE, "Closes an Excel workbook", "BOOK|The workbook alias name|Value|false");
                case M_XL_OPEN:
                    return ParameterCollectionSigniture.Parse(M_XL_OPEN, "Opens an Excel workbook", "PATH|The path to the Excel file|Value|false");
                case M_XL_SAVE:
                    return ParameterCollectionSigniture.Parse(M_XL_SAVE, "Saves an Excel workbook", "BOOK|The workbook alias name|Value|false");
                case M_XL_SAVE_CLOSE:
                    return ParameterCollectionSigniture.Parse(M_XL_SAVE_CLOSE, "Closes and saves an Excel workbook", "BOOK|The workbook alias name|Value|false");
                case M_XL_CREATE:
                    return ParameterCollectionSigniture.Parse(M_XL_CREATE, "Creates an Excel workbook", "PATH|The path to the Excel file|Value|false");

                case M_XL_IMPORT:
                    return ParameterCollectionSigniture.Parse(M_XL_IMPORT, "Imports a range into a table", "BOOK|The workbook alias name|Value|false;SHEET|The worksheet name|Value|false;RANGE|The workbook range|Value|false;DATA|The table to import into|T|false");

                case M_XL_IMPORT_FAST:
                    return ParameterCollectionSigniture.Parse(M_XL_IMPORT_FAST, "Imports a table into a workbook", "PATH|The workbook file path|Value|false;SHEET|The worksheet name|Value|false;RANGE|The workbook range|Value|false;DATA|The table to export|T|false");

                case M_XL_EXPORT:
                    return ParameterCollectionSigniture.Parse(M_XL_EXPORT, "Exports a table into a workbook", "BOOK|The workbook alias name|Value|false;SHEET|The worksheet name|Value|false;RANGE|The workbook range|Value|false;DATA|The table to export|T|false");
                case M_XL_COPY_PASTE:
                    return ParameterCollectionSigniture.Parse(M_XL_COPY_PASTE, "Copies one book/sheet/range into another", "FROM_BOOK|The source workbook|Value|false;FROM_SHEET|The source worksheet|Value|false;FROM_RANGE|The source range|Value|false;TO_BOOK|The destination workbook|Value|false;TO_SHEET|The destination worksheet|Value|false;TO_RANGE|The destination range|Value|false");
                case M_XL_COPY_PASTE_VALUES:
                    return ParameterCollectionSigniture.Parse(M_XL_COPY_PASTE_VALUES, "Copies the values from one book/sheet/range to another", "FROM_BOOK|The source workbook|Value|false;FROM_SHEET|The source worksheet|Value|false;FROM_RANGE|The source range|Value|false;TO_BOOK|The destination workbook|Value|false;TO_SHEET|The destination worksheet|Value|false;TO_RANGE|The destination range|Value|false");
                case M_XL_TELEPORT:
                    return ParameterCollectionSigniture.Parse(M_XL_TELEPORT, "Does a strict range set from one book/sheet/range to another", "FROM_BOOK|The source workbook|Value|false;FROM_SHEET|The source worksheet|Value|false;FROM_RANGE|The source range|Value|false;TO_BOOK|The destination workbook|Value|false;TO_SHEET|The destination worksheet|Value|false;TO_RANGE|The destination range|Value|false");
                case M_XL_COPY_SHEET:
                    return ParameterCollectionSigniture.Parse(M_XL_COPY_SHEET, "Copies one book/sheet to another", "FROM_BOOK|The source workbook|Value|false;FROM_SHEET|The source worksheet|Value|false;TO_BOOK|The destination workbook|Value|false;TO_SHEET|The destination worksheet|Value|false");
                case M_XL_DELETE_SHEET:
                    return ParameterCollectionSigniture.Parse(M_XL_DELETE_SHEET, "Deletes a sheet form a workbook", "BOOK|The workbook alias name|Value|false;SHEET|The worksheet name|Value|false");
                case M_XL_RUN_MACRO:
                    return ParameterCollectionSigniture.Parse(M_XL_RUN_MACRO, "Runs a macro", "NAME|The macro name|Value|false");
                case M_XL_SET_VALUE:
                    return ParameterCollectionSigniture.Parse(M_XL_SET_VALUE, "Set a cell/range to a specific Value", "BOOK|The workbook alias name|Value|false;SHEET|The worksheet name|Value|false;RANGE|The workbook range|Value|false;VALUE|The Value to set|Value|false");
                case M_XL_SET_FORMULA:
                    return ParameterCollectionSigniture.Parse(M_XL_SET_FORMULA, "Sets a formula", "BOOK|The workbook alias name|Value|false;SHEET|The worksheet name|Value|false;RANGE|The workbook range|Value|false;FORMULA|The formula to set|Value|false");


            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override string[] MethodNames
        {
            get { return _MethodNames; }
        }

        public override CellFunction GetFunction(string Name)
        {
            throw new NotImplementedException();
        }

        public override string[] FunctionNames
        {
            get
            {
                return _FunctionNames;
            }
        }

        // Methods //
        private Method Method_XL_Launch(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                this._xl.Launch();

            };
            return new LibraryMethod(Parent, M_XL_LAUNCH, Parameters, false, kappa);

        }

        private Method Method_XL_Shutdown(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                this._xl.ShutDown();

            };
            return new LibraryMethod(Parent, M_XL_SHUTDOWN, Parameters, false, kappa);

        }

        private Method Method_XL_Open(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string path = x.Expressions["PATH"].Evaluate().valueSTRING;
                this._xl.OpenWorkbook(path);

            };
            return new LibraryMethod(Parent, M_XL_SHUTDOWN, Parameters, false, kappa);

        }

        private Method Method_XL_Close(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string book = x.Expressions["BOOK"].Evaluate().valueSTRING;
                this._xl.CloseWorkbook(book);

            };
            return new LibraryMethod(Parent, M_XL_CLOSE, Parameters, false, kappa);

        }

        private Method Method_XL_Save(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string book = x.Expressions["BOOK"].Evaluate().valueSTRING;
                this._xl.SaveWorkbook(book);

            };
            return new LibraryMethod(Parent, M_XL_SAVE, Parameters, false, kappa);

        }

        private Method Method_XL_SaveClose(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string book = x.Expressions["BOOK"].Evaluate().valueSTRING;
                this._xl.SaveCloseWorkbook(book);

            };
            return new LibraryMethod(Parent, M_XL_SAVE_CLOSE, Parameters, false, kappa);

        }

        private Method Method_XL_Create(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string path = x.Expressions["PATH"].Evaluate().valueSTRING;
                this._xl.CreateWorkbook(path);

            };
            return new LibraryMethod(Parent, M_XL_CREATE, Parameters, false, kappa);

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
            return new LibraryMethod(Parent, M_XL_IMPORT, Parameters, false, kappa);

        }

        private Method Method_XL_ImportFast(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData data = x.Tables["DATA"];
                string Path = x.Expressions["PATH"].Evaluate().valueSTRING;
                string Sheet = x.Expressions["SHEET"].Evaluate().valueSTRING;
                string Range = x.Expressions["RANGE"].Evaluate().valueSTRING;
                RecordWriter stream = data.OpenWriter();

                Exchange.MSExcelFast.ImportExcelFast(stream, Path, Sheet, Range);

                stream.Close();

            };
            return new LibraryMethod(Parent, M_XL_IMPORT, Parameters, false, kappa);

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
            return new LibraryMethod(Parent, M_XL_EXPORT, Parameters, false, kappa);

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
            return new LibraryMethod(Parent, M_XL_COPY_PASTE, Parameters, false, kappa);

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
            return new LibraryMethod(Parent, M_XL_COPY_PASTE_VALUES, Parameters, false, kappa);

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
            return new LibraryMethod(Parent, M_XL_TELEPORT, Parameters, false, kappa);

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
            return new LibraryMethod(Parent, M_XL_COPY_SHEET, Parameters, false, kappa);

        }

        private Method Method_XL_DeleteSheet(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string Book = x.Expressions["BOOK"].Evaluate().valueSTRING;
                string Sheet = x.Expressions["SHEET"].Evaluate().valueSTRING;

                this._xl.DeleteSheet(Book, Sheet);

            };
            return new LibraryMethod(Parent, M_XL_DELETE_SHEET, Parameters, false, kappa);

        }

        private Method Method_XL_RunMacro(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string Name = x.Expressions["NAME"].Evaluate().valueSTRING;

                this._xl.RunMacro(Name);

            };
            return new LibraryMethod(Parent, M_XL_RUN_MACRO, Parameters, false, kappa);

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
            return new LibraryMethod(Parent, M_XL_SET_VALUE, Parameters, false, kappa);

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
            return new LibraryMethod(Parent, M_XL_SET_FORMULA, Parameters, false, kappa);

        }





    }

}
