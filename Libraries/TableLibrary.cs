using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Methods;
using Rye.Expressions;
using Rye.Data;
using Rye.Interpreter;
using Rye.Structures;
using System.IO;

namespace Rye.Libraries
{


    // BaseTable Library //
    public sealed class TableLibrary : Library
    {

        public const string M_IMPORT = "IMPORT";
        public const string M_EXPORT = "EXPORT";
        public const string M_ABOUT = "ABOUT";

        private static string[] _MethodNames = new string[]
        {
            M_IMPORT,
            M_EXPORT,
            M_ABOUT
        };

        public const string F_ROW_COUNT = "ROW_COUNT";
        public const string F_COLUMN_COUNT = "COLUMN_COUNT";
        public const string F_EXTENT_COUNT = "EXTENT_COUNT";
        public const string F_CELL_COUNT = "CELL_COUNT";
        public const string F_IS_SORTED = "IS_SORTED";
        public const string F_IS_SORTED_BY = "IS_SORTED_BY";
        public const string F_EXISTS = "EXISTS";

        private static string[] _FunctionNames =
        {
            F_ROW_COUNT,
            F_COLUMN_COUNT,
            F_EXTENT_COUNT,
            F_CELL_COUNT,
            F_IS_SORTED,
            F_IS_SORTED_BY,
            F_EXISTS
        };

        public TableLibrary(Session Session)
            : base(Session, "TABLIX")
        {
        }

        public override Method GetMethod(Method Parent, string Name, ParameterCollection Parameters)
        {

            switch (Name.ToUpper())
            {
                case M_IMPORT:
                    return this.Method_Import(Parent, Parameters);
                case M_EXPORT:
                    return this.Method_Export(Parent, Parameters);
                case M_ABOUT:
                    return this.Method_About(Parent, Parameters);
            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override ParameterCollectionSigniture GetMethodSigniture(string Name)
        {

            switch (Name.ToUpper())
            {
                case M_IMPORT:
                    return ParameterCollectionSigniture.Parse(M_IMPORT, "Loads a file into an existing table", "DATA|The table to load|T|false;PATH|The flat file location|Value|false;DELIM|The column delimitor|Value|false;ESCAPE|The escape sequence character|Value|true;SKIP|The number of lines to skip|Value|true");
                case M_EXPORT:
                    return ParameterCollectionSigniture.Parse(M_EXPORT, "Exports a table into a new file", "DATA|The table to export|T|false;PATH|The path to the exported file|Value|false;DELIM|The column delimitor|Value|false");
                case M_ABOUT:
                    return ParameterCollectionSigniture.Parse(M_ABOUT, "Prints meta data abouta table", "DATA|The table to export|T|false");
            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override string[] MethodNames
        {
            get { return _MethodNames; }
        }

        public override CellFunction GetFunction(string Name)
        {

            switch (Name.ToUpper())
            {
                case F_ROW_COUNT:
                    return this.LambdaRowCount();
                case F_COLUMN_COUNT:
                    return this.LambdaColumnCount();
                case F_EXTENT_COUNT:
                    return this.LambdaExtentCount();
                case F_CELL_COUNT:
                    return this.LambdaCellCount();
                case F_IS_SORTED:
                    return this.LambdaIsSorted();
                case F_IS_SORTED_BY:
                    return this.LambdaIsSortedBy();
                case F_EXISTS:
                    return this.LambdaExists();

            };

            throw new ArgumentException(string.Format("Cell function '{0}' does not exist", Name));

        }

        public override string[] FunctionNames
        {
            get
            {
                return _FunctionNames;
            }
        }

        // Methods //
        private Method Method_Export(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData Data = x.Tables["DATA"];
                string Path = x.Expressions["PATH"].Evaluate().valueSTRING;
                char Delim = (x.Expressions["DELIM"] == null ? '\t' : x.Expressions["DELIM"].Evaluate().valueSTRING.First());
                this._Session.Kernel.TextDump(Data, Path, Delim);

            };
            return new LibraryMethod(Parent, M_EXPORT, Parameters, false, kappa);

        }

        private Method Method_Import(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData Data = x.Tables["DATA"];
                string Path = x.Expressions["PATH"].Evaluate().valueSTRING;
                char[] Delim = x.Expressions["DELIM"].Evaluate().valueSTRING.ToCharArray();
                char Escape = (x.Expressions["ESCAPE"] != null ? x.Expressions["ESCAPE"].Evaluate().valueSTRING.First() : char.MaxValue);
                int Skip = (x.Expressions["SKIP"] != null ? (int)x.Expressions["SKIP"].Evaluate().valueINT : 0);
                this._Session.Kernel.TextPop(Data, Path, Delim, Escape, Skip);

            };
            return new LibraryMethod(Parent, M_EXPORT, Parameters, false, kappa);

        }

        private Method Method_About(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData Data = x.Tables["DATA"];
                this._Session.IO.WriteLine(Data.InfoString);

            };
            return new LibraryMethod(Parent, M_ABOUT, Parameters, false, kappa);

        }

        // Functions //
        private CellFunction LambdaRowCount()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                string[] name = x[0].valueSTRING.Split('.');
                TabularData t = this._Session.GetTabularData(name[0], name[1]);
                return new Cell(t.RecordCount);
            };
            return new CellFunctionFixedShell(F_ROW_COUNT, 1, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaColumnCount()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                string[] name = x[0].valueSTRING.Split('.');
                TabularData t = this._Session.GetTabularData(name[0], name[1]);
                return new Cell(t.Columns.Count);
            };
            return new CellFunctionFixedShell(F_COLUMN_COUNT, 1, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaExtentCount()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                string[] name = x[0].valueSTRING.Split('.');
                TabularData t = this._Session.GetTabularData(name[0], name[1]);
                return new Cell(t.ExtentCount);
            };
            return new CellFunctionFixedShell(F_EXTENT_COUNT, 1, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaCellCount()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                string[] name = x[0].valueSTRING.Split('.');
                TabularData t = this._Session.GetTabularData(name[0], name[1]);
                return new Cell(t.CellCount);
            };
            return new CellFunctionFixedShell(F_CELL_COUNT, 1, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaIsSorted()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {

                string[] name = x[0].valueSTRING.Split('.');
                TabularData t = this._Session.GetTabularData(name[0], name[1]);
                return new Cell(t.IsSorted);

            };
            return new CellFunctionFixedShell(F_IS_SORTED, 1, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaIsSortedBy()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {

                string[] name = x[0].valueSTRING.Split('.');
                TabularData t = this._Session.GetTabularData(name[0], name[1]);
                string values = x[1].valueSTRING;
                Key k = Key.Parse(values);
                return new Cell(t.IsSortedBy(k));

            };
            return new CellFunctionFixedShell(F_IS_SORTED_BY, 2, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaExists()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {

                string[] name = x[0].valueSTRING.Split('.');
                return new Cell(this._Session.TabularDataExists(name[0], name[1]));

            };
            return new CellFunctionFixedShell(F_EXISTS, 1, CellAffinity.BOOL, lambda);

        }


    }


}
