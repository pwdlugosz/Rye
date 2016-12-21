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

    public sealed class TableFunctionLibrary : FunctionLibrary
    {

        public const string LIBRARY_NAME = "TABLIX";

        public TableFunctionLibrary(Session Session)
            : base(Session)
        {
            this.LibName = LIBRARY_NAME;
        }

        // Functions //
        public const string ROW_COUNT = "ROW_COUNT";
        public const string COLUMN_COUNT = "COLUMN_COUNT";
        public const string EXTENT_COUNT = "EXTENT_COUNT";
        public const string CELL_COUNT = "CELL_COUNT";
        public const string IS_SORTED = "IS_SORTED";
        public const string IS_SORTED_BY = "IS_SORTED_BY";
        public const string EXISTS = "EXISTS";

        private static string[] _FunctionNames =
        {
            ROW_COUNT,
            COLUMN_COUNT,
            EXTENT_COUNT,
            CELL_COUNT,
            IS_SORTED,
            IS_SORTED_BY,
            EXISTS
        };

        public override string[] Names
        {
            get { return _FunctionNames; }
        }

        public override CellFunction RenderFunction(string Name)
        {

            switch (Name.ToUpper())
            {
                case ROW_COUNT:
                    return this.LambdaRowCount();
                case COLUMN_COUNT:
                    return this.LambdaColumnCount();
                case EXTENT_COUNT:
                    return this.LambdaExtentCount();
                case CELL_COUNT:
                    return this.LambdaCellCount();
                case IS_SORTED:
                    return this.LambdaIsSorted();
                case IS_SORTED_BY:
                    return this.LambdaIsSortedBy();
                case EXISTS:
                    return this.LambdaExists();

            };

            throw new ArgumentException(string.Format("Cell function '{0}' does not exist", Name));

        }

        private CellFunction LambdaRowCount()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                string[] name = x[0].valueSTRING.Split('.');
                TabularData t = this._Session.GetTabularData(name[0], name[1]);
                return new Cell(t.RecordCount);
            };
            return new CellFunctionFixedShell(ROW_COUNT, 1, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaColumnCount()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                string[] name = x[0].valueSTRING.Split('.');
                TabularData t = this._Session.GetTabularData(name[0], name[1]);
                return new Cell(t.Columns.Count);
            };
            return new CellFunctionFixedShell(COLUMN_COUNT, 1, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaExtentCount()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                string[] name = x[0].valueSTRING.Split('.');
                TabularData t = this._Session.GetTabularData(name[0], name[1]);
                return new Cell(t.ExtentCount);
            };
            return new CellFunctionFixedShell(EXTENT_COUNT, 1, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaCellCount()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                string[] name = x[0].valueSTRING.Split('.');
                TabularData t = this._Session.GetTabularData(name[0], name[1]);
                return new Cell(t.CellCount);
            };
            return new CellFunctionFixedShell(CELL_COUNT, 1, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaIsSorted()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {

                string[] name = x[0].valueSTRING.Split('.');
                TabularData t = this._Session.GetTabularData(name[0], name[1]);
                return new Cell(t.IsSorted);

            };
            return new CellFunctionFixedShell(IS_SORTED, 1, CellAffinity.INT, lambda);

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
            return new CellFunctionFixedShell(IS_SORTED_BY, 2, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaExists()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {

                string[] name = x[0].valueSTRING.Split('.');
                return new Cell(this._Session.TabularDataExists(name[0], name[1]));

            };
            return new CellFunctionFixedShell(EXISTS, 1, CellAffinity.BOOL, lambda);

        }


    }

    public sealed class TableMethodLibrary : MethodLibrary
    {

        public const string LIBRARY_NAME = "TABLIX";
        public const string IMPORT = "IMPORT";
        public const string EXPORT = "EXPORT";
        public const string ABOUT = "ABOUT";
        
        public TableMethodLibrary(Session Session)
            : base(Session)
        {
            this.Build();
            this.LibName = LIBRARY_NAME;
        }

        // Methods //

        private static string[] _MethodNames = new string[]
        {
            IMPORT,
            EXPORT,
            ABOUT
        };

        private Heap2<string, string> _CompressedSig;

        /*
        * Name | Description | Affinity | Can Be Null?
        * 
        * Affinity can be:
        * Value: expression
        * V: vector
        * M: matrix expression
        * T: table (or extent)
        * 
        */

        private void Build()
        {

            this._CompressedSig = new Heap2<string, string>();

            this._CompressedSig.Allocate(TableMethodLibrary.IMPORT, "Loads a file into an existing table", "DATA|The table to load|T|false;PATH|The flat file location|Value|false;DELIM|The column delimitor|Value|false;ESCAPE|The escape sequence character|Value|true;SKIP|The number of lines to skip|Value|true");
            this._CompressedSig.Allocate(TableMethodLibrary.EXPORT, "Exports a table into a new file", "DATA|The table to export|T|false;PATH|The path to the exported file|Value|false;DELIM|The column delimitor|Value|false");
            this._CompressedSig.Allocate(TableMethodLibrary.ABOUT, "Prints meta data abouta table", "DATA|The table to export|T|false");
            
        }

        public override Method RenderMethod(Method Parent, string Name, ParameterCollection Parameters)
        {

            switch (Name.ToUpper())
            {
                case TableMethodLibrary.IMPORT:
                    return this.Method_Import(Parent, Parameters);
                case TableMethodLibrary.EXPORT:
                    return this.Method_Export(Parent, Parameters);
                case TableMethodLibrary.ABOUT:
                    return this.Method_About(Parent, Parameters);
            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override ParameterCollectionSigniture RenderSigniture(string Name)
        {

            if (this._CompressedSig.Exists(Name))
                return ParameterCollectionSigniture.Parse(Name, this._CompressedSig[Name].Item1, this._CompressedSig[Name].Item2);
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override string[] Names
        {
            get { return _MethodNames; }
        }

        private Method Method_Export(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData Data = x.Tables["DATA"];
                string Path = x.Expressions["PATH"].Evaluate().valueSTRING;
                char Delim = (x.Expressions["DELIM"] == null ? '\t' : x.Expressions["DELIM"].Evaluate().valueSTRING.First());
                this._Session.Kernel.TextDump(Data, Path, Delim);

            };
            return new LibraryMethod(Parent, EXPORT, Parameters, false, kappa);

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
            return new LibraryMethod(Parent, EXPORT, Parameters, false, kappa);

        }

        private Method Method_About(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData Data = x.Tables["DATA"];
                this._Session.IO.WriteLine(Data.InfoString);

            };
            return new LibraryMethod(Parent, ABOUT, Parameters, false, kappa);

        }


    }

}
