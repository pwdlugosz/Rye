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

        private static string[] _FunctionNames =
        {
            ROW_COUNT,
            COLUMN_COUNT,
            EXTENT_COUNT,
            CELL_COUNT,
            IS_SORTED,
            IS_SORTED_BY
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

    }

    public sealed class TableMethodLibrary : MethodLibrary
    {

        public const string LIBRARY_NAME = "TABLE";

        public TableMethodLibrary(Session Session)
            : base(Session)
        {
            this.Build();
            this.LibName = LIBRARY_NAME;
        }

        // Methods //

        private static string[] _MethodNames = new string[]
        {
        };

        private Heap2<string, string> _CompressedSig;

        /*
        * Name | Description | Affinity | Can Be Null?
        * 
        * Affinity can be:
        * E: expression
        * V: vector
        * M: matrix expression
        * T: table (or extent)
        * 
        */

        private void Build()
        {

            this._CompressedSig = new Heap2<string, string>();

            this._CompressedSig.Allocate(FileMethodLibrary.ZIP, "Zips a file", "IN_PATH|The path of the file to zip|E|false;OUT_PATH|The path to the zipped file|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.UNZIP, "Unzips a file", "IN_PATH|The path of the file to unzip (*.zip)|E|false;OUT_PATH|The path to directory|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.WRITE_ALL_TEXT, "Writes text to a file; if the file already exists, it will be overwritten", "PATH|The path of the file to dump text to|E|false;TEXT|Text to write|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.WRITE_ALL_BYTES, "Writes bytes to a file; if the file already exists, it will be overwritten", "PATH|The path of the file to dump text to|E|false;OUT_PATH|Bytes to write|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.APPEND_ALL_TEXT, "Writes text to a file; if the file exists, it will append the text to the end", "PATH|The path of the file to dump text to|E|false;TEXT|Text to write|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.APPEND_ALL_BYTES, "Writes bytes to a file; if the file exists, it will append the bytes to the end", "PATH|The path of the file to dump text to|E|false;OUT_PATH|Bytes to write|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.DELETE, "Deletes a file", "PATH|The file to delete|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.MOVE, "Moves a file to another location", "FROM_PATH|The original file to move|E|false;TO_PATH|The new location to move to|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.COPY, "Copies a file to another location", "FROM_PATH|The original file to copy|E|false;TO_PATH|The path to put the copy in|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.IMPORT, "Loads a file into an existing table", "DATA|The table to load|T|false;PATH|The flat file location|E|false;DELIM|The column delimitor|E|false;ESCAPE|The escape sequence character|E|true;SKIP|The number of lines to skip|E|true");
            this._CompressedSig.Allocate(FileMethodLibrary.EXPORT, "Exports a table into a new file", "DATA|The table to export|T|false;PATH|The path to the exported file|E|false;DELIM|The column delimitor|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.DOWNLOAD, "Downloads a url to a file", "URL|The URL to download|E|false;PATH|The path to the exported file|E|false");

        }

        public override Method RenderMethod(Method Parent, string Name, ParameterCollection Parameters)
        {

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

    }






}
