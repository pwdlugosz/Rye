using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Methods;
using Rye.Expressions;
using Rye.Data;
using Rye.Interpreter;
using System.IO;

namespace Rye.Structures
{

    public sealed class TableStructure : MemoryStructure
    {

        public const string STRUCT_NAME = "DATA";

        private Workspace _Session;

        public TableStructure(Workspace Session)
            : base(STRUCT_NAME)
        {
            this._Session = Session;
            this._functions = new TableFunctionLibrary(Session);
            this._procedures = ProcedureLibrary.NullLibrary;
        }

        public sealed class TableFunctionLibrary : FunctionLibrary
        {

            public const string ROW_COUNT = "ROW_COUNT";
            public const string COLUMN_COUNT = "COLUMN_COUNT";
            public const string EXTENT_COUNT = "EXTENT_COUNT";
            public const string CELL_COUNT = "CELL_COUNT";
            public const string IS_SORTED = "IS_SORTED";
            public const string IS_SORTED_BY = "IS_SORTED_BY";

            private static string[] _Names =
            {
                ROW_COUNT,
                COLUMN_COUNT,
                EXTENT_COUNT,
                CELL_COUNT,
                IS_SORTED,
                IS_SORTED_BY
            };

            private Workspace _Session;

            public TableFunctionLibrary(Workspace Session)
                : base()
            {
                this._Session = Session;
            }

            public override string[] Names
            {
                get { return _Names; }
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
                    DataSet t = this._Session.GetData(name[0], name[1]);
                    return new Cell(t.RecordCount);
                };
                return new CellFunctionFixedShell(ROW_COUNT, 1, CellAffinity.INT, lambda);

            }

            private CellFunction LambdaColumnCount()
            {

                Func<Cell[], Cell> lambda = (x) =>
                {
                    string[] name = x[0].valueSTRING.Split('.');
                    DataSet t = this._Session.GetData(name[0], name[1]);
                    return new Cell(t.Columns.Count);
                };
                return new CellFunctionFixedShell(COLUMN_COUNT, 1, CellAffinity.INT, lambda);

            }

            private CellFunction LambdaExtentCount()
            {

                Func<Cell[], Cell> lambda = (x) =>
                {
                    string[] name = x[0].valueSTRING.Split('.');
                    DataSet t = this._Session.GetData(name[0], name[1]);
                    return new Cell(t.ExtentCount);
                };
                return new CellFunctionFixedShell(EXTENT_COUNT, 1, CellAffinity.INT, lambda);

            }

            private CellFunction LambdaCellCount()
            {

                Func<Cell[], Cell> lambda = (x) =>
                {
                    string[] name = x[0].valueSTRING.Split('.');
                    DataSet t = this._Session.GetData(name[0], name[1]);
                    return new Cell(t.CellCount);
                };
                return new CellFunctionFixedShell(CELL_COUNT, 1, CellAffinity.INT, lambda);

            }

            private CellFunction LambdaIsSorted()
            {

                Func<Cell[], Cell> lambda = (x) =>
                {

                    string[] name = x[0].valueSTRING.Split('.');
                    DataSet t = this._Session.GetData(name[0], name[1]);
                    return new Cell(t.IsSorted);

                };
                return new CellFunctionFixedShell(IS_SORTED, 1, CellAffinity.INT, lambda);

            }

            private CellFunction LambdaIsSortedBy()
            {

                Func<Cell[], Cell> lambda = (x) =>
                {
                    
                    string[] name = x[0].valueSTRING.Split('.');
                    DataSet t = this._Session.GetData(name[0], name[1]);
                    string values = x[1].valueSTRING;
                    Key k = Key.Parse(values);
                    return new Cell(t.IsSortedBy(k));
                    
                };
                return new CellFunctionFixedShell(IS_SORTED_BY, 2, CellAffinity.INT, lambda);

            }

        }



    }

}
