using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Methods;

namespace Rye.Structures
{
    
    public class MemoryStructure
    {

        protected Heap<Cell> _scalar;
        protected Heap<CellMatrix> _matrix;
        protected Heap<Extent> _extents;
        protected FunctionLibrary _functions;
        protected ProcedureLibrary _procedures;
        protected string _name;

        public MemoryStructure(string Name)
        {
            this._scalar = new Heap<Cell>();
            this._matrix = new Heap<CellMatrix>();
            this._extents = new Heap<Extent>();
            this._name = Name;
        }

        public string Name
        {
            get { return this._name; }
        }

        public Heap<Cell> Scalars
        {
            get { return this._scalar; }
        }

        public Heap<CellMatrix> Matricies
        {
            get { return this._matrix; }
        }

        public Heap<Extent> Extents
        {
            get { return this._extents; }
        }

        public FunctionLibrary Functions
        {
            get { return this._functions; }
        }

        public ProcedureLibrary Procedures
        {
            get { return this._procedures; }
        }

    }

    public sealed class GlobalStructure : MemoryStructure
    {

        public const string DEFAULT_NAME = "GLOBAL";

        public GlobalStructure()
            : base(DEFAULT_NAME)
        {
            this._functions = new SystemFunctionLibrary();
        }

    }

}
