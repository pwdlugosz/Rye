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
        protected string _name;

        public MemoryStructure(string Name)
        {
            this._scalar = new Heap<Cell>();
            this._matrix = new Heap<CellMatrix>();
            this._extents = new Heap<Extent>();
            this._name = Name;
        }

        /// <summary>
        /// Gets the name of memory structure
        /// </summary>
        public string Name
        {
            get { return this._name; }
        }

        /// <summary>
        /// Gets the cell resource
        /// </summary>
        public Heap<Cell> Scalars
        {
            get { return this._scalar; }
        }

        /// <summary>
        /// Gets the cell matrix resource
        /// </summary>
        public Heap<CellMatrix> Matricies
        {
            get { return this._matrix; }
        }

        /// <summary>
        /// Gets the extent resource
        /// </summary>
        public Heap<Extent> Extents
        {
            get { return this._extents; }
        }

        /// <summary>
        /// This method is intended to release any resources used by the structure
        /// </summary>
        public virtual void Burn()
        {
        }

    }

    public sealed class GlobalStructure : MemoryStructure
    {

        public const string DEFAULT_NAME = "GLOBAL";

        public GlobalStructure()
            : base(DEFAULT_NAME)
        {

            this._scalar.Allocate("MS_TICKS", new Cell(10000L));
            this._scalar.Allocate("S_TICKS", new Cell(10000000L));
            this._scalar.Allocate("M_TICKS", new Cell(600000000L));
            this._scalar.Allocate("H_TICKS", new Cell(36000000000L));
            this._scalar.Allocate("D_TICKS", new Cell(864000000000L));

        }

    }

}
