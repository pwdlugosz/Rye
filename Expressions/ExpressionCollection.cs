using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Structures;

namespace Rye.Expressions
{

    public sealed class ExpressionCollection : IRegisterExtractor
    {

        private Heap<Expression> _Nodes;
        //private Dictionary<Guid, Register> _Registers;
        private Heap<Register> _Registers;
        
        // Constructor //
        public ExpressionCollection()
        {
            this._Nodes = new Heap<Expression>();
            //this._Registers = new Dictionary<Guid, Register>();
            this._Registers = new Heap<Register>();
        }

        public ExpressionCollection(params Expression[] Nodes)
            :this()
        {
            foreach (Expression n in Nodes)
                this.Add(n);
        }

        // Properties //
        public int Count
        {
            get
            {
                return this._Nodes.Count;
            }
        }

        public Schema Columns
        {
            get
            {

                Schema cols = new Schema();
                for (int i = 0; i < this.Count; i++)
                    cols.Add(this._Nodes.Name(i), this._Nodes[i].ReturnAffinity(), true, this._Nodes[i].DataSize());
                return cols;

            }
        }

        public Expression this[int Index]
        {
            get { return this._Nodes[Index]; }
        }

        public Expression this[string Alias]
        {
            get
            {
                return this._Nodes[Alias];
            }
        }

        public IEnumerable<Expression> Nodes
        {
            get { return this._Nodes.Values; }
        }

        public List<int> FieldRefs
        {

            get
            {

                List<int> refs = new List<int>();
                foreach (Expression n in this.Nodes)
                {
                    refs.AddRange(Analytics.AllFieldRefs(n));
                }
                return refs.Distinct().ToList();

            }

        }

        public bool IsVolatile
        {
            get
            {
                foreach (Expression e in this._Nodes.Values)
                {
                    if (e.IsVolatile)
                        return true;
                }
                return false;
            }
        }

        // Adds //
        public void Add(Expression Node, string Alias)
        {

            // Check if alias exists //
            if (this._Nodes.Exists(Alias))
                throw new Exception(string.Format("Alias '{0}' already exists", Alias));

            // Handle the registers //
            this._Registers.Import(Node.GetMemoryRegisters());

            this._Nodes.Allocate(Alias, Node);

        }

        public void Add(Expression Node)
        {
            this.Add(Node, "Value" + this.Count.ToString());
        }

        // Methods //
        public Record Evaluate()
        {
            Cell[] c = new Cell[this.Count];
            for (int i = 0; i < this.Count; i++)
                c[i] = this._Nodes[i].Evaluate();
            return new Record(c);
        }

        public int Reference(string Name)
        {

            for (int i = 0; i < this.Count; i++)
            {
                if (StringComparer.OrdinalIgnoreCase.Compare(this._Nodes.Name(i), Name) == 0)
                    return i;
            }
            return -1;

        }

        public string Alias(int Index)
        {
            return this._Nodes.Name(Index);
        }

        public ExpressionCollection CloneOfMe()
        {
            ExpressionCollection nodes = new ExpressionCollection();
            for (int i = 0; i < this.Count; i++)
            {
                nodes.Add(this._Nodes[i].CloneOfMe(), this._Nodes.Name(i));
            }
            return nodes;
        }

        public string Unparse(Schema Columns)
        {
            StringBuilder sb = new StringBuilder();
            foreach (Expression n in this.Nodes)
                sb.Append(n.Unparse(Columns) + " , ");
            return sb.ToString();
        }

        // Register Handles //
        public Heap<Register> GetMemoryRegisters()
        {
            return this._Registers;
        }

        public void ForceMemoryRegister(Register NewRegister)
        {

            foreach (Expression x in this._Nodes.Values)
            {
                Expression.AssignRegister(x, NewRegister);
            }

        }

        public void ForceMemoryRegister(Heap<Register> NewRegisters)
        {

            foreach (Expression x in this._Nodes.Values)
            {
                Expression.AssignRegister(x, NewRegisters);
            }

        }

        public void ForceCellHeap(Heap<Cell> NewHeap)
        {
            foreach (Expression x in this._Nodes.Values)
            {
                Expression.AssignCellHeap(x, NewHeap);
            }
        }

        public void ForceCellMaxtrixHeap(Heap<CellMatrix> NewHeap)
        {
            foreach (Expression x in this._Nodes.Values)
            {
                Expression.AssignMatrixHeap(x, NewHeap);
            }
        }

        // Statics //
        public static ExpressionCollection Union(params ExpressionCollection[] NodeSets)
        {

            ExpressionCollection f = new ExpressionCollection();
            foreach (ExpressionCollection n in NodeSets)
            {

                for (int i = 0; i < n.Count; i++)
                {
                    f.Add(n._Nodes[i], n._Nodes.Name(i));
                }

            }
            return f;

        }

        public static ExpressionCollection Render(Schema Columns, string Alias, Register Memory, Key KeepValues)
        {
            return ExpressionCollection.Render(Schema.Split(Columns, KeepValues), Alias, Memory);
        }

        public static ExpressionCollection Render(Schema Columns, string Alias, Register Memory)
        {

            ExpressionCollection col = new ExpressionCollection();
            for (int i = 0; i < Columns.Count; i++)
            {
                Expression e = new ExpressionFieldRef(null, i, Columns.ColumnAffinity(i), Columns.ColumnSize(i), Memory);
                col.Add(e, Columns.ColumnName(i));
            }
            return col;

        }

        public static Key DecompileToKey(ExpressionCollection E)
        {

            // Check that there is only one register //
            if (E._Registers.Count != 1)
                return new Key();

            Key K = new Key();
            foreach (Expression e in E._Nodes.Values)
            {
                int idx = Expression.DecompileToFieldRef(e);
                if (idx != -1)
                    K.Add(idx);
            }

            return K;

        }

        /// <summary>
        /// Forces all registers to be assigned to the given register; does not check that the new register and the old register share the same identifier
        /// </summary>
        /// <param name="E"></param>
        /// <param name="R"></param>
        public static void ForceAssignRegister(ExpressionCollection E, Register R)
        {

            foreach (Expression e in E.Nodes)
                Expression.ForceAssignRegister(e, R);

        }


    }


}
