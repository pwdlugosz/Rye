using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Structures;

namespace Rye.Expressions
{

    public sealed class ExpressionCollection
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

        // Adds //
        public void Add(Expression Node, string Alias)
        {

            // Check if alias exists //
            if (this._Nodes.Exists(Alias))
                throw new Exception(string.Format("Alias '{0}' already exists", Alias));

            // Handle the registers //
            List<Register> reg = Node.GetMemoryRegisters();
            foreach (Register r in reg)
            {
                
                // If it exists, check the UID //
                if (this._Registers.Exists(r.Name))
                {
                    if (r.UID != this._Registers[r.Name].UID)
                        throw new DuplicateWaitObjectException(string.Format("Register with alias '{0}' already exists with UID '{1}'; '{2}' is the UID of new expression", r.Name, this._Registers[r.Name].UID, r.UID));
                }
                // Otherwise, add the register //
                else
                {
                    this._Registers.Allocate(r.Name, r);
                }

            }

            this._Nodes.Allocate(Alias, Node);

        }

        public void Add(Expression Node)
        {
            this.Add(Node, "E" + this.Count.ToString());
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
        public List<Register> GetMemoryRegisters()
        {
            return this._Registers.Values;
        }

        public void AssignMemoryRegister(Register OldMemoryRegister, Register NewMemoryRegister)
        {

            // Check that the cache entry's UID matches the old register's UID //
            if (!this._Registers.Exists(OldMemoryRegister.Name))
                return; // Don't want to error out if we try to override a non-existant register //

            // Actually assign the heap //
            this._Registers[OldMemoryRegister.Name] = NewMemoryRegister;

            // Update the root nodes for each expression //
            foreach (Expression e in this._Nodes.Values)
            {
                e.AssignMemoryRegister(OldMemoryRegister, NewMemoryRegister);
            }


        }

        public Register GetMemoryRegister(string Alias)
        {
            return this._Registers[Alias];
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

        public static ExpressionCollection Render(Schema Columns, string Alias, Key KeepValues)
        {
            return ExpressionCollection.Render(Schema.Split(Columns, KeepValues), Alias);
        }

        public static ExpressionCollection Render(Schema Columns, string Alias)
        {

            Register reg = new Register(Alias, Columns);
            ExpressionCollection col = new ExpressionCollection();
            for (int i = 0; i < Columns.Count; i++)
            {
                Expression e = new ExpressionFieldRef(null, i, Columns.ColumnAffinity(i), Columns.ColumnSize(i), reg);
                col.Add(e, Columns.ColumnName(i));
            }
            return col;

        }


    }



}
