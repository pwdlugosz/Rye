using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Expressions
{

    public sealed class ExpressionFieldRef : Expression
    {

        private int _idx;
        private CellAffinity _affinity;
        private Register _memory;
        private int _size;

        public ExpressionFieldRef(Expression Parent, int Index, CellAffinity Affinity, int Size, Register MemoryRef)
            : base(Parent, ExpressionAffinity.Field)
        {
            this._idx = Index;
            this._affinity = Affinity;
            this._memory = MemoryRef;
            this._size = Size;
            this._name = MemoryRef.Columns.ColumnName(Index);
        }

        public override Cell Evaluate()
        {
            return this._memory.Value[_idx];
        }

        public override CellAffinity ReturnAffinity()
        {
            return _affinity;
        }

        public override string ToString()
        {
            return this.Affinity.ToString() + " : " + this._idx.ToString();
        }

        public override int GetHashCode()
        {
            return this._idx ^ Expression.HashCode(this._Cache);
        }

        public override string Unparse(Schema S)
        {
            if (S == null)
                return string.Format("@R[{0}]", this._idx) + (this._memory == null ? "NULL_MEM" : "");
            return S.ColumnName(this._idx) + (this._memory == null ? "NULL_MEM" : "");
        }

        public override Expression CloneOfMe()
        {
            return new ExpressionFieldRef(this.ParentNode, this._idx, this._affinity, this._size, this._memory.CloneOfMe());
        }

        public override void AssignMemoryRegister(Register OldMemoryRegister, Register NewMemoryRegister)
        {

            if (this._memory.UID == OldMemoryRegister.UID)
                this._memory = NewMemoryRegister;

        }

        public override int DataSize()
        {
            return this._size;
        }

        public override List<Register> GetMemoryRegisters()
        {
            return new List<Register> { this._memory };
        }

        public int Index
        {
            get { return this._idx; }
        }

        public Register MemoryRegister
        {
            get { return this._memory;}
            set { this._memory = value; }
        }

        public void Repoint(Schema OriginalSchema, Schema NewSchema)
        {

            if (this._idx >= OriginalSchema.Count)
                throw new Exception("Original schema is invalid");
            if (OriginalSchema.ColumnAffinity(this._idx) != this._affinity)
                throw new Exception("Original schema is invalid");

            string name = OriginalSchema.ColumnName(this._idx);
            int new_index = NewSchema.ColumnIndex(name);

            if (new_index == -1)
                throw new Exception("New schema is invalid");
            if (NewSchema.ColumnAffinity(new_index) != this._affinity)
                throw new Exception("New schema is invalid");

            this._idx = new_index;

        }

    }

}
