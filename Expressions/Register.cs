using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Expressions
{
    public sealed class Register
    {

        private Guid _UID;

        public Register(string Name, Schema Columns)
        {
            this.Value = null;
            this.NullValue = Columns.NullRecord;
            this.Columns = Columns;
            this._UID = Guid.NewGuid();
            this.Name = Name;
        }

        public Record Value;

        public Record NullValue;

        public Schema Columns;

        public string Name;

        public Guid UID
        {
            get { return this._UID; }
        }

        public Register CloneOfMe()
        {
            return new Register(this.Name, this.Columns);
        }

    }

    public sealed class RegisterComparer : IEqualityComparer<Register>
    {

        public bool Equals(Register A, Register B)
        {
            return A.UID == B.UID;
        }

        public int GetHashCode(Register A)
        {
            return A.UID.GetHashCode();
        }

    }

}
