using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Structures;

namespace Rye.Expressions
{

    public sealed class Register
    {

        public Register(string Name, Schema Columns)
        {
            this.Value = null;
            this.NullValue = Columns.NullRecord;
            this.Columns = Columns;
            this.Name = Name;
            this.UID = Guid.NewGuid();
        }

        public Record Value;

        public Record NullValue;

        public Schema Columns;

        public string Name;

        public Guid UID
        {
            get;
            private set;
        }

        public Register CloneOfMe()
        {
            return new Register(this.Name, this.Columns);
        }

        public static Heap<Register> GetMemoryRegisters(params IRegisterExtractor[] Nodes)
        {

            Heap<Register> bag = new Heap<Register>();
            foreach (IRegisterExtractor x in Nodes)
            {
                bag.Import(x.GetMemoryRegisters());
            }
            return bag;

        }

    }

    public interface IRegisterExtractor
    {

        Heap<Register> GetMemoryRegisters();

    }


}
