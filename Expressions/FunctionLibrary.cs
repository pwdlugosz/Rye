using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Expressions;

namespace Rye.Expressions
{

    public abstract class FunctionLibrary
    {

        public abstract CellFunction RenderFunction(string Name);

        public abstract string[] Names { get; }

        public virtual bool Exists(string Name)
        {
            return this.Names.Contains(Name, StringComparer.OrdinalIgnoreCase);
        }

        public virtual int Count
        {
            get { return this.Names.Length; }
        }

    }

}
