using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Expressions;
using Rye.Data;

namespace Rye.Libraries
{

    public abstract class FunctionLibrary
    {

        protected Session _Session;

        public FunctionLibrary(Session Session)
        {
            this._Session = Session;
        }

        public abstract CellFunction RenderFunction(string Name);

        public string LibName
        {
            get;
            protected set;
        }

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
