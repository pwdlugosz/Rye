using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Expressions;
using Rye.Data;
using Rye.Structures;
using Rye.Interpreter;
using Rye.Methods;

namespace Rye.Libraries
{

    public abstract class MethodLibrary
    {

        protected Session _Session;

        public MethodLibrary(Session Session)
        {
            this._Session = Session;
        }
        
        public string LibName
        {
            get;
            protected set;
        }

        public abstract Method RenderMethod(Method Parent, string Name, ParameterCollection Parameters);

        public virtual Method RenderMethod(string Name, ParameterCollection Parameters)
        {
            return this.RenderMethod(null, Name, Parameters);
        }

        public abstract ParameterCollectionSigniture RenderSigniture(string Name);

        public abstract string[] Names { get; }

        public virtual bool Exists(string Name)
        {
            return this.Names.Contains(Name, StringComparer.OrdinalIgnoreCase);
        }

        public virtual int Count
        {
            get { return this.Names.Length; }
        }

        public sealed class EmptyLibrary : MethodLibrary
        {

            public EmptyLibrary()
                : base(null)
            {
            }

            public override Method RenderMethod(Method Parent, string Name, ParameterCollection Parameters)
            {
                throw new NotImplementedException();
            }

            public override ParameterCollectionSigniture RenderSigniture(string Name)
            {
                throw new NotImplementedException();
            }

            public override string[] Names
            {
                get { return new string[] {""}; }
            }

        }

        public static EmptyLibrary NullLibrary
        {
            get { return new EmptyLibrary(); }
        }

    }

    

}
