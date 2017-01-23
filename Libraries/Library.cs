using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Methods;

namespace Rye.Libraries
{

    public abstract class Library
    {

        protected Session _Session;
        private string _Name;

        public Library(Session Session, string Name)
        {
            this._Session = Session;
            this._Name = Name;
        }

        // Hard Methods //
        public string LibraryName
        {
            get { return this._Name; }
        }

        // Methods //
        public abstract string[] MethodNames
        {
            get;
        }

        public abstract Method GetMethod(Method Parent, string Name, ParameterCollection Parameters);

        public abstract ParameterCollectionSigniture GetMethodSigniture(string Name);
        
        public bool MethodExists(string Name)
        {
            return this.MethodNames.Contains(Name, StringComparer.OrdinalIgnoreCase);
        }

        // Functions //
        public abstract string[] FunctionNames
        {
            get;
        }

        public abstract CellFunction GetFunction(string Name);
        
        public bool FunctionExists(string Name)
        {
            return this.FunctionNames.Contains(Name, StringComparer.OrdinalIgnoreCase);
        }

        
    }

}
