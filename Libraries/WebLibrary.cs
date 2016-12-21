using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;
using Rye.Data;
using Rye.Methods;
using Rye.Expressions;

namespace Rye.Libraries
{
    
    public sealed class WebMathodLibrary : MethodLibrary
    {

        public const string LIBRARY_NAME = "WEB";

        public const string NAME_NAVIGATE_URL = "NAVIGATE_URL";
        
        private Exchange.WebProvider _web;

        private string[] _Names = new string[]
        {
            NAME_NAVIGATE_URL
        };

        public WebMathodLibrary(Session Session, Exchange.WebProvider Provider)
            : base(Session)
        {
            this.LibName = LIBRARY_NAME;
            this._web = Provider;
        }

        public override string[] Names
        {
            get { return this._Names; }
        }

        public override Method RenderMethod(Method Parent, string Name, ParameterCollection Parameters)
        {

            switch (Name.ToUpper())
            {
                case NAME_NAVIGATE_URL:
                    return this.NavigateURL(Parent, Parameters);
            }

            throw new ArgumentException(string.Format("Method {0} not found", Name));

        }

        public override ParameterCollectionSigniture RenderSigniture(string Name)
        {

            switch (Name.ToUpper())
            {
                case NAME_NAVIGATE_URL:
                    return new ParameterCollectionSigniture(NAME_NAVIGATE_URL, "URL|URL to navigate to|Value|false");
            }
            
            throw new ArgumentException(string.Format("Method {0} not found", Name));
            
        }

        // URL //
        public Method NavigateURL(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                // Get the data parameters //
                string URL = Parameters.Expressions["URL"].Evaluate().valueSTRING;
                
                // Navigate //
                this._web.NavigateURL(URL);

            };
            return new LibraryMethod(Parent, NAME_NAVIGATE_URL, Parameters, false, kappa);

        }

    }

    public sealed class WebFunctionLibrary : FunctionLibrary
    {

        public const string LIBRARY_NAME = "WEB";
        
        private Exchange.WebProvider _web;

        public WebFunctionLibrary(Session Session, Exchange.WebProvider Provider)
            : base(Session)
        {
            this.LibName = LIBRARY_NAME;
            this._web = Provider;
        }

        // Functions //
        public const string NAME_EXTRACT = "EXTRACT";

        private static string[] _FunctionNames = new string[]
        {
            NAME_EXTRACT
        };

        public override CellFunction RenderFunction(string Name)
        {

            switch (Name)
            {

                case WebFunctionLibrary.NAME_EXTRACT:
                    return this.Function_Extract();

            }

            throw new ArgumentException(string.Format("Function '{0}' does not exist", Name));

        }

        public override string[] Names
        {
            get { return WebFunctionLibrary._FunctionNames; }
        }

        private CellFunction Function_Extract()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {

                string path = x[0].valueSTRING;
                int index = (int)x[1].valueINT;
                string value = this._web.Extract(path, index);
                Cell c = new Cell(value);
                return c;

            };
            return new CellFunctionFixedShell(NAME_EXTRACT, 2, CellAffinity.STRING, lambda);

        }

    }


}
