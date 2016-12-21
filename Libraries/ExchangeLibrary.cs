using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Exchange;
using Rye.Data;
using Rye.Expressions;
using Rye.Methods;

namespace Rye.Libraries
{

    public sealed class ExchangeLibrary : MethodLibrary
    {

        
        public const string LIBRARY_NAME = "EXCHANGE";

        public ExchangeLibrary(Session Session)
            : base(Session)
        {
            this.LibName = LIBRARY_NAME;
        }

        // Methods //
        public const string FROM_HTML = "FROM_HTML";
        
        private static string[] _MethodNames = new string[]
        {
            FROM_HTML
        };

        public override Method RenderMethod(Method Parent, string Name, ParameterCollection Parameters)
        {

            switch (Name.ToUpper())
            {

                case ExchangeLibrary.FROM_HTML:
                    return this.Method_FROM_HTML(Parent, Parameters);
                
            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override ParameterCollectionSigniture RenderSigniture(string Name)
        {

            switch (Name.ToUpper())
            {

                case ExchangeLibrary.FROM_HTML:
                    return ParameterCollectionSigniture.Parse(ExchangeLibrary.FROM_HTML, "Extracts a table from an HTML document", "PATH|The path to the HTML document|Value|false;HTML_TAGS|The format string for the HTML table|Value|false;DATA|The output table|T|false");

            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override string[] Names
        {
            get { return ExchangeLibrary._MethodNames; }
        }

        private Method Method_FROM_HTML(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData Data = x.Tables["DATA"];
                string Path = x.Expressions["PATH"].Evaluate().valueSTRING;
                string HTML_Tags = x.Expressions["HTML_TAGS"].Evaluate().valueSTRING;

                RecordWriter writer = Data.OpenWriter();
                try
                {
                    HTMLProvider.WriteToStream(Path, HTML_Tags, writer);
                }
                catch
                {
                    this._Session.IO.WriteLine("Error parsing HTML file: {0}", Path);
                }
                writer.Close();

            };
            return new LibraryMethod(Parent, ExchangeLibrary.FROM_HTML, Parameters, false, kappa);

        }


    }

    public sealed class ExchangeFunctionLibrary : FunctionLibrary
    {

        public const string HTML_GET = "HTML_GET";

        private string[] _Names = new string[]
        {
            HTML_GET

        };

        public ExchangeFunctionLibrary(Session Session)
            : base(Session)
        {
            this.LibName = "EXCHANGE";
        }

        public override Expressions.CellFunction RenderFunction(string Name)
        {

            switch (Name.ToUpper())
            {

                case HTML_GET:
                    return new CellFunctionFixedShell(HTML_GET, 2, CellAffinity.STRING, (x) => 
                    {
                        return new Cell(Exchange.HTMLProvider.ToString(x[0].valueSTRING, x[1].valueSTRING));
                    }
                    );
                
            }

            throw new ArgumentException(string.Format("Function '{0}' does not exist", Name));

        }

        public override string[] Names
        {
            get { return this._Names; }
        }

        

    }


}
