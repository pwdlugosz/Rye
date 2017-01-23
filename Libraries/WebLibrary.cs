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


    // Web Library //
    public sealed class WebLibrary : Library
    {

        private Exchange.WebProvider _web;

        public const string M_NAVIGATE_URL = "NAVIGATE_URL";
        public const string M_DOWNLOAD = "DOWNLOAD";
        public const string M_FROM_HTML = "FROM_HTML";

        private static string[] _MethodNames = new string[]
        {
            M_NAVIGATE_URL,
            M_DOWNLOAD,
            M_FROM_HTML
        };

        public const string F_EXTRACT = "EXTRACT";
        public const string F_HTML_GET = "HTML_GET";

        private static string[] _FunctionNames = new string[]
        {
            F_EXTRACT,
            F_HTML_GET
        };

        public WebLibrary(Session Session)
            : base(Session, "WEB")
        {
        }

        public override Method GetMethod(Method Parent, string Name, ParameterCollection Parameters)
        {

            switch (Name.ToUpper())
            {

                case M_NAVIGATE_URL:
                    return this.Method_NavigateURL(Parent, Parameters);
                case M_DOWNLOAD:
                    return this.Method_Download(Parent, Parameters);
                case M_FROM_HTML:
                    return this.Method_FromHTML(Parent, Parameters);

            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override ParameterCollectionSigniture GetMethodSigniture(string Name)
        {

            switch (Name.ToUpper())
            {

                case M_NAVIGATE_URL:
                    return ParameterCollectionSigniture.Parse(M_NAVIGATE_URL, "Downloads a url to a file", "URL|URL to navigate to|Value|false");
                case M_DOWNLOAD:
                    return ParameterCollectionSigniture.Parse(M_DOWNLOAD, "Downloads a url to a file", "URL|The URL to download|Value|false;PATH|The path to the exported file|Value|false;POST|The post string|Value|true");
                case M_FROM_HTML:
                    return ParameterCollectionSigniture.Parse(M_FROM_HTML, "Extracts a table from an HTML document", "PATH|The path to the HTML document|Value|false;HTML_TAGS|The format string for the HTML table|Value|false;DATA|The output table|T|false");

            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override string[] MethodNames
        {
            get { return _MethodNames; }
        }

        public override CellFunction GetFunction(string Name)
        {

            switch (Name.ToUpper())
            {

                case F_EXTRACT:
                    return this.Function_Extract();
                case F_HTML_GET:
                    return this.Function_HTMLGet();

            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override string[] FunctionNames
        {
            get
            {
                return _FunctionNames;
            }
        }

        // Methods //
        private Method Method_NavigateURL(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                // Get the data parameters //
                string URL = Parameters.Expressions["URL"].Evaluate().valueSTRING;

                // Navigate //
                this._web.NavigateURL(URL);

            };
            return new LibraryMethod(Parent, M_NAVIGATE_URL, Parameters, false, kappa);

        }

        private Method Method_Download(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string url = Parameters.Expressions["URL"].Evaluate().valueSTRING;
                string path = Parameters.Expressions["PATH"].Evaluate().valueSTRING;
                string post = Parameters.Exists("POST") ? Parameters.Expressions["POST"].Evaluate().valueSTRING : null;

                if (post == null)
                {
                    WebSupport.HTTP_Request_Get(url, path);
                }
                else
                {
                    WebSupport.HTTP_Request_Post(url, path, post);
                }

            };
            return new LibraryMethod(Parent, M_DOWNLOAD, Parameters, false, kappa);

        }

        private Method Method_FromHTML(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData Data = x.Tables["DATA"];
                string Path = x.Expressions["PATH"].Evaluate().valueSTRING;
                string HTML_Tags = x.Expressions["HTML_TAGS"].Evaluate().valueSTRING;

                RecordWriter writer = Data.OpenWriter();
                try
                {
                    Exchange.HTMLProvider.WriteToStream(Path, HTML_Tags, writer);
                }
                catch
                {
                    this._Session.IO.WriteLine("Error parsing HTML file: {0}", Path);
                }
                writer.Close();

            };
            return new LibraryMethod(Parent, M_FROM_HTML, Parameters, false, kappa);

        }

        // Functions //
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
            return new CellFunctionFixedShell(F_EXTRACT, 2, CellAffinity.STRING, lambda);

        }

        private CellFunction Function_HTMLGet()
        {

            return new CellFunctionFixedShell(F_HTML_GET, 2, CellAffinity.STRING, (x) =>
            {
                return new Cell(Exchange.HTMLProvider.ToString(x[0].valueSTRING, x[1].valueSTRING));
            }
            );

        }

    }


}
