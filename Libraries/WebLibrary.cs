using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Rye.Data;
using Rye.Methods;
using Rye.Expressions;

namespace Rye.Libraries
{
    class WebLibrary : MethodLibrary
    {

        public const string LIB_NAME = "WEB";


        private string[] _Names = new string[]
        {
        };

        public WebLibrary(Session Session)
            : base(Session)
        {
            this.LibName = LIB_NAME;
        }

        public override string[] Names
        {
            get { return this._Names; }
        }

        public override Method RenderMethod(Method Parent, string Name, ParameterCollection Parameters)
        {
            throw new ArgumentException(string.Format("Method {0} not found", Name));
        }

        public override ParameterCollectionSigniture RenderSigniture(string Name)
        {

            
            throw new ArgumentException(string.Format("Method {0} not found", Name));
            
        }

    }
}
