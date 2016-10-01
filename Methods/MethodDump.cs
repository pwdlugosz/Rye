using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Methods
{


    // Note: cannot be called from RYE script //
    public sealed class MethodDump : Method
    {

        private TabularData _data;
        private string _path;
        private char _delim;
        private Session _session;
        public int _State = 0;

        public MethodDump(Method Parent, TabularData Data, string Path, char Delim, Session Session)
            : base(Parent)
        {
            this._data = Data;
            this._path = Path;
            this._delim = Delim;
            this._session = Session;
            this._State = 1;
        }

        public int State
        {
            get { return this._State; }
        }

        public string Path
        {
            get { return this._path; }
        }

        public override void Invoke()
        {

            if (this._State == 0)
                return;

            this._session.Kernel.TextDump(this._data, this._path, this._delim);

        }

        public override Method CloneOfMe()
        {
            return new MethodDump(this.Parent, this._data, this._path, this._delim, this._session);
        }

        public static MethodDump Empty
        {
            get
            {
                MethodDump x = new MethodDump(null, null, null, char.MinValue, null);
                x._State = 0;
                return x;
            }
        }

    }


}
