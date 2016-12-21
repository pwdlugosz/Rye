using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Structures;
using Rye.Interpreter;

namespace Rye.Methods
{

    public sealed class MethodExecScript : Method
    {

        private Heap<Expression> _Parameters;
        private string _Script;
        private RyeScriptProcessor _engine;
        private bool _NoPrint = false;
        private Session _Session;

        public MethodExecScript(Method Parent, Session Enviro, string Script, Heap<Expression> Parameters, bool NoPrint)
            :base(Parent)
        {

            this._Session = Enviro;
            this._Parameters = Parameters;
            this._Script = Script;
            this._engine = new RyeScriptProcessor(Enviro);
            this._NoPrint = NoPrint;

        }

        public override bool CanBeAsync
        {
            get
            {
                return false;
            }
        }

        public override string Message()
        {
            return "EXEC";
        }

        public override void Invoke()
        {

            StringBuilder sb = new StringBuilder(this._Script);

            foreach (KeyValuePair<string, Expression> kv in this._Parameters.Entries)
            {

                string name = kv.Key;
                string value = kv.Value.Evaluate().valueSTRING;
                sb.Replace(name, value);

            }

            // Handle the supression reversion //
            bool RevertBackToPrinting = this._Session.IO.Supress; // If not supressed, this tells us to un-supress
            this._Session.IO.Supress = this._NoPrint;

            // Run the script //
            this._engine.Execute(sb.ToString());

            // Reverse the supression back to true //
            this._Session.IO.Supress = RevertBackToPrinting;

        }

        public override void EndInvoke()
        {
        }

        public override Method CloneOfMe()
        {

            Heap<Expression> par = new Heap<Expression>();
            foreach (KeyValuePair<string, Expression> x in this._Parameters.Entries)
            {
                par.Allocate(x.Key, x.Value.CloneOfMe());
            }

            return new MethodExecScript(this._Parent, this._engine.Enviro, this._Script, this._Parameters, this._NoPrint);
        }

        public override List<Expression> InnerExpressions()
        {
            return this._Parameters.Values;
        }

    }

}
