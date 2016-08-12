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

        public MethodExecScript(Method Parent, Workspace Enviro, string Script, Heap<Expression> Parameters)
            :base(Parent)
        {

            this._Parameters = Parameters;
            this._Script = Script;
            this._engine = new RyeScriptProcessor(Enviro);

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

            this._engine.Execute(sb.ToString());

        }

        public override Method CloneOfMe()
        {
            return new MethodExecScript(this._Parent, this._engine.Enviro, this._Script, this._Parameters);
        }

    }

}
