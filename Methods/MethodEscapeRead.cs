using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Methods
{

    /// <summary>
    /// Breaks out of a read statement
    /// </summary>
    public sealed class MethodEscapeRead : Method
    {

        public MethodEscapeRead(Method Parent)
            : base(Parent)
        {
        }

        public override void Invoke()
        {
            // Raise a break read state //
            this.RaiseUp(2);
        }

        public override string Message()
        {
            return "Escape Select";
        }

        public override Method CloneOfMe()
        {
            return new MethodEscapeRead(this.Parent);
        }

    }

}
