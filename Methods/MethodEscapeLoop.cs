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
    /// Represents an escape loop statement
    /// </summary>
    public sealed class MethodEscapeLoop : Method
    {

        public MethodEscapeLoop(Method Parent)
            : base(Parent)
        {
        }

        public override void Invoke()
        {

            // Raise a break loop state //
            this.RaiseUp(1);

        }

        public override string Message()
        {
            return "Escape Loop";
        }

        public override Method CloneOfMe()
        {
            return new MethodEscapeLoop(this.Parent);
        }

    }


}
