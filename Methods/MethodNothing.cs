using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Methods
{
    
    public sealed class MethodNothing : Method
    {

        public MethodNothing(Method Parent)
            : base(Parent)
        {
        }

        public override void Invoke()
        {
        }

        public override Method CloneOfMe()
        {
            return new MethodNothing(this.Parent);
        }

        public override string Message()
        {
            return "Nothing";
        }

        public override List<Expression> InnerExpressions()
        {
            return new List<Expression>();
        }

    }

}
