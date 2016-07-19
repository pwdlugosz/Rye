using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Interpreter
{

    public sealed class RyeDataException : Exception
    {

        public RyeDataException(string Message, params object[] Args)
            : base(string.Format(Message, Args))
        {
        }

    }

    public sealed class RyeCompileException : Exception
    {

        public RyeCompileException(string Message, params object[] Args)
            : base(string.Format(Message, Args))
        {
        }

    }

    public sealed class RyeParseException : Exception
    {

        public RyeParseException(string Message, params object[] Args)
            : base(string.Format(Message, Args))
        {
        }

    }


}
