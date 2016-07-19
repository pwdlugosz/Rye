using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Expressions
{

    public enum ExpressionAffinity
    {

        // Ref nodes //
        Field,
        Heap,
        Matrix,

        // Result nodes //
        Result,

        // Value nodes //
        Pointer,
        Value

    }

}
