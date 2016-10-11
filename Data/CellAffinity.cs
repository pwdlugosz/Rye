using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data
{

    /// <summary>
    /// Rye data type
    /// </summary>
    public enum CellAffinity : byte
    {
        BOOL = 0,
        DATE_TIME = 1,
        INT = 2,
        DOUBLE = 3,
        BLOB = 4,
        STRING = 5
    }

}
