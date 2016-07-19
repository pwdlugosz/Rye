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
        BOOL,
        DATE_TIME,
        INT,
        DOUBLE,
        BLOB,
        STRING
    }

}
