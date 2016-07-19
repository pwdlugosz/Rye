using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Helpers
{

    public class BooleanParserHelper
    {

        private static string[] TRUE_STRINGS = new string[]
        {
            "TRUE",
            "T",
            "YES",
            "Y",
            "1"
        };

        public static bool Parse(string Text)
        {
            return TRUE_STRINGS.Contains(Text, StringComparer.OrdinalIgnoreCase);
        }

    }

}
