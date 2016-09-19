using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data
{

    public enum CellFormat
    {

        // Bool //
        BoolTrueFalse,
        BoolYesNo,
        BoolOnOff,

        // Int //
        IntSimple,
        IntComma,
        
        // Double //
        NumSimple,
        NumRound0,
        NumRound1,
        NumRound2,
        NumRound5,
        NumMoney,
        NumPercent0,
        NumPercent1,
        NumPercent2,

        // Date //
        DateYYYYMMDD,
        DateHHMMSSMM,
        DateYYYYMMDDHHMMSSMM,

        //String //
        
    }

}
