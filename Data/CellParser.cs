using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data
{

    public static class CellParser
    {

        private static string[] _NullTokens = { Cell.NULL_STRING_TEXT, "#NULL", ".", "NIL", "NULL" };
        
        private static string[] _BoolTrueTokens = { "TRUE", "T", "YES", "1", "ON" };
        private static string[] _BoolFalseTokens = { "FALSE", "F", "NO", "0", "OFF" };

        private static char[] _DateDelims = { '-', '#', '\\', '/', '.', ':', ' ', ',' };
        private static string[] _DateMonthFull = { "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER" };
        private static string[] _DateMonthAbrev = { "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };
        private static string[] _DateAMTokens = { "AM", "A.M." };
        private static string[] _DatePMToekns = { "PM", "Key.M." };

        private static char[] _NegativeTokens = { '-', '~' };

        private static char[] _IntegerTokens = { ',', '-', '~', 'L', 'l', ' ', '\0' };

        private static char[] _NumericTokens = { ',', '$', '-', '~', 'D', 'd', '%', ' ', '\0' };
        private static char[] _NumericPercentTokens = { '%' };
        private static char[] _NumericClearTokens = { ',', '$', '-', '~', 'D', 'd', '%', ' ', '\0', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
        
        private static string _BLOBToken = "0X";

        public static Cell ParseBOOL(string Text)
        {

            if (Text == null)
                return Cell.NULL_BOOL;

            Text = Text.Trim();
            if (Text.Length == 0)
                return Cell.NULL_BOOL;

            if (_NullTokens.Contains(Text, StringComparer.OrdinalIgnoreCase))
                return Cell.NULL_BOOL;

            if (_BoolTrueTokens.Contains(Text, StringComparer.OrdinalIgnoreCase))
                return Cell.TRUE;

            if (_BoolFalseTokens.Contains(Text, StringComparer.OrdinalIgnoreCase))
                return Cell.FALSE;

            return Cell.NULL_BOOL;

        }

        public static Cell ParseDATE(string Text)
        {

            if (Text == null)
                return Cell.NULL_DATE;

            Text = Text.Trim();
            if (Text.Length == 0)
                return Cell.NULL_DATE;

            if (_NullTokens.Contains(Text, StringComparer.OrdinalIgnoreCase))
                return Cell.NULL_DATE;

            // Standard date format //
            Cell c = ParseDateStandard(Text);
            if (c.NULL == 0) 
                return c;

            // Simple date format //
            c = ParseDateSimple(Text);
            if (c.NULL == 0) 
                return c;

            // Standard date format - text //
            c = ParseDateStandardText(Text);
            if (c.NULL == 0) 
                return c;

            // Simple date format - text //
            c = ParseDateSimpleText(Text);
            if (c.NULL == 0) 
                return c;

            // Proper date format //
            c = ParseDateProper(Text);
            if (c.NULL == 0) 
                return c;

            // We FAILED!!! //
            return Cell.NULL_DATE;

        }

        public static Cell ParseINT(string Text)
        {

            if (Text == null)
                return Cell.NULL_INT;

            Text = Text.Trim();
            if (Text.Length == 0)
                return Cell.NULL_INT;

            if (_NullTokens.Contains(Text, StringComparer.OrdinalIgnoreCase))
                return Cell.NULL_INT;

            bool IsNegative = _NegativeTokens.Contains(Text.First());

            Text = Text.Trim(_NumericTokens);

            bool RightParse = false;
            long val = 0;

            RightParse = long.TryParse(Text, out val);

            if (!RightParse)
                return Cell.NULL_INT;

            if (IsNegative)
                val = -val;

            return new Cell(val);

        }

        public static Cell ParseNUM(string Text)
        {

            if (Text == null)
                return Cell.NULL_DOUBLE;

            Text = Text.Trim();
            if (Text.Length == 0)
                return Cell.NULL_DOUBLE;

            if (_NullTokens.Contains(Text, StringComparer.OrdinalIgnoreCase))
                return Cell.NULL_DOUBLE;

            bool IsNegative = _NegativeTokens.Contains(Text.First()), IsPercent = _NumericPercentTokens.Contains(Text.Last());

            Text = Text.Trim(_NumericTokens);

            bool RightParse = false;
            double val = 0;

            RightParse = double.TryParse(Text, out val);

            if (!RightParse)
                return Cell.NULL_DOUBLE;

            if (IsNegative)
                val = -val;

            if (IsPercent)
                val = val / 100d;

            return new Cell(val);

        }

        public static Cell ParseBLOB(string Text)
        {

            if (Text == null)
                return Cell.NULL_BLOB;

            Text = Text.Trim();
            if (Text.Length == 0)
                return Cell.NULL_BLOB;

            if (_NullTokens.Contains(Text, StringComparer.OrdinalIgnoreCase))
                return Cell.NULL_BLOB;

            Text = Text.ToUpper().Replace(_BLOBToken, "");
            byte[] b = new byte[(Text.Length) / 2];

            for (int i = 0; i < Text.Length; i += 2)
                b[i / 2] = Convert.ToByte(Text.Substring(i, 2), 16);

            return new Cell(b);

        }

        public static Cell ParseSTRING(string Text)
        {

            if (Text == null)
                return Cell.NULL_STRING;

            // Note: for strings, we don't want to trim or check the length

            if (_NullTokens.Contains(Text.Trim(), StringComparer.OrdinalIgnoreCase))
                return Cell.NULL_STRING;

            // Yield Value //
            return new Cell(Text);

        }

        // Date Parsing //
        /// <summary>
        /// YYYY-MM-DD
        /// YYYY-MM-DD:HH:MM:SS
        /// YYYY-MM-DD:HH:MM:SS:NNNNN
        /// MM-DD-YYYY
        /// MM-DD-YYYY:HH:MM:SS
        /// MM-DD-YYYY:HH:MM:SS:NNNNN
        /// </summary>
        /// <param name="Text"></param>
        /// <returns></returns>
        internal static Cell ParseDateStandard(string Text)
        {

            string[] s = Text.Trim().Replace("'", "").Split(_DateDelims);

            if (s.Length < 3)
                return Cell.NULL_DATE;

            if (!IsNumeric(s))
                return Cell.NULL_DATE;

            // Get each piece //
            int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0, millisecond = 0;
            if (s.Length == 3)
            {
                year = int.Parse(s[0]);
                month = int.Parse(s[1]);
                day = int.Parse(s[2]);
            }
            else if (s.Length == 6)
            {
                year = int.Parse(s[0]);
                month = int.Parse(s[1]);
                day = int.Parse(s[2]);
                hour = int.Parse(s[3]);
                minute = int.Parse(s[4]);
                second = int.Parse(s[5]);
            }
            else if (s.Length == 7)
            {
                year = int.Parse(s[0]);
                month = int.Parse(s[1]);
                day = int.Parse(s[2]);
                hour = int.Parse(s[3]);
                minute = int.Parse(s[4]);
                second = int.Parse(s[5]);
                millisecond = int.Parse(s[6]);
            }
            else
            {
                return Cell.NULL_DATE;
            }

            // Handle the potential of MM-DD-YYYY format instead of YYYY-MM-DD //
            if (month > 1000 && year < 31)
            {

                // year = day
                int x = year;
                year = day;

                // day = month
                day = month;

                // month = year
                month = x;
                
            }

            // Handle the year potentially being just two digits //
            if (year < 100)
                year += 2000;

            // Check bounds //
            if (year >= 1 && year <= 9999 && month >= 1 && month <= 12 && day >= 1 && day <= 31 && hour >= 0 && minute >= 0 && second >= 0 && millisecond >= 0)
            {
                return new Cell(new DateTime(year, month, day, hour, minute, second, millisecond));
            }

            return Cell.NULL_DATE;

        }

        /// <summary>
        /// MM/DD/YYYY
        /// </summary>
        /// <param name="Text"></param>
        /// <returns></returns>
        internal static Cell ParseDateSimple(string Text)
        {

            string[] s = Text.Trim().Replace("'", "").Split(_DateDelims);

            if (s.Length != 3)
                return Cell.NULL_DATE;

            if (!IsNumeric(s))
                return Cell.NULL_DATE;

            // Parse out the year, month, and day //
            int year = int.Parse(s[0]);
            int month = int.Parse(s[1]);
            int day = int.Parse(s[2]);

            // Handle the potential of MM-DD-YYYY format instead of YYYY-MM-DD //
            if (month > 1000 && year < 31)
            {

                // year = day
                int x = year;
                year = day;

                // day = month
                day = month;

                // month = year
                month = x;

            }

            // Handle the year potentially being just two digits //
            if (year < 100)
                year += 2000;

            // Check bounds //
            if (year >= 1 && year <= 9999 && month >= 1 && month <= 12 && day >= 1 && day <= 31)
            {
                return new Cell(new DateTime(year, month, day));
            }

            return Cell.NULL_DATE;

        }

        /// <summary>
        /// DD-MONTH-YYYY
        /// </summary>
        /// <param name="Text"></param>
        /// <returns></returns>
        internal static Cell ParseDateStandardText(string Text)
        {

            string[] s = Text.Trim().Replace("'", "").Split(_DateDelims);

            if (s.Length != 3)
                return Cell.NULL_DATE;

            if (!IsNumeric(s[2]) || !IsNumeric(s[0]))
                return Cell.NULL_DATE;

            // Parse out the year, month, and day //
            int year = int.Parse(s[2]);
            int month = ParseMonth(s[1]);
            int day = int.Parse(s[0]);

            // Check if year and day are flipped //
            if (year < 32 && day > 1000)
            {
                int x = year;
                year = day;
                day = x;
            }
            
            // Handle the year potentially being just two digits //
            if (year < 100)
                year += 2000;

            // Check bounds //
            if (year >= 1 && year <= 9999 && month >= 1 && month <= 12 && day >= 1 && day <= 31)
            {
                return new Cell(new DateTime(year, month, day));
            }

            return Cell.NULL_DATE;

        }

        /// <summary>
        /// DD{MONTH}YYYY
        /// 1JAN2015 as an example
        /// </summary>
        /// <param name="Text"></param>
        /// <returns></returns>
        internal static Cell ParseDateSimpleText(string Text)
        {

            Text = Text.Trim();

            if (Text.Length < 6)
                return Cell.NULL_DATE;

            int year = 0, month = 0, day = 0, len = Text.Length;

            // Get the first part //
            if (char.IsNumber(Text[0]) && char.IsNumber(Text[1]) && char.IsNumber(Text[2]) && char.IsNumber(Text[2]))
                day = int.Parse(Text.Substring(0, 4));
            else if (char.IsNumber(Text[0]) && char.IsNumber(Text[1]))
                day = int.Parse(Text.Substring(0, 2));
            else if (char.IsNumber(Text[0]))
                day = (int)Text[0];

            // Get the third part //
            if (char.IsNumber(Text[len - 4]) && char.IsNumber(Text[len - 3]) && char.IsNumber(Text[len - 2]) && char.IsNumber(Text[len - 1]))
                year = int.Parse(Text.Substring(len - 4, 4));
            else if (char.IsNumber(Text[len - 4]) && char.IsNumber(Text[len - 3]))
                year = int.Parse(Text.Substring(len - 2, 2));
            else if (char.IsNumber(Text[len - 1]))
                year = (int)Text[len - 1];

            // Get month string //
            string month_string = Text.Trim('1', '2', '3', '4', '5', '6', '7', '8', '9', '0');
            month = ParseMonth(month_string);
            
            // Check if year and day are flipped //
            if (year < 32 && day > 1000)
            {
                int x = year;
                year = day;
                day = x;
            }

            // Handle the year potentially being just two digits //
            if (year < 100)
                year += 2000;

            // Check bounds //
            if (year >= 1 && year <= 9999 && month >= 1 && month <= 12 && day >= 1 && day <= 31)
            {
                return new Cell(new DateTime(year, month, day));
            }

            return Cell.NULL_DATE;

        }

        /// <summary>
        /// Month Day, Year
        /// </summary>
        /// <param name="Text"></param>
        /// <returns></returns>
        internal static Cell ParseDateProper(string Text)
        {

            string[] s = Text.Trim().Replace("'", "").Split(_DateDelims);

            if (s.Length != 3)
                return Cell.NULL_DATE;

            if (!IsNumeric(s[2]) && !IsNumeric(s[1]))
                return Cell.NULL_DATE;

            // Parse out the year, month, and day //
            int year = int.Parse(s[2]);
            int month = ParseMonth(s[0]);
            int day = int.Parse(s[1]);

            // Check bounds //
            if (year >= 1 && year <= 9999 && month >= 1 && month <= 12 && day >= 1 && day <= 31)
            {
                return new Cell(new DateTime(year, month, day));
            }

            return Cell.NULL_DATE;

        }

        /// <summary>
        /// Takes a month string, like the formal name or abreviation, and returns the integer month
        /// </summary>
        /// <param name="Text"></param>
        /// <returns></returns>
        internal static int ParseMonth(string Text)
        {

            switch (Text.Trim().ToUpper())
            {

                case "1":
                case "01":
                case "JAN":
                case "JANUARY": return 1;

                case "2":
                case "02":
                case "FEB":
                case "FEBRUARY": return 2;

                case "3":
                case "03":
                case "MAR":
                case "MARCH": return 3;

                case "4":
                case "04":
                case "APR":
                case "APRIL": return 4;

                case "5":
                case "05":
                case "MAY": return 5;

                case "6":
                case "06":
                case "JUNE":
                case "JUN": return 6;

                case "7":
                case "07":
                case "JULY":
                case "JUL": return 7;

                case "8":
                case "08":
                case "AUG":
                case "AUGUST": return 8;

                case "9":
                case "09":
                case "SEP":
                case "SEPT":
                case "SEPTEMBER": return 9;

                case "10":
                case "OCT":
                case "OCTOBER": return 10;

                case "11":
                case "NOV":
                case "NOVEMBER": return 11;

                case "12":
                case "DEC":
                case "DECEMBER": return 12;

            }

            return -1;

        }

        internal static bool IsNumeric(string Text)
        {
            return Text.Trim(_NumericClearTokens).Length == 0;
        }

        internal static bool IsNumeric(string[] Text)
        {

            bool x = true;
            foreach(string Taxt in Text)
            {
                x = x && IsNumeric(Taxt);
                if (x == false)
                    return false;
            }

            return true;

        }

        public static Cell Parse(string Text, CellAffinity Affinity)
        {

            switch (Affinity)
            {
                case CellAffinity.BOOL: return ParseBOOL(Text);
                case CellAffinity.DATE_TIME: return ParseDATE(Text);
                case CellAffinity.INT: return ParseINT(Text);
                case CellAffinity.DOUBLE: return ParseNUM(Text);
                case CellAffinity.BLOB: return ParseBLOB(Text);
                case CellAffinity.STRING: return ParseSTRING(Text);
                default: return Cell.NULL_INT;
            }

        }

        public static Cell TryParse(string Text, CellAffinity Affinity)
        {

            try
            {

                switch (Affinity)
                {
                    case CellAffinity.BOOL: return ParseBOOL(Text);
                    case CellAffinity.DATE_TIME: return ParseDATE(Text);
                    case CellAffinity.INT: return ParseINT(Text);
                    case CellAffinity.DOUBLE: return ParseNUM(Text);
                    case CellAffinity.BLOB: return ParseBLOB(Text);
                    case CellAffinity.STRING: return ParseSTRING(Text);
                    default: return Cell.NULL_INT;
                }

            }
            catch
            {
                return Cell.NULL_INT;
            }

        }

    }

    public static class CellCaster
    {

        public static Cell ToBOOL(Cell C)
        {

            if (C.AFFINITY == CellAffinity.BOOL)
                return C;

            if (C.NULL == 1)
                return Cell.NULL_BOOL;

            if (C.AFFINITY == CellAffinity.INT)
            {
                C.BOOL = (C.INT != 0);
            }
            else if (C.AFFINITY == CellAffinity.DOUBLE)
            {
                C.BOOL = (C.DOUBLE != 0);
            }
            else if (C.AFFINITY == CellAffinity.BLOB)
            {
                C.BOOL = true;
                if (C.BLOB.Length != 0) C.BOOL = (C.BLOB[0] != 0);
            }
            else if (C.AFFINITY == CellAffinity.STRING)
            {
                return CellParser.ParseBOOL(C.STRING);
            }

            C.AFFINITY = CellAffinity.BOOL;

            return C;

        }

    }

}
