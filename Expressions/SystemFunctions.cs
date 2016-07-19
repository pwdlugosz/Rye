using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;
using Rye.Data;

namespace Rye.Expressions
{

    public abstract class CellFunction
    {

        protected string _sig = "";
        protected int _params = 0;
        protected int _size = -1;

        public CellFunction(string Sig, int Params, bool IsVolatile)
        {
            this._sig = Sig;
            this._params = Params;
            this.IsVolatile = IsVolatile;
        }

        public string NameSig
        {
            get { return _sig; }
        }

        public int ParamCount
        {
            get { return this._params; }
            set { this._params = value; }
        }

        public bool IsVolatile
        {
            get;
            set;
        }

        public void SetSize(int Size)
        {
            this._size = Size;
        }

        public virtual int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            if (Type == CellAffinity.BOOL)
                return 1;
            if (Type == CellAffinity.DATE_TIME || Type == CellAffinity.DOUBLE || Type == CellAffinity.INT)
                return 8;
            if (this._size != -1)
                return this._size;
            return Sizes.Length == 0 ? Schema.FixSize(Type,-1) : Sizes.First();
        }

        public override int GetHashCode()
        {
            return this._sig.GetHashCode();
        }

        // Abstracts //
        public abstract Cell Evaluate(Cell[] Data);

        public abstract CellAffinity ReturnAffinity(params CellAffinity[] Data);

        public abstract string Unparse(string[] Text);

    }

    public sealed class CellFunctionFixedShell : CellFuncFixedKnown
    {

        private Func<Cell[], Cell> _lambda;

        public CellFunctionFixedShell(string Name, int Params, CellAffinity RType, Func<Cell[], Cell> Lambda)
            : base(Name, Params, RType)
        {
            this._lambda = Lambda;
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return this._lambda(Data);
        }

    }

    public sealed class CellFunctionVariableShell : CellFuncFixedVariable
    {

        private Func<Cell[], Cell> _lambda;

        public CellFunctionVariableShell(string Name, int Params, Func<Cell[], Cell> Lambda)
            : base(Name, Params)
        {
            this._lambda = Lambda;
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return this._lambda(Data);
        }


    }

    // Used for +, -, !, NOT unary opperations
    #region UniOpperations

    public abstract class CellUnaryOpperation : CellFunction
    {

        protected string _Token;

        public CellUnaryOpperation(string Name, string Token)
            : base(Name, 1, false)
        {
            this._Token = Token;
        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinityHelper.Highest(Data);
        }

        public override string Unparse(string[] Text)
        {

            StringBuilder sb = new StringBuilder();
            //sb.Append("(");
            sb.Append(this._Token);
            sb.Append(Text[0]);
            //sb.Append(")");
            return sb.ToString();

        }

    }

    public sealed class CellUniPlus : CellUnaryOpperation
    {

        public CellUniPlus()
            : base(SystemFunctionLibrary.UNI_PLUS, "+")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return +Data[0];
        }

    }

    public sealed class CellUniMinus : CellUnaryOpperation
    {

        public CellUniMinus()
            : base(SystemFunctionLibrary.UNI_MINUS, "-")
        { 
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return -Data[0];
        }

    }

    public sealed class CellUniNot : CellUnaryOpperation
    {

        public CellUniNot()
            : base(SystemFunctionLibrary.UNI_NOT, "!")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return !Data[0];
        }

    }

    public sealed class CellUniAutoInc : CellUnaryOpperation
    {

        public CellUniAutoInc()
            : base(SystemFunctionLibrary.UNI_AUTO_INC, "++")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0]++;
        }

    }

    public sealed class CellUniAutoDec : CellUnaryOpperation
    {

        public CellUniAutoDec()
            : base(SystemFunctionLibrary.UNI_AUTO_DEC, "--")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0]--;
        }

    }

    #endregion

    // Used for +, -, *, /, %
    #region BinaryOpperations

    public abstract class CellBinaryOpperation : CellFunction
    {

        protected string _Token;

        public CellBinaryOpperation(string Name, string Token)
            : base(Name, 2, false)
        {
            this._Token = Token;
        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinityHelper.Highest(Data);
        }

        public override string Unparse(string[] Text)
        {

            StringBuilder sb = new StringBuilder();
            sb.Append("(");
            sb.Append(Text[0]);
            sb.Append(this._Token);
            sb.Append(Text[1]);
            sb.Append(")");
            return sb.ToString();

        }

    }

    public sealed class CellBinPlus : CellBinaryOpperation
    {

        public CellBinPlus()
            : base(SystemFunctionLibrary.OP_ADD, "+")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] + Data[1];
        }

    }

    public sealed class CellBinMinus : CellBinaryOpperation
    {

        public CellBinMinus()
            : base(SystemFunctionLibrary.OP_SUB, "-")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] - Data[1];
        }
        
    }

    public sealed class CellBinMult : CellBinaryOpperation
    {

        public CellBinMult()
            : base(SystemFunctionLibrary.OP_MUL, "*")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] * Data[1];
        }

    }

    public sealed class CellBinDiv : CellBinaryOpperation
    {

        public CellBinDiv()
            : base(SystemFunctionLibrary.OP_DIV, "/")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] / Data[1];
        }

    }

    public sealed class CellBinDiv2 : CellBinaryOpperation
    {

        public CellBinDiv2()
            : base(SystemFunctionLibrary.OP_DIV2, "/?")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.CheckDivide(Data[0], Data[1]);
        }

    }

    public sealed class CellBinMod : CellBinaryOpperation
    {

        public CellBinMod()
            : base(SystemFunctionLibrary.OP_MOD, "%")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] % Data[1];
        }

    }

    #endregion

    // Used for ==, !=, <, <=, >, >=, AND, OR, XOR
    #region BooleanOpperations

    public abstract class CellBooleanOpperation : CellFunction
    {

        protected string _Token;

        public CellBooleanOpperation(string Name, string Token)
            : base(Name, 2, false)
        {
            this._Token = Token;
        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return 1;
        }
        
        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinity.BOOL;
        }

        public override string Unparse(string[] Text)
        {

            StringBuilder sb = new StringBuilder();
            sb.Append("(");
            sb.Append(Text[0]);
            sb.Append(this._Token);
            sb.Append(Text[1]);
            sb.Append(")");
            return sb.ToString();

        }

    }

    public sealed class CellBoolEQ : CellBooleanOpperation
    {

        public CellBoolEQ()
            : base(SystemFunctionLibrary.BOOL_EQ, "==")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] == Data[1] ? Cell.TRUE : Cell.FALSE;
        }

    }

    public sealed class CellBoolNEQ : CellBooleanOpperation
    {

        public CellBoolNEQ()
            : base(SystemFunctionLibrary.BOOL_NEQ, "!=")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] != Data[1] ? Cell.TRUE : Cell.FALSE;
        }

    }

    public sealed class CellBoolLT : CellBooleanOpperation
    {

        public CellBoolLT()
            : base(SystemFunctionLibrary.BOOL_LT, "<")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] < Data[1] ? Cell.TRUE : Cell.FALSE;
        }

    }

    public sealed class CellBoolLTE : CellBooleanOpperation
    {

        public CellBoolLTE()
            : base(SystemFunctionLibrary.BOOL_LTE, "<=")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] <= Data[1] ? Cell.TRUE : Cell.FALSE;
        }

    }

    public sealed class CellBoolGT : CellBooleanOpperation
    {

        public CellBoolGT()
            : base(SystemFunctionLibrary.BOOL_GT, ">")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] > Data[1] ? Cell.TRUE : Cell.FALSE;
        }

    }

    public sealed class CellBoolGTE : CellBooleanOpperation
    {

        public CellBoolGTE()
            : base(SystemFunctionLibrary.BOOL_GTE, ">=")
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] >= Data[1] ? Cell.TRUE : Cell.FALSE;
        }

    }

    #endregion

    // Fixed parameter known return type //
    #region FixedKnownFunctions

    public abstract class CellFuncFixedKnown : CellFunction
    {

        private CellAffinity _Affinity;

        public CellFuncFixedKnown(string Name, int Params, CellAffinity RType)
            : base(Name, Params, false)
        {
            this._Affinity = RType;
        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return this._Affinity;
        }

        public override string Unparse(string[] Text)
        {

            StringBuilder sb = new StringBuilder();
            sb.Append(this.NameSig);
            sb.Append("(");
            for (int i = 0; i < Text.Length; i++)
            {
                sb.Append(Text[i]);
                if (i != Text.Length - 1) sb.Append(",");
            }
            sb.Append(")");
            return sb.ToString();

        }


    }

    public sealed class CellFuncFKYear : CellFuncFixedKnown
    {

        public CellFuncFKYear()
            : base(SystemFunctionLibrary.FUNC_YEAR, 1, CellAffinity.INT)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Year(Data[0]);
        }

    }

    public sealed class CellFuncFKMonth : CellFuncFixedKnown
    {

        public CellFuncFKMonth()
            : base(SystemFunctionLibrary.FUNC_MONTH, 1, CellAffinity.INT)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Month(Data[0]);
        }

    }

    public sealed class CellFuncFKDay : CellFuncFixedKnown
    {

        public CellFuncFKDay()
            : base(SystemFunctionLibrary.FUNC_DAY, 1, CellAffinity.INT)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Day(Data[0]);
        }

    }
    
    public sealed class CellFuncFKHour : CellFuncFixedKnown
    {

        public CellFuncFKHour()
            : base(SystemFunctionLibrary.FUNC_HOUR, 1, CellAffinity.INT)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Hour(Data[0]);
        }

    }

    public sealed class CellFuncFKMinute : CellFuncFixedKnown
    {

        public CellFuncFKMinute()
            : base(SystemFunctionLibrary.FUNC_MINUTE, 1, CellAffinity.INT)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Minute(Data[0]);
        }

    }

    public sealed class CellFuncFKSecond : CellFuncFixedKnown
    {

        public CellFuncFKSecond()
            : base(SystemFunctionLibrary.FUNC_SECOND, 1, CellAffinity.INT)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Second(Data[0]);
        }

    }

    public sealed class CellFuncFKMillisecond : CellFuncFixedKnown
    {

        public CellFuncFKMillisecond()
            : base(SystemFunctionLibrary.FUNC_MILLISECOND, 1, CellAffinity.INT)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Millisecond(Data[0]);
        }

    }

    public sealed class CellFuncFKTicks : CellFuncFixedKnown
    {

        public CellFuncFKTicks()
            : base(SystemFunctionLibrary.FUNC_TIMESPAN, 1, CellAffinity.STRING)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            if (Data[0].Affinity != CellAffinity.INT)
                return new Cell(Data[0].AFFINITY);
            return new Cell(new TimeSpan(Data[0].INT).ToString());
        }

    }

    public sealed class CellFuncFKSubstring : CellFuncFixedKnown
    {

        public CellFuncFKSubstring()
            : base(SystemFunctionLibrary.FUNC_SUBSTR, 3, CellAffinity.STRING)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Substring(Data[0], Data[1].INT, Data[2].INT);
        }

    }

    public sealed class CellFuncFKLeft : CellFuncFixedKnown
    {

        public CellFuncFKLeft()
            : base(SystemFunctionLibrary.FUNC_SLEFT, 2, CellAffinity.STRING)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Left(Data[0], Data[1].INT);
        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return Sizes.First();
        }

    }

    public sealed class CellFuncFKRight : CellFuncFixedKnown
    {

        public CellFuncFKRight()
            : base(SystemFunctionLibrary.FUNC_SRIGHT, 2, CellAffinity.STRING)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Right(Data[0], Data[1].INT);
        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return Sizes.First();
        }

    }

    public sealed class CellFuncFKReplace : CellFuncFixedKnown
    {

        public CellFuncFKReplace()
            : base(SystemFunctionLibrary.FUNC_REPLACE, 3, CellAffinity.STRING)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Replace(Data[0], Data[1], Data[2]);
        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return Sizes.First();
        }

    }

    public sealed class CellFuncFKPosition : CellFuncFixedKnown
    {

        public CellFuncFKPosition()
            : base(SystemFunctionLibrary.FUNC_POSITION, 3, CellAffinity.INT)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Position(Data[0], Data[1], (int)Data[2].valueINT);
        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return 8;
        }

    }

    public sealed class CellFuncFKLength : CellFuncFixedKnown
    {

        public CellFuncFKLength()
            : base(SystemFunctionLibrary.FUNC_LENGTH, 1, CellAffinity.INT)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            Cell c = new Cell(8);
            if (Data[0].AFFINITY == CellAffinity.STRING)
                c.INT = (long)Data[0].STRING.Length;
            else if (Data[0].AFFINITY == CellAffinity.BLOB)
                c.INT = (long)Data[0].BLOB.Length;
            return c;
        }

    }

    public sealed class CellFuncFKIsNull : CellFuncFixedKnown
    {

        public CellFuncFKIsNull()
            : base(SystemFunctionLibrary.FUNC_IS_NULL, 1, CellAffinity.BOOL)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0].NULL == 1 ? Cell.TRUE : Cell.FALSE;
        }

    }

    public sealed class CellFuncFKIsNotNull : CellFuncFixedKnown
    {

        public CellFuncFKIsNotNull()
            : base(SystemFunctionLibrary.FUNC_IS_NOT_NULL, 1, CellAffinity.BOOL)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0].NULL != 1 ? Cell.TRUE : Cell.FALSE;
        }

    }

    public sealed class CellFuncFKTypeOf : CellFuncFixedKnown
    {

        public CellFuncFKTypeOf()
            : base(SystemFunctionLibrary.FUNC_TYPEOF, 1, CellAffinity.INT)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return new Cell((long)Data[0].AFFINITY);
        }

    }

    public sealed class CellFuncFKSTypeOf : CellFuncFixedKnown
    {

        public CellFuncFKSTypeOf()
            : base(SystemFunctionLibrary.FUNC_STYPEOF, 1, CellAffinity.STRING)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return new Cell(Data[0].AFFINITY.ToString());
        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return 6; // DOUBLE is the longest type name
        }

    }

    public sealed class CellFuncFKRound : CellFuncFixedKnown
    {

        public CellFuncFKRound()
            : base(SystemFunctionLibrary.FUNC_ROUND, 2, CellAffinity.DOUBLE)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            if (Data[0].AFFINITY != CellAffinity.DOUBLE)
                return Cell.NULL_DOUBLE;
            Data[0].DOUBLE = Math.Round(Data[0].DOUBLE, (int)Data[1].INT);
            return Data[0];
        }

    }

    public sealed class CellFuncFKToUTF16 : CellFuncFixedKnown
    {

        public CellFuncFKToUTF16()
            : base(SystemFunctionLibrary.FUNC_TO_UTF16, 1, CellAffinity.STRING)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            if (Data[0].AFFINITY != CellAffinity.BLOB || Data[0].NULL == 1)
                return Cell.NULL_STRING;

            Data[0].STRING = Cell.ByteArrayToUTF16String(Data[0].BLOB);
            Data[0].AFFINITY = CellAffinity.STRING;

            return Data[0];

        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return Sizes.First() / 2; // divide by 2 because two bytes == 1 char
        }

    }

    public sealed class CellFuncFKToUTF8 : CellFuncFixedKnown
    {

        public CellFuncFKToUTF8()
            : base(SystemFunctionLibrary.FUNC_TO_UTF8, 1, CellAffinity.STRING)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            if (Data[0].AFFINITY != CellAffinity.BLOB || Data[0].NULL == 1)
                return Cell.NULL_STRING;

            Data[0].STRING = ASCIIEncoding.UTF8.GetString(Data[0].BLOB);
            Data[0].AFFINITY = CellAffinity.STRING;

            return Data[0];

        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return Sizes.First(); 
        }

    }

    public sealed class CellFuncFKToHEX : CellFuncFixedKnown
    {

        public CellFuncFKToHEX()
            : base(SystemFunctionLibrary.FUNC_TO_HEX, 1, CellAffinity.STRING)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            if (Data[0].AFFINITY != CellAffinity.BLOB || Data[0].NULL == 1)
                return Cell.NULL_STRING;

            Data[0].STRING = Cell.HEX_LITERARL + BitConverter.ToString(Data[0].BLOB).Replace("-","");
            Data[0].AFFINITY = CellAffinity.STRING;

            return Data[0];

        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return Sizes.First() * 2 + 2; // 1 byte = 2 chars + 2 for '0x'
        }

    }

    public sealed class CellFuncFKFromUTF16 : CellFuncFixedKnown
    {

        public CellFuncFKFromUTF16()
            : base(SystemFunctionLibrary.FUNC_FROM_UTF16, 1, CellAffinity.BLOB)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            if (Data[0].AFFINITY != CellAffinity.STRING || Data[0].NULL == 1)
                return Cell.NULL_BLOB;

            Data[0].BLOB = ASCIIEncoding.BigEndianUnicode.GetBytes(Data[0].STRING);
            Data[0].AFFINITY = CellAffinity.BLOB;

            return Data[0];

        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return Sizes.First() * 2; // 1 byte = 2 chars
        }

    }

    public sealed class CellFuncFKFromUTF8 : CellFuncFixedKnown
    {

        public CellFuncFKFromUTF8()
            : base(SystemFunctionLibrary.FUNC_FROM_UTF8, 1, CellAffinity.BLOB)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            if (Data[0].AFFINITY != CellAffinity.STRING || Data[0].NULL == 1)
                return Cell.NULL_BLOB;

            Data[0].BLOB = ASCIIEncoding.UTF8.GetBytes(Data[0].STRING);
            Data[0].AFFINITY = CellAffinity.BLOB;

            return Data[0];

        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return Sizes.First(); // 1 byte = 1 chars for utf 8
        }

    }

    public sealed class CellFuncFKFromHEX : CellFuncFixedKnown
    {

        public CellFuncFKFromHEX()
            : base(SystemFunctionLibrary.FUNC_FROM_HEX, 1, CellAffinity.BLOB)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            if (Data[0].AFFINITY != CellAffinity.STRING || Data[0].NULL == 1)
                return Cell.NULL_BLOB;

            Data[0] = Cell.ByteParse(Data[0].STRING);

            return Data[0];

        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return Sizes.First();
        }

    }

    public sealed class CellFuncFKNormal : CellFuncFixedKnown
    {

        public CellFuncFKNormal()
            : base(SystemFunctionLibrary.FUNC_NDIST, 1, CellAffinity.DOUBLE)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            Cell c = Data[0];
            if (c.AFFINITY != CellAffinity.DOUBLE)
                c.AFFINITY = CellAffinity.DOUBLE;

            //c.DOUBLE = Equus.Numerics.SpecialFunctions.NormalCDF(c.DOUBLE);

            return c;

        }

    }

    public sealed class CellFuncFKThreadID : CellFuncFixedKnown
    {

        public CellFuncFKThreadID()
            : base(SystemFunctionLibrary.FUNC_THREAD_ID, 0, CellAffinity.INT)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return new Cell(Environment.CurrentManagedThreadId);
        }

    }

    public sealed class CellFuncFKIsPrime : CellFuncFixedKnown
    {

        public CellFuncFKIsPrime()
            : base(SystemFunctionLibrary.FUNC_ISPRIME, 1, CellAffinity.BOOL)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            if (Data[0].AFFINITY != CellAffinity.INT)
                return Cell.NULL_BOOL;

            long n = Data[0].INT;
            if (n <= 1)
                return Cell.FALSE;

            if (n < 6)
                return (n == 2 || n == 3) ? Cell.TRUE : Cell.FALSE;

            if (((n + 1) % 6 != 0) && ((n - 1) % 6 != 0))
                return Cell.FALSE;

            for (long i = 6; i <= (long)Math.Sqrt(n) + 1; i++)
                if (n % i == 0)
                    return Cell.FALSE;
            return Cell.TRUE;

        }

    }

    #endregion

    // Fixed parameter, returns type based on hierarchy //
    #region FixedVariableFunctions

    public abstract class CellFuncFixedVariable : CellFunction
    {

        public CellFuncFixedVariable(string Name, int Params)
            : base(Name, Params, false)
        {
        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinityHelper.Highest(Data);
        }

        public override string Unparse(string[] Text)
        {

            StringBuilder sb = new StringBuilder();
            sb.Append(this.NameSig);
            sb.Append("(");
            for (int i = 0; i < Text.Length; i++)
            {
                sb.Append(Text[i]);
                if (i != Text.Length - 1) sb.Append(",");
            }
            sb.Append(")");
            return sb.ToString();

        }

    }

    public sealed class CellFuncFVLog : CellFuncFixedVariable
    {

        public CellFuncFVLog()
            : base(SystemFunctionLibrary.FUNC_LOG, 1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Log(Data[0]);
        }

    }

    public sealed class CellFuncFVExp : CellFuncFixedVariable
    {

        public CellFuncFVExp()
            : base(SystemFunctionLibrary.FUNC_EXP, 1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Exp(Data[0]);
        }

    }

    public sealed class CellFuncFVPower : CellFuncFixedVariable
    {

        public CellFuncFVPower()
            : base(SystemFunctionLibrary.FUNC_POWER, 2)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Power(Data[0], Data[1]);
        }

    }

    public sealed class CellFuncFVSin : CellFuncFixedVariable
    {

        public CellFuncFVSin()
            : base(SystemFunctionLibrary.FUNC_SIN, 1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Sin(Data[0]);
        }

    }

    public sealed class CellFuncFVCos : CellFuncFixedVariable
    {

        public CellFuncFVCos()
            : base(SystemFunctionLibrary.FUNC_COS, 1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Cos(Data[0]);
        }

    }

    public sealed class CellFuncFVTan : CellFuncFixedVariable
    {

        public CellFuncFVTan()
            : base(SystemFunctionLibrary.FUNC_TAN, 1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Tan(Data[0]);
        }

    }

    public sealed class CellFuncFVSinh : CellFuncFixedVariable
    {

        public CellFuncFVSinh()
            : base(SystemFunctionLibrary.FUNC_SINH, 1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Sinh(Data[0]);
        }

    }

    public sealed class CellFuncFVCosh : CellFuncFixedVariable
    {

        public CellFuncFVCosh()
            : base(SystemFunctionLibrary.FUNC_COSH, 1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Cosh(Data[0]);
        }

    }

    public sealed class CellFuncFVTanh : CellFuncFixedVariable
    {

        public CellFuncFVTanh()
            : base(SystemFunctionLibrary.FUNC_TANH, 1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Tanh(Data[0]);
        }

    }

    public sealed class CellFuncFVLogit : CellFuncFixedVariable
    {

        public CellFuncFVLogit()
            : base(SystemFunctionLibrary.FUNC_LOGIT, 1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            switch (Data[0].AFFINITY)
            {
                case CellAffinity.DOUBLE:
                    Data[0].DOUBLE = 1 / (1 + Math.Exp(-Data[0].DOUBLE));
                    break;
                case CellAffinity.INT:
                    Data[0].INT = (long)(1 / (1 + Math.Exp(-Data[0].valueDOUBLE)));
                    break;
                default:
                    Data[0].NULL = 1;
                    break;
            }
            return Data[0];

        }

    }

    public sealed class CellFuncFVIfNull : CellFuncFixedVariable
    {

        public CellFuncFVIfNull()
            : base(SystemFunctionLibrary.FUNC_IF_NULL, 2)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            if (Data[0].NULL != 1)
                return Data[0];
            else if (Data[1].AFFINITY == Data[0].AFFINITY)
                return Data[1];
            else
                return Cell.Cast(Data[1], Data[0].AFFINITY);
        }

    }

    public sealed class CellFuncFVAND : CellFuncFixedVariable
    {

        public CellFuncFVAND()
            : base(SystemFunctionLibrary.FUNC_AND, 2)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] & Data[1];
        }

    }

    public sealed class CellFuncFVOR : CellFuncFixedVariable
    {

        public CellFuncFVOR()
            : base(SystemFunctionLibrary.FUNC_OR, 2)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] | Data[1];
        }

    }

    public sealed class CellFuncFVXOR : CellFuncFixedVariable
    {

        public CellFuncFVXOR()
            : base(SystemFunctionLibrary.FUNC_XOR, 2)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Data[0] ^ Data[1];
        }

    }

    public sealed class CellFuncFVSMax : CellFuncFixedVariable
    {

        public CellFuncFVSMax()
            : base(SystemFunctionLibrary.FUNC_SMAX, -1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Max(Data);
        }

    }

    public sealed class CellFuncFVSMin : CellFuncFixedVariable
    {

        public CellFuncFVSMin()
            : base(SystemFunctionLibrary.FUNC_SMIN, -1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Min(Data);
        }

    }

    #endregion

    // Crypto Hashes //
    #region CryptoFunctions

    public abstract class CellFuncCryptHash : CellFunction
    {

        private HashAlgorithm _hasher;

        public CellFuncCryptHash(string Name, HashAlgorithm Algorithm)
            : base(Name, 1, false)
        {
            this._hasher = Algorithm;
        }

        public HashAlgorithm InnerHasher
        {
            get { return this._hasher; }
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            byte[] b = this._hasher.ComputeHash(Data[0].valueBLOB);
            return new Cell(b);
        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinity.BLOB;
        }

        public override string Unparse(string[] Text)
        {

            StringBuilder sb = new StringBuilder();
            sb.Append(this.NameSig);
            sb.Append("(");
            for (int i = 0; i < Text.Length; i++)
            {
                sb.Append(Text[i]);
                if (i != Text.Length - 1) sb.Append(",");
            }
            sb.Append(")");
            return sb.ToString();

        }

    }

    public sealed class CellFuncCHMD5 : CellFuncCryptHash
    {

        public CellFuncCHMD5()
            : base(SystemFunctionLibrary.HASH_MD5, new MD5CryptoServiceProvider())
        {
        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return 16; // md5 has a hash size of 16 bytes
        }

    }

    public sealed class CellFuncCHSHA1 : CellFuncCryptHash
    {

        public CellFuncCHSHA1()
            : base(SystemFunctionLibrary.HASH_SHA1, new SHA1CryptoServiceProvider())
        {
        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return 20; // sha1 has a hash size of 20 bytes
        }

    }

    public sealed class CellFuncCHSHA256 : CellFuncCryptHash
    {

        public CellFuncCHSHA256()
            : base(SystemFunctionLibrary.HASH_SHA256, new SHA256CryptoServiceProvider())
        {
        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return 32; // sha256 has a hash size of 32 bytes
        }

    }

    #endregion

    // Special //
    #region SpecialFunctions

    public sealed class CellDateBuild : CellFuncFixedKnown
    {

        public CellDateBuild()
            : base(SystemFunctionLibrary.SPECIAL_DATE_BUILD, -1, CellAffinity.DATE_TIME)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            if (!(Data.Length == 3 || Data.Length == 6 || Data.Length == 7))
                throw new ArgumentException(string.Format("Invalid argument legnth passed : {0}", Data.Length));

            int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0, millisecond = 0;

            // Get the Year, Month, Day //
            if (Data.Length == 3 || Data.Length == 6 || Data.Length == 7)
            {
                year = (int)Data[0].valueINT;
                month = (int)Data[1].valueINT;
                day = (int)Data[2].valueINT;
            }

            // Hour, Minute, Second //
            if (Data.Length == 6 || Data.Length == 7)
            {
                hour = (int)Data[3].valueINT;
                minute = (int)Data[4].valueINT;
                second = (int)Data[5].valueINT;
            }

            // Millisecond //
            if (Data.Length == 7)
            {
                millisecond = (int)Data[6].valueINT;
            }

            DateTime t = new DateTime(year, month, day, hour, minute, second, millisecond);

            return new Cell(t);

        }

    }

    public sealed class CellFuncIf : CellFunction
    {

        public CellFuncIf()
            : base(SystemFunctionLibrary.SPECIAL_IF, 3, false)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            if (Data[0].BOOL)
                return Data[1];
            else if (Data[1].AFFINITY == Data[2].AFFINITY)
                return Data[2];
            else
                return Cell.Cast(Data[2], Data[1].AFFINITY);
        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinityHelper.Highest(Data);
        }

        public override string Unparse(string[] Text)
        {
            
            return Text[0] + " ? " + Text[1] + " : " + Text[2];

        }

    }

    public sealed class CellCast : CellFunction
    {

        private CellAffinity _Return;

        public CellCast(CellAffinity RType)
            : base("cast", 1, false)
        {
            this._Return = RType;
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Cast(Data[0], this._Return);
        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return this._Return;
        }

        public override string Unparse(string[] Text)
        {
            return Text[0] + " -> " + this._Return.ToString();
        }

    }

    /*
    public sealed class CellFuncCase : CellFunction
    {

        private List<Expression> _Whens;
        private List<Expression> _Thens;
        private Expression _Else;
        private CellAffinity _Re
        
        public CellFuncCase(List<Expression> Whens, List<Expression> Thens, Expression ELSE)
            : base(SystemFunctionLibrary.SPECIAL_CASE, -1, null, CellFunction.FuncHierStandard(), null)
        {

            if (Thens.Count != Whens.Count)
                throw new Exception("When and then statements have different counts");

            this._Whens = Whens;
            this._Thens = Thens;
            this._Else = ELSE ?? new ExpressionValue(null, new Cell(Thens.First().ReturnAffinity()));

            

        }

        public override Cell Evaluate(params Cell[] Data)
        {

            for (int i = 0; i < this._Whens.Count; i++)
            {

                if (this._Whens[i].Evaluate().valueBOOL)
                    return this._Thens[i].Evaluate();

            }

            return this._Else.Evaluate();

        }

        public override string Unparse(string[] Text)
        {

            StringBuilder sb = new StringBuilder();
            sb.AppendLine("CASE ");
            for (int i = 0; i < this._Whens.Count; i++)
            {
                sb.AppendLine(string.Format("WHEN {0} THEN {1} ", this._Whens[i].Unparse(y), this._Thens[i].Unparse(Columns)));
            }

            sb.AppendLine(string.Format("ELSE {0} ", this._Else.Unparse(Columns)));

            sb.AppendLine("END");

            return sb.ToString();

        }



        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            if (Type == CellAffinity.STRING || Type == CellAffinity.BLOB)
                return Sizes.Max();
            return Schema.FixSize(Type, -1);
        }

    }
    */

    #endregion

    // Mutable or instance level functions //
    #region MutableFunctions

    public sealed class CellRandom : CellFunction
    {

        private Random _r = null;
        
        public CellRandom()
            : base(SystemFunctionLibrary.MUTABLE_RAND, -1, true)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            if (_r == null)
            {
                int seed = CellRandom.RandomSeed;
                if (Data.Length != 0)
                    seed = (int)Data[0].valueINT;
                _r = new Random(seed);
            }

            return new Cell(_r.NextDouble());

        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinity.DOUBLE;
        }

        public override string Unparse(string[] Text)
        {
            if (Text.Length == 0)
                return "RAND()";
            return "RAND(" + Text[0] + ")";
        }

        /// <summary>
        /// Returns a random seed variable not based on Enviorment.Ticks; this function makes it extemely unlikely that the same seed will be returned twice,
        /// which is a risk with Enviorment.Ticks if the function is called many times in a row.
        /// </summary>
        internal static int RandomSeed
        {

            get
            {

                Guid g = Guid.NewGuid();
                byte[] bits = g.ToByteArray();
                HashAlgorithm sha256 = SHA256CryptoServiceProvider.Create();
                for (int i = 0; i < (int)bits[3] * (int)bits[6]; i++)
                {
                    bits = sha256.ComputeHash(bits);
                }
                int seed = BitConverter.ToInt32(bits, 8);
                if (seed < 0)
                    return seed = -seed;
                
                return seed;

            }

        }

    }

    public sealed class CellRandomInt : CellFunction
    {

        private Random _r = null;

        public CellRandomInt()
            : base(SystemFunctionLibrary.MUTABLE_RANDINT, -1, true)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            if (_r == null)
            {
                int seed = CellRandom.RandomSeed;
                if (Data.Length != 0)
                    seed = (int)Data[0].INT;
                _r = new Random(seed);
            }

            if (Data.Length == 2)
                return new Cell(this._r.Next((int)Data[0].INT, (int)Data[1].INT));
            else if (Data.Length == 3)
                return new Cell(this._r.Next((int)Data[1].INT, (int)Data[2].INT));

            return new Cell(this._r.Next());

        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinity.INT;
        }

        public override string Unparse(string[] Text)
        {
            if (Text.Length == 0)
                return "RAND()";
            return "RAND(" + Text[0] + ")";
        }



    }

    #endregion

    // Window Functions //
    #region WindowFunctions

    /*
    public static class CellCollectionFunctions
    {

        /// <summary>
        /// Record: 0 = sum weights, 1 = sum data, 2 = sum data squared
        /// </summary>
        /// <param name="Table"></param>
        /// <param name="WEIGHT"></param>
        /// <returns></returns>
        internal static Record Univariate(IEnumerable<Cell> Data, IEnumerable<Cell> Weight)
        {

            // If the counts are different, then throw an exceptions //
            if (Data.Count() != Weight.Count())
                throw new Exception(string.Format("WEIGHT and Table have different lengths {0} : {1}", Weight.Count(), Data.Count()));

            // Define variables //
            Cell w, x;
            Record r = Record.Stitch(Cell.ZeroValue(Weight.First().Affinity), Cell.ZeroValue(Data.First().Affinity), Cell.ZeroValue(Data.First().Affinity));
            for (int i = 0; i < Data.Count(); i++)
            {
                w = Weight.ElementAt(i);
                x = Data.ElementAt(i);
                if (!x.IsNull && !w.IsNull)
                {
                    r[0] += w;
                    r[1] += x * w;
                    r[2] += x * x * w;
                }
            }
            return r;

        }

        /// <summary>
        /// Record: 0 = weight, 1 = sum data x, 2 = sum data x squared, 3 = sum data y, 4 = sum data y squared, 5 = sum data x * y
        /// </summary>
        /// <param name="XData"></param>
        /// <param name="YData"></param>
        /// <param name="WEIGHT"></param>
        /// <returns></returns>
        internal static Record Bivariate(IEnumerable<Cell> XData, IEnumerable<Cell> YData, IEnumerable<Cell> Weight)
        {

            // If the counts are different, then throw an exceptions //
            if (XData.Count() != Weight.Count() || YData.Count() != Weight.Count())
                throw new Exception(string.Format("WEIGHT and Table have different lengths {0} : {1} : {2}", Weight.Count(), XData.Count(), YData.Count()));

            // Define variables //
            Cell x, y, w;
            Record r = Record.Stitch(Cell.ZeroValue(Weight.First().Affinity), Cell.ZeroValue(XData.First().Affinity), Cell.ZeroValue(XData.First().Affinity),
                Cell.ZeroValue(YData.First().Affinity), Cell.ZeroValue(YData.First().Affinity), Cell.ZeroValue(XData.First().Affinity));
            for (int i = 0; i < XData.Count(); i++)
            {
                x = XData.ElementAt(i);
                y = YData.ElementAt(i);
                w = Weight.ElementAt(i);
                if (!x.IsNull && !y.IsNull && !w.IsNull)
                {
                    r[0] += w;
                    r[1] += x * w;
                    r[2] += x * x * w;
                    r[3] += y * w;
                    r[4] += y * y * w;
                    r[5] += x * y * w;
                }
            }
            return r;
        }

        public static Cell Sum(IEnumerable<Cell> Data, IEnumerable<Cell> Weight)
        {
            // Record: 0 = sum weights, 1 = sum data, 2 = sum data squared
            Record r = Univariate(Data, Weight);
            return r[1];
        }

        public static Cell Average(IEnumerable<Cell> Data, IEnumerable<Cell> Weight)
        {
            // Record: 0 = sum weights, 1 = sum data, 2 = sum data squared
            Record r = Univariate(Data, Weight);
            return r[1] / r[0];
        }

        public static Cell Variance(IEnumerable<Cell> Data, IEnumerable<Cell> Weight)
        {
            // Record: 0 = sum weights, 1 = sum data, 2 = sum data squared
            Record r = Univariate(Data, Weight);
            Cell m = r[1] / r[0];
            return r[2] / r[0] - m * m;
        }

        public static Cell SDeviation(IEnumerable<Cell> Data, IEnumerable<Cell> Weight)
        {
            return Cell.Sqrt(Variance(Data, Weight));
        }

        public static Cell Covariance(IEnumerable<Cell> XData, IEnumerable<Cell> YData, IEnumerable<Cell> Weight)
        {
            // Record: 0 = weight, 1 = sum data x, 2 = sum data x squared, 3 = sum data y, 4 = sum data y squared, 5 = sum data x * y
            Record r = Bivariate(XData, YData, Weight);
            Cell avgx = r[1] / r[0], avgy = r[3] / r[0];
            return r[5] / r[0] - avgx * avgy;
        }

        public static Cell Correlation(IEnumerable<Cell> XData, IEnumerable<Cell> YData, IEnumerable<Cell> Weight)
        {
            // Record: 0 = weight, 1 = sum data x, 2 = sum data x squared, 3 = sum data y, 4 = sum data y squared, 5 = sum data x * y
            Record r = Bivariate(XData, YData, Weight);
            Cell avgx = r[1] / r[0], avgy = r[3] / r[0];
            Cell stdx = Cell.Sqrt(r[2] / r[0] - avgx * avgx), stdy = Cell.Sqrt(r[4] / r[0] - avgy * avgy);
            Cell covar = r[5] / r[0] - avgx * avgy;
            return covar / (stdx * stdy);
        }

        public static Cell Slope(IEnumerable<Cell> XData, IEnumerable<Cell> YData, IEnumerable<Cell> Weight)
        {
            // Record: 0 = weight, 1 = sum data x, 2 = sum data x squared, 3 = sum data y, 4 = sum data y squared, 5 = sum data x * y
            Record r = Bivariate(XData, YData, Weight);
            Cell avgx = r[1] / r[0], avgy = r[3] / r[0];
            Cell stdx = Cell.Sqrt(r[2] / r[0] - avgx * avgx);
            Cell covar = r[5] / r[0] - avgx * avgy;
            return covar / (stdx * stdx);
        }

        public static Cell Intercept(IEnumerable<Cell> XData, IEnumerable<Cell> YData, IEnumerable<Cell> Weight)
        {
            // Record: 0 = weight, 1 = sum data x, 2 = sum data x squared, 3 = sum data y, 4 = sum data y squared, 5 = sum data x * y
            Record r = Bivariate(XData, YData, Weight);
            Cell avgx = r[1] / r[0], avgy = r[3] / r[0];
            Cell stdx = Cell.Sqrt(r[2] / r[0] - avgx * avgx);
            Cell covar = r[5] / r[0] - avgx * avgy;
            return avgy - avgx * covar / (stdx * stdx);
        }


    }

    public abstract class CellMovingUni : CellFunction
    {

        private int _LagCount = -1;
        private Queue<Cell> _Xcache;
        private Queue<Cell> _Wcache;

        private const int OFFSET_LAG = 0;
        private const int OFFSET_DATA_X = 1;
        private const int OFFSET_DATA_W = 2;
        
        public CellMovingUni(string Name)
            : base(Name, -1, null, CellAffinity.DOUBLE)
        {
            this._Xcache = new Queue<Cell>();
            this._Wcache = new Queue<Cell>();
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            // Check the lag count //
            if (this._LagCount == -1) 
                this._LagCount = (int)Data[OFFSET_LAG].INT;

            // Accumulate and enque the data //
            this._Xcache.Enqueue(Data[OFFSET_DATA_X]);

            // Accumulate weights //
            if (Data.Length == 3)
                this._Wcache.Enqueue(Data[OFFSET_DATA_W]);
            else
                this._Wcache.Enqueue(Cell.OneValue(Data[OFFSET_DATA_X].Affinity));

            // Check for no accumulation //
            Cell x;
            if (this._Xcache.Count != this._LagCount)
            {
                x = new Cell(Data[OFFSET_DATA_X].Affinity);
            }
            else
            {
                x = this.Motion(this._Xcache, this._Wcache);
                this._Xcache.Dequeue();
                this._Wcache.Dequeue();
            }

            return x;

        }

        public abstract Cell Motion(Queue<Cell> Data, Queue<Cell> Weight);

    }

    public abstract class CellMovingBi : CellFunction
    {

        private int _LagCount = -1;
        private Queue<Cell> _Xcache;
        private Queue<Cell> _Ycache;
        private Queue<Cell> _Wcache;

        private const int OFFSET_LAG = 0;
        private const int OFFSET_DATA_X = 1;
        private const int OFFSET_DATA_Y = 2;
        private const int OFFSET_DATA_W = 3;

        public CellMovingBi(string Name)
            : base(Name, -1, null, CellAffinity.DOUBLE)
        {
            this._Xcache = new Queue<Cell>();
            this._Ycache = new Queue<Cell>();
            this._Wcache = new Queue<Cell>();
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            // Check the lag count //
            if (this._LagCount == -1)
                this._LagCount = (int)Data[OFFSET_LAG].INT;

            // Accumulate and enque the data //
            this._Xcache.Enqueue(Data[OFFSET_DATA_X]);
            this._Ycache.Enqueue(Data[OFFSET_DATA_Y]);

            // Accumulate weights //
            if (Data.Length == 4)
                this._Wcache.Enqueue(Data[OFFSET_DATA_W]);
            else
                this._Wcache.Enqueue(Cell.OneValue(Data[OFFSET_DATA_X].Affinity));

            // Check for no accumulation //
            Cell x;
            if (this._Xcache.Count != this._LagCount)
            {
                x = new Cell(Data[OFFSET_DATA_X].Affinity);
            }
            else
            {
                x = this.Motion(this._Xcache, this._Ycache, this._Wcache);
                this._Xcache.Dequeue();
                this._Ycache.Dequeue();
                this._Wcache.Dequeue();
            }

            return x;

        }

        public abstract Cell Motion(Queue<Cell> XData, Queue<Cell> YData, Queue<Cell> Weight);

    }

    public sealed class CellMSum : CellMovingUni
    {

        public CellMSum()
            : base(SystemFunctionLibrary.MUTABLE_MSUM)
        {
        }

        public override Cell Motion(Queue<Cell> Data, Queue<Cell> Weight)
        {
            return CellCollectionFunctions.Sum(Data, Weight);
        }

    }

    public sealed class CellMAvg : CellMovingUni
    {

        public CellMAvg()
            : base(SystemFunctionLibrary.MUTABLE_MAVG)
        {
        }

        public override Cell Motion(Queue<Cell> Data, Queue<Cell> Weight)
        {
            return CellCollectionFunctions.Average(Data, Weight);
        }

    }

    public sealed class CellMVar : CellMovingUni
    {

        public CellMVar()
            : base(SystemFunctionLibrary.MUTABLE_MVAR)
        {
        }

        public override Cell Motion(Queue<Cell> Data, Queue<Cell> Weight)
        {
            return CellCollectionFunctions.Variance(Data, Weight);
        }

    }

    public sealed class CellMStdev : CellMovingUni
    {

        public CellMStdev()
            : base(SystemFunctionLibrary.MUTABLE_MSTDEV)
        {
        }

        public override Cell Motion(Queue<Cell> Data, Queue<Cell> Weight)
        {
            return CellCollectionFunctions.SDeviation(Data, Weight);
        }

    }

    public sealed class CellMCovar : CellMovingBi
    {

        public CellMCovar()
            : base(SystemFunctionLibrary.MUTABLE_MCOVAR)
        {
        }

        public override Cell Motion(Queue<Cell> XData, Queue<Cell> YData, Queue<Cell> Weight)
        {
            return CellCollectionFunctions.Covariance(XData, YData, Weight);
        }

    }

    public sealed class CellMCorr : CellMovingBi
    {

        public CellMCorr()
            : base(SystemFunctionLibrary.MUTABLE_MCORR)
        {
        }

        public override Cell Motion(Queue<Cell> XData, Queue<Cell> YData, Queue<Cell> Weight)
        {
            return CellCollectionFunctions.Correlation(XData, YData, Weight);
        }

    }

    public sealed class CellMIntercept : CellMovingBi
    {

        public CellMIntercept()
            : base(SystemFunctionLibrary.MUTABLE_MINTERCEPT)
        {
        }

        public override Cell Motion(Queue<Cell> XData, Queue<Cell> YData, Queue<Cell> Weight)
        {
            return CellCollectionFunctions.Intercept(XData, YData, Weight);
        }

    }

    public sealed class CellMSlope : CellMovingBi
    {

        public CellMSlope()
            : base(SystemFunctionLibrary.MUTABLE_MSLOPE)
        {
        }

        public override Cell Motion(Queue<Cell> XData, Queue<Cell> YData, Queue<Cell> Weight)
        {
            return CellCollectionFunctions.Slope(XData, YData, Weight);
        }

    }
    */

    #endregion

    // Single value functions //
    #region VolatileFunctions

    public sealed class CellGUID : CellFunction
    {

        public CellGUID()
            : base(SystemFunctionLibrary.VOLATILE_GUID, 0, true)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return new Cell(Guid.NewGuid().ToByteArray());
        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return 16; // GUIDs are 16 bytes
        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinity.BLOB;
        }

        public override string Unparse(string[] Text)
        {
            return "GUID()";
        }

    }

    public sealed class CellTicks : CellFunction
    {

        public CellTicks()
            : base(SystemFunctionLibrary.VOLATILE_TICKS, 0, true)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return new Cell((long)Environment.TickCount);
        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinity.INT;
        }

        public override string Unparse(string[] Text)
        {
            return "TICKS()";
        }

    }

    public sealed class CellNow : CellFunction
    {

        public CellNow()
            : base(SystemFunctionLibrary.VOLATILE_NOW, 0, true)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return new Cell(DateTime.Now);
        }

        public override string Unparse(string[] Text)
        {
            return "NOW()";
        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinity.DATE_TIME;
        }

    }

    #endregion

    // Optimization helpers hidden form Dressage //
    #region HiddenFunctions

    public sealed class AndMany : CellFunction
    {

        public AndMany()
            : base(SystemFunctionLibrary.FUNC_AND_MANY, -1, false)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            bool b = true;
            foreach (Cell c in Data)
            {
                b = b && c.BOOL;
                if (!b) return new Cell(false);
            }
            
            return new Cell(true);

        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinity.BOOL;
        }

        public override string Unparse(string[] Text)
        {

            StringBuilder sb = new StringBuilder();
            sb.Append(this.NameSig);
            sb.Append("(");
            for (int i = 0; i < Text.Length; i++)
            {
                sb.Append(Text[i]);
                if (i != Text.Length - 1) 
                    sb.Append(",");
            }
            sb.Append(")");
            return sb.ToString();

        }

    }

    public sealed class OrMany : CellFunction
    {

        public OrMany()
            : base(SystemFunctionLibrary.FUNC_OR_MANY, -1, false)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            bool b = false;
            foreach (Cell c in Data)
            {
                b = b || c.BOOL;
                if (b) return new Cell(true);
            }

            return new Cell(false);

        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinity.BOOL;
        }

        public override string Unparse(string[] Text)
        {

            StringBuilder sb = new StringBuilder();
            sb.Append(this.NameSig);
            sb.Append("(");
            for (int i = 0; i < Text.Length; i++)
            {
                sb.Append(Text[i]);
                if (i != Text.Length - 1)
                    sb.Append(",");
            }
            sb.Append(")");
            return sb.ToString();

        }

    }

    public sealed class AddMany : CellFunction
    {

        public AddMany()
            : base(SystemFunctionLibrary.FUNC_ADD_MANY, -1, false)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            Cell s = Data[0];
            for (int i = 1; i < Data.Length; i++)
                s += Data[i];

            return s;

        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinityHelper.Highest(Data);
        }

        public override string Unparse(string[] Text)
        {

            StringBuilder sb = new StringBuilder();
            sb.Append(this.NameSig);
            sb.Append("(");
            for (int i = 0; i < Text.Length; i++)
            {
                sb.Append(Text[i]);
                if (i != Text.Length - 1)
                    sb.Append(",");
            }
            sb.Append(")");
            return sb.ToString();

        }

    }

    public sealed class ProductMany : CellFunction
    {

        public ProductMany()
            : base(SystemFunctionLibrary.FUNC_PRODUCT_MANY, -1, false)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            Cell s = Data[0];
            for (int i = 1; i < Data.Length; i++)
                s *= Data[i];

            return s;

        }

        public override CellAffinity ReturnAffinity(params CellAffinity[] Data)
        {
            return CellAffinityHelper.Highest(Data);
        }

        public override string Unparse(string[] Text)
        {

            StringBuilder sb = new StringBuilder();
            sb.Append(this.NameSig);
            sb.Append("(");
            for (int i = 0; i < Text.Length; i++)
            {
                sb.Append(Text[i]);
                if (i != Text.Length - 1)
                    sb.Append(",");
            }
            sb.Append(")");
            return sb.ToString();

        }

    }

    #endregion

    // Static Classes //

    /*
    public static class SystemFunctionLibrary
    {

        public const string UNI_PLUS = "uniplus";
        public const string UNI_MINUS = "uniminus";
        public const string UNI_NOT = "uninot";
        public const string UNI_AUTO_INC = "autoinc";
        public const string UNI_AUTO_DEC = "autodec";

        public const string OP_ADD = "add";
        public const string OP_SUB = "subract";
        public const string OP_MUL = "multiply";
        public const string OP_DIV = "divide";
        public const string OP_DIV2 = "divide2";
        public const string OP_MOD = "modulo";

        public const string BOOL_EQ = "equals";
        public const string BOOL_NEQ = "notequals";
        public const string BOOL_LT = "lessthan";
        public const string BOOL_LTE = "lessthanorequalto";
        public const string BOOL_GT = "greaterthan";
        public const string BOOL_GTE = "greaterthanorequalto";
        
        public const string FUNC_YEAR = "year";
        public const string FUNC_MONTH = "month";
        public const string FUNC_DAY = "day";
        public const string FUNC_HOUR = "hour";
        public const string FUNC_MINUTE = "minute";
        public const string FUNC_SECOND = "second";
        public const string FUNC_MILLISECOND = "millisecond";
        public const string FUNC_TIMESPAN = "timespan";
        public const string FUNC_SUBSTR = "substr";
        public const string FUNC_SLEFT = "sleft";
        public const string FUNC_SRIGHT = "sright";
        public const string FUNC_REPLACE = "replace";
        public const string FUNC_POSITION = "position";
        public const string FUNC_LENGTH = "length";
        public const string FUNC_IS_NULL = "isnull";
        public const string FUNC_IS_NOT_NULL = "isnotnull";
        public const string FUNC_TYPEOF = "typeof";
        public const string FUNC_STYPEOF = "stypeof";
        public const string FUNC_ROUND = "round";
        public const string FUNC_TO_UTF16 = "toutf16";
        public const string FUNC_TO_UTF8 = "toutf8";
        public const string FUNC_TO_HEX = "tohex";
        public const string FUNC_FROM_UTF16 = "fromutf16";
        public const string FUNC_FROM_UTF8 = "fromutf8";
        public const string FUNC_FROM_HEX = "fromhex";
        public const string FUNC_NDIST = "ndist";
        public const string FUNC_THREAD_ID = "threadid";
        public const string FUNC_ISPRIME = "isprime";
        
        // Differentiable Functions //
        public const string FUNC_LOG = "log";
        public const string FUNC_EXP = "exp";
        public const string FUNC_POWER = "power";
        public const string FUNC_SIN = "sin";
        public const string FUNC_COS = "cos";
        public const string FUNC_TAN = "tan";
        public const string FUNC_SINH = "sinh";
        public const string FUNC_COSH = "cosh";
        public const string FUNC_TANH = "tanh";
        public const string FUNC_LOGIT = "logit";
        public const string FUNC_SMAX = "smax";
        public const string FUNC_SMIN = "smin";

        public const string FUNC_IF_NULL = "ifnull";
        public const string FUNC_AND = "and";
        public const string FUNC_OR = "or";
        public const string FUNC_XOR = "xor";

        public const string HASH_MD5 = "md5";
        public const string HASH_SHA1 = "sha1";
        public const string HASH_SHA256 = "sha256";

        public const string MUTABLE_RAND = "rand";
        public const string MUTABLE_RANDINT = "randint";

        
        public const string MUTABLE_MAVG = "mavg";
        public const string MUTABLE_MVAR = "mvar";
        public const string MUTABLE_MSTDEV = "mstdev";
        public const string MUTABLE_MCOVAR = "mcovar";
        public const string MUTABLE_MCORR = "mcorr";
        public const string MUTABLE_MINTERCEPT = "mintercept";
        public const string MUTABLE_MSLOPE = "mslope";
        public const string MUTABLE_MSUM = "msum";
        public const string MUTABLE_KEY_CHANGE = "key_change";
        

        public const string VOLATILE_GUID = "guid";
        public const string VOLATILE_NOW = "now";
        public const string VOLATILE_TICKS = "ticks";

        public const string FUNC_AND_MANY = "andmany";
        public const string FUNC_OR_MANY = "ormany";
        public const string FUNC_ADD_MANY = "addmany";
        public const string FUNC_PRODUCT_MANY =  "productmany";

        public const string TOKEN_UNI_PLUS = "u+";
        public const string TOKEN_UNI_MINUS = "u-";
        public const string TOKEN_UNI_NOT = "!";
        public const string TOKEN_UNI_AUTO_INC = "++";
        public const string TOKEN_UNI_AUTO_DEC = "--";

        public const string TOKEN_OP_ADD = "+";
        public const string TOKEN_OP_SUB = "-";
        public const string TOKEN_OP_MUL = "*";
        public const string TOKEN_OP_DIV = "/";
        public const string TOKEN_OP_DIV2 = "/?";
        public const string TOKEN_OP_MOD = "%";

        public const string TOKEN_BOOL_EQ = "==";
        public const string TOKEN_BOOL_NEQ = "!=";
        public const string TOKEN_BOOL_LT = "<";
        public const string TOKEN_BOOL_LTE = "<=";
        public const string TOKEN_BOOL_GT = ">";
        public const string TOKEN_BOOL_GTE = ">=";
        public const string TOKEN_FUNC_IF_NULL = "??";

        public const string SPECIAL_IF = "if";
        public const string SPECIAL_DATE_BUILD = "date_build";

    }

    public static class CellFunctionFactory
    {

        private static Dictionary<string, Func<CellFunction>> FUNCTION_TABLE = new Dictionary<string, Func<CellFunction>>(StringComparer.OrdinalIgnoreCase)
        {

            { SystemFunctionLibrary.TOKEN_UNI_PLUS, () => { return new CellUniPlus();}},
            { SystemFunctionLibrary.UNI_PLUS, () => { return new CellUniPlus();}},
            { SystemFunctionLibrary.TOKEN_UNI_MINUS, () => { return new CellUniMinus();}},
            { SystemFunctionLibrary.UNI_MINUS, () => { return new CellUniMinus();}},
            { SystemFunctionLibrary.TOKEN_UNI_NOT, () => { return new CellUniNot();}},
            { SystemFunctionLibrary.UNI_NOT, () => { return new CellUniNot();}},
            { SystemFunctionLibrary.TOKEN_UNI_AUTO_INC, () => { return new CellUniAutoInc();}},
            { SystemFunctionLibrary.UNI_AUTO_INC, () => { return new CellUniAutoInc();}},
            { SystemFunctionLibrary.TOKEN_UNI_AUTO_DEC, () => { return new CellUniAutoDec();}},
            { SystemFunctionLibrary.UNI_AUTO_DEC, () => { return new CellUniAutoDec();}},

            { SystemFunctionLibrary.TOKEN_OP_ADD, () => { return new CellBinPlus();}},
            { SystemFunctionLibrary.OP_ADD, () => { return new CellBinPlus();}},
            { SystemFunctionLibrary.TOKEN_OP_SUB, () => { return new CellBinMinus();}},
            { SystemFunctionLibrary.OP_SUB, () => { return new CellBinMinus();}},
            { SystemFunctionLibrary.TOKEN_OP_MUL, () => { return new CellBinMult();}},
            { SystemFunctionLibrary.OP_MUL, () => { return new CellBinMult();}},
            { SystemFunctionLibrary.TOKEN_OP_DIV, () => { return new CellBinDiv();}},
            { SystemFunctionLibrary.OP_DIV, () => { return new CellBinDiv();}},
            { SystemFunctionLibrary.TOKEN_OP_DIV2, () => { return new CellBinDiv2();}},
            { SystemFunctionLibrary.OP_DIV2, () => { return new CellBinDiv2();}},
            { SystemFunctionLibrary.TOKEN_OP_MOD, () => { return new CellBinMod();}},
            { SystemFunctionLibrary.OP_MOD, () => { return new CellBinMod();}},
            
            { SystemFunctionLibrary.TOKEN_BOOL_EQ, () => { return new CellBoolEQ();}},
            { SystemFunctionLibrary.BOOL_EQ, () => { return new CellBoolEQ();}},
            { SystemFunctionLibrary.TOKEN_BOOL_NEQ, () => { return new CellBoolNEQ();}},
            { SystemFunctionLibrary.BOOL_NEQ, () => { return new CellBoolNEQ();}},
            { SystemFunctionLibrary.TOKEN_BOOL_LT, () => { return new CellBoolLT();}},
            { SystemFunctionLibrary.BOOL_LT, () => { return new CellBoolLT();}},
            { SystemFunctionLibrary.TOKEN_BOOL_LTE, () => { return new CellBoolLTE();}},
            { SystemFunctionLibrary.BOOL_LTE, () => { return new CellBoolLTE();}},
            { SystemFunctionLibrary.TOKEN_BOOL_GT, () => { return new CellBoolGT();}},
            { SystemFunctionLibrary.BOOL_GT, () => { return new CellBoolGT();}},
            { SystemFunctionLibrary.TOKEN_BOOL_GTE, () => { return new CellBoolGTE();}},
            { SystemFunctionLibrary.BOOL_GTE, () => { return new CellBoolGTE();}},

            { SystemFunctionLibrary.FUNC_AND, () => { return new CellFuncFVAND();}},
            { SystemFunctionLibrary.FUNC_OR, () => { return new CellFuncFVOR();}},
            { SystemFunctionLibrary.FUNC_XOR, () => { return new CellFuncFVXOR();}},
            
            { SystemFunctionLibrary.FUNC_YEAR, () => { return new CellFuncFKYear();}},
            { SystemFunctionLibrary.FUNC_MONTH, () => { return new CellFuncFKMonth();}},
            { SystemFunctionLibrary.FUNC_DAY, () => { return new CellFuncFKDay();}},
            { SystemFunctionLibrary.FUNC_HOUR, () => { return new CellFuncFKHour();}},
            { SystemFunctionLibrary.FUNC_MINUTE, () => { return new CellFuncFKMinute();}},
            { SystemFunctionLibrary.FUNC_SECOND, () => { return new CellFuncFKSecond();}},
            { SystemFunctionLibrary.FUNC_MILLISECOND, () => { return new CellFuncFKMillisecond();}},
            { SystemFunctionLibrary.FUNC_TIMESPAN, () => { return new CellFuncFKTicks();}},
            
            { SystemFunctionLibrary.FUNC_SUBSTR, () => { return new CellFuncFKSubstring();}},
            { SystemFunctionLibrary.FUNC_SLEFT, () => { return new CellFuncFKLeft();}},
            { SystemFunctionLibrary.FUNC_SRIGHT, () => { return new CellFuncFKRight();}},
            { SystemFunctionLibrary.FUNC_REPLACE, () => { return new CellFuncFKReplace();}},
            { SystemFunctionLibrary.FUNC_POSITION, () => { return new CellFuncFKPosition();}},
            { SystemFunctionLibrary.FUNC_LENGTH, () => { return new CellFuncFKLength();}},
            { SystemFunctionLibrary.FUNC_IS_NULL, () => { return new CellFuncFKIsNull();}},
            { SystemFunctionLibrary.FUNC_IS_NOT_NULL, () => { return new CellFuncFKIsNotNull();}},
            { SystemFunctionLibrary.FUNC_TYPEOF, () => { return new CellFuncFKTypeOf();}},
            { SystemFunctionLibrary.FUNC_STYPEOF, () => { return new CellFuncFKSTypeOf();}},
            { SystemFunctionLibrary.FUNC_ROUND, () => { return new CellFuncFKRound();}},
            
            { SystemFunctionLibrary.FUNC_TO_UTF16, () => { return new CellFuncFKToUTF16();}},
            { SystemFunctionLibrary.FUNC_TO_UTF8, () => { return new CellFuncFKToUTF8();}},
            { SystemFunctionLibrary.FUNC_TO_HEX, () => { return new CellFuncFKToHEX();}},
            { SystemFunctionLibrary.FUNC_FROM_UTF16, () => { return new CellFuncFKFromUTF16();}},
            { SystemFunctionLibrary.FUNC_FROM_UTF8, () => { return new CellFuncFKFromUTF8();}},
            { SystemFunctionLibrary.FUNC_FROM_HEX, () => { return new CellFuncFKFromHEX();}},
            { SystemFunctionLibrary.FUNC_NDIST, () => { return new CellFuncFKNormal();}},
            { SystemFunctionLibrary.FUNC_THREAD_ID, () => { return new CellFuncFKThreadID();}},
            { SystemFunctionLibrary.FUNC_ISPRIME, () => { return new CellFuncFKIsPrime();}},

            { SystemFunctionLibrary.FUNC_LOG, () => { return new CellFuncFVLog();}},
            { SystemFunctionLibrary.FUNC_EXP, () => { return new CellFuncFVExp();}},
            { SystemFunctionLibrary.FUNC_POWER, () => { return new CellFuncFVPower();}},
            { SystemFunctionLibrary.FUNC_SIN, () => { return new CellFuncFVSin();}},
            { SystemFunctionLibrary.FUNC_COS, () => { return new CellFuncFVCos();}},
            { SystemFunctionLibrary.FUNC_TAN, () => { return new CellFuncFVTan();}},
            { SystemFunctionLibrary.FUNC_SINH, () => { return new CellFuncFVSinh();}},
            { SystemFunctionLibrary.FUNC_COSH, () => { return new CellFuncFVCosh();}},
            { SystemFunctionLibrary.FUNC_TANH, () => { return new CellFuncFVTanh();}},
            { SystemFunctionLibrary.FUNC_LOGIT, () => { return new CellFuncFVLogit();}},
            { SystemFunctionLibrary.TOKEN_FUNC_IF_NULL, () => { return new CellFuncFVIfNull();}},
            { SystemFunctionLibrary.FUNC_IF_NULL, () => { return new CellFuncFVIfNull();}},
            { SystemFunctionLibrary.FUNC_SMIN, () => { return new CellFuncFVSMin();}},
            { SystemFunctionLibrary.FUNC_SMAX, () => { return new CellFuncFVSMax();}},
            
            { SystemFunctionLibrary.SPECIAL_IF, () => { return new CellFuncIf();}},
            { SystemFunctionLibrary.SPECIAL_DATE_BUILD, () => { return new CellDateBuild();}},
            
            { SystemFunctionLibrary.HASH_MD5, () => { return new CellFuncCHMD5();}},
            { SystemFunctionLibrary.HASH_SHA1, () => { return new CellFuncCHSHA1();}},
            { SystemFunctionLibrary.HASH_SHA256, () => { return new CellFuncCHSHA256();}},
            
            { SystemFunctionLibrary.MUTABLE_RAND, () => { return new CellRandom();}},
            { SystemFunctionLibrary.MUTABLE_RANDINT, () => { return new CellRandomInt();}},
            
            { SystemFunctionLibrary.VOLATILE_GUID, () => { return new CellGUID();}},
            { SystemFunctionLibrary.VOLATILE_TICKS, () => { return new CellTicks();}},
            { SystemFunctionLibrary.VOLATILE_NOW, () => { return new CellNow();}},

            { SystemFunctionLibrary.FUNC_ADD_MANY, () => { return new AddMany();}},
            { SystemFunctionLibrary.FUNC_PRODUCT_MANY, () => { return new ProductMany();}},
            { SystemFunctionLibrary.FUNC_AND_MANY, () => { return new AndMany();}},
            { SystemFunctionLibrary.FUNC_OR_MANY, () => { return new OrMany();}},


        };

        public static bool Exists(string FunctionName)
        {
            return FUNCTION_TABLE.ContainsKey(FunctionName);
        }

        public static CellFunction LookUp(string FunctionName)
        {
            return FUNCTION_TABLE[FunctionName].Invoke(); 
        }

        public static string PrintString()
        {
            StringBuilder sb = new StringBuilder();
            foreach (KeyValuePair<string, Func<CellFunction>> kv in FUNCTION_TABLE)
                sb.AppendLine(kv.Key);
            return sb.ToString();
        }

    }
    */
    
    public sealed class SystemFunctionLibrary : FunctionLibrary
    {

        private static SystemFunctionLibrary _base = new SystemFunctionLibrary();

        #region FUNCTION_NAMES

        public const string UNI_PLUS = "uniplus";
        public const string UNI_MINUS = "uniminus";
        public const string UNI_NOT = "uninot";
        public const string UNI_AUTO_INC = "autoinc";
        public const string UNI_AUTO_DEC = "autodec";

        public const string OP_ADD = "add";
        public const string OP_SUB = "subract";
        public const string OP_MUL = "multiply";
        public const string OP_DIV = "divide";
        public const string OP_DIV2 = "divide2";
        public const string OP_MOD = "modulo";

        public const string BOOL_EQ = "equals";
        public const string BOOL_NEQ = "notequals";
        public const string BOOL_LT = "lessthan";
        public const string BOOL_LTE = "lessthanorequalto";
        public const string BOOL_GT = "greaterthan";
        public const string BOOL_GTE = "greaterthanorequalto";

        public const string FUNC_YEAR = "year";
        public const string FUNC_MONTH = "month";
        public const string FUNC_DAY = "day";
        public const string FUNC_HOUR = "hour";
        public const string FUNC_MINUTE = "minute";
        public const string FUNC_SECOND = "second";
        public const string FUNC_MILLISECOND = "millisecond";
        public const string FUNC_TIMESPAN = "timespan";
        public const string FUNC_SUBSTR = "substr";
        public const string FUNC_SLEFT = "sleft";
        public const string FUNC_SRIGHT = "sright";
        public const string FUNC_REPLACE = "replace";
        public const string FUNC_POSITION = "position";
        public const string FUNC_LENGTH = "length";
        public const string FUNC_IS_NULL = "isnull";
        public const string FUNC_IS_NOT_NULL = "isnotnull";
        public const string FUNC_TYPEOF = "typeof";
        public const string FUNC_STYPEOF = "stypeof";
        public const string FUNC_ROUND = "round";
        public const string FUNC_TO_UTF16 = "toutf16";
        public const string FUNC_TO_UTF8 = "toutf8";
        public const string FUNC_TO_HEX = "tohex";
        public const string FUNC_FROM_UTF16 = "fromutf16";
        public const string FUNC_FROM_UTF8 = "fromutf8";
        public const string FUNC_FROM_HEX = "fromhex";
        public const string FUNC_NDIST = "ndist";
        public const string FUNC_THREAD_ID = "threadid";
        public const string FUNC_ISPRIME = "isprime";

        public const string FUNC_LOG = "log";
        public const string FUNC_EXP = "exp";
        public const string FUNC_POWER = "power";
        public const string FUNC_SIN = "sin";
        public const string FUNC_COS = "cos";
        public const string FUNC_TAN = "tan";
        public const string FUNC_SINH = "sinh";
        public const string FUNC_COSH = "cosh";
        public const string FUNC_TANH = "tanh";
        public const string FUNC_LOGIT = "logit";
        public const string FUNC_SMAX = "smax";
        public const string FUNC_SMIN = "smin";

        public const string FUNC_IF_NULL = "ifnull";
        public const string FUNC_AND = "and";
        public const string FUNC_OR = "or";
        public const string FUNC_XOR = "xor";

        public const string HASH_MD5 = "md5";
        public const string HASH_SHA1 = "sha1";
        public const string HASH_SHA256 = "sha256";

        public const string MUTABLE_RAND = "rand";
        public const string MUTABLE_RANDINT = "randint";

        public const string VOLATILE_GUID = "guid";
        public const string VOLATILE_NOW = "now";
        public const string VOLATILE_TICKS = "ticks";

        public const string FUNC_AND_MANY = "andmany";
        public const string FUNC_OR_MANY = "ormany";
        public const string FUNC_ADD_MANY = "addmany";
        public const string FUNC_PRODUCT_MANY = "productmany";

        public const string TOKEN_UNI_PLUS = "u+";
        public const string TOKEN_UNI_MINUS = "u-";
        public const string TOKEN_UNI_NOT = "!";
        public const string TOKEN_UNI_AUTO_INC = "++";
        public const string TOKEN_UNI_AUTO_DEC = "--";

        public const string TOKEN_OP_ADD = "+";
        public const string TOKEN_OP_SUB = "-";
        public const string TOKEN_OP_MUL = "*";
        public const string TOKEN_OP_DIV = "/";
        public const string TOKEN_OP_DIV2 = "/?";
        public const string TOKEN_OP_MOD = "%";

        public const string TOKEN_BOOL_EQ = "==";
        public const string TOKEN_BOOL_NEQ = "!=";
        public const string TOKEN_BOOL_LT = "<";
        public const string TOKEN_BOOL_LTE = "<=";
        public const string TOKEN_BOOL_GT = ">";
        public const string TOKEN_BOOL_GTE = ">=";
        public const string TOKEN_FUNC_IF_NULL = "??";

        public const string SPECIAL_IF = "if";
        public const string SPECIAL_DATE_BUILD = "date_build";

        #endregion

        #region NAME_ARRAY

        private static string[] _BaseNames = 
        {

            UNI_PLUS,
            UNI_MINUS,
            UNI_NOT,
            UNI_AUTO_INC,
            UNI_AUTO_DEC,

            OP_ADD,
            OP_SUB,
            OP_MUL,
            OP_DIV,
            OP_DIV2,
            OP_MOD,

            BOOL_EQ,
            BOOL_NEQ,
            BOOL_LT,
            BOOL_LTE,
            BOOL_GT,
            BOOL_GTE,

            FUNC_YEAR,
            FUNC_MONTH,
            FUNC_DAY,
            FUNC_HOUR,
            FUNC_MINUTE,
            FUNC_SECOND,
            FUNC_MILLISECOND,
            FUNC_TIMESPAN,
            FUNC_SUBSTR,
            FUNC_SLEFT,
            FUNC_SRIGHT,
            FUNC_REPLACE,
            FUNC_POSITION,
            FUNC_LENGTH,
            FUNC_IS_NULL,
            FUNC_IS_NOT_NULL,
            FUNC_TYPEOF,
            FUNC_STYPEOF,
            FUNC_ROUND,
            FUNC_TO_UTF16,
            FUNC_TO_UTF8,
            FUNC_TO_HEX,
            FUNC_FROM_UTF16,
            FUNC_FROM_UTF8,
            FUNC_FROM_HEX,
            FUNC_NDIST,
            FUNC_THREAD_ID,
            FUNC_ISPRIME,

            FUNC_LOG,
            FUNC_EXP,
            FUNC_POWER,
            FUNC_SIN,
            FUNC_COS,
            FUNC_TAN,
            FUNC_SINH,
            FUNC_COSH,
            FUNC_TANH,
            FUNC_LOGIT,
            FUNC_SMAX,
            FUNC_SMIN,

            FUNC_IF_NULL,
            FUNC_AND,
            FUNC_OR,
            FUNC_XOR,

            HASH_MD5,
            HASH_SHA1,
            HASH_SHA256,

            MUTABLE_RAND,
            MUTABLE_RANDINT,

            VOLATILE_GUID,
            VOLATILE_NOW,
            VOLATILE_TICKS,

            FUNC_AND_MANY,
            FUNC_OR_MANY,
            FUNC_ADD_MANY,
            FUNC_PRODUCT_MANY,

            TOKEN_UNI_PLUS,
            TOKEN_UNI_MINUS,
            TOKEN_UNI_NOT,
            TOKEN_UNI_AUTO_INC,
            TOKEN_UNI_AUTO_DEC,

            TOKEN_OP_ADD,
            TOKEN_OP_SUB,
            TOKEN_OP_MUL,
            TOKEN_OP_DIV,
            TOKEN_OP_DIV2,
            TOKEN_OP_MOD,

            TOKEN_BOOL_EQ,
            TOKEN_BOOL_NEQ,
            TOKEN_BOOL_LT,
            TOKEN_BOOL_LTE,
            TOKEN_BOOL_GT,
            TOKEN_BOOL_GTE,
            TOKEN_FUNC_IF_NULL,

            SPECIAL_IF,
            SPECIAL_DATE_BUILD

        };

        #endregion

        public SystemFunctionLibrary()
        {
        }

        public override CellFunction RenderFunction(string Name)
        {
            
            switch (Name.ToLower())
            {
                case TOKEN_UNI_PLUS: return new CellUniPlus();
                case UNI_PLUS: return new CellUniPlus();
                case TOKEN_UNI_MINUS: return new CellUniMinus();
                case UNI_MINUS: return new CellUniMinus();
                case TOKEN_UNI_NOT: return new CellUniNot();
                case UNI_NOT: return new CellUniNot();
                case TOKEN_UNI_AUTO_INC: return new CellUniAutoInc();
                case UNI_AUTO_INC: return new CellUniAutoInc();
                case TOKEN_UNI_AUTO_DEC: return new CellUniAutoDec();
                case UNI_AUTO_DEC: return new CellUniAutoDec();

                case TOKEN_OP_ADD: return new CellBinPlus();
                case OP_ADD: return new CellBinPlus();
                case TOKEN_OP_SUB: return new CellBinMinus();
                case OP_SUB: return new CellBinMinus();
                case TOKEN_OP_MUL: return new CellBinMult();
                case OP_MUL: return new CellBinMult();
                case TOKEN_OP_DIV: return new CellBinDiv();
                case OP_DIV: return new CellBinDiv();
                case TOKEN_OP_DIV2: return new CellBinDiv2();
                case OP_DIV2: return new CellBinDiv2();
                case TOKEN_OP_MOD: return new CellBinMod();
                case OP_MOD: return new CellBinMod();

                case TOKEN_BOOL_EQ: return new CellBoolEQ();
                case BOOL_EQ: return new CellBoolEQ();
                case TOKEN_BOOL_NEQ: return new CellBoolNEQ();
                case BOOL_NEQ: return new CellBoolNEQ();
                case TOKEN_BOOL_LT: return new CellBoolLT();
                case BOOL_LT: return new CellBoolLT();
                case TOKEN_BOOL_LTE: return new CellBoolLTE();
                case BOOL_LTE: return new CellBoolLTE();
                case TOKEN_BOOL_GT: return new CellBoolGT();
                case BOOL_GT: return new CellBoolGT();
                case TOKEN_BOOL_GTE: return new CellBoolGTE();
                case BOOL_GTE: return new CellBoolGTE();

                case FUNC_AND: return new CellFuncFVAND();
                case FUNC_OR: return new CellFuncFVOR();
                case FUNC_XOR: return new CellFuncFVXOR();

                case FUNC_YEAR: return new CellFuncFKYear();
                case FUNC_MONTH: return new CellFuncFKMonth();
                case FUNC_DAY: return new CellFuncFKDay();
                case FUNC_HOUR: return new CellFuncFKHour();
                case FUNC_MINUTE: return new CellFuncFKMinute();
                case FUNC_SECOND: return new CellFuncFKSecond();
                case FUNC_MILLISECOND: return new CellFuncFKMillisecond();
                case FUNC_TIMESPAN: return new CellFuncFKTicks();

                case FUNC_SUBSTR: return new CellFuncFKSubstring();
                case FUNC_SLEFT: return new CellFuncFKLeft();
                case FUNC_SRIGHT: return new CellFuncFKRight();
                case FUNC_REPLACE: return new CellFuncFKReplace();
                case FUNC_POSITION: return new CellFuncFKPosition();
                case FUNC_LENGTH: return new CellFuncFKLength();
                case FUNC_IS_NULL: return new CellFuncFKIsNull();
                case FUNC_IS_NOT_NULL: return new CellFuncFKIsNotNull();
                case FUNC_TYPEOF: return new CellFuncFKTypeOf();
                case FUNC_STYPEOF: return new CellFuncFKSTypeOf();
                case FUNC_ROUND: return new CellFuncFKRound();

                case FUNC_TO_UTF16: return new CellFuncFKToUTF16();
                case FUNC_TO_UTF8: return new CellFuncFKToUTF8();
                case FUNC_TO_HEX: return new CellFuncFKToHEX();
                case FUNC_FROM_UTF16: return new CellFuncFKFromUTF16();
                case FUNC_FROM_UTF8: return new CellFuncFKFromUTF8();
                case FUNC_FROM_HEX: return new CellFuncFKFromHEX();
                case FUNC_NDIST: return new CellFuncFKNormal();
                case FUNC_THREAD_ID: return new CellFuncFKThreadID();
                case FUNC_ISPRIME: return new CellFuncFKIsPrime();

                case FUNC_LOG: return new CellFuncFVLog();
                case FUNC_EXP: return new CellFuncFVExp();
                case FUNC_POWER: return new CellFuncFVPower();
                case FUNC_SIN: return new CellFuncFVSin();
                case FUNC_COS: return new CellFuncFVCos();
                case FUNC_TAN: return new CellFuncFVTan();
                case FUNC_SINH: return new CellFuncFVSinh();
                case FUNC_COSH: return new CellFuncFVCosh();
                case FUNC_TANH: return new CellFuncFVTanh();
                case FUNC_LOGIT: return new CellFuncFVLogit();
                case TOKEN_FUNC_IF_NULL: return new CellFuncFVIfNull();
                case FUNC_IF_NULL: return new CellFuncFVIfNull();
                case FUNC_SMIN: return new CellFuncFVSMin();
                case FUNC_SMAX: return new CellFuncFVSMax();

                case SPECIAL_IF: return new CellFuncIf();
                case SPECIAL_DATE_BUILD: return new CellDateBuild();

                case HASH_MD5: return new CellFuncCHMD5();
                case HASH_SHA1: return new CellFuncCHSHA1();
                case HASH_SHA256: return new CellFuncCHSHA256();

                case MUTABLE_RAND: return new CellRandom();
                case MUTABLE_RANDINT: return new CellRandomInt();

                case VOLATILE_GUID: return new CellGUID();
                case VOLATILE_TICKS: return new CellTicks();
                case VOLATILE_NOW: return new CellNow();

                case FUNC_ADD_MANY: return new AddMany();
                case FUNC_PRODUCT_MANY: return new ProductMany();
                case FUNC_AND_MANY: return new AndMany();
                case FUNC_OR_MANY: return new OrMany();

            }

            throw new ArgumentException(string.Format("System function '{0}' does not exist", Name));

        }

        public override string[] Names
        {
            get { return SystemFunctionLibrary._BaseNames; }
        }

        public static CellFunction LookUp(string Name)
        {
            return _base.RenderFunction(Name);
        }

    }

}
