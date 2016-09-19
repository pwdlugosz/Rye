using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography;
using Rye.Data;
using Rye.Libraries;

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
            : base(BaseFunctionLibrary.UNI_PLUS, "+")
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
            : base(BaseFunctionLibrary.UNI_MINUS, "-")
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
            : base(BaseFunctionLibrary.UNI_NOT, "!")
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
            : base(BaseFunctionLibrary.UNI_AUTO_INC, "++")
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
            : base(BaseFunctionLibrary.UNI_AUTO_DEC, "--")
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
            : base(BaseFunctionLibrary.OP_ADD, "+")
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
            : base(BaseFunctionLibrary.OP_SUB, "-")
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
            : base(BaseFunctionLibrary.OP_MUL, "*")
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
            : base(BaseFunctionLibrary.OP_DIV, "/")
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
            : base(BaseFunctionLibrary.OP_DIV2, "/?")
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
            : base(BaseFunctionLibrary.OP_MOD, "%")
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
            : base(BaseFunctionLibrary.BOOL_EQ, "==")
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
            : base(BaseFunctionLibrary.BOOL_NEQ, "!=")
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
            : base(BaseFunctionLibrary.BOOL_LT, "<")
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
            : base(BaseFunctionLibrary.BOOL_LTE, "<=")
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
            : base(BaseFunctionLibrary.BOOL_GT, ">")
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
            : base(BaseFunctionLibrary.BOOL_GTE, ">=")
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
            : base(BaseFunctionLibrary.FUNC_YEAR, 1, CellAffinity.INT)
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
            : base(BaseFunctionLibrary.FUNC_MONTH, 1, CellAffinity.INT)
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
            : base(BaseFunctionLibrary.FUNC_DAY, 1, CellAffinity.INT)
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
            : base(BaseFunctionLibrary.FUNC_HOUR, 1, CellAffinity.INT)
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
            : base(BaseFunctionLibrary.FUNC_MINUTE, 1, CellAffinity.INT)
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
            : base(BaseFunctionLibrary.FUNC_SECOND, 1, CellAffinity.INT)
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
            : base(BaseFunctionLibrary.FUNC_MILLISECOND, 1, CellAffinity.INT)
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
            : base(BaseFunctionLibrary.FUNC_TIMESPAN, 1, CellAffinity.STRING)
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
            : base(BaseFunctionLibrary.FUNC_SUBSTR, 3, CellAffinity.STRING)
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
            : base(BaseFunctionLibrary.FUNC_SLEFT, 2, CellAffinity.STRING)
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
            : base(BaseFunctionLibrary.FUNC_SRIGHT, 2, CellAffinity.STRING)
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
            : base(BaseFunctionLibrary.FUNC_REPLACE, 3, CellAffinity.STRING)
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
            : base(BaseFunctionLibrary.FUNC_POSITION, 3, CellAffinity.INT)
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
            : base(BaseFunctionLibrary.FUNC_LENGTH, 1, CellAffinity.INT)
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

    public sealed class CellFuncFKTrim : CellFuncFixedKnown
    {

        public CellFuncFKTrim()
            : base(BaseFunctionLibrary.FUNC_TRIM, 1, CellAffinity.INT)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {

            if (Data[0].NULL == 1)
                return Data[0];

            if (Data[0].AFFINITY == CellAffinity.STRING)
            {
                Cell c = new Cell(Data[0].STRING.Trim());
                return c;
            }

            return Data[0];

        }

    }

    public sealed class CellFuncFKIsNull : CellFuncFixedKnown
    {

        public CellFuncFKIsNull()
            : base(BaseFunctionLibrary.FUNC_IS_NULL, 1, CellAffinity.BOOL)
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
            : base(BaseFunctionLibrary.FUNC_IS_NOT_NULL, 1, CellAffinity.BOOL)
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
            : base(BaseFunctionLibrary.FUNC_TYPEOF, 1, CellAffinity.INT)
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
            : base(BaseFunctionLibrary.FUNC_STYPEOF, 1, CellAffinity.STRING)
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
            : base(BaseFunctionLibrary.FUNC_ROUND, 2, CellAffinity.DOUBLE)
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
            : base(BaseFunctionLibrary.FUNC_TO_UTF16, 1, CellAffinity.STRING)
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
            : base(BaseFunctionLibrary.FUNC_TO_UTF8, 1, CellAffinity.STRING)
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
            : base(BaseFunctionLibrary.FUNC_TO_HEX, 1, CellAffinity.STRING)
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
            : base(BaseFunctionLibrary.FUNC_FROM_UTF16, 1, CellAffinity.BLOB)
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
            : base(BaseFunctionLibrary.FUNC_FROM_UTF8, 1, CellAffinity.BLOB)
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
            : base(BaseFunctionLibrary.FUNC_FROM_HEX, 1, CellAffinity.BLOB)
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
            : base(BaseFunctionLibrary.FUNC_NDIST, 1, CellAffinity.DOUBLE)
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
            : base(BaseFunctionLibrary.FUNC_THREAD_ID, 0, CellAffinity.INT)
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
            : base(BaseFunctionLibrary.FUNC_ISPRIME, 1, CellAffinity.BOOL)
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

            for (long i = 2; i <= (long)Math.Sqrt(n) + 1; i++)
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
            : base(BaseFunctionLibrary.FUNC_LOG, 1)
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
            : base(BaseFunctionLibrary.FUNC_EXP, 1)
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
            : base(BaseFunctionLibrary.FUNC_POWER, 2)
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
            : base(BaseFunctionLibrary.FUNC_SIN, 1)
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
            : base(BaseFunctionLibrary.FUNC_COS, 1)
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
            : base(BaseFunctionLibrary.FUNC_TAN, 1)
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
            : base(BaseFunctionLibrary.FUNC_SINH, 1)
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
            : base(BaseFunctionLibrary.FUNC_COSH, 1)
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
            : base(BaseFunctionLibrary.FUNC_TANH, 1)
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
            : base(BaseFunctionLibrary.FUNC_LOGIT, 1)
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
            : base(BaseFunctionLibrary.FUNC_IF_NULL, 2)
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
            : base(BaseFunctionLibrary.FUNC_AND, 2)
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
            : base(BaseFunctionLibrary.FUNC_OR, 2)
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
            : base(BaseFunctionLibrary.FUNC_XOR, 2)
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
            : base(BaseFunctionLibrary.FUNC_SMAX, -1)
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
            : base(BaseFunctionLibrary.FUNC_SMIN, -1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Min(Data);
        }

    }

    public sealed class CellFuncFVAbs : CellFuncFixedVariable
    {

        public CellFuncFVAbs()
            : base(BaseFunctionLibrary.FUNC_ABS, 1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Abs(Data[0]);
        }

    }

    public sealed class CellFuncFVSign : CellFuncFixedVariable
    {

        public CellFuncFVSign()
            : base(BaseFunctionLibrary.FUNC_SIGN, 1)
        {
        }

        public override Cell Evaluate(params Cell[] Data)
        {
            return Cell.Sign(Data[0]);
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
            : base(BaseFunctionLibrary.HASH_MD5, new MD5CryptoServiceProvider())
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
            : base(BaseFunctionLibrary.HASH_SHA1, new SHA1CryptoServiceProvider())
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
            : base(BaseFunctionLibrary.HASH_SHA256, new SHA256CryptoServiceProvider())
        {
        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return 32; // sha256 has a hash size of 32 bytes
        }

    }

    public sealed class CellFuncBASH : CellFuncFixedKnown
    {

        public CellFuncBASH()
            : base(BaseFunctionLibrary.HASH_BASH, -1, CellAffinity.BLOB)
        {
        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return Sizes.Sum();
        }

        public override Cell Evaluate(Cell[] Data)
        {
            List<byte> bash = new List<byte>();
            foreach (Cell c in Data)
            {

                foreach (byte b in c.valueBLOB)
                {
                    bash.Add(b);
                }

            }
            return new Cell(bash.ToArray());

        }

    }

    public sealed class CellFuncLASH : CellFuncFixedKnown
    {

        public CellFuncLASH()
            : base(BaseFunctionLibrary.HASH_LASH, -1, CellAffinity.INT)
        {
        }

        public override int ReturnSize(CellAffinity Type, params int[] Sizes)
        {
            return 8;
        }

        public override Cell Evaluate(Cell[] Data)
        {

            long l = 0, i = 0;
            foreach (Cell c in Data)
            {
                i++;
                l += i * c.LASH;
            }
            return new Cell(l);

        }


    }

    #endregion

    // Special //
    #region SpecialFunctions

    public sealed class CellDateBuild : CellFuncFixedKnown
    {

        public CellDateBuild()
            : base(BaseFunctionLibrary.SPECIAL_DATE_BUILD, -1, CellAffinity.DATE_TIME)
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
            : base(BaseFunctionLibrary.SPECIAL_IF, 3, false)
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
            : base(BaseFunctionLibrary.SPECIAL_CASE, -1, null, CellFunction.FuncHierStandard(), null)
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
            : base(BaseFunctionLibrary.MUTABLE_RAND, -1, true)
        {
            this.IsVolatile = true;
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
            : base(BaseFunctionLibrary.MUTABLE_RANDINT, -1, true)
        {
            this.IsVolatile = true;
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
        /// <param name="ShartTable"></param>
        /// <param name="WEIGHT"></param>
        /// <returns></returns>
        internal static Record Univariate(IEnumerable<Cell> Data, IEnumerable<Cell> Weight)
        {

            // If the counts are different, then throw an exceptions //
            if (Data.Count() != Weight.Count())
                throw new Exception(string.Format("WEIGHT and ShartTable have different lengths {0} : {1}", Weight.Count(), Data.Count()));

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
                throw new Exception(string.Format("WEIGHT and ShartTable have different lengths {0} : {1} : {2}", Weight.Count(), XData.Count(), YData.Count()));

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
            : base(BaseFunctionLibrary.MUTABLE_MSUM)
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
            : base(BaseFunctionLibrary.MUTABLE_MAVG)
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
            : base(BaseFunctionLibrary.MUTABLE_MVAR)
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
            : base(BaseFunctionLibrary.MUTABLE_MSTDEV)
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
            : base(BaseFunctionLibrary.MUTABLE_MCOVAR)
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
            : base(BaseFunctionLibrary.MUTABLE_MCORR)
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
            : base(BaseFunctionLibrary.MUTABLE_MINTERCEPT)
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
            : base(BaseFunctionLibrary.MUTABLE_MSLOPE)
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
            : base(BaseFunctionLibrary.VOLATILE_GUID, 0, true)
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
            : base(BaseFunctionLibrary.VOLATILE_TICKS, 0, true)
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
            : base(BaseFunctionLibrary.VOLATILE_NOW, 0, true)
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
            : base(BaseFunctionLibrary.FUNC_AND_MANY, -1, false)
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
            : base(BaseFunctionLibrary.FUNC_OR_MANY, -1, false)
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
            : base(BaseFunctionLibrary.FUNC_ADD_MANY, -1, false)
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
            : base(BaseFunctionLibrary.FUNC_PRODUCT_MANY, -1, false)
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

}
