using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Methods;

namespace Rye.Libraries
{

    public sealed class BaseMethodLibrary : MethodLibrary
    {

        public const string LIBRARY_NAME = "SYSTEM";

        // Main //
        public BaseMethodLibrary(Session Session)
            : base(Session)
        {
            this.LibName = LIBRARY_NAME;
        }

        // Methods //
        private string[] _MethodNames = new string[]
        {
        };

        public override Method RenderMethod(Method Parent, string Name, ParameterCollection Parameters)
        {
            throw new NotImplementedException();
        }

        public override ParameterCollectionSigniture RenderSigniture(string Name)
        {
            throw new NotImplementedException();
        }

        public override string[] Names
        {
            get { return this._MethodNames; }
        }

    }

    public sealed class BaseFunctionLibrary : FunctionLibrary
    {

        public const string LIBRARY_NAME = "BASE";

        // Main //
        public BaseFunctionLibrary(Session Session)
            : base(Session)
        {
            this.LibName = LIBRARY_NAME;
        }

        // Functions //
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
        public const string FUNC_TRIM = "trim";
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
        public const string FUNC_MODPOW = "modpow";

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
        public const string FUNC_ABS = "abs";
        public const string FUNC_SIGN = "sign";

        public const string FUNC_IF_NULL = "ifnull";
        public const string FUNC_AND = "and";
        public const string FUNC_OR = "or";
        public const string FUNC_XOR = "xor";

        public const string HASH_MD5 = "md5";
        public const string HASH_SHA1 = "sha1";
        public const string HASH_SHA256 = "sha256";
        public const string HASH_BASH = "bash";
        public const string HASH_LASH = "lash";

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
        public const string SPECIAL_CASE = "case";

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
            FUNC_TRIM,
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
            FUNC_MODPOW,

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
            FUNC_ABS,
            FUNC_SIGN,

            FUNC_IF_NULL,
            FUNC_AND,
            FUNC_OR,
            FUNC_XOR,

            HASH_MD5,
            HASH_SHA1,
            HASH_SHA256,
            HASH_BASH,
            HASH_LASH,

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
                case FUNC_TRIM: return new CellFuncFKTrim();
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
                case FUNC_MODPOW: return new CellFuncFKModPow();

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
                case FUNC_ABS: return new CellFuncFVAbs();
                case FUNC_SIGN: return new CellFuncFVSign();

                case SPECIAL_IF: return new CellFuncIf();
                case SPECIAL_DATE_BUILD: return new CellDateBuild();

                case HASH_MD5: return new CellFuncCHMD5();
                case HASH_SHA1: return new CellFuncCHSHA1();
                case HASH_SHA256: return new CellFuncCHSHA256();
                case HASH_BASH: return new CellFuncBASH();
                case HASH_LASH: return new CellFuncLASH();

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
            get { return BaseFunctionLibrary._BaseNames; }
        }


    }


}
