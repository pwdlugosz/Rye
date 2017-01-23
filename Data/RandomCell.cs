using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Expressions;

namespace Rye.Data
{

    public class RandomCell
    {

        private static string ASCIIPrintable = " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
        private static string ASCIIPrintableNoSpace = "!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
        private static string UpperLowerNumText = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        private static string UpperNumText = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        private static string LowerNumText = "0123456789abcdefghijklmnopqrstuvwxyz";
        private static string UpperText = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        private static string LowerText = "abcdefghijklmnopqrstuvwxyz";
        private static string Num = "0123456789";

        protected Random _base;
        private object _lock;

        public RandomCell(int Seed)
        {
            this._base = new Random(Seed);
            this._lock = new object();
        }

        public RandomCell()
            : this(RandomCell.TruelyRandomSeed())
        {
        }

        // ### THREAD SAFE ###
        public void Remix(int Seed)
        {

            lock (this._lock)
            {
                this._base = new Random(Seed);
            }

        }

        public void Remix()
        {
            this.Remix(RandomCell.TruelyRandomSeed());
        }

        // Booleans //
        public Cell NextBool()
        {
            return new Cell(this.BaseDouble() < 0.50);
        }

        public Cell NextBool(double Likelyhood)
        {
            return new Cell(this.BaseDouble() < Likelyhood);
        }

        // Integers //
        public Cell NextLong()
        {
            return new Cell(this.BaseLong());
        }

        public Cell NextLong(long Lower, long Upper)
        {
            int t = this._base.Next((int)Lower, (int)Upper);
            return new Cell(t);
        }

        public Cell NextLongPrime()
        {
            return new Cell(this.NextPrimeBase());
        }

        // Dates //
        public Cell NextDate()
        {

            byte[] x = this.ByteArrayBase(4);

            DateTime y = DateTime.Now;

            int year = (int)BitConverter.ToUInt16(x, 0) % y.Year;

            int month = (int)x[2] % 12 + 1;

            int divisor = 31;

            bool isLeap = (year % 4 == 0);

            if (year % 100 == 0)
                isLeap = false;

            if (year % 400 == 0)
                isLeap = true;

            if (isLeap && month == 2)
                divisor = 29;

            if (month == 4 || month == 6 || month == 9 || month == 11)
                divisor = 30;

            int day = x[3] % divisor + 1;

            DateTime z = new DateTime(year, month, day);

            return new Cell(z);

        }

        public Cell NextDate(DateTime Lower, DateTime Upper)
        {

            long span = (Upper.Ticks - Lower.Ticks) / TimeSpan.TicksPerDay;
            long t = this.BaseLong() % span;
            DateTime x = new DateTime(t * TimeSpan.TicksPerDay + Lower.Ticks);
            return new Cell(x);

        }

        // Doubles //
        public Cell NextDouble()
        {
            return new Cell(this.BaseDouble());
        }

        public Cell NextDouble(double Lower, double Upper)
        {
            double d = (Upper - Lower) * this.BaseDouble() + Lower;
            return new Cell(d);
        }

        public Cell NextDoubleGauss()
        {
            double u = this.BaseDouble();
            double v = this.BaseDouble();
            double x = Math.Sqrt(-Math.Log(u) * 2D) * Math.Cos(2D * v * Math.PI);
            return new Cell(x);
        }

        // Strings //
        public Cell NextUTF16String(int Len)
        {
            return this.NextString(Len, char.MaxValue);
        }

        public Cell NextUTF8String(int Len)
        {
            return this.NextString(Len, 255);
        }

        public Cell NextUTF7String(int Len)
        {
            return this.NextString(Len, 127);
        }

        public Cell NextStringASCIIPrintable(int Len)
        {
            return this.NextString(Len, ASCIIPrintable);
        }

        public Cell NextStringASCIIPrintableNoSpace(int Len)
        {
            return this.NextString(Len, ASCIIPrintableNoSpace);
        }

        public Cell NextStringUpperLowerNumText(int Len)
        {
            return this.NextString(Len, UpperLowerNumText);
        }

        public Cell NextStringUpperNumText(int Len)
        {
            return this.NextString(Len, UpperNumText);
        }

        public Cell NextStringLowerNumText(int Len)
        {
            return this.NextString(Len, LowerNumText);
        }

        public Cell NextStringUpperText(int Len)
        {
            return this.NextString(Len, UpperText);
        }

        public Cell NextStringLowerText(int Len)
        {
            return this.NextString(Len, LowerText);
        }

        public Cell NextStringNum(int Len)
        {
            return this.NextString(Len, Num);
        }
        
        public Cell NextString(int Len, string Corpus)
        {

            if (Len <= 0)
                return Cell.NULL_STRING;

            Len = Len % Schema.DEFAULT_STRING_SIZE;

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < Len; i++)
            {
                int idx = this.BaseInt() % Corpus.Length;
                sb.Append(Corpus[idx]);
            }

            return new Cell(sb.ToString());

        }

        public Cell NextString(int Len, int Max)
        {

            if (Len <= 0)
                return Cell.NULL_STRING;

            Len = Len % Schema.DEFAULT_STRING_SIZE;

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < Len; i++)
            {
                char c = (char)(this.BaseInt() % Max);
                sb.Append(c);
            }

            return new Cell(sb.ToString());

        }

        // BLOBs //
        public Cell NextBLOB(int Len)
        {

            if (Len <= 0)
                return Cell.NULL_BLOB;

            Len = Len % Schema.DEFAULT_BLOB_SIZE;

            byte[] b = new byte[Len];
            this._base.NextBytes(b);

            return new Cell(b);

        }

        // ### THREAD SAFE ###
        private int BaseInt()
        {

            lock (this._lock)
            {
                return this._base.Next();
            }

        }

        // ### THREAD SAFE ###
        private long BaseLong()
        {

            lock (this._lock)
            {
                return (long)(this._base.Next());
            }
            
        }

        // ### THREAD SAFE ###
        private long BaseLong(long Lower, long Upper)
        {
            return (long)(this._base.Next((int)(Lower & int.MaxValue), (int)(Upper & int.MaxValue)));
        }

        // ### THREAD SAFE ###
        private double BaseDouble()
        {

            lock (this._lock)
            {
                return this._base.NextDouble();
            }

        }

        // ### THREAD SAFE ###
        private byte[] ByteArrayBase(int Len)
        {

            lock (this._lock)
            {
                byte[] b = new byte[Len];
                this._base.NextBytes(b);
                return b;
            }

        }

        private long NextPrimeBase()
        {

            long x = this.BaseLong();
            if (x < 0) x = -x;

            while (!RandomCell.IsPrime(x))
            {
                x = this.BaseLong();
                if (x < 0) x = -x;
            }

            return x;

        }

        // Statics //
        public static int TruelyRandomSeed()
        {

            byte[] a = Guid.NewGuid().ToByteArray();
            
            int b = System.Threading.Thread.CurrentThread.ManagedThreadId;
            byte[] c = BitConverter.GetBytes(b * b * b * b);

            int d = ((short)DateTime.Now.Ticks % short.MaxValue);
            byte[] e = BitConverter.GetBytes(d * d);

            ushort f = 0;

            while (f == 0)
            {
                f = (ushort)(BitConverter.ToUInt16(Guid.NewGuid().ToByteArray(), 7) ^ (ushort.MaxValue));
            }

            byte[] g = new byte[24];
            Array.Copy(a, g, 16);
            Array.Copy(c, 0, g, 16, 4);
            Array.Copy(e, 0, g, 20, 4);

            using (System.Security.Cryptography.SHA1Managed sha1 = new System.Security.Cryptography.SHA1Managed())
            {

                for (UInt16 h = 0; h < f; h++)
                {
                    g = sha1.ComputeHash(g);
                }

            }

            return BitConverter.ToInt32(g, 9);

        }

        public static bool IsPrime(long Value)
        {

            if (Value <= 1)
                return false;

            if (Value < 6)
                return (Value == 2 || Value == 3 || Value == 5) ? true : false;

            if (((Value + 1) % 6 != 0) && ((Value - 1) % 6 != 0))
                return false;

            for (long i = 2; i <= (long)Math.Sqrt(Value) + 1; i++)
                if (Value % i == 0)
                    return false;
            return true;

        }

    }


}
