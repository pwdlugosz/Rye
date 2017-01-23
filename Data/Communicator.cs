using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data
{


    public abstract class Communicator
    {

        public string BREAKER_LINE = "----------------------------------------------------------------------";
        public string HEADER_LINE = "----------------------------------- {0} -----------------------------------";
        public string NEW_LINE = "\n";

        public Communicator()
        {
        }

        public bool Supress
        {
            get;
            set;
        }

        public abstract void Write(string Message, params object[] Paramters);

        public virtual void WriteLine(string Message, params object[] Parameters)
        {
            this.Write(Message + "\n", Parameters);
        }

        public virtual void WriteLine(string Message)
        {
            this.WriteLine(Message);
        }

        public virtual void WriteLine()
        {
            this.Write(NEW_LINE);
        }

        public virtual void WriteBreaker()
        {
            this.WriteLine(BREAKER_LINE);
        }

        public virtual void WriteHeader(string Message)
        {
            this.WriteLine(BlankHeader(Message.Length), Message);
        }

        public static string BlankHeader(int StringSize)
        {
            int len = (StringSize > 70 ? 1 : (70 - StringSize) / 2 + 1);

            string s = new string('-', len);
            return s + "{0}" + s;
        }

        public abstract void ShutDown();

    }

    public sealed class CommandLineCommunicator : Communicator
    {

        public CommandLineCommunicator()
            : base()
        {
        }

        public override void Write(string Message, params object[] Paramters)
        {

            if (this.Supress)
                return;
            Console.Write(Message, Paramters);

        }

        public override void WriteLine(string Message)
        {

            if (this.Supress)
                return;
            Console.WriteLine(Message);

        }

        public override void ShutDown()
        {

        }

    }

    public sealed class FileCommunicator : Communicator
    {

        private System.IO.StreamWriter _FileLog;

        public FileCommunicator(string Path)
            : base()
        {
            this._FileLog = new System.IO.StreamWriter(Path, false);
        }

        public FileCommunicator()
            : this(FileCommunicator.RandomLogFile())
        {
        }

        public override void Write(string Message, params object[] Paramters)
        {
            if (this.Supress)
                return;
            this._FileLog.WriteLine(Message, Paramters);
        }

        public override void WriteLine(string Message)
        {
            if (this.Supress)
                return;
            this._FileLog.WriteLine(Message);
        }

        public override void ShutDown()
        {
            this._FileLog.Flush();
            this._FileLog.Close();
        }

        public static string RandomLogFile()
        {

            string Dir = Kernel.RyeLogDir;
            DateTime now = DateTime.Now;
            string Name = string.Format("Log_{0}{1}{2}_{3}{4}{5}.txt", now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Millisecond);
            return Dir + Name;

        }

    }

    public sealed class HybridCommunicator : Communicator
    {

        private System.IO.StreamWriter _FileLog;

        public HybridCommunicator(string Path)
            : base()
        {
            this._FileLog = new System.IO.StreamWriter(Path, false);
        }

        public HybridCommunicator()
            : this(HybridCommunicator.RandomLogFile())
        {
        }

        public override void Write(string Message, params object[] Paramters)
        {
            if (this.Supress)
                return;
            Console.Write(Message, Paramters);
            this._FileLog.WriteLine(Message, Paramters);
        }

        public override void WriteLine(string Message)
        {
            if (this.Supress)
                return;
            Console.WriteLine(Message);
            this._FileLog.WriteLine(Message);
        }

        public override void ShutDown()
        {
            this._FileLog.Flush();
            this._FileLog.Close();
        }

        public static string RandomLogFile()
        {

            string Dir = Kernel.RyeLogDir;
            DateTime now = DateTime.Now;
            string Name = string.Format("Log_{0}{1}{2}_{3}{4}{5}.txt", now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Millisecond);
            return Dir + Name;

        }

    }



}
