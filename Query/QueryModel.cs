using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using Rye.Data;

namespace Rye.Query
{
    
    public abstract class QueryModel
    {

        protected Session _Session;
        protected Stopwatch _Timer;
        protected int _Threads = 1;
        protected StringBuilder _Message;

        // Instance Methods //
        public QueryModel(Session Session)
        {
            this._Session = Session;
            this._Timer = new Stopwatch();
            this._Threads = 1;
            this._Message = new StringBuilder();
        }

        public TimeSpan RunTime
        {
            get
            {
                return this._Timer.Elapsed;
            }
        }

        public Session Enviroment
        {
            get
            {
                return this._Session;
            }
        }

        public int ThreadCount
        {
            
            get 
            { 
                return this._Threads; 
            }
            set
            {
                this._Threads = Math.Min(this._Session.MaxThreadCount, Math.Max(1, value));
            }

        }

        // Abstract Methods //
        /// <summary>
        /// Executes the process over the internal thread count
        /// </summary>
        public virtual void Execute()
        {

            if (this._Threads == 1 || !this._Session.AllowConcurrent)
                this.ExecuteAsynchronous();
            else
                this.ExecuteConcurrent(this._Threads);

        }

        /// <summary>
        /// Executes the process over multiple threads
        /// </summary>
        /// <param name="ThreadCount">The desired thread count</param>
        public abstract void ExecuteConcurrent(int ThreadCount);

        /// <summary>
        /// Executes the process over a single thread
        /// </summary>
        public abstract void ExecuteAsynchronous();

        /// <summary>
        /// Gets meta data about the process
        /// </summary>
        /// <returns></returns>
        public virtual string ResponseString()
        {
            return this._Message.ToString();
        }

    }

}
