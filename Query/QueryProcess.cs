﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Query
{

    public enum QueryNodeState
    {
        NotStarted,
        Running,
        Finished
    }

    public abstract class QueryNode
    {

        
        protected int _ThreadID;
        
        public QueryNode(int ThreadID)
        {
            this._ThreadID = ThreadID;
        }

        public virtual void BeginInvoke()
        {
            
        }

        public virtual void EndInvoke()
        {
        }

        public abstract void Invoke();
    }

    public abstract class QueryConsolidation<Q> where Q : QueryNode
    {

        public QueryConsolidation()
        {
        }

        public abstract void Consolidate(List<Q> Nodes);

    }

    public sealed class QueryProcess<Q> where Q : QueryNode
    {

        private int _ThreadCount;
        private List<Q> _Nodes;
        private QueryConsolidation<Q> _Consolidator;

        public QueryProcess(List<Q> Nodes, QueryConsolidation<Q> Consolidator)
        {
            this._ThreadCount = Nodes.Count;
            this._Consolidator = Consolidator;
            this._Nodes = Nodes;
        }

        public void Execute()
        {

            foreach (Q q in this._Nodes)
            {
                q.BeginInvoke();
                q.Invoke(); // main drag happening here
                q.EndInvoke();
            }

            this._Consolidator.Consolidate(this._Nodes);

        }

        public void ExecuteAsync()
        {

            List<Task> t = new List<Task>();
            foreach (Q q in this._Nodes)
            {
                q.BeginInvoke();
                Task x = new Task(q.Invoke);
                t.Add(x);
            }

            // Kick off the run //
            t.ForEach((x) => 
                { 
                    x.Start(); 
                }
            );
            
            // Tell each to wait //
            t.ForEach((x) => 
                { 
                    x.Wait(); 
                }
            );

            // End the invoke //
            foreach (Q q in this._Nodes)
            {
                q.EndInvoke();
            }

            this._Consolidator.Consolidate(this._Nodes);

        }

    }

    //public sealed class PartitionedStream
    //{

    //    private DataSet _Source;
    //    private int _ThreadID;
    //    private int _ThreadCount;
    //    private IEnumerator<Extent> _extents;
    //    private bool _EndOfStream = false;

    //    public PartitionedStream(DataSet Source, int ThreadCount, int ThreadID)
    //    {

    //        this._Source = Source;
    //        this._ThreadCount = ThreadCount;
    //        this._ThreadID = ThreadID;
    //        this._extents = Source.ThreadedExtents(ThreadID, ThreadCount).GetEnumerator();

    //    }

    //    public void Advance()
    //    {
    //        this._EndOfStream = !this._extents.MoveNext();
    //    }

    //    public bool EndOfStream
    //    {
    //        get { return this._EndOfStream; }
    //    }

    //    public Extent Current
    //    {
    //        get 
    //        { 
    //            return this._extents.Current; 
    //        }
    //    }

    //}

}