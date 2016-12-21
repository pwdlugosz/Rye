using System;
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
        protected Session _Session;
        
        public QueryNode(int ThreadID, Session Session)
        {
            this._ThreadID = ThreadID;
            this._Session = Session;
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

        protected Session _Session;

        public QueryConsolidation(Session Session)
        {
            this._Session = Session;
        }

        public abstract void Consolidate(List<Q> Nodes);

    }

    public sealed class QueryProcess<Q> where Q : QueryNode
    {

        private int _ThreadCount;
        private List<Q> _Nodes;
        private QueryConsolidation<Q> _Consolidator;
        private List<PreProcessor> _PreProcessorNodes;
        private Methods.MethodDo _PreProcessor;
        private Methods.MethodDo _PostProcessor;
        
        public QueryProcess(List<Q> Nodes, QueryConsolidation<Q> Consolidator)
        {
            
            this._ThreadCount = Nodes.Count;
            this._Consolidator = Consolidator;
            this._Nodes = Nodes;
            this._PreProcessorNodes = new List<PreProcessor>();
            this._PreProcessor = new Methods.MethodDo(null);
            this._PostProcessor = new Methods.MethodDo(null);

        }

        public long PreProcessorClicks
        {
            get
            {
                return this._PreProcessorNodes.Sum((x) => { return x.Clicks; });
            }
        }

        public void Execute()
        {

            // Preprocessor //
            this.RunPreProcessor();

            // Nodes //
            this.RunNodes();

            // Consolidate //
            this.RunConsolidator();

            // Post processor //
            this.RunPostProcessor();

        }

        public void ExecuteAsync()
        {

            // Preprocessor //
            this.RunPreProcessor();

            // Nodes //
            this.RunNodesAsync();
            
            // Consolidate //
            this.RunConsolidator();

            // Post processor //
            this.RunPostProcessor();

        }

        public Methods.Method PreProcessor
        {
            get 
            { 
                return this._PreProcessor; 
            }
        }

        public Methods.Method PostProcessor
        {
            get
            {
                return this._PostProcessor;
            }
        }

        // Private methods //
        public void RunPreProcessor()
        {
            this._PreProcessor.BeginInvoke();
            this._PreProcessor.Invoke();
            this._PreProcessor.EndInvoke();
        }

        private void RunNodes()
        {

            // Render Each node, only on one thread //
            foreach (Q q in this._Nodes)
            {
                q.BeginInvoke();
                q.Invoke(); // current drag happening here
                q.EndInvoke();
            }

        }

        private void RunNodesAsync()
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

        }

        private void RunConsolidator()
        {

            // Run the consolidation process //
            this._Consolidator.Consolidate(this._Nodes);
        }

        private void RunPostProcessor()
        {
            this._PostProcessor.BeginInvoke();
            this._PostProcessor.Invoke();
            this._PostProcessor.EndInvoke();
        }

    }

}
