using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Query;

namespace Rye.Mining
{

    public enum MiningModelAffinity
    {
        Supervised,
        Unsupervised
    }

    /// <summary>
    /// Holds all the needed input data for a mining model to opperate
    /// </summary>
    public sealed class MiningModelSource
    {

        private MiningModelAffinity _Affinity;
        private Volume _Source;
        private ExpressionCollection _Inputs;
        private ExpressionCollection _Outputs; // Will be null if affinity is Unsupervised
        private Expression _Weight;
        private Filter _Where;
        private Register _Memory;
        private Lambda _Lambda;

        // Constructors //
        public MiningModelSource(MiningModelAffinity Affinity, Volume Source, Register Memory, ExpressionCollection Inputs, ExpressionCollection Outputs, 
            Expression Weight, Filter Where, Lambda Func)
        {

            /* Default some items
             * -- Outputs become empty if null
             * -- Weight becomes '1' if null
             * -- Filter becomes true for all if null
             * 
             */

            // Fix null outputs //
            if (Outputs == null)
                Outputs = new ExpressionCollection();

            // Fix weight //
            if (Weight == null)
                Weight = new ExpressionValue(null, new Cell(1d));

            // Fix filter //
            if (Where == null)
                Where = Filter.TrueForAll;

            // Check that if we're supervised, we've got a non-null, non-empty output //
            if (Affinity == MiningModelAffinity.Supervised && Outputs.Count == 0)
                throw new ArgumentException("For supervised mining models, the output must be non-empty");

            // Assign all variables //
            this._Affinity = Affinity;
            this._Source = Source;
            this._Memory = Memory;
            this._Inputs = Inputs;
            this._Outputs = Outputs;
            this._Weight = Weight;
            this._Where = Where;
            this._Lambda = Func;

            // Go though and make sure all vairables are linked together //
            CloneFactory emrc = new CloneFactory();
            emrc.Append(this._Memory);
            emrc.Extract(this._Inputs);
            emrc.Extract(this._Outputs);
            emrc.Extract(this._Weight);
            emrc.Extract(this._Where);
            emrc.Link(this._Inputs);
            emrc.Link(this._Outputs);
            emrc.Link(this._Weight);
            emrc.Link(this._Where);

        }

        // Properties //
        public MiningModelAffinity Affinity
        {
            get { return this._Affinity; }
        }

        public Volume Source
        {
            get { return this._Source; }
        }

        public ExpressionCollection Inputs
        {
            get { return this._Inputs; }
        }

        public ExpressionCollection Outputs
        {
            get { return this._Outputs; }
        }

        public Expression Weight
        {
            get { return this._Weight; }
        }

        public Filter Where
        {
            get { return this._Where; }
        }

        public Register Memory
        {
            get { return this._Memory; }
        }

        public Lambda Lambda
        {
            get { return this._Lambda; }
        }

    }

}
