using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Mining
{

    public enum MiningModelAffinity
    {
        Supervised,
        Unsupervised
    }

    public sealed class MiningModelSource
    {

        private MiningModelAffinity _Affinity;
        private Volume _Source;
        private ExpressionCollection _Inputs;
        private ExpressionCollection _Outputs; // Will be null if affinity is Unsupervised
        private Expression _Where;
        private Expression _Weight;
        private Register _Memory;
        private Lambda _Lambda;

        public MiningModelSource(MiningModelAffinity Affinity, Volume Source, Register Memory, ExpressionCollection Inputs, ExpressionCollection Outputs, Filter Where, Expression Weight, Lambda Func)
        {

        }

    }

}
