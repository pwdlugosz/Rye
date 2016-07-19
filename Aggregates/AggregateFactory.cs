using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Aggregates
{


    public static class CellReductions
    {

        public static Aggregate Average(Expression M)
        {
            return new AggregateAverage(M);
        }

        public static Aggregate Average(Expression M, Expression W)
        {
            return new AggregateAverage(M, W);
        }

        public static Aggregate Correl(Expression M, Expression N)
        {
            return new AggregateCorrelation(M, N);
        }

        public static Aggregate Correl(Expression M, Expression N, Expression W)
        {
            return new AggregateCorrelation(M, N, W);
        }

        public static Aggregate Count(Expression M)
        {
            return new AggregateCount(M);
        }

        public static Aggregate CountAll()
        {
            return new AggregateCountAll(new ExpressionValue(null, new Cell((long)0)));
        }

        public static Aggregate CountNull(Expression M)
        {
            return new AggregateCountNull(M);
        }

        public static Aggregate Covar(Expression M, Expression N)
        {
            return new AggregateCovariance(M, N);
        }

        public static Aggregate Covar(Expression M, Expression N, Expression W)
        {
            return new AggregateCovariance(M, N, W);
        }

        public static Aggregate Frequency(Filter P)
        {
            return new AggregateFreq(P);
        }

        public static Aggregate Frequency(Filter P, Expression W)
        {
            return new AggregateFreq(W, P);
        }

        public static Aggregate Intercept(Expression M, Expression N)
        {
            return new AggregateIntercept(M, N);
        }

        public static Aggregate Intercept(Expression M, Expression N, Expression W)
        {
            return new AggregateIntercept(M, N, W);
        }

        public static Aggregate Max(Expression M)
        {
            return new AggregateMax(M);
        }

        public static Aggregate Min(Expression M)
        {
            return new AggregateMin(M);
        }

        public static Aggregate Slope(Expression M, Expression N)
        {
            return new AggregateSlope(M, N);
        }

        public static Aggregate Slope(Expression M, Expression N, Expression W)
        {
            return new AggregateSlope(M, N, W);
        }

        public static Aggregate Stdev(Expression M)
        {
            return new AggregateStdevP(M);
        }

        public static Aggregate Stdev(Expression M, Expression W)
        {
            return new AggregateStdevP(M, W);
        }

        public static Aggregate Sum(Expression M)
        {
            return new AggregateSum(M);
        }

        public static Aggregate Var(Expression M)
        {
            return new AggregateVarianceP(M);
        }

        public static Aggregate Var(Expression M, Expression W)
        {
            return new AggregateVarianceP(M, W);
        }

    }


}
