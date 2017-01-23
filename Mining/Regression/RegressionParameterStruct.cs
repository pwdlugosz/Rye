using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Structures;
using Rye.Query;

namespace Rye.Mining.Regression
{

    /// <summary>
    /// Holds regression parameters
    /// </summary>
    public sealed class RegressionParameterStruct
    {

        internal CellMatrix _XtX;
        internal CellMatrix _XtY;
        internal CellMatrix _Beta;
        internal Cell _WSum;
        internal Cell _Obs;

        public RegressionParameterStruct(int ParameterCount, int ResponseCount)
        {
            this._XtX = new CellMatrix(ParameterCount, ParameterCount, Cell.ZeroValue(CellAffinity.DOUBLE));
            this._XtY = new CellMatrix(ParameterCount, ResponseCount, Cell.ZeroValue(CellAffinity.DOUBLE));
            this._Beta = new CellMatrix(ParameterCount, ResponseCount, Cell.ZeroValue(CellAffinity.DOUBLE));
            this._WSum = Cell.ZeroValue(CellAffinity.DOUBLE);
            this._Obs = Cell.ZeroValue(CellAffinity.DOUBLE);
        }

        public RegressionParameterStruct(int ParameterCount)
            : this(ParameterCount, 1)
        {
        }

        public RegressionParameterStruct(MiningModelSource Source)
            : this(Source.Inputs.Count, Source.Outputs.Count)
        {
        }

        public int ParameterCount
        {
            get { return this._XtY.RowCount; }
        }

        public int ResponseCount
        {
            get { return this._XtY.ColumnCount; }
        }

        public CellMatrix XtX
        {
            get { return this._XtX; }
        }

        public CellMatrix XtY
        {
            get { return this._XtY; }
        }

        public CellMatrix Beta
        {
            get { return this._Beta; }
        }

        public Cell WSum
        {
            get { return this._WSum; }
        }

        public CellMatrix XtXInverse
        {
            get { return this._XtX.Inverse; }
        }

        public void StepWeight(Cell Weight)
        {
            this._WSum += Weight;
        }

        public void StepWeight()
        {
            this._WSum++;
        }

        public void StepObsCount()
        {
            this._Obs++;
        }

        public void RenderBeta()
        {
            this._Beta = this.XtX.Inverse ^ this._XtY;
        }

        public void RandomizeBeta(int Seed)
        {
            
            RandomCell rng = new RandomCell(Seed);
            for (int i = 0; i < this._Beta.RowCount; i++)
            {

                for (int j = 0; j < this._Beta.ColumnCount; j++)
                {
                    this._Beta[i, j] = rng.NextDoubleGauss();
                }

            }

        }

        /// <summary>
        /// Merges the XtX, XtY and WSum variables of two regression model structures; Order doesnt matter; this will not calculate the beta Value
        /// </summary>
        /// <param name="A">First structure</param>
        /// <param name="B">Second structure</param>
        /// <returns>The merged regression parameter structure</returns>
        public static RegressionParameterStruct Merge(RegressionParameterStruct A, RegressionParameterStruct B)
        {

            if (A.ParameterCount != B.ParameterCount || A.ResponseCount != B.ResponseCount)
            {
                throw new ArgumentException("Both RegressionParameterStruct objects must have the same parameter and response counts");
            }

            RegressionParameterStruct C = new RegressionParameterStruct(A.ParameterCount, A.ResponseCount);

            C._XtX = A._XtX + B._XtX;
            C._XtY = A._XtY + B._XtY;
            C._WSum = A._WSum + B._WSum;

            return C;

        }

        /// <summary>
        /// Merges many regression parameter structures into one
        /// </summary>
        /// <param name="Parameters">The collection of individual strucs</param>
        /// <returns>The combined struct</returns>
        public static RegressionParameterStruct Merge(IEnumerable<RegressionParameterStruct> Parameters)
        {

            RegressionParameterStruct[] p = Parameters.ToArray();
            
            if (p.Length == 0)
                return null;

            if (p.Length == 1)
                return p[0];

            RegressionParameterStruct prime = p[0];

            for (int i = 1; i < p.Length; i++)
            {
                prime = RegressionParameterStruct.Merge(prime, p[i]);
            }

            return prime;
            
        }

        public static double BetaChange(RegressionParameterStruct A, RegressionParameterStruct B)
        {

            if (A.ParameterCount != B.ParameterCount || A.ResponseCount != B.ResponseCount)
                throw new ArgumentException("Parameter structures must have the same parameter and response counts");

            double sse = 0;

            for (int i = 0; i < A.ParameterCount; i++)
            {

                for (int j = 0; j < A.ResponseCount; j++)
                {
                    sse += Math.Pow(A.Beta[i, j].valueDOUBLE - B.Beta[i, j].valueDOUBLE, 2D);
                }

            }

            return Math.Sqrt(sse);

        }

    }

    /// <summary>
    /// Holds statistics surrounding the beta structure
    /// </summary>
    public sealed class RegressionBetaStruct
    {

        internal CellMatrix _Beta;
        internal CellMatrix _BetaUpper95;
        internal CellMatrix _BetaLower95;
        internal CellMatrix _BetaVariance;
        internal CellMatrix _BetaBetaStandardDeviation;
        internal CellMatrix _BetaPValue;

        public RegressionBetaStruct(int ParameterCount, int ResponseCount)
        {

            Cell x = new Cell(0D);
            this._Beta = new CellMatrix(ParameterCount, ResponseCount, x);
            this._BetaLower95 = new CellMatrix(ParameterCount, ResponseCount, x);
            this._BetaUpper95 = new CellMatrix(ParameterCount, ResponseCount, x);
            this._BetaVariance = new CellMatrix(ParameterCount, ResponseCount, x);
            this._BetaBetaStandardDeviation = new CellMatrix(ParameterCount, ResponseCount, x);
            this._BetaPValue = new CellMatrix(ParameterCount, ResponseCount, x);

        }

    }

    /// <summary>
    /// Represents regression model dispersion
    /// </summary>
    public interface IDispersion
    {
        
        double SSE { get; }

        double Phi { get; }

    }

    // Algorithms //
    public static class RegressionModelHelper
    {

        /// <summary>
        /// Represents the absolute value of the 97.5th percentile of the normal distribution; this is used rather than 95% since it's symetrical
        /// </summary>
        public static Cell NORM95 = new Cell(1.965);

        /// <summary>
        /// Given a source and a beta, generates an expression of the form OriginalNode[0] * beta[0] + ... + OriginalNode[n] * beta[n]
        /// </summary>
        /// <param name="Source">The mining model input</param>
        /// <param name="Parameters">The parameter construct</param>
        /// <returns></returns>
        public static Expression LinearPredictor(MiningModelSource Source, RegressionParameterStruct Parameters, int ResponseIndex)
        {

            if (Source.Inputs.Count != Parameters.ParameterCount || Source.Outputs.Count != Parameters.ResponseCount)
                throw new ArgumentException("Inputs and parameters must be the same size; Outputs and responses must have the same count");

            // Handle the case of one parameter //
            if (Parameters.ParameterCount == 1)
                return Source.Inputs[0] * new ExpressionValue(null, Parameters.Beta[0, ResponseIndex]);

            // Handle the case of two or more parameters //
            Expression x = Source.Inputs[0] * new ExpressionValue(null, Parameters.Beta[0, ResponseIndex]);

            for (int i = 1; i < Parameters.ParameterCount; i++)
            {
                x = x + Source.Inputs[i] * new ExpressionValue(null, Parameters.Beta[i, ResponseIndex]);
            }

            return x;

        }

        /// <summary>
        /// Returns an expression collection representing all the linear predictors
        /// </summary>
        /// <param name="Source"></param>
        /// <param name="Parameters"></param>
        /// <returns></returns>
        public static ExpressionCollection LinearPredictor(MiningModelSource Source, RegressionParameterStruct Parameters)
        {
            
            if (Source.Inputs.Count != Parameters.ParameterCount || Source.Outputs.Count != Parameters.ResponseCount)
                throw new ArgumentException("Inputs and parameters must be the same size; Outputs and responses must have the same count");

            ExpressionCollection x = new ExpressionCollection();

            for (int i = 0; i < Source.Outputs.Count; i++)
            {
                Expression y = LinearPredictor(Source, Parameters, i);
                x.Add(y, Source.Outputs.Alias(i));
            }

            return x;

        }

        /// <summary>
        /// Retruns Link(NU)
        /// </summary>
        /// <param name="Source"></param>
        /// <param name="Parameters"></param>
        /// <param name="Link"></param>
        /// <returns></returns>
        public static ExpressionCollection Expected(ExpressionCollection Linear, Lambda Link)
        {

            ExpressionCollection x = new ExpressionCollection();

            foreach (Expression y in Linear.Nodes)
            {

                Expression z = Link.Bind(new List<Expression>() { y });
                x.Add(z);

            }

            return x;

        }

        /// <summary>
        ///  Does ordinary least squares 
        /// </summary>
        /// <param name="Source"></param>
        /// <param name="X"></param>
        /// <param name="Y"></param>
        /// <param name="W"></param>
        /// <param name="R"></param>
        /// <param name="F"></param>
        /// <returns></returns>
        public static RegressionParameterStruct OLS(Volume Source, ExpressionCollection X, ExpressionCollection Y, Expression W, Register R, Filter F)
        {

            // Create a reader stream //
            RecordReader stream = Source.OpenReader(R, F);

            // Create a regression input structure //
            RegressionParameterStruct parameters = new RegressionParameterStruct(X.Count, Y.Count);

            // Walk the reader //
            while (!stream.EndOfData)
            {

                // Load the register //
                R.Value = stream.ReadNext();

                // Evaluate the inputs, outputs and weight //
                Record x = X.Evaluate();
                Record y = Y.Evaluate();
                Cell w = W.Evaluate();

                // Calculate the record level XtX and XtY //
                for (int i = 0; i < x.Count; i++)
                {

                    // Just for XtX //
                    for (int j = 0; j < x.Count; j++)
                    {
                        parameters._XtX[i, j] += new Cell(x[i].valueDOUBLE * x[j].valueDOUBLE * w.valueDOUBLE);
                    }

                    // Just for XtY //
                    for (int k = 0; k < y.Count; k++)
                    {
                        parameters._XtY[i, k] += new Cell(x[i].valueDOUBLE * y[k].valueDOUBLE * w.valueDOUBLE);
                    }

                }

                // Calculate the observation sum //
                parameters._Obs++;

                // Calculate the weight sum //
                parameters._WSum += new Cell(w.valueDOUBLE);

            }

            // Return //
            return parameters;

        }

        /// <summary>
        /// Performs ordinary least squares
        /// </summary>
        /// <param name="Source"></param>
        /// <returns></returns>
        public static RegressionParameterStruct OLS(MiningModelSource Source)
        {
            return OLS(Source.Source, Source.Inputs, Source.Outputs, Source.Weight, Source.Memory, Source.Where); 
        }

        /// <summary>
        /// Calculats the OLS error statistics
        /// </summary>
        /// <param name="Source"></param>
        /// <param name="X"></param>
        /// <param name="Y"></param>
        /// <param name="W"></param>
        /// <param name="R"></param>
        /// <param name="F"></param>
        /// <param name="Parameters"></param>
        /// <returns></returns>
        public static MiningModelErrorStruct OLSError(Volume Source, ExpressionCollection X, ExpressionCollection Y, Expression W, Register R, Filter F, RegressionParameterStruct Parameters)
        {

            // Create the cell matrixes //
            CellMatrix sse = new CellMatrix(Y.Count, 1, Cell.ZeroValue(CellAffinity.DOUBLE));
            CellMatrix ssto = new CellMatrix(Y.Count, 1, Cell.ZeroValue(CellAffinity.DOUBLE));
            CellMatrix mean_sum = new CellMatrix(Y.Count, 1, Cell.ZeroValue(CellAffinity.DOUBLE));
            CellMatrix mean2_sum = new CellMatrix(Y.Count, 1, Cell.ZeroValue(CellAffinity.DOUBLE));
            Cell wsum = Cell.ZeroValue(CellAffinity.DOUBLE);
            Cell obs = Cell.ZeroValue(CellAffinity.DOUBLE);

            // Create a reader stream //
            RecordReader stream = Source.OpenReader(R, F);

            // Walk the reader //
            while (!stream.EndOfData)
            {

                // Load the register //
                R.Value = stream.ReadNext();

                // Evaluate the inputs, outputs and weight //
                CellMatrix x = new CellMatrix(X.Evaluate(), CellAffinity.DOUBLE);
                CellMatrix y = new CellMatrix(Y.Evaluate(), CellAffinity.DOUBLE);
                Cell w = W.Evaluate();

                // Get NewNode-hat //
                CellMatrix y_hat = new CellMatrix(y.RowCount, 1, Cell.ZeroValue(CellAffinity.DOUBLE));

                // Calculate NewNode-variables //
                y_hat = x.Transposition ^ Parameters._Beta;

                // Calculate SSE //
                sse = (y_hat - y) * (y_hat - y) * w;

                // Calculate mean and mean2 //
                mean_sum += y * w;
                mean2_sum += y * y * w;

                // Observations and weight
                wsum += w;
                obs++;

            }

            // Calculate SSTO //
            ssto = (mean2_sum / wsum - (mean_sum / wsum) * (mean_sum / wsum)) * wsum;

            // Return //
            return new MiningModelErrorStruct(sse, ssto, obs, new Cell(Parameters._XtY.ColumnCount));

        }

        /// <summary>
        /// Calculates the OLS error structure
        /// </summary>
        /// <param name="Source"></param>
        /// <param name="Parameters"></param>
        /// <returns></returns>
        public static MiningModelErrorStruct OLSError(MiningModelSource Source, RegressionParameterStruct Parameters)
        {

            return OLSError(Source.Source, Source.Inputs, Source.Outputs, Source.Weight, Source.Memory, Source.Where, Parameters);

        }

        /// <summary>
        /// Calculates the OLS beta variance, standard deviation, Key value, upper and lower bounds
        /// </summary>
        /// <param name="Parameters"></param>
        /// <param name="Error"></param>
        /// <returns></returns>
        public static RegressionBetaStruct OLSBetaStatistics(RegressionParameterStruct Parameters, MiningModelErrorStruct Error)
        {

            // Create the shell //
            RegressionBetaStruct val = new RegressionBetaStruct(Parameters.ParameterCount, Parameters.ResponseCount);

            // Save the beta //
            val._Beta = new CellMatrix(Parameters.Beta);

            // Save the two matricies needed for the variance calculation //
            CellMatrix xtx_inv = Parameters.XtXInverse;
            CellMatrix mse = Error.MSE;

            // Calculate the variance and standard deviation //
            for (int i = 0; i < Parameters.ParameterCount; i++)
            {

                for (int j = 0; j < Parameters.ResponseCount; j++)
                {

                    // Variance //
                    val._BetaVariance[i, j] = mse[j] * xtx_inv[i, i];
                    
                    // Standard Deviation //
                    val._BetaBetaStandardDeviation[i, j] = Cell.Sqrt(val._BetaVariance[i, j]);

                    // Key-Value //
                    val._BetaPValue[i, j] = new Cell(SpecialFunction.ProbabilityDistributions.NormalCDF(Math.Abs(val._Beta[i, j].valueDOUBLE / val._BetaBetaStandardDeviation[i, j].valueDOUBLE)));

                }

            }

            // Lower and Upper 95th //
            val._BetaLower95 = val._Beta - NORM95 * val._BetaBetaStandardDeviation;
            val._BetaUpper95 = val._Beta + NORM95 * val._BetaBetaStandardDeviation;

            return val;

        }

    }

    // Processes Nodes //
    public sealed class OLSFitNode : QueryNode
    {

        private MiningModelSource _Source;
        private RegressionParameterStruct _Parameters;

        public OLSFitNode(Session Session, int ThreadID, MiningModelSource Source)
            : base(ThreadID, Session)
        {
            this._Source = Source;
        }

        public RegressionParameterStruct InnerParameters
        {
            get
            {
                return this._Parameters;
            }
        }   

        public override void Invoke()
        {

            this._Parameters = RegressionModelHelper.OLS(this._Source);

        }

        public static List<OLSFitNode> Generate(Session Session, MiningModelSource Source, int ThreadCount)
        {

            List<MiningModelSource> sources = MiningModelSource.Split(Source, ThreadCount);

            List<OLSFitNode> nodes = new List<OLSFitNode>();

            for (int i = 0; i < ThreadCount; i++)
            {

                OLSFitNode n = new OLSFitNode(Session, i, sources[i]);
                nodes.Add(n);

            }

            return nodes;

        }

    }

    public sealed class OLSErrorNode : QueryNode
    {

        private MiningModelSource _Source;
        private RegressionParameterStruct _Parameters;
        private MiningModelErrorStruct _Error;

        public OLSErrorNode(Session Session, int ThreadID, MiningModelSource Source, RegressionParameterStruct Parameters)
            : base(ThreadID, Session)
        {
            this._Source = Source;
            this._Parameters = Parameters;
        }

        public RegressionParameterStruct InnerParameters
        {
            get
            {
                return this._Parameters;
            }
        }

        public MiningModelErrorStruct InnerErrors
        {
            get
            {
                return this._Error;
            }
        }

        public override void Invoke()
        {

            this._Error = RegressionModelHelper.OLSError(this._Source, this._Parameters);

        }

        public static List<OLSErrorNode> Generate(Session Session, MiningModelSource Source, RegressionParameterStruct Parameters, int ThreadCount)
        {

            List<MiningModelSource> sources = MiningModelSource.Split(Source, ThreadCount);

            List<OLSErrorNode> nodes = new List<OLSErrorNode>();

            for (int i = 0; i < ThreadCount; i++)
            {

                OLSErrorNode n = new OLSErrorNode(Session, i, sources[i], Parameters);
                nodes.Add(n);

            }

            return nodes;

        }

    }

    // Consolidators //
    public sealed class OLSFitConsolidator : QueryConsolidation<OLSFitNode>
    {

        private RegressionParameterStruct _Parameters;

        public OLSFitConsolidator(Session Session)
            : base(Session)
        {
        }

        public override void Consolidate(List<OLSFitNode> Nodes)
        {

            List<RegressionParameterStruct> Parameters = OLSFitConsolidator.ToParameters(Nodes);
            this._Parameters = RegressionParameterStruct.Merge(Parameters);
            this._Parameters.RenderBeta();

        }

        public static List<RegressionParameterStruct> ToParameters(List<OLSFitNode> Nodes)
        {

            List<RegressionParameterStruct> parameters = new List<RegressionParameterStruct>();
            foreach (OLSFitNode node in Nodes)
            {
                parameters.Add(node.InnerParameters);
            }
            return parameters;

        }

        public RegressionParameterStruct InnerParameters
        {
            get
            {
                return this._Parameters;
            }
        }

    }

    public sealed class OLSErrorConsolidator : QueryConsolidation<OLSErrorNode>
    {

        private MiningModelErrorStruct _Errors;

        public OLSErrorConsolidator(Session Session)
            : base(Session)
        {
        }

        public override void Consolidate(List<OLSErrorNode> Nodes)
        {

            List<MiningModelErrorStruct> Parameters = OLSErrorConsolidator.ToError(Nodes);
            this._Errors = MiningModelErrorStruct.Merge(Parameters);

        }

        public static List<MiningModelErrorStruct> ToError(List<OLSErrorNode> Nodes)
        {

            List<MiningModelErrorStruct> parameters = new List<MiningModelErrorStruct>();
            foreach (OLSErrorNode node in Nodes)
            {
                parameters.Add(node.InnerErrors);
            }
            return parameters;

        }

        public MiningModelErrorStruct InnerError
        {
            get
            {
                return this._Errors;
            }
        }

    }

    // Processes //
    public sealed class LinearRegressionModel
    {

        private MiningModelSource _Source;
        private RegressionParameterStruct _Parameters;
        private MiningModelErrorStruct _Errors;
        private Session _Session;

        public LinearRegressionModel(Session Session, MiningModelSource Source)
        {
            this._Source = Source;
            this._Session = Session;
        }

        // Properties //
        public MiningModelSource InnerSource
        {
            get { return this._Source; }
        }

        public RegressionParameterStruct InnerParameters
        {
            get { return this._Parameters; }
        }

        public MiningModelErrorStruct InnerError
        {
            get { return this._Errors; }
        }

        public RegressionBetaStruct InnerParamterStatistics
        {
            get 
            {
                return RegressionModelHelper.OLSBetaStatistics(this.InnerParameters, this.InnerError);
            }
        }

        public string MetaData
        {

            get
            {

                RegressionBetaStruct betas = this.InnerParamterStatistics;

                StringBuilder sb = new StringBuilder();

                sb.AppendLine(string.Format("Observations: {0}", this._Parameters._Obs));
                sb.AppendLine(string.Format("Weight Sum: {0}", this._Parameters._WSum));
                
                for (int i = 0; i < this._Parameters.ResponseCount; i++)
                {

                    string out_val = this._Source.Outputs[i].Unparse(this._Source.Source.Columns);
                    sb.AppendLine(string.Format("Output: {0}", out_val));
                    sb.AppendLine(string.Format("SSE / MSE: {0} / {1}", this._Errors.SSE[i, 0], this._Errors.MSE[i, 0]));
                    sb.AppendLine(string.Format("SSR / MSR: {0} / {1}", this._Errors.SSR[i, 0], this._Errors.MSR[i, 0]));
                    sb.AppendLine(string.Format("SSTO / MSTO: {0} / {1}", this._Errors.SSTO[i, 0], this._Errors.MSTO[i, 0]));
                    sb.AppendLine(string.Format("RSQ: {0}", this._Errors.RSquare[i,0]));

                    sb.AppendLine("-------------------------------------------");
                    sb.AppendLine("Parameter\t\tStdev\t\tPValue\t\tL95\t\tU95");
                    for (int j = 0; j < this._Parameters.ParameterCount; j++)
                    {
                        sb.AppendLine(string.Format("{0}\t\t{1}\t\t{2}\t\t{3}\t\t{4}", betas._Beta[j, i], betas._BetaBetaStandardDeviation[j, i], betas._BetaPValue[j, i], betas._BetaLower95[j, i], betas._BetaUpper95[j, i]));
                    }
                    sb.AppendLine();

                }

                return sb.ToString();

            }

        }

        // Methods //
        public void RenderModel()
        {

            // Create query process //
            List<OLSFitNode> nodes = OLSFitNode.Generate(this._Session, this._Source, 1);
            OLSFitConsolidator reducer = new OLSFitConsolidator(this._Session);
            QueryProcess<OLSFitNode> process = new QueryProcess<OLSFitNode>(nodes, reducer);

            // Execute //
            process.Execute();

            // Set the parameters //
            this._Parameters = reducer.InnerParameters;

        }

        public void RenderModel(int ThreadCount)
        {

            // Create query process //
            List<OLSFitNode> nodes = OLSFitNode.Generate(this._Session, this._Source, ThreadCount);
            OLSFitConsolidator reducer = new OLSFitConsolidator(this._Session);
            QueryProcess<OLSFitNode> process = new QueryProcess<OLSFitNode>(nodes, reducer);

            // Execute //
            process.ExecuteThreaded();

            // Set the parameters //
            this._Parameters = reducer.InnerParameters;

        }

        public void RenderError()
        {

            // Create query process //
            List<OLSErrorNode> nodes = OLSErrorNode.Generate(this._Session, this._Source, this._Parameters, 1);
            OLSErrorConsolidator reducer = new OLSErrorConsolidator(this._Session);
            QueryProcess<OLSErrorNode> process = new QueryProcess<OLSErrorNode>(nodes, reducer);

            // Execute //
            process.Execute();

            // Set the parameters //
            this._Errors = reducer.InnerError;

        }

        public void RenderError(int ThreadCount)
        {

            // Create query process //
            List<OLSErrorNode> nodes = OLSErrorNode.Generate(this._Session, this._Source, this._Parameters, ThreadCount);
            OLSErrorConsolidator reducer = new OLSErrorConsolidator(this._Session);
            QueryProcess<OLSErrorNode> process = new QueryProcess<OLSErrorNode>(nodes, reducer);

            // Execute //
            process.ExecuteThreaded();

            // Set the parameters //
            this._Errors = reducer.InnerError;

        }

    }

    public class GeneralizedLinearModel
    {

        protected Lambda _Link;
        protected MiningModelSource _Source;
        protected RegressionParameterStruct _Parameters;
        protected MiningModelErrorStruct _Errors;
        protected Session _Session;
        protected int _MaxItterations = 100;
        protected double _ExitCriteria = 0.001;
        protected int _ActualItterations = 0;
        protected double _Dispersion = 1D;
        
        public GeneralizedLinearModel(Session Session, MiningModelSource Source, Lambda Link)
        {

            this._Source = Source;
            this._Session = Session;
            this._Link = Link;
            this.Seed = 127;

        }

        // Properties //
        public MiningModelSource InnerSource
        {
            get { return this._Source; }
        }

        public RegressionParameterStruct InnerParameters
        {
            get { return this._Parameters; }
        }

        public MiningModelErrorStruct InnerError
        {
            get { return this._Errors; }
        }

        public RegressionBetaStruct InnerParamterStatistics
        {
            get
            {
                return RegressionModelHelper.OLSBetaStatistics(this.InnerParameters, this.InnerError);
            }
        }

        public virtual string MetaData
        {

            get
            {

                RegressionBetaStruct betas = this.InnerParamterStatistics;

                StringBuilder sb = new StringBuilder();

                sb.AppendLine(string.Format("Observations: {0}", this._Parameters._Obs));
                sb.AppendLine(string.Format("Weight Sum: {0}", this._Parameters._WSum));
                sb.AppendLine(string.Format("Itterations: {0}", this._ActualItterations));
                
                for (int i = 0; i < this._Parameters.ResponseCount; i++)
                {

                    string out_val = this._Source.Outputs[i].Unparse(this._Source.Source.Columns);
                    sb.AppendLine(string.Format("Output: {0}", out_val));
                    sb.AppendLine(string.Format("SSE / MSE: {0} / {1}", this._Errors.SSE[i, 0], this._Errors.MSE[i, 0]));
                    sb.AppendLine(string.Format("SSR / MSR: {0} / {1}", this._Errors.SSR[i, 0], this._Errors.MSR[i, 0]));
                    sb.AppendLine(string.Format("SSTO / MSTO: {0} / {1}", this._Errors.SSTO[i, 0], this._Errors.MSTO[i, 0]));
                    sb.AppendLine(string.Format("RSQ: {0}", this._Errors.RSquare[i, 0]));
                    sb.AppendLine("-------------------------------------------");
                    sb.AppendLine("Name\t\tParameter\t\tStdev\t\tPValue\t\tL95\t\tU95");
                    for (int j = 0; j < this._Parameters.ParameterCount; j++)
                    {
                        sb.AppendLine(string.Format("{0}\t\t{1}\t\t{2}\t\t{3}\t\t{4}\t\t{5}", this._Source.Inputs[j].Unparse(this._Source.Source.Columns), betas._Beta[j, i], betas._BetaBetaStandardDeviation[j, i], betas._BetaPValue[j, i], betas._BetaLower95[j, i], betas._BetaUpper95[j, i]));
                    }
                    sb.AppendLine();

                }

                return sb.ToString();

            }

        }

        public virtual ExpressionCollection Gamma(RegressionParameterStruct Parameters)
        {
         
            // Get the expressions //
            ExpressionCollection LinearPredictor = RegressionModelHelper.LinearPredictor(this._Source, Parameters);
            ExpressionCollection Expected = RegressionModelHelper.Expected(LinearPredictor, this._Link);
            ExpressionCollection Gradient = RegressionModelHelper.Expected(LinearPredictor, this._Link.Gradient("DX","NU"));
            ExpressionCollection Actual = this._Source.Outputs;

            // Link everything to the mining model source //
            this._Source.Marry(LinearPredictor);
            this._Source.Marry(Expected);
            this._Source.Marry(Gradient);
            this._Source.Marry(Actual);

            return IRWLSResponse.IRWLSResponses(LinearPredictor, Actual, Expected, Gradient);

        }

        public virtual ExpressionCollection Expected
        {

            get
            {
                // Get the expressions //
                ExpressionCollection LinearPredictor = RegressionModelHelper.LinearPredictor(this._Source, this._Parameters);
                ExpressionCollection Expected = RegressionModelHelper.Expected(LinearPredictor, this._Link);
            
                // Link everything to the mining model source //
                this._Source.Marry(LinearPredictor);
                this._Source.Marry(Expected);

                return Expected;
                }
        }

        public int Seed
        {
            get;
            set;
        }

        public double Dispersion
        {
            get { return this._Dispersion; }
        }

        // Methods //
        public virtual void RenderModel()
        {

            RegressionParameterStruct beta_lag = new RegressionParameterStruct(this._Source.Inputs.Count, this._Source.Outputs.Count);
            beta_lag.RandomizeBeta(127);

            for (int i = 0; i < this._MaxItterations; i++)
            {

                // Re-build the IRWLS expression //
                ExpressionCollection irwls = this.Gamma(beta_lag);

                // Fit the model //
                RegressionParameterStruct beta = RegressionModelHelper.OLS(this._Source.Source, this._Source.Inputs, irwls, this._Source.Weight, this._Source.Memory, this._Source.Where);
                beta.RenderBeta();

                //Console.WriteLine(irwls[0].Unparse(this._Source.Source.Columns));
                
                // Calculate sse //
                double sse = RegressionParameterStruct.BetaChange(beta, beta_lag);
                beta_lag = beta;
                //Console.WriteLine("SSE: {0}", sse);
                
                // Decide if we should exit //
                if (this._ExitCriteria > sse)
                {
                    this._Parameters = beta;
                    this._ActualItterations = i;
                    return;
                }

            }

            this._ActualItterations = -1;

        }

        public virtual void RenderError()
        {

            this._Errors = RegressionModelHelper.OLSError(this._Source.Source, this._Source.Inputs, this.Expected, 
                this._Source.Weight, this._Source.Memory, this._Source.Where, this._Parameters);

        }

        /// <summary>
        /// Represents an expression of the form: NU + (Actual - Expected) / Gradient, which is used to fit itteratively re-weighted least squares
        /// </summary>
        public sealed class IRWLSResponse : Expression, IDispersion
        {

            private Expression _Linear;
            private Expression _Actual;
            private Expression _YHat;
            private Expression _Gradient;
            private const double EPSILON = 0.001;
            private const double LEARNING = 0.5;
            private double _SSE;
            private double _Phi;

            public IRWLSResponse(Expression Parent, Expression Linear, Expression Actual, Expression YHat, Expression Gradient)
                : base(Parent, ExpressionAffinity.Result)
            {

                this._Linear = Linear;
                this._Actual = Actual;
                this._YHat = YHat;
                this._Gradient = Gradient;

                this._Cache.Add(this._Linear);
                this._Cache.Add(this._Actual);
                this._Cache.Add(this._YHat);
                this._Cache.Add(this._Gradient);

            }

            public override CellAffinity ReturnAffinity()
            {
                return CellAffinity.DOUBLE;
            }

            public override Cell Evaluate()
            {

                double lin = this._Linear.Evaluate().valueDOUBLE;
                double act = this._Actual.Evaluate().valueDOUBLE;
                double est = this._YHat.Evaluate().valueDOUBLE;
                double dx = this._Gradient.Evaluate().valueDOUBLE;


                this._SSE += (act - est) * (act - est);

                if (Math.Abs(dx) > EPSILON)
                {
                    this._Phi += (act - est) / dx;
                    return new Cell(lin + (act - est) / dx * LEARNING);
                }
                else
                {
                    return new Cell(lin);
                }

            }

            public override string Unparse(Schema S)
            {

                string lin = this._Linear.Unparse(S);
                string act = this._Actual.Unparse(S);
                string est = this._YHat.Unparse(S);
                string dx = this._Gradient.Unparse(S);

                return string.Format("{0} + ({1} - {2}) / ({3})", lin, act, est, dx);
            }

            public override Expression CloneOfMe()
            {
                return new IRWLSResponse(this.ParentNode, this._Linear.CloneOfMe(), this._Actual.CloneOfMe(), this._YHat.CloneOfMe(), this._Gradient.CloneOfMe());
            }

            public double SSE
            {
                get { return this._SSE; }
            }

            public double Phi
            {
                get { return this._Phi; }
            }

            public static ExpressionCollection IRWLSResponses(ExpressionCollection Linear, ExpressionCollection Actual, ExpressionCollection YHat, ExpressionCollection Gradient)
            {

                if (Linear.Count != Actual.Count || YHat.Count != Gradient.Count || Linear.Count != YHat.Count)
                    throw new ArgumentException(string.Format("Collections passed don't all have the same length {0}, {1}, {2}, {3}", Linear.Count, Actual.Count, YHat.Count, Gradient.Count));

                ExpressionCollection x = new ExpressionCollection();

                for (int i = 0; i < Linear.Count; i++)
                {

                    Expression y = new IRWLSResponse(null, Linear[i], Actual[i], YHat[i], Gradient[i]);
                    x.Add(y, Actual.Alias(i) + "_IRWLS");

                }

                return x;

            }

        }

    }

    public sealed class PoissonRegression :GeneralizedLinearModel
    {

        public PoissonRegression(Session Session, MiningModelSource Source)
            : base(Session, Source, PoissonLink)
        {
        }

        public override ExpressionCollection Gamma(RegressionParameterStruct Parameters)
        {
            
            // Get the expressions //
            ExpressionCollection LinearPredictor = RegressionModelHelper.LinearPredictor(this._Source, Parameters);
            ExpressionCollection Actual = this._Source.Outputs;

            // Link everything to the mining model source //
            this._Source.Marry(LinearPredictor);
            this._Source.Marry(Actual);

            return PoissonRegression.PoissonResponse.PoissonResponses(LinearPredictor, Actual);
        }

        /// <summary>
        /// Optimized version of IRWLSResponse for the special case of poisson regression
        /// </summary>
        public sealed class PoissonResponse : Expression, IDispersion
        {

            private Expression _Linear;
            private Expression _Actual;
            private const double EPSILON = 0.001;
            private const double LEARNING = 0.5;
            private double _SSE;
            private double _Phi;

            public PoissonResponse(Expression Parent, Expression Linear, Expression Actual)
                : base(Parent, ExpressionAffinity.Result)
            {

                this._Linear = Linear;
                this._Actual = Actual;

                this._Cache.Add(this._Linear);
                this._Cache.Add(this._Actual);

            }

            public override CellAffinity ReturnAffinity()
            {
                return CellAffinity.DOUBLE;
            }

            public override Cell Evaluate()
            {

                double lin = this._Linear.Evaluate().valueDOUBLE;
                double act = this._Actual.Evaluate().valueDOUBLE;
                double est = Math.Exp(lin);
                double dx = est;

                this._SSE += (act - est) * (act - est);
                
                if (Math.Abs(dx) > EPSILON)
                {
                    this._Phi += (act - est) / dx;
                    return new Cell(lin + (act - est) / dx * LEARNING);
                }
                else
                {
                    return new Cell(lin);
                }

            }

            public override string Unparse(Schema S)
            {
                string lin = this._Linear.Unparse(S);
                string act = this._Actual.Unparse(S);

                return string.Format("{0} + ({1} - EXP({2})) / EXP({3})", lin, act, lin, lin);
            }

            public override Expression CloneOfMe()
            {
                return new PoissonResponse(this.ParentNode, this._Linear.CloneOfMe(), this._Actual.CloneOfMe());
            }

            public double SSE
            {
                get { return this._SSE; }
            }

            public double Phi
            {
                get { return this._Phi; }
            }

            public static ExpressionCollection PoissonResponses(ExpressionCollection Linear, ExpressionCollection Actual)
            {

                if (Linear.Count != Actual.Count)
                    throw new ArgumentException(string.Format("Collections passed don't all have the same length {0}, {1}", Linear.Count, Actual.Count));

                ExpressionCollection x = new ExpressionCollection();

                for (int i = 0; i < Linear.Count; i++)
                {

                    Expression y = new PoissonResponse(null, Linear[i], Actual[i]);
                    x.Add(y, Actual.Alias(i) + "_POISSON");

                }

                return x;

            }


        }

        public static Lambda PoissonLink
        {
            get
            {
                ExpressionPointer ptr = new ExpressionPointer(null, "NU", CellAffinity.DOUBLE, 8);
                ExpressionResult func = new ExpressionResult(null, new CellFuncFVExp());
                func.AddChildNode(ptr);
                return new Lambda("LINK", func);
            }
        }

    }

    public sealed class LogisticRegression : GeneralizedLinearModel
    {

        public LogisticRegression(Session Session, MiningModelSource Source)
            : base(Session, Source, LogisticLink)
        {
        }

        public override ExpressionCollection Gamma(RegressionParameterStruct Parameters)
        {

            // Get the expressions //
            ExpressionCollection LinearPredictor = RegressionModelHelper.LinearPredictor(this._Source, Parameters);
            ExpressionCollection Actual = this._Source.Outputs;

            // Link everything to the mining model source //
            this._Source.Marry(LinearPredictor);
            this._Source.Marry(Actual);

            return LogisticRegression.LogisticResponse.LogisticResponses(LinearPredictor, Actual);

        }

        public override ExpressionCollection Expected
        {

            get
            {
                // Get the expressions //
                ExpressionCollection LinearPredictor = RegressionModelHelper.LinearPredictor(this._Source, this._Parameters);
                ExpressionCollection Expected = RegressionModelHelper.Expected(LinearPredictor, this._Link);

                // Link everything to the mining model source //
                this._Source.Marry(LinearPredictor);
                this._Source.Marry(Expected);

                return Expected;
            }
        }

        /// <summary>
        /// Optimized version of IRWLSResponse for the special case of poisson regression
        /// </summary>
        public sealed class LogisticResponse : Expression, IDispersion
        {

            private Expression _Linear;
            private Expression _Actual;
            private const double EPSILON = 0.001;
            private const double LEARNING = 0.5;
            private double _SSE;
            private double _Phi;

            public LogisticResponse(Expression Parent, Expression Linear, Expression Actual)
                : base(Parent, ExpressionAffinity.Result)
            {

                this._Linear = Linear;
                this._Actual = Actual;

                this._Cache.Add(this._Linear);
                this._Cache.Add(this._Actual);

            }

            public override CellAffinity ReturnAffinity()
            {
                return CellAffinity.DOUBLE;
            }

            public override Cell Evaluate()
            {

                double lin = this._Linear.Evaluate().valueDOUBLE;
                double act = this._Actual.Evaluate().valueDOUBLE;
                double est = 1D / (1D + Math.Exp(-lin));
                double dx = est * (1 - est);

                this._SSE += (act - est) * (act - est);

                if (Math.Abs(dx) > EPSILON)
                {
                    this._Phi += (act - est) / dx;
                    return new Cell(lin + (act - est) / dx * LEARNING);
                }
                else
                {
                    return new Cell(lin);
                }

            }

            public override string Unparse(Schema S)
            {
                string lin = this._Linear.Unparse(S);
                string act = this._Actual.Unparse(S);

                return string.Format("{0} + ({1} - LOGIT({0})) / LOGIT({0})*(1 - LOGIT({0})", lin, act);
            }

            public override Expression CloneOfMe()
            {
                return new LogisticResponse(this.ParentNode, this._Linear.CloneOfMe(), this._Actual.CloneOfMe());
            }

            public double SSE
            {
                get { return this._SSE; }
            }

            public double Phi
            {
                get { return this._Phi; }
            }

            public static ExpressionCollection LogisticResponses(ExpressionCollection Linear, ExpressionCollection Actual)
            {

                if (Linear.Count != Actual.Count)
                    throw new ArgumentException(string.Format("Collections passed don't all have the same length {0}, {1}", Linear.Count, Actual.Count));

                ExpressionCollection x = new ExpressionCollection();

                for (int i = 0; i < Linear.Count; i++)
                {

                    Expression y = new LogisticResponse(null, Linear[i], Actual[i]);
                    x.Add(y, Actual.Alias(i) + "_LOGISTIC");

                }

                return x;

            }

        }

        public static Lambda LogisticLink
        {
            get
            {
                ExpressionPointer ptr = new ExpressionPointer(null, "NU", CellAffinity.DOUBLE, 8);
                ExpressionResult func = new ExpressionResult(null, new CellFuncFVLogit());
                func.AddChildNode(ptr);
                return new Lambda("LINK", func);
            }
        }

    }

}
