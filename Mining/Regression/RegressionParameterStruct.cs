using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Structures;

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
            get { return this._XtY; }
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
            this._Beta = (this.XtX.Inverse) ^ this._XtY;
        }

        /// <summary>
        /// Merges the XtX, XtY and WSum variables of two regression model structures; Order doesnt matter; this will not calculate the beta value
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

    }

    /// <summary>
    /// Holds regression error data
    /// </summary>
    public sealed class RegressionErrorStruct
    {

        private CellMatrix _SSE;
        private CellMatrix _SSTO;
        private CellMatrix _One;
        private Cell _Obs;
        private Cell _Params;
        
        public RegressionErrorStruct(CellMatrix SSE, CellMatrix SSTO, Cell Observations, Cell Parameters)
        {

            if (SSE.RowCount != SSTO.RowCount || SSE.ColumnCount != 1 || SSTO.ColumnCount != 1)
                throw new ArgumentException("SSE and SSTO must have the row count and must have a column count equal to one");

            this._SSE = SSE;
            this._SSTO = SSTO;
            this._Obs = Observations;
            this._Params = Parameters;
            this._One = new CellMatrix(this._SSTO.RowCount, 1, Cell.OneValue(CellAffinity.DOUBLE));
        }

        public CellMatrix SSE
        {
            get { return this._SSE; }
        }

        public CellMatrix SSTO
        {
            get { return this._SSTO; }
        }

        public CellMatrix SSR
        {
            get { return this._SSTO - this._SSE; }
        }

        public CellMatrix RSquare
        {
            get { return this._One - CellMatrix.CheckDivide(this._SSE, this._SSTO); }
        }
        
        /// <summary>
        /// Merges the SSE, SSTO and SSR values
        /// </summary>
        /// <param name="A">First structure</param>
        /// <param name="B">Second structure</param>
        /// <returns>The merged regression error structure</returns>
        public static RegressionErrorStruct Merge(RegressionErrorStruct A, RegressionErrorStruct B)
        {

            if (A._Params != B._Params)
            {
                throw new ArgumentException("The RegressionErrorStruct's must have the same parameter counts");
            }

            RegressionErrorStruct C = new RegressionErrorStruct(A._SSE.CloneOfMe() + B._SSE.CloneOfMe(), A._SSTO.CloneOfMe() + B._SSTO.CloneOfMe(), A._Obs + B._Obs, A._Params);

            return C;

        }

        /// <summary>
        /// Merges the SSE, SSTO and SSR values
        /// </summary>
        /// <param name="Parameters">The collection of individual strucs</param>
        /// <returns>The combined struct</returns>
        public static RegressionErrorStruct Merge(IEnumerable<RegressionErrorStruct> Parameters)
        {

            RegressionErrorStruct[] p = Parameters.ToArray();
            
            if (p.Length == 0)
                return null;

            if (p.Length == 1)
                return p[0];

            RegressionErrorStruct prime = p[0];

            for (int i = 1; i < p.Length; i++)
            {
                prime = RegressionErrorStruct.Merge(prime, p[i]);
            }

            return prime;
            
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

        public RegressionBetaStruct(RegressionParameterStruct Parameters, RegressionErrorStruct Statistics)
        {
        }

    }

    // Algorithms //
    public abstract class RegressionAlgorithm
    {

        public abstract RegressionParameterStruct Render(MiningModelSource Source);

        public abstract RegressionParameterStruct Render(MiningModelSource Source, int ThreadCount);

        public abstract RegressionErrorStruct RenderError(MiningModelSource Source, RegressionParameterStruct Parameters);

        public abstract RegressionErrorStruct RenderError(MiningModelSource Source, RegressionParameterStruct Parameters, int ThreadCount);

    }

    public sealed class RegressionAlgorithmLeastSquares : RegressionAlgorithm
    {

        public override RegressionParameterStruct Render(MiningModelSource Source)
        {

            // Create a reader stream //
            RecordReader stream = Source.Source.OpenReader(Source.Memory, Source.Where);

            // Create a regression input structure //
            RegressionParameterStruct parameters = new RegressionParameterStruct(Source);

            // Split out other variables so we don't need to keep calling the 'get' methods //
            ExpressionCollection BigX = Source.Inputs;
            ExpressionCollection BigY = Source.Outputs;
            Expression BigW = Source.Weight;
            Register BigR = Source.Memory;

            // Walk the reader //
            while (!stream.EndOfData)
            {

                // Load the register //
                BigR.Value = stream.ReadNext();

                // Evaluate the inputs, outputs and weight //
                Record x = BigX.Evaluate();
                Record y = BigY.Evaluate();
                Cell w = BigW.Evaluate();

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

                    // Calculate the weight sum //
                    parameters._WSum += new Cell(w.valueDOUBLE);

                    // Calculate the observation sum //
                    parameters._Obs++;

                }

            }

            // Return //
            return parameters;
                
        }

        public override RegressionParameterStruct Render(MiningModelSource Source, int ThreadCount)
        {

            throw new NotImplementedException();

        }

        public override RegressionErrorStruct RenderError(MiningModelSource Source, RegressionParameterStruct Parameters)
        {

            // Create the cell matrixes //
            CellMatrix sse = new CellMatrix(Source.Outputs.Count, 1, Cell.ZeroValue(CellAffinity.DOUBLE));
            CellMatrix ssto = new CellMatrix(Source.Outputs.Count, 1, Cell.ZeroValue(CellAffinity.DOUBLE));
            CellMatrix mean_sum = new CellMatrix(Source.Outputs.Count, 1, Cell.ZeroValue(CellAffinity.DOUBLE));
            CellMatrix mean2_sum = new CellMatrix(Source.Outputs.Count, 1, Cell.ZeroValue(CellAffinity.DOUBLE));
            Cell wsum = Cell.ZeroValue(CellAffinity.DOUBLE);
            Cell obs = Cell.ZeroValue(CellAffinity.DOUBLE);

            // Create a reader stream //
            RecordReader stream = Source.Source.OpenReader(Source.Memory, Source.Where);

            // Split out other variables so we don't need to keep calling the 'get' methods //
            ExpressionCollection BigX = Source.Inputs;
            ExpressionCollection BigY = Source.Outputs;
            Expression BigW = Source.Weight;
            Register BigR = Source.Memory;

            // Walk the reader //
            while (!stream.EndOfData)
            {

                // Load the register //
                BigR.Value = stream.ReadNext();

                // Evaluate the inputs, outputs and weight //
                CellMatrix x = new CellMatrix(BigX.Evaluate(), CellAffinity.DOUBLE);
                CellMatrix y = new CellMatrix(BigY.Evaluate(), CellAffinity.DOUBLE);
                Cell w = BigW.Evaluate();

                // Get y-hat //
                CellMatrix y_hat = new CellMatrix(y.RowCount, 1, Cell.ZeroValue(CellAffinity.DOUBLE));
                    
                // Calculate y-variables //
                y_hat = x ^ Parameters._Beta;

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
            return new RegressionErrorStruct(sse, ssto, obs, new Cell(Parameters._XtY.ColumnCount));

        }

        public override RegressionErrorStruct RenderError(MiningModelSource Source, RegressionParameterStruct Parameters, int ThreadCount)
        {
            throw new NotImplementedException();
        }

    }


    // Processes //


}
