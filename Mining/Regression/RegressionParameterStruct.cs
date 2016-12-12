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

    public sealed class RegressionInputStruct
    {

        internal Volume _Data;
        internal ExpressionCollection _Inputs;
        internal ExpressionCollection _Outputs;
        internal Expression _Weight;
        internal Filter _Where;
        internal Register _Memory;

        public RegressionInputStruct(Volume Data, Register Memory, ExpressionCollection Inputs, ExpressionCollection Outputs, Expression Weight, Filter Where)
        {
            this._Data = Data;
            this._Memory = Memory;
            this._Inputs = Inputs;
            this._Outputs = Outputs;
            this._Weight = Weight;
            this._Where = Where;

        }

        public RegressionParameterStruct BlankParameterStruct()
        {
            return new RegressionParameterStruct(this._Inputs.Count, this._Outputs.Count);
        }

    }

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

    }
    
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

        public abstract RegressionParameterStruct Render(RegressionInputStruct Inputs);

        public abstract RegressionErrorStruct RenderError(RegressionInputStruct Inputs, RegressionParameterStruct Parameters);

    }

    public sealed class RegressionAlgorithmLeastSquares : RegressionAlgorithm
    {

        public override RegressionParameterStruct Render(RegressionInputStruct Inputs)
        {

            // Create a reader stream //
            RecordReader stream = Inputs._Data.OpenReader(Inputs._Memory, Inputs._Where);

            // Create a regression input structure //
            RegressionParameterStruct parameters = Inputs.BlankParameterStruct();

            // Walk the reader //
            while (!stream.EndOfData)
            {

                // Load the register //
                Inputs._Memory.Value = stream.ReadNext();

                // Evaluate the inputs, outputs and weight //
                Record x = Inputs._Inputs.Evaluate();
                Record y = Inputs._Outputs.Evaluate();
                Cell w = Inputs._Weight.Evaluate();

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

        public override RegressionErrorStruct RenderError(RegressionInputStruct Inputs, RegressionParameterStruct Parameters)
        {

            // Create the cell matrixes //
            CellMatrix sse = new CellMatrix(Inputs._Outputs.Count, 1, Cell.ZeroValue(CellAffinity.DOUBLE));
            CellMatrix ssto = new CellMatrix(Inputs._Outputs.Count, 1, Cell.ZeroValue(CellAffinity.DOUBLE));
            CellMatrix mean_sum = new CellMatrix(Inputs._Outputs.Count, 1, Cell.ZeroValue(CellAffinity.DOUBLE));
            CellMatrix mean2_sum = new CellMatrix(Inputs._Outputs.Count, 1, Cell.ZeroValue(CellAffinity.DOUBLE));
            Cell wsum = Cell.ZeroValue(CellAffinity.DOUBLE);
            Cell obs = Cell.ZeroValue(CellAffinity.DOUBLE);

            // Create a reader stream //
            RecordReader stream = Inputs._Data.OpenReader(Inputs._Memory, Inputs._Where);

            // Walk the reader //
            while (!stream.EndOfData)
            {

                // Load the register //
                Inputs._Memory.Value = stream.ReadNext();

                // Evaluate the inputs, outputs and weight //
                CellMatrix x = new CellMatrix(Inputs._Inputs.Evaluate(), CellAffinity.DOUBLE);
                CellMatrix y = new CellMatrix(Inputs._Outputs.Evaluate(), CellAffinity.DOUBLE);
                Cell w = Inputs._Weight.Evaluate();

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

    }

    // Processes //


}
