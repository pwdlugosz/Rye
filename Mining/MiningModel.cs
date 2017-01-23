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

        private Session _Session;
        private MiningModelAffinity _Affinity;
        private Volume _Source;
        private ExpressionCollection _Inputs;
        private ExpressionCollection _Outputs; // Will be null if affinity is Unsupervised
        private Expression _Weight;
        private Filter _Where;
        private Register _Memory;

        // Constructors //
        public MiningModelSource(Session Session, Volume Source, ExpressionCollection Inputs, ExpressionCollection Outputs, Expression Weight, Filter Where)
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
            this._Session = Session;
            this._Affinity = (Outputs == null ? MiningModelAffinity.Unsupervised : MiningModelAffinity.Supervised);
            this._Source = Source;
            this._Memory = new Register(this._Source.Parent.Header.Name, Source.Columns);
            this._Inputs = Inputs;
            this._Outputs = Outputs;
            this._Weight = Weight;
            this._Where = Where;

            // Go though and make sure all vairables are linked together //
            CloneFactory emrc = new CloneFactory();
            emrc.Append(this._Memory);
            emrc.Append(this._Session.Scalars);
            emrc.Append(this._Session.Matrixes);

            emrc.Link(this._Inputs);
            emrc.Link(this._Outputs);
            emrc.Link(this._Weight);
            emrc.Link(this._Where);

        }

        public MiningModelSource(Session Session, TabularData Source, ExpressionCollection Inputs, ExpressionCollection Outputs, Expression Weight, Filter Where)
            : this(Session, Source.CreateVolume(), Inputs, Outputs, Weight, Where)
        {
        }

        // Properties //
        public Session Enviroment
        {
            get { return this._Session; }
        }

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

        public void Marry(Lambda LinkFunction)
        {

            this.Marry(LinkFunction.InnerNode);

        }

        public void Marry(Expression Node)
        {

            CloneFactory spiderweb = new CloneFactory();
            spiderweb.Append(this._Memory);
            spiderweb.Append(this._Session.Scalars);
            spiderweb.Append(this._Session.Matrixes);

            spiderweb.Link(Node);

        }

        public void Marry(ExpressionCollection Nodes)
        {

            foreach (Expression x in Nodes.Nodes)
            {
                this.Marry(x);
            }

        }

        public static List<MiningModelSource> Split(Session Session, TabularData Source, ExpressionCollection Inputs, ExpressionCollection Outputs, 
            Expression Weight, Filter Where, int Threads)
        {

            List<MiningModelSource> sources = new List<MiningModelSource>();
            for (int i = 0; i < Threads; i++)
            {

                MiningModelSource x = new MiningModelSource(Session, Source.CreateVolume(i, Threads), Inputs, Outputs, Weight, Where);
                sources.Add(x);

            }

            return sources;

        }

        public static List<MiningModelSource> Split(MiningModelSource Source, int Threads)
        {
            return MiningModelSource.Split(Source._Session, Source._Source.Parent, Source._Inputs, Source._Outputs, Source._Weight, Source._Where, Threads);
        }

    }

    /// <summary>
    /// Holds mining model error data
    /// </summary>
    public sealed class MiningModelErrorStruct
    {

        private CellMatrix _SSE;
        private CellMatrix _SSTO;
        private CellMatrix _One;
        private Cell _Obs;
        private Cell _Params;

        public MiningModelErrorStruct(CellMatrix SSE, CellMatrix SSTO, Cell Observations, Cell Parameters)
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

        public CellMatrix MSE
        {
            get
            {
                return this._SSE / (this._Obs - this._Params);
            }
        }

        public CellMatrix MSTO
        {
            get
            {
                return this._SSTO / (this._Obs - Cell.OneValue(CellAffinity.DOUBLE));
            }
        }

        public CellMatrix MSR
        {
            get
            {
                return this.SSR / (this._Params - Cell.OneValue(CellAffinity.DOUBLE));
            }
        }

        /// <summary>
        /// Merges the SSE, SSTO and SSR values
        /// </summary>
        /// <param name="A">First structure</param>
        /// <param name="B">Second structure</param>
        /// <returns>The merged regression error structure</returns>
        public static MiningModelErrorStruct Merge(MiningModelErrorStruct A, MiningModelErrorStruct B)
        {

            if (A._Params != B._Params)
            {
                throw new ArgumentException("The MiningModelErrorStruct's must have the same parameter counts");
            }

            MiningModelErrorStruct C = new MiningModelErrorStruct(A._SSE.CloneOfMe() + B._SSE.CloneOfMe(), A._SSTO.CloneOfMe() + B._SSTO.CloneOfMe(), A._Obs + B._Obs, A._Params);

            return C;

        }

        /// <summary>
        /// Merges the SSE, SSTO and SSR values
        /// </summary>
        /// <param name="Parameters">The collection of individual strucs</param>
        /// <returns>The combined struct</returns>
        public static MiningModelErrorStruct Merge(IEnumerable<MiningModelErrorStruct> Parameters)
        {

            MiningModelErrorStruct[] p = Parameters.ToArray();

            if (p.Length == 0)
                return null;

            if (p.Length == 1)
                return p[0];

            MiningModelErrorStruct prime = p[0];

            for (int i = 1; i < p.Length; i++)
            {
                prime = MiningModelErrorStruct.Merge(prime, p[i]);
            }

            return prime;

        }

    }
    
    /// <summary>
    /// Used to describe the model's inner data and fit statistics
    /// </summary>
    public interface IMiningModelExtension
    {

        /// <summary>
        /// This is the unique name of the model
        /// </summary>
        string ModelName { get; }

        /// <summary>
        /// This is a list of all prediction variables in the model
        /// </summary>
        string[] Names { get; }

        /// <summary>
        /// Checks to see if a variable exists in the model with this name
        /// </summary>
        /// <param name="Name">Name to lookup</param>
        /// <returns>True if the variable exists, false otherwise</returns>
        bool Exists(string Name);

        /// <summary>
        /// Gets an expression collection containing all variables in the model
        /// </summary>
        /// <param name="Inputs">The input expressions</param>
        /// <returns>A collection of all prediction variables</returns>
        ExpressionCollection Prediction(ExpressionCollection Inputs);

        /// <summary>
        /// Gets a single expression for a prediction variable in the model
        /// </summary>
        /// <param name="Inputs">The input expressions</param>
        /// <returns>A single expression</returns>
        Expression Prediction(ExpressionCollection Inputs, string Name);

        /// <summary>
        /// Model meta data
        /// </summary>
        string MetaData { get; }

        /// <summary>
        /// The model's error information
        /// </summary>
        MiningModelErrorStruct Errors { get; }

    }

}
