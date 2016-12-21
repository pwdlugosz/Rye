using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Query
{

    /// <summary>
    /// Provides support for joining data over a single thread
    /// </summary>
    public class JoinProcessNode : QueryNode
    {

        protected JoinAlgorithm _BaseAlgorithm;
        protected JoinType _BaseType;
        protected Volume _V1, _V2;
        protected Register _M1, _M2;
        protected RecordComparer _RC;
        protected Filter _F;
        protected ExpressionCollection _C;
        protected RecordWriter _W;

        public JoinProcessNode(int ThreadID, Session Session, JoinAlgorithm JA, JoinType JT, Volume V1, Register M1, Volume V2, Register M2, 
            RecordComparer RC, Filter F, ExpressionCollection C, RecordWriter W)
            : base(ThreadID, Session)
        {
            this._BaseAlgorithm = JA;
            this._BaseType = JT;
            this._V1 = V1;
            this._V2 = V2;
            this._M1 = M1;
            this._M2 = M2;
            this._RC = RC;
            this._F = F;
            this._C = C;
            this._W = W;

        }

        public override void Invoke()
        {
            this.IOCalls = this._BaseAlgorithm.VolumeJoin(this._BaseType, this._V1, this._M1, this._V2, this._M2, this._RC, this._F, this._C, this._W);
        }

        public override void EndInvoke()
        {
            this._W.Close();
        }

        public long IOCalls
        {
            get;
            protected set;
        }

        public long ActualCost
        {
            get { return this._RC.Clicks; }
        }

    }

    /// <summary>
    /// Provides support for consolidating join threads
    /// </summary>
    public class JoinConsolidation : QueryConsolidation<JoinProcessNode>
    {

        public JoinConsolidation(Session Session)
            : base(Session)
        {
        }

        public long IOCalls
        {
            get;
            protected set;
        }

        public long ActualCost
        {
            get;
            private set;
        }

        public override void Consolidate(List<JoinProcessNode> Nodes)
        {
            foreach (JoinProcessNode n in Nodes)
            {
                this.IOCalls += n.IOCalls;
                this.ActualCost += n.ActualCost;
            }
        }

    }

    /// <summary>
    /// Represents each base join type
    /// </summary>
    public enum JoinType
    {
        Inner,
        Left,
        Right,
        AntiInner,
        AntiLeft,
        AntiRight,
        Full,
        Cross
    }

    /// <summary>
    /// Represents each join algorithm type
    /// </summary>
    public enum JoinAlgorithmType
    {

        SortMerge,
        NestedLoop

    }

    /// <summary>
    /// Represents each join implementation type
    /// </summary>
    public enum JoinImplementationType
    {

        Block_ExE,
        Block_ExV,
        Block_ExT,
        Block_VxV,
        Block_VxT,
        Block_TxT

    }

    /// <summary>
    /// JoinAlgorithm defines how two extents get merged
    /// </summary>
    public abstract class JoinAlgorithm
    {

        /*
         * There are three core join implementations:
         * -- INNER
         * -- LEFT
         * -- ANTI-LEFT
         * 
         * Left is technically INNER + ANTI-LEFT, but LEFT is common enough that it should be optimized
         * 
         * We can derive the other joins like this:
         *      Atomic Joins
         *      --------------------------------------------------
         *      INNER
         *      LEFT
         *      ANTI-LEFT
         *      
         *      Derived Joins
         *      --------------------------------------------------
         *      RIGHT = LEFT swap arguments
         *      ANTI-RIGHT = ANTI-LEFT swap arguments
         *      ANTI-INNER = ANTI-LEFT + ANTI-RIGHT
         *      FULL = LEFT + ANTI-RIGHT
         * 
         * The CROSS JOIN is implemented using a nested loop in the base class
         * 
         * There are two key implementations:
         *      Value x Value
         *      V x V
         * 
         * The following can be derived from the above
         *      Value x V
         *      Value x T
         *      V x T
         *      T x T
         *      
         * The 'Block' joins, implement V x V via Value x Value in blocks
         * 
         */

        public JoinAlgorithm()
        {
            // TICK TOCK...
        }

        // Properties //
        public static IEnumerable<JoinType> JoinTypes
        {

            get
            {

                return new JoinType[8] 
                { 
                    JoinType.Inner,
                    JoinType.AntiInner,
                    JoinType.Left,
                    JoinType.AntiLeft,
                    JoinType.Right,
                    JoinType.AntiRight,
                    JoinType.Full,
                    JoinType.Cross
                };

            }

        }

        public static IEnumerable<JoinAlgorithmType> JoinAlgorithmTypes
        {

            get
            {

                return new JoinAlgorithmType[2] 
                { 
                    JoinAlgorithmType.SortMerge,
                    JoinAlgorithmType.NestedLoop
                };

            }

        }

        public static IEnumerable<JoinImplementationType> JoinImplementationTypes
        {

            get
            {

                return new JoinImplementationType[6] 
                { 
                    JoinImplementationType.Block_ExE, 
                    JoinImplementationType.Block_ExT, 
                    JoinImplementationType.Block_ExV, 
                    JoinImplementationType.Block_VxV, 
                    JoinImplementationType.Block_VxT, 
                    JoinImplementationType.Block_TxT
                };

            }

        }

        public JoinAlgorithmType BaseJoinAlgorithmType
        {
            get;
            protected set;
        }

        // Core Joins //
        public long VolumeJoin(JoinType Type, Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {

            // Try to save some time by checking for no-run situations

            switch (Type)
            {

                case JoinType.Inner: 
                    return this.InnerJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.AntiInner: 
                    return this.AntiInnerJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.Left: 
                    return this.LeftJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.AntiLeft: 
                    return this.AntiLeftJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.Right: 
                    return this.RightJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.AntiRight: 
                    return this.AntiRightJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.Full: 
                    return this.FullJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.Cross:
                    return this.Cartesian(LeftVolume, LeftMemory, RightVolume, RightMemory, Where, Output, OutputStream);
            
            }

            return -1;

        }

        public long BlockJoin(JoinType Type, Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {

            switch (Type)
            {

                case JoinType.Inner:
                    return this.BlockInnerJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.AntiInner:
                    return this.BlockAntiInnerJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.Left:
                    return this.BlockLeftJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.AntiLeft:
                    return this.BlockAntiLeftJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.Right:
                    return this.BlockRightJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.AntiRight:
                    return this.BlockAntiRightJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.Full:
                    return this.BlockFullJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
                case JoinType.Cross:
                    return this.Cartesian(LeftVolume, LeftMemory, RightVolume, RightMemory, Where, Output, OutputStream);

            }

            return -1;

        }

        // Value x Value //
        public abstract long InnerJoin(Extent LeftExtent, Register LeftMemory, Extent RightExtent, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream);

        public abstract long LeftJoin(Extent LeftExtent, Register LeftMemory, Extent RightExtent, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream, BitArray NullReferenceMap);

        public abstract long AntiLeftJoin(Extent LeftExtent, Register LeftMemory, Extent RightExtent, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream, BitArray NullReferenceMap);

        // V x V //
        public abstract long InnerJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream);

        public abstract long LeftJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream);

        public abstract long AntiLeftJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream);

        public virtual long RightJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            return this.LeftJoin(RightVolume, RightMemory, LeftVolume, LeftMemory, JoinPredicate.Reverse(), Where, Output, OutputStream);
        }

        public virtual long AntiRightJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            return this.AntiLeftJoin(RightVolume, RightMemory, LeftVolume, LeftMemory, JoinPredicate.Reverse(), Where, Output, OutputStream);
        }

        public virtual long AntiInnerJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            long clicks = this.AntiLeftJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
            clicks += this.AntiRightJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
            return clicks;
        }

        public virtual long FullJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            long clicks = this.LeftJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
            clicks += this.AntiRightJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
            return clicks;
        }

        // Block Joins //
        public virtual long BlockInnerJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {

            // Tracker //
            long clicks = 0;

            // Cycle Left //
            foreach (Extent left in LeftVolume.Extents)
            {

                // Cycle right //
                foreach (Extent right in RightVolume.Extents)
                {

                    clicks += this.InnerJoin(left, LeftMemory, right, RightMemory, JoinPredicate, Where, Output, OutputStream);

                }

            }

            return clicks;

        }

        public virtual long BlockAntiInnerJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {

            long clicks = this.BlockAntiLeftJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
            long clocks = this.BlockAntiRightJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
            return clicks + clocks;

        }

        public virtual long BlockLeftJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {


            // Tracker //
            long clicks = 0;

            // Cycle Left //
            foreach (Extent left in LeftVolume.Extents)
            {

                // Create a bit array for a null referencce map //
                BitArray NullMap = new BitArray(left.Count, false);

                // Cycle right //
                foreach (Extent right in RightVolume.Extents)
                {

                    clicks += this.LeftJoin(left, LeftMemory, right, RightMemory, JoinPredicate, Where, Output, OutputStream, NullMap);

                }

                // Collapse the map //
                clicks += this.Collapse(left, LeftMemory, RightVolume.Columns.NullRecord, RightMemory, Where, Output, OutputStream, NullMap);

            }

            return clicks;

        }

        public virtual long BlockAntiLeftJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {

            // Tracker //
            long clicks = 0;

            // Cycle Left //
            foreach (Extent left in LeftVolume.Extents)
            {

                // Create a bit array for a null referencce map //
                BitArray NullMap = new BitArray(left.Count, false);

                // Cycle right //
                foreach (Extent right in RightVolume.Extents)
                {

                    clicks += this.AntiLeftJoin(left, LeftMemory, right, RightMemory, JoinPredicate, Where, Output, OutputStream, NullMap);

                }

                // Collapse the map //
                clicks += this.Collapse(left, LeftMemory, RightVolume.Columns.NullRecord, RightMemory, Where, Output, OutputStream, NullMap);

            }

            return clicks;

        }

        public virtual long BlockRightJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            return this.BlockLeftJoin(RightVolume, RightMemory, LeftVolume, LeftMemory, JoinPredicate, Where, Output, OutputStream);
        }

        public virtual long BlockAntiRightJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            return this.BlockAntiLeftJoin(RightVolume, RightMemory, LeftVolume, LeftMemory, JoinPredicate, Where, Output, OutputStream);
        }

        public virtual long BlockFullJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {

            long clicks = this.BlockLeftJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
            long clocks = this.BlockAntiRightJoin(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream);
            return clicks + clocks;

        }

        // Cartesian //
        public long Cartesian(Extent LeftExtent, Register LeftMemory, Extent RightExtent, Register RightMemory,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {

            // Tracker //
            long clicks = 0;

            // Cycle left //
            for (int l = 0; l < LeftExtent.Count; l++)
            {

                // Assign the left record //
                LeftMemory.Value = LeftExtent[l];

                // Cycle right //
                for (int r = 0; r < RightExtent.Count; r++)
                {

                    // Assign the right record //
                    RightMemory.Value = RightExtent[r];

                    // Output //
                    if (Where.Render())
                    {

                        // Output //
                        Record x = Output.Evaluate();
                        OutputStream.Insert(x);
                        clicks++;

                    }

                }

            }

            return clicks;

        }

        public long Cartesian(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {

            // Tracker //
            long clicks = 0;

            foreach (Extent e in LeftVolume.Extents)
            {

                foreach (Extent f in RightVolume.Extents)
                {

                    clicks += this.Cartesian(e, LeftMemory, f, RightMemory, Where, Output, OutputStream);
                
                }

            }

            return clicks;

        }

        // Collapsing //
        public long Collapse(Extent NonNullTable, Register NonNullMemory, Record NullRecord, Register NullRecordMemory,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream, BitArray NullReferenceMap)
        {

            // Create a null record //
            NullRecordMemory.Value = NullRecord;

            // Tracker //
            long clicks = 0;
            
            // Cycle through each fail mapping //
            for (int i = 0; i < NullReferenceMap.Count; i++)
            {
                
                // Assign the records to each register //
                NonNullMemory.Value = NonNullTable[i];

                // Output //
                if (Where.Render() && NullReferenceMap[i])
                {

                    Record x = Output.Evaluate();
                    OutputStream.Insert(x);
                    clicks++;

                }

            }

            return clicks;

        }

        // Cost metrics //
        public abstract double Cost_Block_ExE(TabularData T1, TabularData T2, int Threads, double TupleRatioHint);

        public abstract double Cost_Block_ExV(TabularData T1, TabularData T2, int Threads, double TupleRatioHint);

        public abstract double Cost_Block_ExT(TabularData T1, TabularData T2, int Threads, double TupleRatioHint);

        public abstract double Cost_Block_VxV(TabularData T1, TabularData T2, int Threads, double TupleRatioHint);

        public abstract double Cost_Block_VxT(TabularData T1, TabularData T2, int Threads, double TupleRatioHint);

        public abstract double Cost_Block_TxT(TabularData T1, TabularData T2, int Threads, double TupleRatioHint);

        public double Cost(TabularData T1, TabularData T2, int Threads, double TupleRatioHint, JoinImplementationType Type)
        {

            switch (Type)
            {
                case JoinImplementationType.Block_ExE: 
                    return this.Cost_Block_ExE(T1, T2, Threads, TupleRatioHint);
                case JoinImplementationType.Block_ExV: 
                    return this.Cost_Block_ExV(T1, T2, Threads, TupleRatioHint);
                case JoinImplementationType.Block_ExT: 
                    return this.Cost_Block_ExT(T1, T2, Threads, TupleRatioHint);
                case JoinImplementationType.Block_VxV: 
                    return this.Cost_Block_VxV(T1, T2, Threads, TupleRatioHint);
                case JoinImplementationType.Block_VxT: 
                    return this.Cost_Block_VxT(T1, T2, Threads, TupleRatioHint);
                case JoinImplementationType.Block_TxT: 
                    return this.Cost_Block_TxT(T1, T2, Threads, TupleRatioHint);
            }

            return double.MaxValue;

        }

        public JoinImplementationType LowestCost(TabularData T1, TabularData T2, int Threads, double TupleRatioHint)
        {

            JoinImplementationType MinType = JoinImplementationType.Block_TxT;
            double MinCost = double.MaxValue;

            foreach (JoinImplementationType t in JoinAlgorithm.JoinImplementationTypes)
            {

                double cost = this.Cost(T1, T2, Threads, TupleRatioHint, t);
                if (cost < MinCost)
                {
                    MinCost = cost;
                    MinType = t;
                }

            }

            return MinType;
        }

        // Sort Costs //
        public double Cost_FullSort(Extent E)
        {
            return Math.Log(E.Count, 2D) * E.Count;
        }

        public double Cost_FullSort(Volume V)
        {

            double AllRecords = 0; // (double)V.RecordCount;
            double AllExtents = (double)V.ExtentCount;
            double RecordPerExtentEstimate = AllRecords / AllExtents;
            return (Math.Log(RecordPerExtentEstimate, 2) * RecordPerExtentEstimate) * AllExtents + (AllExtents - 1) * AllExtents / 2D * RecordPerExtentEstimate;

        }

        public double Cost_FullSort(Table T)
        {

            double PartialSortPhase = 0D;
            double MergeSortPhase = 0D;
            double TotalRecordCount = (double)T.RecordCount;
            for (int i = 0; i < T.ExtentCount; i++)
            {

                PartialSortPhase += Math.Log(T.ReferenceTable[i][1].valueDOUBLE) * T.ReferenceTable[i][1].valueDOUBLE;
                MergeSortPhase += (TotalRecordCount - T.ReferenceTable[i][1].valueDOUBLE);

            }

            return PartialSortPhase + MergeSortPhase;

        }

        public double Cost_AvgExtentSize(TabularData T)
        {
            if (T.ExtentCount == 0)
                return 0;
            return (double)T.RecordCount / (double)T.ExtentCount;
        }

        public double Cost_AvgVolumeSize(TabularData T, int Threads)
        {
            if (T.ExtentCount <= Threads)
                return (double)T.RecordCount / T.ExtentCount;

            return (double)T.RecordCount / (double)Threads;

        }

        public double Cost_Volumes(TabularData T, int Threads)
        {
            if (T.ExtentCount <= Threads)
                return (double)T.ExtentCount;

            return (double)Threads;

        }

    }

    /// <summary>
    /// Merge algorithms for data that's either pre-sorted or will be sorted 
    /// </summary>
    public class SortMerge : JoinAlgorithm
    {

        public SortMerge()
            : base()
        {
            this.BaseJoinAlgorithmType = JoinAlgorithmType.SortMerge;
        }

        // Value x Value Joins //
        public override long InnerJoin(Extent LeftExtent, Register LeftMemory, Extent RightExtent, Register RightMemory, 
            RecordComparer JoinPredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            return this.SortMergeBase(LeftExtent, LeftMemory, RightExtent, RightMemory, JoinPredicate, Where, Output, OutputStream, null, true, false);
        }

        public override long LeftJoin(Extent LeftExtent, Register LeftMemory, Extent RightExtent, Register RightMemory, 
            RecordComparer JoinPredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream, BitArray NullReferenceMap)
        {
            return this.SortMergeBase(LeftExtent, LeftMemory, RightExtent, RightMemory, JoinPredicate, Where, Output, OutputStream, NullReferenceMap, true, true);
        }

        public override long AntiLeftJoin(Extent LeftExtent, Register LeftMemory, Extent RightExtent, Register RightMemory, 
            RecordComparer JoinPredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream, BitArray NullReferenceMap)
        {
            return this.SortMergeBase(LeftExtent, LeftMemory, RightExtent, RightMemory, JoinPredicate, Where, Output, OutputStream, NullReferenceMap, false, true);
        }

        // V x V Joins //
        public override long InnerJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, 
            RecordComparer JoinPredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            return this.SortMergeBase(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream, true, false);
        }

        public override long LeftJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, 
            RecordComparer JoinPredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            return this.SortMergeBase(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream, true, true);
        }

        public override long AntiLeftJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, 
            RecordComparer JoinPredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            return this.SortMergeBase(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream, false, true);
        }

        // Base Methods //
        private long SortMergeBase(Extent LeftTable, Register LeftMemory, Extent RightTable, Register RightMemory, RecordComparer JoinPredicate, 
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream, BitArray NullReferenceMap, bool Intersection, bool Difference)
        {

            // Do some checking //
            if (OutputStream.Columns.Count != Output.Count)
                throw new ArgumentException("The RecordWriter and ExpressionCollection passed have invalid lengths");
            if (Difference && NullReferenceMap == null)
                throw new ArgumentNullException("The left null map cannot be null if this is a left\anti-left\anti-inner\full join");

            // If either table is empty, just return //
            //if (LeftTable.Count == 0 || RightTable.Count == 0)
            //    return 0L;

            // Check the sort //
            //this.CheckSort(LeftTable, JoinPredicate.LeftKey);
            //this.CheckSort(RightTable, JoinPredicate.RightKey);

            // Create out work variables //
            int IndexLeft = 0;
            int IndexRight = 0;
            int CountLeft = LeftTable.Count;
            int CountRight = RightTable.Count;
            int NestedLoopSavePoint = 0;
            int CompareResult = 0;
            Record RecordLeft;
            Record RecordRight;
            long Tocks = 0;

            // Walk the two tables //
            while (IndexLeft < CountLeft && IndexRight < CountRight)
            {

                // Assign both the records and the registers //
                RecordLeft = LeftTable[IndexLeft];
                LeftMemory.Value = RecordLeft;
                RecordRight = RightTable[IndexRight];
                RightMemory.Value = RecordRight;

                // Do the comparison //
                CompareResult = JoinPredicate.Compare(RecordLeft, RecordRight);

                // If CompareResult > 0, left is higher than right, need to advance right //
                if (CompareResult > 0)
                {

                    // Only set the fail match for the left table //
                    IndexRight++;

                }
                // If CompareResult < 0, right is higher than left and need to advance left //
                else if (CompareResult < 0)
                {

                    if (Difference)
                        NullReferenceMap[IndexLeft] = true;
                    IndexLeft++;

                }
                // Otherwise the records are equal and we need to check for multiple tuples //
                else if (Intersection)
                {

                    // Save the loop-result //
                    NestedLoopSavePoint = IndexRight;

                    // Loop through all possible tuples //
                    while (CompareResult == 0)
                    {

                        // Render the record and potentially output //
                        Record x = Output.Evaluate();
                        if (Where.Render())
                        {
                            Tocks++;
                            OutputStream.Insert(x);
                        }

                        // Advance the right table //
                        IndexRight++;

                        // Check if this advancing pushed us to the end of the table //
                        if (IndexRight >= CountRight)
                            break;

                        // Read and assign the register //
                        RightMemory.Value = RightTable[IndexRight];

                        // Reset the compare token //
                        CompareResult = JoinPredicate.Compare(RightMemory.Value, LeftMemory.Value);

                    }

                    // Now reset the right table pointer //
                    IndexRight = NestedLoopSavePoint;

                    // Advance the left table pointer //
                    IndexLeft++;

                }
                else
                {

                    // Advance the left table pointer //
                    IndexLeft++;

                }


            }

            return Tocks;

        }

        private long SortMergeBase(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream, bool Intersection, bool Difference)
        {

            // Do some checking //
            if (OutputStream.Columns.Count != Output.Count)
                throw new ArgumentException("The RecordWriter and ExpressionCollection passed have invalid lengths");
            
            // Check the sort //
            //this.CheckSort(LeftVolume, JoinPredicate.LeftKey);
            //this.CheckSort(RightVolume, JoinPredicate.RightKey);

            // Create out work variables //
            ModeStep left = new ModeStep(LeftVolume, LeftMemory);
            ModeStep right = new ModeStep(RightVolume, RightMemory);
            int NestedLoopDepth = 0;
            int CompareResult = 0;
            long clicks = 0;

            // Start the current sort loop: note this should'nt run if either volume has no records / extents //
            while (!left.AtEnd && !right.AtEnd && !left.IsEmpty && !right.IsEmpty)
            {

                // Get the first record compare //
                CompareResult = JoinPredicate.Compare(left.Memory.Value, right.Memory.Value);

                //Console.WriteLine("{0} : {1} : {2}", CompareResult, Record.Split(left.Value.Value, JoinPredicate.LeftKey), Record.Split(right.Value.Value, JoinPredicate.RightKey));

                // If CompareResult > 0, left is higher than right, need to advance right //
                if (CompareResult > 0)
                {

                    // Only set the fail match for the left table //
                    right.Advance();

                }
                // If CompareResult < 0, right is higher than left and need to advance left //
                else if (CompareResult < 0)
                {

                    if (Difference)
                    {
                        Record r = right.Memory.Value;
                        RightMemory.Value = RightMemory.NullValue;
                        if (Where.Render())
                        {
                            OutputStream.Insert(Output.Evaluate());
                            clicks++;
                        }
                        RightMemory.Value = r;
                    }
                    left.Advance();

                }
                // Otherwise the records are equal and we need to check for multiple tuples //
                else if (Intersection)
                {

                    // Reset the nested loop depth tracker //
                    NestedLoopDepth = 0;

                    // Loop through all possible tuples //
                    while (CompareResult == 0)
                    {

                        // Render the record and potentially output //
                        if (Where.Render())
                        {
                            OutputStream.Insert(Output.Evaluate());
                            clicks++;
                        }

                        // Advance the right table //
                        right.Advance();
                        NestedLoopDepth++;

                        // Check if this advancing pushed us to the end of the table //
                        if (right.AtEnd)
                        {
                            break;
                        }

                        // Reset the compare token //
                        CompareResult = JoinPredicate.Compare(left.Memory.Value, right.Memory.Value);

                    }

                    // Now reset the right table pointer //
                    right.Revert(NestedLoopDepth);

                    // Advance the left table pointer //
                    left.Advance();

                } // Exit == predicate //
                else // If anti-join //
                {

                    // Advance the left table pointer //
                    left.Advance();

                }

            }

            // Do Anti-Join //
            if (Difference)
            {

                // Assign the right table to null //
                RightMemory.Value = RightMemory.NullValue;

                // Walk the rest of the left table //
                while (!left.AtEnd)
                {

                    if (Where.Render())
                    {
                        OutputStream.Insert(Output.Evaluate());
                        clicks++;
                    }

                    left.Advance();

                }

            }

            return clicks;

        }

        private long OptimizedFullJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory, RecordComparer JoinPredicate,
            Filter Where, ExpressionCollection Output, RecordWriter OutputStream, bool Intersection)
        {

            // Do some checking //
            if (OutputStream.Columns.Count != Output.Count)
                throw new ArgumentException("The RecordWriter and ExpressionCollection passed have invalid lengths");

            // If either table is empty, just return //
            //if (LeftVolume.RecordCount == 0 || RightVolume.RecordCount == 0)
            //    return 0L;

            // Sort //
            //this.CheckSort(LeftVolume, JoinPredicate.LeftKey);
            //this.CheckSort(RightVolume, JoinPredicate.RightKey);

            // Create out work variables //
            ModeStep left = new ModeStep(LeftVolume, LeftMemory);
            ModeStep right = new ModeStep(RightVolume, RightMemory);
            int NestedLoopDepth = 0;
            int CompareResult = 0;
            long clicks = 0;

            // Start the current sort loop //
            while (!left.AtEnd && !right.AtEnd && !left.IsEmpty && !right.IsEmpty)
            {

                CompareResult = JoinPredicate.Compare(left.Memory.Value, right.Memory.Value);

                // If CompareResult > 0, left is higher than right, need to advance right //
                if (CompareResult > 0)
                {

                    Record r = left.Memory.Value;
                    LeftMemory.Value = LeftMemory.NullValue;
                    if (Where.Render())
                    {
                        OutputStream.Insert(Output.Evaluate());
                    }
                    LeftMemory.Value = r;
                    right.Advance();
                    Console.WriteLine("A");

                }
                // If CompareResult < 0, right is higher than left and need to advance left //
                else if (CompareResult < 0)
                {

                    Record r = right.Memory.Value;
                    RightMemory.Value = RightMemory.NullValue;
                    if (Where.Render())
                    {
                        OutputStream.Insert(Output.Evaluate());
                    }
                    RightMemory.Value = r;
                    left.Advance();
                    Console.WriteLine("B");

                }
                // Otherwise the records are equal and we need to check for multiple tuples //
                else if (Intersection)
                {

                    // Reset the nested loop depth tracker //
                    NestedLoopDepth = 0;

                    // Loop through all possible tuples //
                    while (CompareResult == 0)
                    {

                        // Render the record and potentially output //
                        if (Where.Render())
                        {
                            OutputStream.Insert(Output.Evaluate());
                            clicks++;
                        }

                        // Advance the right table //
                        right.Advance();
                        NestedLoopDepth++;

                        // Check if this advancing pushed us to the end of the table //
                        if (right.AtEnd)
                        {
                            break;
                        }

                        // Reset the compare token //
                        CompareResult = JoinPredicate.Compare(left.Memory.Value, right.Memory.Value);
                        Console.WriteLine("C");
                    }

                    // Now reset the right table pointer //
                    right.Revert(NestedLoopDepth);

                    // Advance the left table pointer //
                    left.Advance();

                } // Exit == predicate //
                else // If anti-join //
                {

                    // Advance the left table pointer //
                    left.Advance();
                    Console.WriteLine("D");

                }

            }

            // Walk the rest of the left table //
            Record s = RightMemory.Value;
            RightMemory.Value = RightMemory.NullValue;
            //Console.WriteLine("{0} : {1}", left.ExtentPosition, left.RecordPosition);
            while (!left.AtEnd)
            {

                if (Where.Render())
                {
                    OutputStream.Insert(Output.Evaluate());
                    clicks++;
                }
                left.Advance();

            }

            // Walk the rest of the left table //
            RightMemory.Value = s;
            LeftMemory.Value = LeftMemory.Columns.NullRecord;
            //Console.WriteLine("{0} : {1}", right.ExtentPosition, right.RecordPosition);
            while (!right.AtEnd)
            {

                if (Where.Render())
                {
                    OutputStream.Insert(Output.Evaluate());
                    clicks++;
                }
                right.Advance();

            }

            // Finsih //
            return clicks;

        }

        private void CheckSort(Volume Datum, Key K)
        {
            if (Key.EqualsStrong(Datum.SortKey, K))
                return;
            Datum.Sort(K);
        }

        private void CheckSort(Extent Datum, Key K)
        {
            if (Key.EqualsStrong(Datum.SortBy, K))
                return;
            SortMaster.Sort(Datum, K);
        }

        // Cost metrics //
        public override double Cost_Block_ExE(TabularData T1, TabularData T2, int Threads, double TupleRatioHint)
        {
            double avg1 = this.Cost_AvgExtentSize(T1);
            double avg2 = this.Cost_AvgExtentSize(T2);
            double Combinations = T1.ExtentCount * T2.ExtentCount;
            double ThreadFactor = Math.Min(Combinations, Threads);
            return Math.Max(avg1, avg2) * Combinations * TupleRatioHint / ThreadFactor;
        }

        public override double Cost_Block_ExV(TabularData T1, TabularData T2, int Threads, double TupleRatioHint)
        {

            double avg1 = this.Cost_AvgExtentSize(T1);
            double avg2 = this.Cost_AvgVolumeSize(T2, Threads);
            double Combinations = T1.ExtentCount * Math.Min(T2.ExtentCount, Threads);
            double ThreadFactor = Math.Min(Combinations, Threads);
            return Math.Max(avg1, avg2) * Combinations * TupleRatioHint / ThreadFactor;

        }

        public override double Cost_Block_ExT(TabularData T1, TabularData T2, int Threads, double TupleRatioHint)
        {

            double avg1 = this.Cost_AvgExtentSize(T1);
            double avg2 = T2.RecordCount;
            double Combinations = T1.ExtentCount * Math.Min(T2.ExtentCount, Threads);
            double ThreadFactor = Math.Min(Combinations, Threads);
            return Math.Max(avg1, avg2) * Combinations * TupleRatioHint / ThreadFactor;

        }

        public override double Cost_Block_VxV(TabularData T1, TabularData T2, int Threads, double TupleRatioHint)
        {

            double avg1 = this.Cost_AvgVolumeSize(T1, Threads);
            double avg2 = this.Cost_AvgVolumeSize(T2, Threads);
            double Combinations = Math.Min(T2.ExtentCount + T1.ExtentCount, Threads);
            double ThreadFactor = Math.Min(Combinations, Threads);
            return Math.Max(avg1, avg2) * Combinations * TupleRatioHint / ThreadFactor;

        }

        public override double Cost_Block_VxT(TabularData T1, TabularData T2, int Threads, double TupleRatioHint)
        {

            double avg1 = this.Cost_AvgVolumeSize(T1, Threads);
            double avg2 = T2.RecordCount;
            double Combinations = T1.ExtentCount;
            double ThreadFactor = Math.Min(Combinations, Threads);
            return Math.Max(avg1, avg2) * Combinations * TupleRatioHint / ThreadFactor;

        }

        public override double Cost_Block_TxT(TabularData T1, TabularData T2, int Threads, double TupleRatioHint)
        {

            double avg1 = T1.RecordCount;
            double avg2 = T2.RecordCount;
            double Combinations = 1;
            double ThreadFactor = Math.Min(Combinations, Threads);
            return Math.Max(avg1, avg2) * Combinations * TupleRatioHint / ThreadFactor;

        }

    }

    /// <summary>
    /// Merge algorithms that use nested loops
    /// </summary>
    public class NestedLoop : JoinAlgorithm
    {

        public NestedLoop()
            : base()
        {
            this.BaseJoinAlgorithmType = JoinAlgorithmType.NestedLoop;
        }

        // Value x Value Joins //
        public override long InnerJoin(Extent LeftExtent, Register LeftMemory, Extent RightExtent, Register RightMemory,
            RecordComparer JoinPredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            return this.NestedLoopBase(LeftExtent, LeftMemory, RightExtent, RightMemory, JoinPredicate, Where, Output, OutputStream, null, true, false);
        }

        public override long LeftJoin(Extent LeftExtent, Register LeftMemory, Extent RightExtent, Register RightMemory,
            RecordComparer JoinPredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream, BitArray NullReferenceMap)
        {
            return this.NestedLoopBase(LeftExtent, LeftMemory, RightExtent, RightMemory, JoinPredicate, Where, Output, OutputStream, NullReferenceMap, true, true);
        }

        public override long AntiLeftJoin(Extent LeftExtent, Register LeftMemory, Extent RightExtent, Register RightMemory,
            RecordComparer JoinPredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream, BitArray NullReferenceMap)
        {
            return this.NestedLoopBase(LeftExtent, LeftMemory, RightExtent, RightMemory, JoinPredicate, Where, Output, OutputStream, NullReferenceMap, false, true);
        }

        // V x V Joins //
        public override long InnerJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory,
            RecordComparer JoinPredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            return this.NestedLoopBase(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream, true, false);
        }

        public override long LeftJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory,
            RecordComparer JoinPredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            return this.NestedLoopBase(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream, true, true);
        }

        public override long AntiLeftJoin(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory,
            RecordComparer JoinPredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream)
        {
            return this.NestedLoopBase(LeftVolume, LeftMemory, RightVolume, RightMemory, JoinPredicate, Where, Output, OutputStream, false, true);
        }

        // Base algorithms //
        private long NestedLoopBase(Extent LeftExtent, Register LeftMemory, Extent RightExtent, Register RightMemory,
            RecordComparer MergePredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream, BitArray NullReferenceMap,
            bool Intersection, bool Difference)
        {

            int IndexLeft = 0;
            int IndexRight = 0;
            int CountLeft = LeftExtent.Count;
            int CountRight = RightExtent.Count;
            bool MatchFound = false;
            long Clicks = 0;
            
            for (IndexLeft = 0; IndexLeft < CountLeft; IndexLeft++)
            {

                // Assign the outer register //
                LeftMemory.Value = LeftExtent[IndexLeft];

                // Turn the MatchFound variable back to false //
                MatchFound = false;

                // Start the inner loop //
                for (IndexRight = 0; IndexRight < CountRight; IndexRight++)
                {

                    // Assing the inner register //
                    RightMemory.Value = RightExtent[IndexRight];

                    // See if the records jive //
                    if (MergePredicate.Equals(LeftMemory.Value, RightMemory.Value) && Where.Render())
                    {

                        // Turn the flag on //
                        MatchFound = true;

                        // Output //
                        if (Intersection)
                        {
                            OutputStream.Insert(Output.Evaluate());
                            NullReferenceMap[IndexLeft] = false;
                            Clicks++;
                        }
                        
                    }

                }

                // Check if we failed to find a match //
                if (!MatchFound && Difference)
                {
                    NullReferenceMap[IndexLeft] = true;
                }

            }

            return Clicks;

        }

        private long NestedLoopBase(Volume LeftVolume, Register LeftMemory, Volume RightVolume, Register RightMemory,
            RecordComparer MergePredicate, Filter Where, ExpressionCollection Output, RecordWriter OutputStream,
            bool Intersection, bool Difference)
        {

            // Tcker //
            long clicks = 0;

            // Go through each left extent //
            foreach (Extent left in LeftVolume.Extents)
            {

                // Create a bit array //
                BitArray NullMap = new BitArray(left.Count, false);

                // Go through each right extent //
                foreach (Extent right in RightVolume.Extents)
                {

                    // Do nested loop //
                    clicks += this.NestedLoopBase(left, LeftMemory, right, RightMemory, MergePredicate, Where, Output, OutputStream, NullMap, Intersection, Difference);

                }

                // Handle the null refernece map //
                if (Difference)
                {
                    clicks += this.Collapse(left, LeftMemory, RightVolume.Columns.NullRecord, RightMemory, Where, Output, OutputStream, NullMap);
                }

            }

            return clicks;

        }

        // Costs //
        public override double Cost_Block_ExE(TabularData T1, TabularData T2, int Threads, double CardnalityHint)
        {
            double ThreadFactor = Math.Min((double)(T1.ExtentCount * T2.ExtentCount), (double)Threads);
            return (double)(T1.RecordCount * T2.RecordCount) / ThreadFactor;
        }

        public override double Cost_Block_ExV(TabularData T1, TabularData T2, int Threads, double CardnalityHint)
        {
            double ThreadFactor = Math.Min((double)T1.ExtentCount, (double)Threads);
            return (double)(T1.RecordCount * T2.RecordCount) / ThreadFactor;
        }

        public override double Cost_Block_ExT(TabularData T1, TabularData T2, int Threads, double CardnalityHint)
        {
            double ThreadFactor = Math.Min((double)T1.ExtentCount, (double)Threads);
            return (double)(T1.RecordCount * T2.RecordCount) / ThreadFactor;
        }

        public override double Cost_Block_VxV(TabularData T1, TabularData T2, int Threads, double CardnalityHint)
        {
            double ThreadFactor = Math.Min((double)Math.Max(T1.ExtentCount, T2.ExtentCount), (double)Threads);
            return (double)(T1.RecordCount * T2.RecordCount) / ThreadFactor;
        }

        public override double Cost_Block_VxT(TabularData T1, TabularData T2, int Threads, double CardnalityHint)
        {
            double ThreadFactor = Math.Min((double)T1.ExtentCount, (double)Threads);
            return (double)(T1.RecordCount * T2.RecordCount) / ThreadFactor;
        }

        public override double Cost_Block_TxT(TabularData T1, TabularData T2, int Threads, double CardnalityHint)
        {
            return (double)(T1.RecordCount * T2.RecordCount);
        }


    }

    /// <summary>
    /// Provides a system to join tables
    /// </summary>
    public class JoinModel
    {

        protected Session _Session;

        protected JoinAlgorithm _BaseAlgorithm;
        protected JoinType _BaseType;
        protected TabularData _LeftTable;
        protected TabularData _RightTable;
        protected string _LeftAlias;
        protected string _RightAlias;
        protected TabularData _Output;
        protected RecordComparer _JoinPredicate;
        protected Filter _Where;
        protected ExpressionCollection _Fields;
        
        public JoinModel(Session Session)
        {
            this._Session = Session;
            this._BaseAlgorithm = new SortMerge();
            this._BaseType = JoinType.Inner;
            this._Where = Filter.TrueForAll;
            this._Fields = new ExpressionCollection();
        }

        public void SetLEFT(TabularData Value, string Alias)
        {
            this._LeftTable = Value;
            this._LeftAlias = Alias;
        }

        public void SetRIGHT(TabularData Value, string Alias)
        {
            this._RightTable = Value;
            this._RightAlias = Alias;
        }

        public void SetOUTPUT(TabularData Value)
        {
            this._Output = Value;
        }

        public void SetPREDICATE(RecordComparer Value)
        {
            this._JoinPredicate = Value;
        }

        public void SetWHERE(Filter Value)
        {
            this._Where = Value;
        }

        public void AddRETAIN(Expression Value, string Alias)
        {
            this._Fields.Add(Value, Alias);
        }

        public void AddRETAIN(ExpressionCollection Value)
        {

            for (int i = 0; i < Value.Count; i++)
            {
                this._Fields.Add(Value[i], Value.Alias(i));
            }

        }

        public void SetTYPE(JoinType Value)
        {
            this._BaseType = Value;
        }

        public void SetALGORITHM(JoinAlgorithm Value)
        {
            this._BaseAlgorithm = Value;
        }

        // Create a single process node //
        public JoinProcessNode RenderNode(int ThreadID, int ThreadCount)
        {

            // Create the volume //
            Volume left = this._LeftTable.CreateVolume(ThreadID, ThreadCount);
            Volume right = this._RightTable.CreateVolume(ThreadID, ThreadCount);

            // Create two registers //
            Register lmem = new Register(this._LeftAlias, this._LeftTable.Columns);
            Register rmem = new Register(this._RightAlias, this._RightTable.Columns);

            // Create the memory envrioment //
            CloneFactory spiderweb = new CloneFactory();
            spiderweb.Append(lmem);
            spiderweb.Append(rmem);
            spiderweb.Append(this._Session.Scalars); // Add in the global scalars
            spiderweb.Append(this._Session.Matrixes); // Add in the global matrixes

            // Create clones of all our inputs //
            ExpressionCollection fields = spiderweb.Clone(this._Fields);
            Filter where = spiderweb.Clone(this._Where);

            // Create the output stream //
            RecordWriter out_stream = this._Output.OpenWriter();

            // Return a node //
            return new JoinProcessNode(ThreadID, this._Session, this._BaseAlgorithm, this._BaseType, left, lmem, right, rmem, this._JoinPredicate, where, fields, out_stream);

        }

        public List<JoinProcessNode> RenderNodes(int ThreadCount)
        {

            List<JoinProcessNode> nodes = new List<JoinProcessNode>();

            for (int i = 0; i < ThreadCount; i++)
            {
                nodes.Add(this.RenderNode(i, ThreadCount));
            }

            return nodes;

        }



    }

}
