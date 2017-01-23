using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Expressions;

namespace Rye.Data
{

    public static class SortMaster
    {

        // Shard //
        public static long Sort(Extent A, RecordComparer RC)
        {

            A.Cache.Sort(RC);
            return RC.Clicks;

        }

        public static long Sort(Extent A, Key K)
        {

            // Checks if K is a subset of A, meaning if A is sorted by 0,1,2,3 and K is 0,1 then we technically don't need to sort //
            if (KeyComparer.IsStrongSubset(A.SortBy ?? new Key(), K))
                return 0;

            KeyedRecordComparer rc = new KeyedRecordComparer(K);
            A.SortBy = K;
            return SortMaster.Sort(A, rc);

        }

        public static long Sort(Extent A, ExpressionCollection E, Register R)
        {
            RecordComparer rc = new ExpressionRecordComparer(E, R);
            return SortMaster.Sort(A, rc);
        }

        public static long Sort(Extent A, ExpressionCollection E, Register R, Key K)
        {
            RecordComparer rc = new ExpressionSortComparer(E, R, K);
            return SortMaster.Sort(A, rc);
        }

        public static long Sort(Extent A)
        {
            Key k = Key.Build(A.Columns.Count);
            RecordComparer rc = new KeyedRecordComparer(k);
            return SortMaster.Sort(A, rc);
        }

        // Sort Merge //
        public static long SortMerge(Extent A, Extent B, RecordComparer RC)
        {

            // Before we do anything, check if A's last record is less than B's first
            if (RC.Compare(A.Cache.Last(), B.Cache.First()) < 0)
            {
                return 0;
            }

            // Variables //
            Extent x = new Extent(A.Columns, A.Header.PageSize);
            Extent y = new Extent(B.Columns, B.Header.PageSize);
            int CompareResult = 0;

            // Main record loop //
            int ptra = 0, ptrb = 0;
            while (ptra < A.Count && ptrb < B.Count)
            {

                // Compare results //
                CompareResult = RC.Compare(A[ptra], B[ptrb]);

                if (CompareResult <= 0 && x.Count < A.Count)
                {
                    x.Add(A[ptra]);
                    ptra++;
                }
                else if (CompareResult > 0 && x.Count < A.Count)
                {
                    x.Add(B[ptrb]);
                    ptrb++;
                }
                else if (CompareResult <= 0 && x.Count >= A.Count)
                {
                    y.Add(A[ptra]);
                    ptra++;
                }
                else if (CompareResult > 0 && x.Count >= A.Count)
                {
                    y.Add(B[ptrb]);
                    ptrb++;
                }

            }

            // Note: for the clean up phase, at most only of these will execute becuase by definition
            // we must have gone through (at least) one of the tables.

            // Clean up first shard //
            while (ptra < A.Count)
            {

                if (x.Count < A.Count)
                    x.Add(A[ptra]);
                else
                    y.Add(A[ptra]);
                ptra++;

            }

            // Clean up second shard //
            while (ptrb < B.Count)
            {

                if (x.Count < A.Count)
                    x.Add(B[ptrb]);
                else
                    y.Add(B[ptrb]);
                ptrb++;

            }

            // Set the heaps //
            Extent.SetCache(A, x); // Has the smallest records
            Extent.SetCache(B, y); // Has the largest records

            return RC.Clicks;

        }

        public static long SortMerge(Extent A, Extent B, Key K)
        {
            KeyedRecordComparer rc = new KeyedRecordComparer(K);
            return SortMaster.SortMerge(A, B, rc);
        }

        public static long SortMerge(Extent A, Extent B, ExpressionCollection E, Register R)
        {

            RecordComparer rc = new ExpressionRecordComparer(E, R);
            return SortMaster.SortMerge(A, B, rc);

        }

        public static long SortMerge(Extent A, Extent B, ExpressionCollection E, Register R, Key K)
        {

            RecordComparer rc = new ExpressionSortComparer(E, R, K);
            return SortMaster.SortMerge(A, B, rc);

        }

        public static long SortMerge(Extent A, Extent B)
        {
            Key k = Key.Build(A.Columns.Count);
            RecordComparer rc = new KeyedRecordComparer(k);
            return SortMaster.SortMerge(A, B, rc);
        }

        // Sort Each //
        public static long SortEach(Table A, RecordComparer RC)
        {

            long Clicks = 0;
            for (int i = 0; i < A.ExtentCount; i++)
            {

                // Buffer record set //
                Extent e = A.IO.RequestBufferExtent(A, i);

                // Check if it is sorted //
                Clicks += SortMaster.Sort(e, RC);

                // FlushRecordUnion //
                A.IO.RequestFlushExtent(e);

            }

            return Clicks;

        }

        public static long SortEach(Table A, RecordComparer RC, int[] Extents)
        {

            long Clicks = 0;
            foreach (int idx in Extents)
            {

                // Buffer record set //
                Extent e = A.GetExtent(idx);

                // Check if it is sorted //
                Clicks += SortMaster.Sort(e, RC);

                // FlushRecordUnion //
                A.IO.RequestFlushExtent(e);

            }
            return Clicks;

        }

        public static long SortEach(Table A, Key K)
        {
            KeyedRecordComparer rc = new KeyedRecordComparer(K);
            return SortMaster.SortEach(A, rc);
        }

        public static long SortEach(Table A, Key K, int[] Extents)
        {
            KeyedRecordComparer rc = new KeyedRecordComparer(K);
            return SortMaster.SortEach(A, rc, Extents);
        }

        public static long SortEach(Table A, ExpressionCollection E, Register R)
        {
            RecordComparer rc = new ExpressionRecordComparer(E, R);
            return SortMaster.SortEach(A, rc);
        }

        public static long SortEach(Table A, ExpressionCollection E, Register R, int[] Extents)
        {
            RecordComparer rc = new ExpressionRecordComparer(E, R);
            return SortMaster.SortEach(A, rc, Extents);
        }

        public static long SortEach(Table A, ExpressionCollection E, Register R, Key K)
        {
            RecordComparer rc = new ExpressionSortComparer(E, R, K);
            return SortMaster.SortEach(A, rc);
        }

        public static long SortEach(Table A, ExpressionCollection E, Register R, Key K, int[] Extents)
        {
            RecordComparer rc = new ExpressionSortComparer(E, R, K);
            return SortMaster.SortEach(A, rc, Extents);
        }

        public static long SortEach(Table A)
        {
            Key k = Key.Build(A.Columns.Count);
            RecordComparer rc = new KeyedRecordComparer(k);
            return SortMaster.SortEach(A, rc);
        }

        public static long SortEach(Table A, int[] Extents)
        {
            Key k = Key.Build(A.Columns.Count);
            RecordComparer rc = new KeyedRecordComparer(k);
            return SortMaster.SortEach(A, rc, Extents);
        }

        // ShartTable //
        public static long Sort(Table A, RecordComparer RC)
        {

            // Variables //
            int ptr_FirstShard = 0;
            int ptr_SecondShard = 0;
            int SortCount = 0;
            long Clicks = 0;

            // Step one: sort all shards //
            Clicks += SortMaster.SortEach(A, RC);

            // if only one shard, break //
            if (A.ExtentCount == 1)
                return Clicks;

            // Step two: do cartesian sort n OriginalNode (n - 1) //
            while (ptr_FirstShard < A.ExtentCount)
            {

                // Secondary loop //
                ptr_SecondShard = ptr_FirstShard + 1;
                while (ptr_SecondShard < A.ExtentCount)
                {

                    // Open shards //
                    Extent t1 = A.GetExtent(ptr_FirstShard);
                    Extent t2 = A.GetExtent(ptr_SecondShard);

                    // Sort merge both shards //
                    Clicks += SortMaster.SortMerge(t1, t2, RC);

                    // Close both shards //
                    A.SetExtent(t1);
                    A.SetExtent(t2);

                    // Increment //
                    ptr_SecondShard++;
                    SortCount++;

                }

                // Increment //
                ptr_FirstShard++;

            }

            // Flush //
            A.RequestFlushMe();

            // Return cost //
            return Clicks;

        }

        public static long Sort(Table A, RecordComparer RC, int[] Extents)
        {

            // Variables //
            int ptr_FirstShard = 0;
            int ptr_SecondShard = 0;
            int SortCount = 0;

            // Step one: sort all shards //
            long Clicks = SortMaster.SortEach(A, RC, Extents);

            // if only one shard, break //
            if (A.ExtentCount == 1)
                return Clicks;

            // Step two: do cartesian sort n OriginalNode (n - 1) //
            while (ptr_FirstShard < Extents.Length)
            {

                // Secondary loop //
                ptr_SecondShard = ptr_FirstShard + 1;
                while (ptr_SecondShard < Extents.Length)
                {

                    // Open shards //
                    Extent t1 = A.GetExtent(Extents[ptr_FirstShard]);
                    Extent t2 = A.GetExtent(Extents[ptr_SecondShard]);

                    // Sort merge both shards //
                    Clicks = SortMaster.SortMerge(t1, t2, RC);

                    // Close both shards //
                    A.SetExtent(t1);
                    A.SetExtent(t2);

                    // Increment //
                    ptr_SecondShard++;
                    SortCount++;

                }

                // Increment //
                ptr_FirstShard++;

            }

            // Flush //
            A.RequestFlushMe();

            return Clicks;

        }

        public static long Sort(Table A, Key K)
        {

            // Checks if K is a subset of A, meaning if A is sorted by 0,1,2,3 and K is 0,1 then we technically don't need to sort //
            if (KeyComparer.IsStrongSubset(A.SortBy ?? new Key(), K))
                return 0;
            KeyedRecordComparer rc = new KeyedRecordComparer(K);
            A.SortBy = K;
            return SortMaster.Sort(A, rc);
        }

        public static long Sort(Table A, Key K, int[] Extents)
        {

            // Checks if K is a subset of A, meaning if A is sorted by 0,1,2,3 and K is 0,1 then we technically don't need to sort //
            if (KeyComparer.IsStrongSubset(A.SortBy ?? new Key(), K))
                return 0;

            KeyedRecordComparer rc = new KeyedRecordComparer(K);
            A.SortBy = K;
            return SortMaster.Sort(A, rc, Extents);

        }

        public static long Sort(Table A, ExpressionCollection E, Register R)
        {
            RecordComparer rc = new ExpressionRecordComparer(E, R);
            return SortMaster.Sort(A, rc);
        }

        public static long Sort(Table A, ExpressionCollection E, Register R, int[] Extents)
        {
            RecordComparer rc = new ExpressionRecordComparer(E, R);
            return SortMaster.Sort(A, rc, Extents);
        }

        public static long Sort(Table A, ExpressionCollection E, Register R, Key K)
        {
            RecordComparer rc = new ExpressionSortComparer(E, R, K);
            return SortMaster.Sort(A, rc);
        }

        public static long Sort(Table A, ExpressionCollection E, Register R, Key K, int[] Extents)
        {
            RecordComparer rc = new ExpressionSortComparer(E, R, K);
            return SortMaster.Sort(A, rc, Extents);
        }

        public static long Sort(Table A)
        {
            Key k = Key.Build(A.Columns.Count);
            RecordComparer rc = new KeyedRecordComparer(k);
            A.SortBy = k;
            return SortMaster.Sort(A, rc);
        }

        public static long Sort(Table A, int[] Extents)
        {
            Key k = Key.Build(A.Columns.Count);
            RecordComparer rc = new KeyedRecordComparer(k);
            A.SortBy = k;
            return SortMaster.Sort(A, rc, Extents);
        }

        // Check sorts //
        public static bool CheckSort(Extent E, RecordComparer RC)
        {

            if (E.Count == 0)
                return false;
            else if (E.Count == 1)
                return true;

            for (int i = 1; i < E.Count; i++)
            {

                if (RC.Compare(E[i - 1], E[i]) > 0)
                    return false;

            }

            return true;

        }

    }

}
