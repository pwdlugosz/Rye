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

        // Extent //
        public static long Sort(Extent A, RecordComparer RC)
        {

            A.Cache.Sort(RC);
            return RC.Clicks;

        }

        public static long Sort(Extent A, Key K)
        {
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

            // Variables //
            Extent x = new Extent(A.Columns);
            Extent y = new Extent(B.Columns);
            x.MaxRecords = A.MaxRecords;
            y.MaxRecords = B.MaxRecords;
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
            Extent.SetCache(A, x);
            Extent.SetCache(B, y);

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
            foreach (Header h in A.Headers)
            {

                // Buffer record set //
                Extent e = Kernel.RequestBufferExtent(h.Path);

                // Check if it is sorted //
                Clicks += SortMaster.Sort(e, RC);

                // FlushRecordUnion //
                Kernel.RequestFlushExtent(e);

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
                Kernel.RequestFlushExtent(e);

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

        // Table //
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

            // Step two: do cartesian sort n x (n - 1) //
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
            Kernel.RequestFlushTable(A);

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

            // Step two: do cartesian sort n x (n - 1) //
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
            Kernel.RequestFlushTable(A);

            return Clicks;

        }

        public static long Sort(Table A, Key K)
        {
            KeyedRecordComparer rc = new KeyedRecordComparer(K);
            A.SortBy = K;
            return SortMaster.Sort(A, rc);
        }

        public static long Sort(Table A, Key K, int[] Extents)
        {
            KeyedRecordComparer rc = new KeyedRecordComparer(K);
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
            return SortMaster.Sort(A, rc);
        }

        public static long Sort(Table A, int[] Extents)
        {
            Key k = Key.Build(A.Columns.Count);
            RecordComparer rc = new KeyedRecordComparer(k);
            return SortMaster.Sort(A, rc, Extents);
        }

    }

}
