using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.Data
{

    /// <summary>
    /// Provides support for concurrent writes
    /// </summary>
    public interface IConcurrentWriteManager
    {

        /// <summary>
        /// Adds an extent to the underlying data structure
        /// </summary>
        /// <param name="E"></param>
        void AddExtent(Extent E);

        /// <summary>
        /// Gets a shell extent
        /// </summary>
        /// <returns></returns>
        Extent GetExtent();

        /// <summary>
        /// Writes the current data to the underlying structure
        /// </summary>
        void Collapse();

    }

    public class ConcurrentTableWriteManager : IConcurrentWriteManager
    {

        // Controll variables //
        private long _MaxRecords = 0;
        private Table _Source;
        private Extent _Current;
        private object _Lock = new object();

        // Flags for the current extent being already a part of the table //
        private bool _CurrentIsLastPop = false;

        public ConcurrentTableWriteManager(Table Source)
        {

            // Set up the source //
            this._Source = Source;

            // If the last extent is full, create a new one and use that to store the data //
            this._Current = this._Source.NewShell();
            if (this._Source.ExtentCount != 0)
            {

                Extent e = this._Source.PopLast();
                if (!e.IsFull)
                {
                    this._Current = e;
                    this._CurrentIsLastPop = true; // Tag this as the last pop if we're pulling in the last pop
                }

            }
            else
            {
                this._Current = Source.PopLastOrGrow();
                this._CurrentIsLastPop = true; // Tag this as the last pop if we're on the very first extent
            }

            this._MaxRecords = (this._Source.Header.PageSize / this._Source.Columns.RecordDiskCost);

        }

        public void AddExtent(Extent E)
        {

            lock (this._Lock)
            {

                // Step one: check the extent schema //
                if (E.Columns.GetHashCode() != this._Current.Columns.GetHashCode())
                {
                    throw new ArgumentException("The schemas for the extent passed does not match the source table");
                }

                // Step two: see if we can add a range of data outright
                if (E.Count + this._Current.Count <= this._MaxRecords)
                {
                    this._Current._Cache.AddRange(E._Cache);
                    return; // At this point, we've processed the entire extent
                }

                // Step three: determine the splits //
                int LowerRange1 = 0, CountRange1 = (int)(this._MaxRecords - this._Current.Count);
                int LowerRange2 = CountRange1, CountRange2 = E.Count - CountRange1;

                // Step four: append the current extent //
                this._Current._Cache.AddRange(E._Cache.GetRange(LowerRange1, CountRange1));

                // Step five: pass the current extent back to the table and get a fresh extent //
                if (this._CurrentIsLastPop)
                {
                    // If the current extent is the last pop, we need to set it rather than add it //
                    this._Source.SetExtent(this._Current);
                    this._CurrentIsLastPop = false;
                }
                else
                {
                    this._Source.AddExtent(this._Current);
                }
                this._Current = this._Source.NewShell();

                // Step six: append the rest of the data //
                this._Current._Cache.AddRange(E._Cache.GetRange(LowerRange2, CountRange2));

            }

        }

        public Extent GetExtent()
        {

            lock (this._Lock)
            {
                Extent e = this._Source.NewShell();
                e.Header.PageSize = this._Source.Header.PageSize;
                return e;
            }

        }

        public void Collapse()
        {

            if (this._CurrentIsLastPop)
            {
                this._Source.SetExtent(this._Current);
            }
            else
            {
                this._Source.AddExtent(this._Current);
            }

        }

    }

    public class ConcurrentExtentWriteManager : IConcurrentWriteManager
    {

        // Controll variables //
        private long _MaxRecords = 0;
        private Extent _Source;
        private object _Lock = new object();

        public ConcurrentExtentWriteManager(Extent Source)
        {

            // Set up the source //
            this._Source = Source;
            this._MaxRecords = (this._Source.Header.PageSize / this._Source.Columns.RecordDiskCost);

        }

        public void AddExtent(Extent E)
        {

            lock (this._Lock)
            {

                // Step one: check the extent schema //
                if (E.Columns.GetHashCode() != this._Source.Columns.GetHashCode())
                {
                    throw new ArgumentException("The schemas for the extent passed does not match the source extent");
                }

                // Step two: see if we can add a range of data outright
                if (E.Count + this._Source.Count <= this._MaxRecords)
                {
                    this._Source._Cache.AddRange(E._Cache);
                    return; // At this point, we've processed the entire extent
                }

                // Step three: throw an exception because the extent is now full //
                throw new OverflowException("The extent cannot be added without overflowing the source extent");

            }

        }

        public Extent GetExtent()
        {

            lock (this._Lock)
            {
                Extent e = new Extent(this._Source.Columns);
                e.Header.PageSize = this._Source.Header.PageSize;
                return e;
            }

        }

        public void Collapse()
        {

            // Do nothing

        }

    }

}
