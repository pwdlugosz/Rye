using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Structures;
using Rye.Aggregates;
using Rye.Methods;
using Rye.MatrixExpressions;

namespace Rye.Query
{

    /*
     * This class is designed to allow full cloning of all expression, method, aggregate and matrix objects while preserving the relationships between
     * each object's register collection, cell heaps, and cell-matrix heaps.
     * 
     * The 'Append' methods will add a given reference to the memory collection object.
     * The 'Extract' methods will pull all references from a given object and load it to the memory object.
     * The 'Link' methods will apply all memory references to a given object
     * The 'Clone' methods will create copies of objects passed and link them to this memory ref instance
     * 
     */

    /// <summary>
    /// Encapsolates all potential objects an expression may contain.
    /// </summary>
    public sealed class CloneFactory
    {

        private Heap<Register> _RegisterRefs;
        private Heap<Heap<Cell>> _CellHeapRefs;
        private Heap<Heap<CellMatrix>> _CellMatrixHeapRefs;

        public CloneFactory()
        {

            this._RegisterRefs = new Heap<Register>();
            this._CellHeapRefs = new Heap<Heap<Cell>>();
            this._CellMatrixHeapRefs = new Heap<Heap<CellMatrix>>();

        }

        // Memory Objects //
        /// <summary>
        /// Gets the inner register collection
        /// </summary>
        /// <param name="Name"></param>
        /// <returns></returns>
        public Register InnerRegister(string Name)
        {
            return this._RegisterRefs[Name];
        }

        /// <summary>
        /// Gets an element from the inner register collection
        /// </summary>
        /// <returns></returns>
        public Heap<Register> InnerRegisters()
        {
            return this._RegisterRefs;
        }

        /// <summary>
        /// Gets an element from the inner cell heap collection
        /// </summary>
        /// <param name="Name"></param>
        /// <returns></returns>
        public Heap<Cell> InnerScalarHeap(string Name)
        {
            return this._CellHeapRefs[Name];
        }

        /// <summary>
        /// Gets the inner cell heap collection
        /// </summary>
        /// <param name="Name"></param>
        /// <returns></returns>
        public Heap<Heap<Cell>> InnerScalarHeaps()
        {
            return this._CellHeapRefs;
        }

        /// <summary>
        /// Gets an element from the inner cell matrix heap collection
        /// </summary>
        /// <param name="Name"></param>
        /// <returns></returns>
        public Heap<CellMatrix> InnerMatrixHeap(string Name)
        {
            return this._CellMatrixHeapRefs[Name];
        }

        /// <summary>
        /// Gets the inner cell matrix heap collection
        /// </summary>
        /// <param name="Name"></param>
        /// <returns></returns>
        public Heap<Heap<CellMatrix>> InnerMatrixHeaps()
        {
            return this._CellMatrixHeapRefs;
        }

        // Appending Methods //
        /// <summary>
        /// Adds a given register to the memory ref collection
        /// </summary>
        /// <param name="Value"></param>
        public void Append(Register Value)
        {
            this._RegisterRefs.Reallocate(Value.Name, Value);
        }

        /// <summary>
        /// Adds a given register to the memory ref collection
        /// </summary>
        /// <param name="Value"></param>
        public void Append(Heap<Register> Value)
        {
            this._RegisterRefs.Import(Value);
        }

        /// <summary>
        /// Adds a given cell heap to the given memory ref collection
        /// </summary>
        /// <param name="Value"></param>
        public void Append(Heap<Cell> Value)
        {
            this._CellHeapRefs.Reallocate(Value.Identifier, Value);
        }

        /// <summary>
        /// Adds a given CellMatrix heap to the memory ref collection
        /// </summary>
        /// <param name="Value"></param>
        public void Append(Heap<CellMatrix> Value)
        {
            this._CellMatrixHeapRefs.Reallocate(Value.Identifier, Value);
        }

        // Extraction Methods //
        /// <summary>
        /// Appends this memory structure with all registers, cell heaps and matrix heaps in the given expression
        /// </summary>
        /// <param name="Value"></param>
        public void Extract(Expression Value)
        {

            // Field refs //
            if (Value is ExpressionFieldRef)
            {
                this._RegisterRefs.Import((Value as ExpressionFieldRef).GetMemoryRegisters());
            }

            // Cell heap refs //
            if (Value is ExpressionHeapRef)
            {
                Heap<Cell> x = (Value as ExpressionHeapRef).InnerHeap;
                this._CellHeapRefs.Reallocate(x.Identifier, x);
            }

            // CellMatrix heap refs
            if (Value is ExpressionMatrixRef)
            {
                Heap<CellMatrix> x = (Value as ExpressionMatrixRef).InnerHeap;
                this._CellMatrixHeapRefs.Reallocate(x.Identifier, x);
            }

        }

        /// <summary>
        /// Appends this memory structure with all registers, cell heaps and matrix heaps in the given expression
        /// </summary>
        /// <param name="Values"></param>
        public void Extract(ExpressionCollection Values)
        {

            foreach (Expression e in Values.Nodes)
            {
                this.Extract(e);
            }

        }

        /// <summary>
        /// Appends this memory structure with all registers, cell heaps and matrix heaps in the given expression
        /// </summary>
        /// <param name="Value"></param>
        public void Extract(Filter Value)
        {
            this.Extract(Value.Node);
        }

        /// <summary>
        /// Appends this memory structure with all registers, cell heaps and matrix heaps in the given expression
        /// </summary>
        /// <param name="Value"></param>
        public void Extract(Aggregate Value)
        {

            foreach (Expression e in Value.InnerExpressions())
            {
                this.Extract(e);
            }

        }

        /// <summary>
        /// Appends this memory structure with all registers, cell heaps and matrix heaps in the given expression
        /// </summary>
        /// <param name="Value"></param>
        public void Extract(AggregateCollection Value)
        {

            foreach (Aggregate x in Value.Nodes)
            {
                this.Extract(x);
            }

        }

        /// <summary>
        /// Appends this memory structure with all registers, cell heaps and matrix heaps in the given expression
        /// </summary>
        /// <param name="Value"></param>
        public void Extract(Method Value)
        {

            // Get registers //
            foreach (Expression e in Value.InnerExpressions())
            {
                this.Extract(e);
            }

            // Assignments
            if (Value is MethodAssignScalar)
            {
                Heap<Cell> x = (Value as MethodAssignScalar).InnerHeap;
                this._CellHeapRefs.Reallocate(x.Identifier, x);
            }

            // For-Loops
            if (Value is MethodFor)
            {
                Heap<Cell> x = (Value as MethodFor).InnerHeap;
                this._CellHeapRefs.Reallocate(x.Identifier, x);
            }

            // Matrix assignments
            if (Value is MethodMatrixAllAssign)
            {
                Heap<CellMatrix> x = (Value as MethodMatrixAllAssign).InnerHeap;
                this._CellMatrixHeapRefs.Reallocate(x.Identifier, x);
            }

            // Matrix assignments
            if (Value is MethodMatrixAssign)
            {
                Heap<CellMatrix> x = (Value as MethodMatrixAssign).InnerHeap;
                this._CellMatrixHeapRefs.Reallocate(x.Identifier, x);
            }

            // Matrix assignments
            if (Value is MethodMatrixUnitAssign)
            {
                Heap<CellMatrix> x = (Value as MethodMatrixUnitAssign).InnerHeap;
                this._CellMatrixHeapRefs.Reallocate(x.Identifier, x);
            }

        }

        /// <summary>
        /// Appends this memory structure with all registers, cell heaps and matrix heaps in the given expression
        /// </summary>
        /// <param name="Value"></param>
        public void Extract(MethodCollection Value)
        {

            foreach (Method x in Value.Nodes)
            {
                this.Extract(x);
            }

        }

        /// <summary>
        /// Appends this memory structure with all registers, cell heaps and matrix heaps in the given expression
        /// </summary>
        /// <param name="Value"></param>
        public void Extract(MatrixExpression Value)
        {

            // Get registers //
            foreach (Expression e in Value.InnerExpressions())
            {
                this.Extract(e);
            }

            // Handle heap refs //
            if (Value is MatrixExpressionHeap)
            {
                MatrixExpressionHeap x = (Value as MatrixExpressionHeap);
                this._CellMatrixHeapRefs.Reallocate(x.InnerHeap.Identifier, x.InnerHeap);
            }

        }

        // Linking methods //
        /// <summary>
        /// Links a given expression to the current memory ref
        /// </summary>
        /// <param name="Value"></param>
        public void Link(Expression Value)
        {

            // Link the registers //
            Expression.AssignRegister(Value, this._RegisterRefs);

            // Link the cell heaps //
            foreach (Heap<Cell> h in this._CellHeapRefs.Values)
            {

                if (Value is ExpressionHeapRef)
                {

                    ExpressionHeapRef x = (Value as ExpressionHeapRef);
                    if (x.InnerHeap.Identifier == h.Identifier)
                    {
                        //Console.WriteLine("Orig {0} : New {1}", OriginalNode.InnerHeap._UID.ToString().Replace("-", ""), h._UID.ToString().Replace("-", ""));
                        //Console.WriteLine(OriginalNode.NodeID + "\n");
                        x.ForceHeap(h);
                    }

                }
                
            }

            // Link the matrix heaps //
            foreach (Heap<CellMatrix> h in this._CellMatrixHeapRefs.Values)
            {
                Expression.AssignMatrixHeap(Value, h);
            }

            // Apply to all the child nodes //
            foreach (Expression x in Value.Children)
            {
                this.Link(x);
            }

        }

        /// <summary>
        /// Links a given expression to the current memory ref
        /// </summary>
        /// <param name="Value"></param>
        public void Link(ExpressionCollection Value)
        {

            foreach (Expression e in Value.Nodes)
            {
                this.Link(e);
            }

        }

        /// <summary>
        /// Links a given expression to the current memory ref
        /// </summary>
        /// <param name="Value"></param>
        public void Link(Filter Value)
        {
            this.Link(Value.Node);
        }

        /// <summary>
        /// Links a given expression to the current memory ref
        /// </summary>
        /// <param name="Value"></param>
        public void Link(Aggregate Value)
        {

            foreach (Expression e in Value.InnerExpressions())
            {
                this.Link(e);
            }

        }

        /// <summary>
        /// Links a given expression to the current memory ref
        /// </summary>
        /// <param name="Value"></param>
        public void Link(AggregateCollection Value)
        {

            foreach (Aggregate x in Value.Nodes)
            {
                this.Link(x);
            }

        }

        /// <summary>
        /// Links each expression in the method to this memory structure
        /// </summary>
        /// <param name="Value"></param>
        public void Link(Method Value)
        {

            // Link each expression node //
            foreach (Expression e in Value.InnerExpressions())
            {
                this.Link(e);
            }

            // Assignments
            if (Value is Methods.MethodAssignScalar)
            {

                MethodAssignScalar x = Value as MethodAssignScalar;
                foreach (Heap<Cell> y in this._CellHeapRefs.Values)
                {

                    if (x.InnerHeap.Identifier.ToUpper() == y.Identifier.ToUpper())
                    {
                        x.InnerHeap = y;
                    }
                    
                }

            }

            // For-Loops
            if (Value is Methods.MethodFor)
            {
                MethodFor x = Value as MethodFor;
                foreach (Heap<Cell> y in this._CellHeapRefs.Values)
                {
                    if (x.InnerHeap.Identifier.ToUpper() == y.Identifier.ToUpper()) 
                        x.InnerHeap = y;
                }
            }

            // Matrix assignments
            if (Value is Methods.MethodMatrixAllAssign)
            {
                MethodMatrixAllAssign x = Value as MethodMatrixAllAssign;
                foreach (Heap<CellMatrix> y in this._CellMatrixHeapRefs.Values)
                {
                    if (x.InnerHeap.Identifier.ToUpper() == y.Identifier.ToUpper())
                        x.InnerHeap = y;
                }
            }

            // Matrix assignments
            if (Value is Methods.MethodMatrixAssign)
            {
                MethodMatrixAssign x = Value as MethodMatrixAssign;
                foreach (Heap<CellMatrix> y in this._CellMatrixHeapRefs.Values)
                {
                    if (x.InnerHeap.Identifier.ToUpper() == y.Identifier.ToUpper())
                        x.InnerHeap = y;
                }
            }

            // Matrix assignments
            if (Value is Methods.MethodMatrixUnitAssign)
            {
                MethodMatrixUnitAssign x = Value as MethodMatrixUnitAssign;
                foreach (Heap<CellMatrix> y in this._CellMatrixHeapRefs.Values)
                {
                    if (x.InnerHeap.Identifier.ToUpper() == y.Identifier.ToUpper())
                        x.InnerHeap = y;
                }
            }

            // Apply to all the child nodes //
            foreach (Method x in Value.Children)
            {
                this.Link(x);
            }

        }

        /// <summary>
        /// Links each expression in the method to this memory structure
        /// </summary>
        /// <param name="Value"></param>
        public void Link(MethodCollection Value)
        {

            foreach (Method x in Value.Nodes)
            {
                this.Link(x);
            }

        }

        /// <summary>
        /// Links each expression in the matrix expression to this memory structure
        /// </summary>
        /// <param name="Value"></param>
        public void Link(MatrixExpression Value)
        {

            // Link each expression //
            foreach (Expression e in Value.InnerExpressions())
            {
                this.Link(e);
            }

            // Handle heap refs //
            if (Value is MatrixExpressionHeap)
            {
                MatrixExpressionHeap x = (Value as MatrixExpressionHeap);
                foreach (Heap<CellMatrix> y in this._CellMatrixHeapRefs.Values)
                {
                    if (x.InnerHeap.Identifier.ToUpper() == y.Identifier.ToUpper())
                    {
                        x.InnerHeap = y;
                    }
                }
            }

            // Apply to all the child nodes //
            foreach (MatrixExpression x in Value.Children)
            {
                this.Link(x);
            }

        }

        // Clone //
        /// <summary>
        /// Creats a clone linking to all objects in this memory reference
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Expression Clone(Expression Value)
        {
            Expression x = Value.CloneOfMe();
            this.Link(x);
            return x;
        }

        /// <summary>
        /// Creats a clone linking to all objects in this memory reference
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        public ExpressionCollection Clone(ExpressionCollection Value)
        {
            ExpressionCollection x = Value.CloneOfMe();
            this.Link(x);
            return x;
        }

        /// <summary>
        /// Creats a clone linking to all objects in this memory reference
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Filter Clone(Filter Value)
        {
            Filter x = Value.CloneOfMe();
            this.Link(x);
            return x;
        }

        /// <summary>
        /// Creats a clone linking to all objects in this memory reference
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Aggregate Clone(Aggregate Value)
        {
            Aggregate x = Value.CloneOfMe();
            this.Link(x);
            return x;
        }

        /// <summary>
        /// Creats a clone linking to all objects in this memory reference
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        public AggregateCollection Clone(AggregateCollection Value)
        {
            AggregateCollection x = Value.CloneOfMe();
            this.Link(x);
            return x;
        }

        /// <summary>
        /// Creats a clone linking to all objects in this memory reference
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        public Method Clone(Method Value)
        {
            Method x = Value.CloneOfMe();
            this.Link(x);
            return x;
        }

        /// <summary>
        /// Creats a clone linking to all objects in this memory reference
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        public MethodCollection Clone(MethodCollection Value)
        {
            MethodCollection x = Value.CloneOfMe();
            this.Link(x);
            return x;
        }

        // Clone of this instance //
        /// <summary>
        /// Creates a by-Value clone of each element in the register
        /// </summary>
        /// <returns></returns>
        public CloneFactory CloneOfMe()
        {

            // Create our shallow body //
            CloneFactory x = new CloneFactory();

            // Clone the register heap //
            x._RegisterRefs = CloneFactory.Clone(this._RegisterRefs);

            // Clone each cell heap //
            foreach (Heap<Cell> hc in this._CellHeapRefs.Values)
            {
                Heap<Cell> q = CloneFactory.Clone(hc);
                x._CellHeapRefs.Allocate(q.Identifier, q);
            }

            // Clone each cell heap //
            foreach (Heap<CellMatrix> hm in this._CellMatrixHeapRefs.Values)
            {
                Heap<CellMatrix> q = CloneFactory.Clone(hm);
                x._CellMatrixHeapRefs.Allocate(q.Identifier, q);
            }

            return x;

        }

        // Static cloning methods //
        /// <summary>
        /// Creates a clone of the register heap
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static Heap<Register> Clone(Heap<Register> Value)
        {

            Heap<Register> val = new Heap<Register>();
            foreach (Register r in Value.Values)
            {
                val.Allocate(r.Name, r);
            }
            return val;

        }

        /// <summary>
        /// Creates a clone of the cell heap 
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static Heap<Cell> Clone(Heap<Cell> Value)
        {

            Heap<Cell> val = new Heap<Cell>();
            for (int i = 0; i < Value.Count; i++)
            {
                val.Allocate(Value.Name(i), Value[i]); // Note: cells are Value types, so we don't need to close
            }

            val.Identifier = Value.Identifier;

            return val;

        }

        /// <summary>
        /// Clones a cell matrix
        /// </summary>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static Heap<CellMatrix> Clone(Heap<CellMatrix> Value)
        {

            Heap<CellMatrix> val = new Heap<CellMatrix>();
            for (int i = 0; i < Value.Count; i++)
            {
                val.Allocate(Value.Name(i), Value[i].CloneOfMe()); // Note: cells are Value types, so we don't need to close
            }

            val.Identifier = Value.Identifier;

            return val;

        }

    }

}
