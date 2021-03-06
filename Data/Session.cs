﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Expressions;
using Rye.Methods;
using Rye.Data;
using Rye.Structures;
using Rye.Libraries;

namespace Rye.Data
{

    /// <summary>
    /// Holds runtime variables, data and functions
    /// </summary>
    public sealed class Session
    {

        // Constants //
        private const string DOT = ".";
        private const string DEFAULT_NAMESPACE = "GLOBAL";
        private const string TEMP_DB = "TEMP";
        private const int SYS_REF = 0;
        public const string VERSION = "0.0.3";

        // Private variables //
        private Heap<Extent> _extents;
        private Heap<string> _connections;
        private Heap<Cell> _scalars;
        private Heap<CellMatrix> _matrixes;
        private Heap<Lambda> _lambdas;
        //private Heap<FunctionLibrary> _functions;
        //private Heap<MethodLibrary> _methods;
        private Heap<Library> _Library;
        private Kernel _kernel;
        private Communicator _comm;
        private string _name_space = DEFAULT_NAMESPACE;
        private Interpreter.ExpressionVisitor _vis;
        private RandomCell _generator;

        public Session(Kernel Driver, Communicator IO, bool AllowConcurrent)
        {

            // Generate all our data objects //
            this._extents = new Heap<Extent>();
            this._extents.Identifier = "GLOBAL";

            this._connections = new Heap<string>();
            this._connections.Identifier = "GLOBAL";

            this._scalars = new Heap<Cell>();
            this._scalars.Identifier = "GLOBAL";

            this._matrixes = new Heap<CellMatrix>();
            this._matrixes.Identifier = "GLOBAL";

            this._lambdas = new Heap<Lambda>();
            this._lambdas.Identifier = "GLOBAL";

            //this._functions = new Heap<FunctionLibrary>();
            //this._functions.Identifier = "GLOBAL";

            //this._methods = new Heap<MethodLibrary>();
            //this._methods.Identifier = "GLOBAL";

            this._Library = new Heap<Library>();
            this._Library.Identifier = "GLOBAL";
            
            // Load our use objects //
            this._kernel = Driver;
            this._comm = IO;
            this.AllowConcurrent = AllowConcurrent;

            // Add in the base function library //
            this._Library.Allocate("BASE", new BaseLibrary(this));

            // Build a visitor //
            this._vis = new Interpreter.ExpressionVisitor(this);

            // RNG //
            this._generator = new RandomCell();
            
            // Default DataPageX PageSize //
            this.DefualtPageSize = Kernel.DefaultPageSize;

            // Add in time variables to the heap //
            this._scalars.Allocate("DAY_TICKS", new Cell(864000000000L));


        }

        public Session()
            : this(new Kernel(null), new CommandLineCommunicator(), false)
        {
        }

        public Kernel Kernel
        {
            get { return this._kernel; }
        }

        public Communicator IO
        {
            get { return this._comm; }
        }

        public bool AllowConcurrent
        {
            get;
            set;
        }

        public Heap<Cell> Scalars
        {
            get { return this._scalars; }
        }

        public Heap<CellMatrix> Matrixes
        {
            get { return this._matrixes; }
        }

        public RandomCell BaseGenerator
        {
            get { return this._generator; }
        }

        public long DefualtPageSize
        {
            get;
            set;
        }

        public int MaxThreadCount
        {
            get { return Environment.ProcessorCount; }
        }

        // Support //
        public bool IsGlobal(string NameSpace)
        {
            return StringComparer.OrdinalIgnoreCase.Equals(NameSpace, this._name_space);
        }

        public string GlobalName
        {
            get { return this._name_space; }
            set { this._name_space = value; }
        }

        public Interpreter.ExpressionVisitor BaseVisitor
        {
            get { return this._vis; }
        }

        // Tabular Data //
        public bool TabularDataExists(string DB, string Name)
        {
            
            if (this.IsGlobal(DB) && this.ExtentExists(Name))
                return true;

            return this.TableExists(DB, Name);

        }

        public TabularData GetTabularData(string DB, string Name)
        {

            if (this.IsGlobal(DB) && this.ExtentExists(Name))
                return this.GetExtent(Name);

            return this.GetTable(DB, Name);

        }

        public void BurnTabularData(string DB, string Name)
        {

            // Check global //
            if (this.IsGlobal(DB))
            {

                if (this._extents.Exists(Name))
                {
                    this._extents.Deallocate(Name);
                }

            }

            // Otherwise, use the kernel to drop the table //
            this._kernel.RequestDropTable(Header.FilePath(DB, Name, this._kernel.DefaultExtension));

        }

        public TabularData CreateTabularData(string DB, string Name, Schema Columns, long PageSize)
        {

            if (this.IsGlobal(DB))
                return this.CreateExtent(Name, Columns, (int)PageSize);
            else if (this.ConnectionExists(DB))
                return this.CreateTable(DB, Name, Columns, PageSize);
            else
                throw new Rye.Interpreter.RyeCompileException("Connection '{0}' does not exist", DB);

        }

        public TabularData CreateTabularData(string FullName, Schema Columns)
        {

            string[] args = FullName.Split('.');
            string db = (args.Length == 1 ? this.GlobalName : args[0]);
            string name = args.Last();

            return this.CreateTabularData(db, name, Columns, TabularData.DEFAULT_PAGE_SIZE);

        }

        // Spectre //
        public bool TableExists(string DB, string Name)
        {
            
            if (!this.ConnectionExists(DB))
                return false;

            string dir = this.GetConnection(DB);
            string path1 = Header.FilePath(dir, Name, this._kernel.DefaultExtension);

            return Kernel.TableExists(path1);

        }

        public Table GetTable(string DB, string Name)
        {

            if (!this.ConnectionExists(DB))
                throw new ArgumentException(string.Format("BaseTable {0}.{1} does not exist", DB, Name));

            string dir = this.GetConnection(DB);
            string path1 = Header.FilePath(dir, Name, this._kernel.DefaultExtension);

            return this._kernel.RequestBufferTable(path1);

        }

        public void SetTable(Table Value)
        {
            this._kernel.RequestFlushTable(Value);
        }

        public Table CreateTable(string NameSpace, string Name, Schema Columns, long PageSize)
        {

            string dir = this.GetConnection(NameSpace);
            return Table.CreateTable(this._kernel, dir, Name, Columns, PageSize);

        }

        // Extents //
        public bool ExtentExists(string Name)
        {
            return this._extents.Exists(Name);
        }

        public Extent GetExtent(string Name)
        {
            return this._extents[Name];
        }

        public void SetExtent(Extent Value)
        {
            this._extents.Reallocate(Value.Header.Name, Value);
        }

        public Extent CreateExtent(string Name, Schema Columns, int PageSize)
        {

            Extent e = new Extent(Columns, Header.NewMemoryOnlyExtentHeader(Name, Columns.Count, (long)PageSize));
            this._extents.Reallocate(Name, e);
            return e;

        }
        
        // Connections //
        public bool ConnectionExists(string Name)
        {
            return this._connections.Exists(Name);
        }

        public string GetConnection(string Name)
        {
            return this._connections[Name];
        }

        public void SetConnection(string Name, string Value)
        {
            this._connections.Reallocate(Name, Value);
        }

        // Scalars //
        public bool ScalarExists(string Name)
        {
            return this._scalars.Exists(Name);
        }

        public Cell GetScalar(string Name)
        {
            return this._scalars[Name];
        }

        public void SetScalar(string Name, Cell Value)
        {
            this._scalars.Reallocate(Name, Value);
        }

        public void BurnScalar(string Name)
        {
            if (this._scalars.Exists(Name))
                this._scalars.Deallocate(Name);
        }

        // Matrix //
        public bool MatrixExists(string Name)
        {
            return this._matrixes.Exists(Name);
        }

        public CellMatrix GetMatrix(string Name)
        {
            return this._matrixes[Name];
        }

        public void SetMatrix(string Name, CellMatrix Value)
        {
            this._matrixes.Reallocate(Name, Value);
        }

        public void BurnMatrix(string Name)
        {
            if (this._matrixes.Exists(Name))
                this._matrixes.Deallocate(Name);
        }

        // Lambda //
        public bool LambdaExists(string Name)
        {
            return this._lambdas.Exists(Name);
        }

        public Lambda GetLambda(string Name)
        {
            return this._lambdas[Name];
        }

        public void SetLambda(string Name, Lambda Value)
        {
            this._lambdas.Reallocate(Name, Value);
        }

        public void BurnLambda(string Name)
        {
            if (this._lambdas.Exists(Name))
                this._lambdas.Deallocate(Name);
        }

        // Functions //
        //public bool FunctionLibraryExists(string Name)
        //{
        //    return this._functions.PageExists(Name);
        //}

        //public FunctionLibrary GetFunctionLibrary(string Name)
        //{
        //    return this._functions[Name];
        //}

        //public void SetFunctionLibrary(string Name, FunctionLibrary Value)
        //{
        //    this._functions.Reallocate(Name, Value);
        //}

        //public void SetFunctionLibrary(FunctionLibrary Value)
        //{
        //    this.SetFunctionLibrary(Value.LibName, Value);
        //}

        //public bool FunctionExists(string NameSpace, string Name)
        //{
        //    if (!this._functions.PageExists(NameSpace))
        //        return false;
        //    return this._functions[NameSpace].PageExists(Name);
        //}

        //public CellFunction GetFunction(string Namespace, string Name)
        //{
        //    return this._functions[Namespace].RenderFunction(Name);
        //}

        //public FunctionLibrary SystemLibrary
        //{
        //    get { return this._functions[SYS_REF]; }
        //}

        //// Methods //
        //public bool MethodLibraryExists(string Name)
        //{
        //    return this._methods.PageExists(Name);
        //}

        //public MethodLibrary GetMethodLibrary(string Name)
        //{
        //    return this._methods[Name];
        //}

        //public void SetMethodLibrary(string Name, MethodLibrary Value)
        //{
        //    this._methods.Reallocate(Name, Value);
        //}

        //public void SetMethodLibrary(MethodLibrary Value)
        //{
        //    this.SetMethodLibrary(Value.LibName, Value);
        //}

        //public bool MethodExists(string NameSpace, string Name)
        //{
        //    if (!this._methods.PageExists(NameSpace))
        //        return false;
        //    return this._methods[NameSpace].PageExists(Name);
        //}

        //public Method GetMethod(string Namespace, string Name, Method Storage, ParameterCollection Parameters)
        //{
        //    return this._methods[Namespace].RenderMethod(Storage, Name, Parameters);
        //}

        //public ParameterCollectionSigniture GetMethodSigniture(string Namespace, string Name)
        //{
        //    return this._methods[Namespace].RenderSigniture(Name);
        //}

        public bool LibraryExists(string Name)
        {
            return this._Library.Exists(Name);
        }

        public Library GetLibrary(string Name)
        {
            return this._Library[Name];
        }

        public void SetLibrary(string Name, Library Value)
        {
            this._Library.Reallocate(Name, Value);
        }

        public void SetLibrary(Library Value)
        {
            this.SetLibrary(Value.LibraryName, Value);
        }

        public bool LibraryExists(string NameSpace, string Name)
        {
            if (!this._Library.Exists(NameSpace))
                return false;
            return true;
        }

        public Method GetMethod(string Namespace, string Name, Method Parent, ParameterCollection Parameters)
        {
            return this._Library[Namespace].GetMethod(Parent, Name, Parameters);
        }

        public ParameterCollectionSigniture GetMethodSigniture(string Namespace, string Name)
        {
            return this._Library[Namespace].GetMethodSigniture(Name);
        }

        public bool MethodExists(string NameSpace, string Name)
        {
            if (!this._Library.Exists(NameSpace))
                return false;
            return this._Library[NameSpace].MethodExists(Name);
        }

        public CellFunction GetFunction(string Namespace, string Name)
        {
            return this._Library[Namespace].GetFunction(Name);
        }

        public bool FunctionExists(string NameSpace, string Name)
        {
            if (!this._Library.Exists(NameSpace))
                return false;
            return this._Library[NameSpace].FunctionExists(Name);
        }

        public Library SystemLibrary
        {
            get 
            { 
                return this._Library[SYS_REF]; 
            }
        }

        // Header data //
        public string MemoryDump
        {

            get
            {
                
                StringBuilder sb = new StringBuilder();

                // Extents
                sb.AppendLine("EXTENT CACHE");
                if (this._extents.Count == 0)
                    sb.AppendLine("\t<EMPTY>");
                foreach (Extent extent in this._extents.Values)
                    sb.AppendLine("\t" + extent.Header.Name);

                // Connections
                sb.AppendLine("CONNECTIONS");
                if (this._connections.Count == 0)
                    sb.AppendLine("\t<EMPTY>");
                foreach (string connection in this._connections.Values)
                    sb.AppendLine("\t" + connection);

                // Scalars 
                sb.AppendLine("SCALAR CACHE");
                if (this._scalars.Count == 0)
                    sb.AppendLine("\t<EMPTY>");
                foreach (KeyValuePair<string,Cell> scalar in this._scalars.Entries)
                    sb.AppendLine("\t" + scalar.Key);

                // Matricies
                sb.AppendLine("MATRIX CACHE");
                if (this._matrixes.Count == 0)
                    sb.AppendLine("\t<EMPTY>");
                foreach (KeyValuePair<string, CellMatrix> matrix in this._matrixes.Entries)
                    sb.AppendLine("\t" + matrix.Key + "[" + matrix.Value.RowCount.ToString() + "," + matrix.Value.ColumnCount.ToString() + "]");

                // Lambdas
                sb.AppendLine("LAMBDA CACHE");
                if (this._lambdas.Count == 0)
                    sb.AppendLine("\t<EMPTY>");
                foreach (Lambda lam in this._lambdas.Values)
                    sb.AppendLine("\t" + lam.Name);

                // Function
                sb.AppendLine("LIBRARIES");
                if (this._Library.Count == 0)
                    sb.AppendLine("\t<EMPTY>");
                foreach (Library flib in this._Library.Values)
                {
                    sb.AppendLine("\tMETHODS: " + flib.LibraryName);
                    foreach (string name in flib.MethodNames)
                    {
                        sb.AppendLine("\t\t" + name);
                    }
                    sb.AppendLine("\tFUNCTIONS: " + flib.LibraryName);
                    foreach (string name in flib.FunctionNames)
                    {
                        sb.AppendLine("\t\t" + name);
                    }
                }

                // Kernel Dump 
                sb.AppendLine("KERNEL CACHE");
                sb.AppendLine("\tSpectre");
                foreach (string m in this._kernel.TableNames)
                {
                    sb.AppendLine("\t\t" + m);
                }
                sb.AppendLine("\tExtents");
                foreach (string m in this._kernel.ExtentNames)
                {
                    sb.AppendLine("\t\t" + m);
                }

                // Return //
                return sb.ToString();

            }


        }

    }

}
