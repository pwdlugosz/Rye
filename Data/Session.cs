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

    public sealed class Session
    {

        // Constants //
        private const string DOT = ".";
        private const string DEFAULT_NAMESPACE = "GLOBAL";
        private const string TEMP_DB = "TEMP";
        private const int SYS_REF = 0;

        // Private variables //
        private Heap<Extent> _extents;
        private Heap<string> _connections;
        private Heap<Cell> _scalars;
        private Heap<CellMatrix> _matrixes;
        private Heap<Lambda> _lambdas;
        private Heap<FunctionLibrary> _functions;
        private Heap<MethodLibrary> _methods;
        private Kernel _kernel;
        private Communicator _comm;
        private string _name_space = DEFAULT_NAMESPACE;

        public Session(Kernel Driver, Communicator IO, bool AllowAsync)
        {

            // Generate all our data objects //
            this._extents = new Heap<Extent>();
            this._connections = new Heap<string>();
            this._scalars = new Heap<Cell>();
            this._matrixes = new Heap<CellMatrix>();
            this._lambdas = new Heap<Lambda>();
            this._functions = new Heap<FunctionLibrary>();
            this._methods = new Heap<MethodLibrary>();
            
            // Load our use objects //
            this._kernel = Driver;
            this._comm = IO;
            this.AllowAsync = AllowAsync;

            // Add in the base function library //
            this._functions.Allocate("BASE", new BaseFunctionLibrary(this));
            
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

        public bool AllowAsync
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

        // Tables //
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
                throw new ArgumentException(string.Format("ShartTable {0}.{1} does not exist", DB, Name));

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

        // Functions //
        public bool FunctionLibraryExists(string Name)
        {
            return this._functions.Exists(Name);
        }

        public FunctionLibrary GetFunctionLibrary(string Name)
        {
            return this._functions[Name];
        }

        public void SetFunctionLibrary(string Name, FunctionLibrary Value)
        {
            this._functions.Reallocate(Name, Value);
        }

        public void SetFunctionLibrary(FunctionLibrary Value)
        {
            this.SetFunctionLibrary(Value.LibName, Value);
        }

        public bool FunctionExists(string NameSpace, string Name)
        {
            if (!this._functions.Exists(NameSpace))
                return false;
            return this._functions[NameSpace].Exists(Name);
        }

        public CellFunction GetFunction(string Namespace, string Name)
        {
            return this._functions[Namespace].RenderFunction(Name);
        }

        public FunctionLibrary SystemLibrary
        {
            get { return this._functions[SYS_REF]; }
        }

        // Methods //
        public bool MethodLibraryExists(string Name)
        {
            return this._methods.Exists(Name);
        }

        public MethodLibrary GetMethodLibrary(string Name)
        {
            return this._methods[Name];
        }

        public void SetMethodLibrary(string Name, MethodLibrary Value)
        {
            this._methods.Reallocate(Name, Value);
        }

        public void SetMethodLibrary(MethodLibrary Value)
        {
            this.SetMethodLibrary(Value.LibName, Value);
        }

        public bool MethodExists(string NameSpace, string Name)
        {
            if (!this._methods.Exists(NameSpace))
                return false;
            return this._methods[NameSpace].Exists(Name);
        }

        public Method GetMethod(string Namespace, string Name, Method Parent, ParameterCollection Parameters)
        {
            return this._methods[Namespace].RenderMethod(Parent, Name, Parameters);
        }

        public ParameterCollectionSigniture GetMethodSigniture(string Namespace, string Name)
        {
            return this._methods[Namespace].RenderSigniture(Name);
        }

    }

    public abstract class Communicator
    {

        public string BREAKER_LINE = "-------------------------------------------------------------------------";
        public string HEADER_LINE = "---------------------------------- {0} ----------------------------------";
        public string NEW_LINE = "\n";

        public Communicator()
        {
        }

        public abstract void Write(string Message, params object[] Paramters);

        public virtual void WriteLine(string Message, params object[] Parameters)
        {
            this.Write(Message + "\n", Parameters);
        }

        public virtual void WriteLine(string Message)
        {
            this.WriteLine(Message);
        }

        public virtual void WriteLine()
        {
            this.Write(NEW_LINE);
        }

        public virtual void WriteBreaker()
        {
            this.WriteLine(BREAKER_LINE);
        }

        public virtual void WriteHeader(string Message)
        {
            this.WriteLine(HEADER_LINE, Message);
        }

        public abstract void ShutDown();

    }

    public sealed class CommandLineCommunicator : Communicator
    {

        public CommandLineCommunicator()
            : base()
        {
        }

        public override void Write(string Message, params object[] Paramters)
        {
            Console.Write(Message,Paramters);
        }

        public override void WriteLine(string Message)
        {
            Console.WriteLine(Message);
        }

        public override void ShutDown()
        {
 	        
        }

    }

}