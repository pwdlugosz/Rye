using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Methods;

namespace Rye.Libraries
{

    // System Library //
    public sealed class SystemLibrary : Library
    {

        public const string M_MEMORY_DUMP = "MEMORY_DUMP";
        public const string M_EMPTY_CACHE = "EMPTY_CACHE";
        public const string M_MARK_TABLE = "MARK_TABLE";
        public const string M_SUPRESS = "SUPRESS";
        public const string M_SET_SEED = "SET_SEED";

        private string[] _MethodNames = new string[]
        {
            M_MEMORY_DUMP,
            M_EMPTY_CACHE,
            M_MARK_TABLE,
            M_SUPRESS,
            M_SET_SEED
        };

        public const string F_HARD_WRITES = "HARD_WRITES";
        public const string F_HARD_READS = "HARD_READS";
        public const string F_SOFT_WRITES = "SOFT_WRITES";
        public const string F_SOFT_READS = "SOFT_READS";
        public const string F_CORES = "CORES";
        public const string F_VERSION = "VERSION";

        public const string F_TEMP_DIR = "TEMP_DIR";
        public const string F_FLAT_DIR = "FLAT_DIR";
        public const string F_SCRIPT_DIR = "SCRIPT_DIR";
        public const string F_LOG_DIR = "LOG_DIR";

        private string[] _FunctionNames = new string[]
        {
            F_HARD_READS,
            F_HARD_WRITES,
            F_SOFT_READS,
            F_SOFT_WRITES,
            F_CORES,
            F_VERSION,

            F_TEMP_DIR,
            F_FLAT_DIR,
            F_SCRIPT_DIR,
            F_LOG_DIR
        };

        public SystemLibrary(Session Session)
            : base(Session, "SYSTEM")
        {
        }

        public override Method GetMethod(Method Parent, string Name, ParameterCollection Parameters)
        {

            switch (Name.ToUpper())
            {

                case M_MEMORY_DUMP:
                    return this.Method_MemoryDump(Parent, Parameters);
                case M_EMPTY_CACHE:
                    return this.Method_EmptyCache(Parent, Parameters);
                case M_MARK_TABLE:
                    return this.Method_MarkTable(Parent, Parameters);
                case M_SUPRESS:
                    return this.Method_Supress(Parent, Parameters);
                case M_SET_SEED:
                    return this.Method_SetSeed(Parent, Parameters);

            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override ParameterCollectionSigniture GetMethodSigniture(string Name)
        {

            switch (Name.ToUpper())
            {

                case M_MEMORY_DUMP:
                    return Methods.ParameterCollectionSigniture.Parse(M_MEMORY_DUMP, "Provides a dump of what's in memory", ParameterCollectionSigniture.ZERO_PARAMETER);
                case M_EMPTY_CACHE:
                    return Methods.ParameterCollectionSigniture.Parse(M_EMPTY_CACHE, "Empties the current kernel extent page cache", ParameterCollectionSigniture.ZERO_PARAMETER);
                case M_MARK_TABLE:
                    return Methods.ParameterCollectionSigniture.Parse(M_MARK_TABLE, "Loads as much of a table as possible into memory", "DATA|A table to load with the form <DB>.<Name>|t|false");
                case M_SUPRESS:
                    return Methods.ParameterCollectionSigniture.Parse(M_SUPRESS, "Toggles IO supression on or off", ParameterCollectionSigniture.ZERO_PARAMETER);
                case M_SET_SEED:
                    return Methods.ParameterCollectionSigniture.Parse(M_SET_SEED, "Sets the seed of the internal Rye random number generator", "SEED|An integer seed|Value|false");

            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override string[] MethodNames
        {
            get { return _MethodNames; }
        }

        public override CellFunction GetFunction(string Name)
        {

            switch (Name.ToUpper())
            {

                case F_HARD_READS:
                    return this.LambdaHardReads();
                case F_HARD_WRITES:
                    return this.LambdaHardWrites();
                case F_SOFT_READS:
                    return this.LambdaVirtualReads();
                case F_SOFT_WRITES:
                    return this.LambdaVirtualWrites();

                case F_CORES:
                    return this.LambdaCores();
                case F_VERSION:
                    return this.LambdaVersion();

                case F_TEMP_DIR:
                    return this.LambdaTempDir();
                case F_FLAT_DIR:
                    return this.LambdaFlatDir();
                case F_SCRIPT_DIR:
                    return this.LambdaScriptDir();
                case F_LOG_DIR:
                    return this.LambdaLogDir();

            };

            throw new ArgumentException(string.Format("Cell function '{0}' does not exist", Name));

        }

        public override string[] FunctionNames
        {
            get
            {
                return _FunctionNames;
            }
        }

        // Methods //
        private Method Method_MemoryDump(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                this._Session.IO.WriteLine(this._Session.MemoryDump);

            };
            return new LibraryMethod(Parent, M_MEMORY_DUMP, Parameters, false, kappa);

        }

        private Method Method_EmptyCache(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                this._Session.Kernel.FlushAndClearCache();

            };
            return new LibraryMethod(Parent, M_MEMORY_DUMP, Parameters, false, kappa);

        }

        private Method Method_MarkTable(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData t = Parameters.Tables["DATA"];
                this._Session.Kernel.MarkTable(t.Header.Path, true);

            };
            return new LibraryMethod(Parent, M_MEMORY_DUMP, Parameters, false, kappa);

        }

        private Method Method_Supress(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {
                this._Session.IO.Supress = !this._Session.IO.Supress;
            };
            return new LibraryMethod(Parent, M_SUPRESS, Parameters, false, kappa);

        }

        private Method Method_SetSeed(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                Cell c = Parameters.Expressions["SEED"].Evaluate();
                int Seed = (int)c.valueINT;
                if (c.Affinity == CellAffinity.STRING || c.Affinity == CellAffinity.BLOB)
                {
                    Seed = (int)c.LASH;
                }

                this._Session.BaseGenerator.Remix(Seed);

            };
            return new LibraryMethod(Parent, M_SET_SEED, Parameters, false, kappa);

        }

        // Functions //
        private CellFunction LambdaHardReads()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(this._Session.Kernel.DiskReads);
            };
            return new CellFunctionFixedShell(F_HARD_READS, 0, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaHardWrites()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(this._Session.Kernel.DiskWrites);
            };
            return new CellFunctionFixedShell(F_HARD_WRITES, 0, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaVirtualReads()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(this._Session.Kernel.VirtualReads);
            };
            return new CellFunctionFixedShell(F_SOFT_READS, 0, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaVirtualWrites()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(this._Session.Kernel.VirtualWrites);
            };
            return new CellFunctionFixedShell(F_SOFT_WRITES, 0, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaVersion()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(Session.VERSION);
            };
            return new CellFunctionFixedShell(F_VERSION, 0, CellAffinity.STRING, lambda);

        }

        private CellFunction LambdaCores()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(Environment.ProcessorCount);
            };
            return new CellFunctionFixedShell(F_CORES, 0, CellAffinity.STRING, lambda);

        }

        private CellFunction LambdaTempDir()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(Kernel.RyeTempDir);
            };
            return new CellFunctionFixedShell(F_TEMP_DIR, 0, CellAffinity.STRING, lambda);

        }

        private CellFunction LambdaFlatDir()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(Kernel.RyeFlatFilesDir);
            };
            return new CellFunctionFixedShell(F_FLAT_DIR, 0, CellAffinity.STRING, lambda);

        }

        private CellFunction LambdaScriptDir()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(Kernel.RyeScriptsDir);
            };
            return new CellFunctionFixedShell(F_SCRIPT_DIR, 0, CellAffinity.STRING, lambda);

        }

        private CellFunction LambdaLogDir()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(Kernel.RyeLogDir);
            };
            return new CellFunctionFixedShell(F_LOG_DIR, 0, CellAffinity.STRING, lambda);

        }


    }


}
