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

    public sealed class SystemFunctionLibrary : FunctionLibrary
    {

        public const string LIB_NAME = "SYSTEM";

        public const string HARD_WRITES = "HARD_WRITES";
        public const string HARD_READS = "HARD_READS";
        public const string SOFT_WRITES = "SOFT_WRITES";
        public const string SOFT_READS = "SOFT_READS";
        public const string CORES = "CORES";
        public const string VERSION = "VERSION";
        
        public const string TEMP_DIR = "TEMP_DIR";
        public const string FLAT_DIR = "FLAT_DIR";
        public const string SCRIPT_DIR = "SCRIPT_DIR";
        public const string LOG_DIR = "LOG_DIR";
        
        private string[] _FunctionNames = new string[]
        {
            HARD_READS,
            HARD_WRITES,
            SOFT_READS,
            SOFT_WRITES,
            CORES,
            VERSION,

            TEMP_DIR,
            FLAT_DIR,
            SCRIPT_DIR,
            LOG_DIR
        };

        public SystemFunctionLibrary(Session Session)
            : base(Session)
        {
            this.LibName = LIB_NAME;
        }

        public override string[] Names
        {
            get { return _FunctionNames; }
        }

        public override CellFunction RenderFunction(string Name)
        {

            switch (Name.ToUpper())
            {

                case HARD_READS:
                    return this.LambdaHardReads();
                case HARD_WRITES:
                    return this.LambdaHardWrites();
                case SOFT_READS:
                    return this.LambdaVirtualReads();
                case SOFT_WRITES:
                    return this.LambdaVirtualWrites();

                case CORES:
                    return this.LambdaCores();
                case VERSION:
                    return this.LambdaVersion();

                case TEMP_DIR:
                    return this.LambdaTempDir();
                case FLAT_DIR:
                    return this.LambdaFlatDir();
                case SCRIPT_DIR:
                    return this.LambdaScriptDir();
                case LOG_DIR:
                    return this.LambdaLogDir();

            };

            throw new ArgumentException(string.Format("Cell function '{0}' does not exist", Name));

        }

        private CellFunction LambdaHardReads()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(this._Session.Kernel.DiskReads);
            };
            return new CellFunctionFixedShell(HARD_READS, 0, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaHardWrites()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(this._Session.Kernel.DiskWrites);
            };
            return new CellFunctionFixedShell(HARD_WRITES, 0, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaVirtualReads()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(this._Session.Kernel.VirtualReads);
            };
            return new CellFunctionFixedShell(SOFT_READS, 0, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaVirtualWrites()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(this._Session.Kernel.VirtualWrites);
            };
            return new CellFunctionFixedShell(SOFT_WRITES, 0, CellAffinity.INT, lambda);

        }

        private CellFunction LambdaVersion()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(Session.VERSION);
            };
            return new CellFunctionFixedShell(VERSION, 0, CellAffinity.STRING, lambda);

        }

        private CellFunction LambdaCores()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(Environment.ProcessorCount);
            };
            return new CellFunctionFixedShell(CORES, 0, CellAffinity.STRING, lambda);

        }

        private CellFunction LambdaTempDir()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(Kernel.RyeTempDir);
            };
            return new CellFunctionFixedShell(TEMP_DIR, 0, CellAffinity.STRING, lambda);

        }

        private CellFunction LambdaFlatDir()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(Kernel.RyeFlatFilesDir);
            };
            return new CellFunctionFixedShell(FLAT_DIR, 0, CellAffinity.STRING, lambda);

        }

        private CellFunction LambdaScriptDir()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(Kernel.RyeScriptsDir);
            };
            return new CellFunctionFixedShell(SCRIPT_DIR, 0, CellAffinity.STRING, lambda);

        }

        private CellFunction LambdaLogDir()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(Kernel.RyeLogDir);
            };
            return new CellFunctionFixedShell(LOG_DIR, 0, CellAffinity.STRING, lambda);

        }

    }

    public sealed class SystemMethodLibrary : MethodLibrary
    {

        public const string LIB_NAME = "SYSTEM";

        public const string MEMORY_DUMP = "MEMORY_DUMP";
        public const string EMPTY_CACHE = "EMPTY_CACHE";
        public const string MARK_TABLE = "MARK_TABLE";
        public const string SUPRESS = "SUPRESS";
        public const string SET_SEED = "SET_SEED";

        private string[] _Names = new string[]
        {
            MEMORY_DUMP,
            EMPTY_CACHE,
            MARK_TABLE,
            SUPRESS,
            SET_SEED
        };

        public SystemMethodLibrary(Session Session)
            : base(Session)
        {
            this.LibName = LIB_NAME;
        }

        public override string[] Names
        {
            get { return this._Names; }
        }

        public override Methods.ParameterCollectionSigniture RenderSigniture(string Name)
        {

            switch (Name.ToUpper())
            {

                case MEMORY_DUMP:
                    return Methods.ParameterCollectionSigniture.Parse(MEMORY_DUMP, "Provides a dump of what's in memory", ParameterCollectionSigniture.ZERO_PARAMETER);
                case EMPTY_CACHE:
                    return Methods.ParameterCollectionSigniture.Parse(EMPTY_CACHE, "Empties the current kernel extent page cache", ParameterCollectionSigniture.ZERO_PARAMETER);
                case MARK_TABLE:
                    return Methods.ParameterCollectionSigniture.Parse(MARK_TABLE, "Loads as much of a table as possible into memory", "DATA|A table to load with the form <DB>.<Name>|t|false");
                case SUPRESS:
                    return Methods.ParameterCollectionSigniture.Parse(SUPRESS, "Toggles IO supression on or off", ParameterCollectionSigniture.ZERO_PARAMETER);
                case SET_SEED:
                    return Methods.ParameterCollectionSigniture.Parse(SET_SEED, "Sets the seed of the internal Rye random number generator", "SEED|An integer seed|Value|false");

            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override Methods.Method RenderMethod(Methods.Method Parent, string Name, Methods.ParameterCollection Parameters)
        {

            switch (Name.ToUpper())
            {

                case MEMORY_DUMP:
                    return this.Method_MemoryDump(Parent, Parameters);
                case EMPTY_CACHE:
                    return this.Method_EmptyCache(Parent, Parameters);
                case MARK_TABLE:
                    return this.Method_MarkTable(Parent, Parameters);
                case SUPRESS:
                    return this.Method_Supress(Parent, Parameters);
                case SET_SEED:
                    return this.Method_SetSeed(Parent, Parameters);

            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        private Method Method_MemoryDump(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                this._Session.IO.WriteLine(this._Session.MemoryDump);

            };
            return new LibraryMethod(Parent, MEMORY_DUMP, Parameters, false, kappa);

        }

        private Method Method_EmptyCache(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                this._Session.Kernel.FlushAndClearCache();

            };
            return new LibraryMethod(Parent, MEMORY_DUMP, Parameters, false, kappa);

        }

        private Method Method_MarkTable(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData t = Parameters.Tables["DATA"];
                this._Session.Kernel.MarkTable(t.Header.Path, true);

            };
            return new LibraryMethod(Parent, MEMORY_DUMP, Parameters, false, kappa);

        }

        private Method Method_Supress(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {
                this._Session.IO.Supress = !this._Session.IO.Supress;
            };
            return new LibraryMethod(Parent, SUPRESS, Parameters, false, kappa);

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
            return new LibraryMethod(Parent, SET_SEED, Parameters, false, kappa);

        }

    }

}
