using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Expressions;
using Rye.Data;
using Rye.Structures;

namespace Rye.Interpreter
{

    public sealed class Workspace
    {

        public Workspace()
        {

            this.Structures = new Heap<MemoryStructure>();
            this.Connections = new Heap<string>();
            this.IO = new CommandLineCommunicator();

            // Accumulate the global structure //
            this.Structures.Allocate(GlobalStructure.DEFAULT_NAME, new GlobalStructure());

            this.AllowAsync = false;

        }

        public Heap<MemoryStructure> Structures
        {
            get;
            private set;
        }

        public Heap<string> Connections
        {
            get;
            private set;
        }

        public Communicator IO
        {
            get;
            private set;
        }

        public MemoryStructure Global
        {
            get { return this.Structures[GlobalStructure.DEFAULT_NAME]; }
        }

        public Table GetTable(string Alias, string Name)
        {

            if (!this.Connections.Exists(Alias))
            {
                throw new RyeParseException("Connection '{0}' not found");
            }

            string dir = this.Connections[Alias];
            string path = Header.FilePath(dir, Name, HeaderType.Table);

            return Kernel.RequestBufferTable(path);

        }

        public DataSet GetData(string FullTableName)
        {

            string[] toks = FullTableName.Split('.');
            if (toks.Length == 2)
                throw new RyeCompileException("Full table name string is in the incorrect format: {0}", FullTableName);
            if (this.Connections.Exists(toks[0]))
                return this.GetTable(toks[0], toks[1]);
            if (this.Structures.Exists(toks[0]))
            {
                if (this.Structures[toks[0]].Extents.Exists(toks[1]))
                    return this.Structures[toks[0]].Extents[toks[1]];
                else
                    throw new RyeCompileException("Extent '{0}' does not exist in '{1}", toks[1], toks[0]);
            }
            throw new RyeCompileException("The structure or database '{0}' does not exist", toks[0]);

        }

        public DataSet GetData(string Database, string Name)
        {

            if (this.Connections.Exists(Database))
                return this.GetTable(Database, Name);
            if (this.Structures.Exists(Database))
            {
                if (this.Structures[Database].Extents.Exists(Name))
                    return this.Structures[Database].Extents[Name];
                else
                    throw new RyeCompileException("Extent '{0}' does not exist in '{1}", Name, Database);
            }
            throw new RyeCompileException("The structure or database '{0}' does not exist", Database);

        }

        public bool AllowAsync
        {
            get;
            set;
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
            Console.Write(string.Format(Message, Paramters));
        }

        public override void ShutDown()
        {
 	        
        }

    }

}
