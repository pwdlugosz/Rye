using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Antlr4;
using Antlr4.Runtime;

namespace Rye.Interpreter
{

    public sealed class RyeScriptProcessor
    {

        public RyeScriptProcessor(Workspace Enviro)
        {
            this.Enviro = Enviro;
        }

        public Workspace Enviro
        {
            get;
            private set;
        }

        public void Execute(string Script)
        {

            // Create a token stream and do lexal analysis //
            AntlrInputStream TextStream = new AntlrInputStream(Script);
            RyeLexer HorseLexer = new RyeLexer(TextStream);

            // Parse the script //
            CommonTokenStream HorseTokenStream = new CommonTokenStream(HorseLexer);
            RyeParser rye = new RyeParser(HorseTokenStream);
            
            // Create an executer object //
            CommandVisitor processor = new CommandVisitor(this.Enviro);

            int runs = 0;
            foreach (RyeParser.CommandContext ctx in rye.compile_unit().command_set().command())
            {
                runs += processor.Visit(ctx);
            }

        }


    }


}
