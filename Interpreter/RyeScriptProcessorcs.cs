using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Antlr4;
using Antlr4.Runtime;
using Rye.Data;

namespace Rye.Interpreter
{

    public sealed class RyeScriptProcessor
    {

        public RyeScriptProcessor(Session Enviro)
        {
            this.Enviro = Enviro;
        }

        public Session Enviro
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
            CommonTokenStream RyeTokenStream = new CommonTokenStream(HorseLexer);
            RyeParser rye = new RyeParser(RyeTokenStream);

            // Handle the error listener //
            rye.RemoveErrorListeners();
            rye.AddErrorListener(new ParserErrorListener());

            // Create an executer object //
            CommandVisitor processor = new CommandVisitor(this.Enviro);

            // Create the call stack and the error catch stack //
            List<RyeParser.CommandContext> CallStack = new List<RyeParser.CommandContext>();
            List<string> Errors = new List<string>();

            // Load the call stack and/or parse the errors
            try
            {

                foreach (RyeParser.CommandContext ctx in rye.compile_unit().command_set().command())
                {
                    CallStack.Add(ctx);
                }

            }
            catch (RyeParseException e1)
            {

                Errors.Add("\tParsing Error Detected 1: ");
                Errors.Add("\t" + "\t" + e1.Message);

            }
            catch (RyeCompileException e2)
            {

                Errors.Add("\tCompile Error Detected 2: ");
                Errors.Add("\t" + "\t" + e2.Message);

            }
            //catch (Exception e3)
            //{

            //    Errors.Add("\tUnknown Error Detected 3: ");
            //    Errors.Add("\t" + "\t" + e3.Message);
                
            //}

            // Check to see if we found any errors //
            if (Errors.Count != 0)
            {

                this.Enviro.IO.WriteLine("Rye encountered one or more critical errors; no statements executed:");
                foreach (string s in Errors)
                {
                    this.Enviro.IO.WriteLine(s);
                }

            }
            
            // Execute each element in the call stack //
            int Runs = 0;
            foreach (RyeParser.CommandContext ctx in CallStack)
            {

                Runs += processor.Visit(ctx);

            }

        }
        

    }

    public sealed class ParserErrorListener : BaseErrorListener
    {

        public ParserErrorListener()
            : base()
        {
        }

        public override void ReportAmbiguity(Parser recognizer, Antlr4.Runtime.Dfa.DFA dfa, int startIndex, int stopIndex, bool exact, Antlr4.Runtime.Sharpen.BitSet ambigAlts, Antlr4.Runtime.Atn.ATNConfigSet configs)
        {
            base.ReportAmbiguity(recognizer, dfa, startIndex, stopIndex, exact, ambigAlts, configs);
        }

        public override void ReportAttemptingFullContext(Parser recognizer, Antlr4.Runtime.Dfa.DFA dfa, int startIndex, int stopIndex, Antlr4.Runtime.Sharpen.BitSet conflictingAlts, Antlr4.Runtime.Atn.SimulatorState conflictState)
        {
            base.ReportAttemptingFullContext(recognizer, dfa, startIndex, stopIndex, conflictingAlts, conflictState);
        }

        public override void ReportContextSensitivity(Parser recognizer, Antlr4.Runtime.Dfa.DFA dfa, int startIndex, int stopIndex, int prediction, Antlr4.Runtime.Atn.SimulatorState acceptState)
        {
            base.ReportContextSensitivity(recognizer, dfa, startIndex, stopIndex, prediction, acceptState);
        }

        public override void SyntaxError(IRecognizer recognizer, IToken offendingSymbol, int line, int charPositionInLine, string msg, RecognitionException e)
        {

            string message = string.Format("Invalid token '{0}' found on line '{1}' at position '{2}'; \nexpecting {3}", offendingSymbol.Text.ToString(), line, charPositionInLine, msg);
            throw new RyeParseException(message);

        }

    }


}
