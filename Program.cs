using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using Rye.Data;
using Rye.Interpreter;
using Rye.Expressions;

namespace Rye
{
    class Program
    {
        static void Main(string[] args)
        {

            System.Diagnostics.Stopwatch sw = Stopwatch.StartNew();

            string script = System.IO.File.ReadAllText(@"C:\Users\pwdlu_000\Documents\Rye\Rye\Interpreter\TestScript.txt");
            Kernel.TempDirectory = @"C:\Users\pwdlu_000\Documents\Data\TempDB";
            Workspace enviro = new Workspace();
            enviro.AllowAsync = true;
            enviro.Structures.Allocate("FILE", new Structures.FileStructure());
            RyeScriptProcessor runner = new RyeScriptProcessor(enviro);
            runner.Execute(script);
            Kernel.ShutDown();

            //Extent e = new Extent(new Schema("KEY INT, VALUE DOUBLE"));
            //for (int i = 0; i < 1000; i++)
            //{
            //    RecordBuilder rb = new RecordBuilder();
            //    rb.Add(i);
            //    rb.Add(3.1415);
            //    e.Add(rb.ToRecord());
            //}

            //Register mem = new Register(e.Columns);
            //ModeStep m = new ModeStep(e.CreateVolume(), mem);
            //while (!m.AtEnd)
            //{
            //    Console.WriteLine(mem.Value);
            //    m.Advance();
            //}

            sw.Stop();
            Console.WriteLine("::::::::::::::::::::::::::::::::: Complete :::::::::::::::::::::::::::::::::");
            Console.WriteLine("Run Time: {0}", sw.Elapsed);
            Console.WriteLine("Virtual Reads: {0}", Kernel.VirtualReads);
            Console.WriteLine("Virtual Writes: {0}", Kernel.VirtualWrites);
            Console.WriteLine("Hard Reads: {0}", Kernel.DiskReads);
            Console.WriteLine("Hard Writes: {0}", Kernel.DiskWrites);
            string z = Console.ReadLine();

        }

    
    
    
    
    
    
    
    
    }
}
