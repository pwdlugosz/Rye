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

            //Console.WriteLine("Char: {0}", 'A');
            //Console.WriteLine("Byte: {0}", (byte)'A');
            
            // Open the file to get the script //
            string script = System.IO.File.ReadAllText(@"C:\Users\pwdlu_000\Documents\Rye\Rye\Interpreter\TestScript.rye");
            Kernel.TempDirectory = @"C:\Users\pwdlu_000\Documents\Data\TempDB";

            // Render a Workspace
            Workspace enviro = new Workspace();
            enviro.AllowAsync = true;

            // Add in the structures //
            enviro.Structures.Allocate(Structures.FileStructure.STRUCT_NAME, new Structures.FileStructure());
            enviro.Structures.Allocate(Structures.TableStructure.STRUCT_NAME, new Structures.TableStructure(enviro));

            // Create a script process //
            RyeScriptProcessor runner = new RyeScriptProcessor(enviro);

            // Run the script //
            runner.Execute(script);

            // Close down the kernel space //
            //Console.WriteLine(Kernel.Status);
            Kernel.ShutDown();

            sw.Stop();
            Console.WriteLine("::::::::::::::::::::::::::::::::: Complete :::::::::::::::::::::::::::::::::");
            Console.WriteLine("Run Time: {0}", sw.Elapsed);
            Console.WriteLine("Virtual Reads: {0}", Kernel.VirtualReads);
            Console.WriteLine("Virtual Writes: {0}", Kernel.VirtualWrites);
            Console.WriteLine("Hard Reads: {0}", Kernel.DiskReads);
            Console.WriteLine("Hard Writes: {0}", Kernel.DiskWrites);
            string z = Console.ReadLine();

        }

        public static long Power1(long Base, long Exp)
        {

            long t = 1;
            for (long l = 0; l < Exp; l++)
            {
                t *= Base;
            }
            return t;

        }

        public static long Power2(long Base, long Exp)
        {

            if (Exp == 0)
                return 1;
            else if (Exp == 1)
                return Base;

            if (Exp % 2 == 1)
                return Power2(Base, Exp / 2) * Exp;
            else
                return Power2(Base, Exp / 2);

        }
    
    
    
    }
}
