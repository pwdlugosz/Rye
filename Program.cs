using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using Rye.Data;
using Rye.Interpreter;
using Rye.Expressions;
using ScrapySharp;
using ScrapySharp.Network;
using ScrapySharp.Extensions;
using Rye.Data.Spectre;

namespace Rye
{

    class Program
    {

        static void Main(string[] args)
        {

            //Program.DebugRun(args);
            //Program.ReleaseRun(args);
            Program.ShellRun();
            string z = Console.ReadLine();

        }

        public static void ShellRun()
        {

            System.Diagnostics.Stopwatch sw = Stopwatch.StartNew();


            Rye.Data.Spectre.Host h = new Data.Spectre.Host();
            Schema s = new Schema("Key int, Value double, xyz int");
            //HeapDreamTable x = new HeapDreamTable(h, "TEMP", s);
            ScribeTable x = new HeapScribeTable(h, "Temp", @"C:\Users\pwdlu_000\Documents\Rye Projects\Temp", s, Page.DEFAULT_SIZE);
            //ScribeTable OriginalNode = new SortedScribeTable(h, "Temp", @"C:\Users\pwdlu_000\Documents\Rye Projects\Temp", s, Page.DEFAULT_SIZE, new Key(0));
            RandomCell rng = new RandomCell(127);
            //BPlusTree tree = new BPlusTree(x, new Key(1, 2));

            Rye.Data.Spectre.WriteStream writer = x.OpenWriter();
            for (int i = 0; i < 2000000; i++)
            {

                Record r = Record.Stitch(rng.NextLong(0, 100), rng.NextDoubleGauss(), new Cell(i));
                x.Insert(r);
                //tree.Insert(r);

            }
            writer.Close();

            Console.WriteLine(x.MetaData());

            //tree.Print(@"C:\Users\pwdlu_000\Documents\Rye Projects\BPlusTree.txt");

            h.ShutDown();

            ScribeTable table = h.PageCache.RequestScribeTable(@"C:\Users\pwdlu_000\Documents\Rye Projects\Temp\Temp.ryev1");
            table.Dump(@"C:\Users\pwdlu_000\Documents\Rye Projects\Test2.txt");

            Console.WriteLine("::::::::::::::::::::::::::::::::: Complete :::::::::::::::::::::::::::::::::");
            Console.WriteLine("Run Time: {0}", sw.Elapsed);
            string t = Console.ReadLine();

        }

        public static void DebugRun(string[] args)
        {

            System.Diagnostics.Stopwatch sw = Stopwatch.StartNew();

            // Open the file to get the script //
            string script = System.IO.File.ReadAllText(System.Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\Rye\Rye\Interpreter\TestScript.rye");
            Kernel k = new Kernel(System.Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\Rye Projects\Temp\");

            // Render a Session
            Session enviro = new Session(k, new CommandLineCommunicator(), true);
            Exchange.WebProvider web = new Exchange.WebProvider();

            // Load the libraries //
            enviro.SetLibrary(new Libraries.FileLibrary(enviro));
            enviro.SetLibrary(new Libraries.TableLibrary(enviro));
            enviro.SetLibrary(new Libraries.WebLibrary(enviro));
            enviro.SetLibrary(new Libraries.SystemLibrary(enviro));
            enviro.SetLibrary(new Libraries.FinanceLibrary(enviro));
            enviro.SetLibrary(new Libraries.MiningLibrary(enviro));
            enviro.SetLibrary(new Libraries.MSOfficeLibrary(enviro));

            // Create a script process //
            RyeScriptProcessor runner = new RyeScriptProcessor(enviro);

            // Run the script //
            runner.Execute(script);

            // Close down the kernel space //
            enviro.Kernel.ShutDown();

            sw.Stop();
            enviro.IO.WriteLine("::::::::::::::::::::::::::::::::: Complete :::::::::::::::::::::::::::::::::");
            enviro.IO.WriteLine("Run Time: {0}", sw.Elapsed);
            enviro.IO.WriteLine("Virtual Reads: {0}", enviro.Kernel.VirtualReads);
            enviro.IO.WriteLine("Virtual Writes: {0}", enviro.Kernel.VirtualWrites);
            enviro.IO.WriteLine("Hard Reads: {0}", enviro.Kernel.DiskReads);
            enviro.IO.WriteLine("Hard Writes: {0}", enviro.Kernel.DiskWrites);

            // Clear the IO cache //
            enviro.IO.ShutDown();

        }

        public static void ReleaseRun(string[] args)
        {

            System.Diagnostics.Stopwatch sw = Stopwatch.StartNew();
            
            // Open the file to get the script //
            string script = System.IO.File.ReadAllText(args[0]);
            Kernel k = new Kernel(System.Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\Rye Projects\Temp\");
            
            // Render a Session
            Session enviro = new Session(k, new CommandLineCommunicator(), true);
            Exchange.WebProvider web = new Exchange.WebProvider();

            // Add in the method library //
            enviro.SetLibrary(new Libraries.FileLibrary(enviro));
            enviro.SetLibrary(new Libraries.TableLibrary(enviro));
            enviro.SetLibrary(new Libraries.WebLibrary(enviro));
            enviro.SetLibrary(new Libraries.SystemLibrary(enviro));
            enviro.SetLibrary(new Libraries.FinanceLibrary(enviro));
            enviro.SetLibrary(new Libraries.MiningLibrary(enviro));
            enviro.SetLibrary(new Libraries.MSOfficeLibrary(enviro));
            
            // Create a script process //
            RyeScriptProcessor runner = new RyeScriptProcessor(enviro);
            
            // Run the script //
             runner.Execute(script);
            
            // Close down the kernel space //
            enviro.Kernel.ShutDown();
            
            sw.Stop();
            enviro.IO.WriteLine("::::::::::::::::::::::::::::::::: Complete :::::::::::::::::::::::::::::::::");
            enviro.IO.WriteLine("Run Time: {0}", sw.Elapsed);
            enviro.IO.WriteLine("Virtual Reads: {0}", enviro.Kernel.VirtualReads);
            enviro.IO.WriteLine("Virtual Writes: {0}", enviro.Kernel.VirtualWrites);
            enviro.IO.WriteLine("Hard Reads: {0}", enviro.Kernel.DiskReads);
            enviro.IO.WriteLine("Hard Writes: {0}", enviro.Kernel.DiskWrites);

            // Clear the IO cache //
            enviro.IO.ShutDown();

            
        }


    }

}
