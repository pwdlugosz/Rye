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

            Program.DebugRun(args);
            //Program.ReleaseRun(args);
            string z = Console.ReadLine();

        }

        public static void DebugRun(string[] args)
        {

            System.Diagnostics.Stopwatch sw = Stopwatch.StartNew();

            // Open the file to get the script //
            string script = System.IO.File.ReadAllText(System.Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\Rye\Rye\Interpreter\TestScript.rye");
            Kernel k = new Kernel(System.Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\Rye Projects\Temp\");
            
            // Render a Session
            Session enviro = new Session(k, new HybridCommunicator(), true);

            // Add in the method library //
            enviro.SetMethodLibrary(new Libraries.FileMethodLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.TableMethodLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.MSOfficeLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.SystemMethodLibrary(enviro));

            // Add the function libraries //
            enviro.SetFunctionLibrary(new Libraries.FileFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.TableFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.FinanceFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.SystemFunctionLibrary(enviro));

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

            // Add in the method library //
            enviro.SetMethodLibrary(new Libraries.FileMethodLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.TableMethodLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.MSOfficeLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.SystemMethodLibrary(enviro));

            // Add the function libraries //
            enviro.SetFunctionLibrary(new Libraries.FileFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.TableFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.FinanceFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.SystemFunctionLibrary(enviro));

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
