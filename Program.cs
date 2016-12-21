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

namespace Rye
{

    class Program
    {

        static void Main(string[] args)
        {

            Program.DebugRun(args);
            //Program.ReleaseRun(args);
            //Program.ShellRun();
            string z = Console.ReadLine();

        }

        public static void ShellRun()
        {

            System.Diagnostics.Stopwatch sw = Stopwatch.StartNew();

            //ScrapySharp.Network.ScrapingBrowser browser = new ScrapySharp.Network.ScrapingBrowser();

            //ScrapySharp.Network.WebPage page = browser.NavigateToPage(new Uri("https://www.medicare.gov/find-a-plan/results/planresults/plan-details.aspx?TT=PUBLIC&HM=False&CNTY=36010|Allen|OH0&SL=2&CY=2017&ADL=-1&MPL=-1&PTL=MAPD&NWC=0|1&AD=0&MOA=0|1&CGO=0|1&FIPSCC=39003&ZC=45877&SP=100&HS=3&elink=yes&cntrctid=H3655&plnid=032&sgmntid=0&DIID=-1"));

            //string[] tags = new string[]
            //{
            //    "ctl00_ctl00_ctl00_MCGMainContentPlaceHolder_ToolContentPlaceHolder_PlanFinderContentPlaceHolder_PlanDetailTabContainer_PlanOverviewPanel_lblOverviewInpatientCareVal",
            //    "ctl00_ctl00_ctl00_MCGMainContentPlaceHolder_ToolContentPlaceHolder_PlanFinderContentPlaceHolder_PlanDetailTabContainer_PlanOverviewPanel_lblOverviewOutpatientPrescriptionDrugsVal",
            //    "ctl00_ctl00_ctl00_MCGMainContentPlaceHolder_ToolContentPlaceHolder_PlanFinderContentPlaceHolder_PlanDetailTabContainer_PlanOverviewPanel_lblOverviewDentalServicesVal",
            //    "ctl00_ctl00_ctl00_MCGMainContentPlaceHolder_ToolContentPlaceHolder_PlanFinderContentPlaceHolder_PlanDetailTabContainer_PlanOverviewPanel_lblOverviewAllOtherServicesVal",
            //    "ctl00_ctl00_ctl00_MCGMainContentPlaceHolder_ToolContentPlaceHolder_PlanFinderContentPlaceHolder_PlanDetailTabContainer_PlanOverviewPanel_lblOverviewTotalMonthlyEstimateVal",
            //    "ctl00_ctl00_ctl00_MCGMainContentPlaceHolder_ToolContentPlaceHolder_PlanFinderContentPlaceHolder_PlanDetailTabContainer_PlanOverviewPanel_lblOverviewTotalAnnualEstimateVal",
            //    "ctl00_ctl00_ctl00_MCGMainContentPlaceHolder_ToolContentPlaceHolder_PlanFinderContentPlaceHolder_PlanDetailTabContainer_DrugCostPanel_AverageAnnualDrugCostValue",
            //    "ctl00_ctl00_ctl00_MCGMainContentPlaceHolder_ToolContentPlaceHolder_PlanFinderContentPlaceHolder_PlanDetailTabContainer_PlanOverviewPanel_lblOverviewPartBPremiumVal",
            //    "ctl00_ctl00_ctl00_MCGMainContentPlaceHolder_ToolContentPlaceHolder_PlanFinderContentPlaceHolder_PlanDetailTabContainer_PlanOverviewPanel_lblOverviewTotalPremiumVal",
            //    "ctl00_ctl00_ctl00_MCGMainContentPlaceHolder_ToolContentPlaceHolder_PlanFinderContentPlaceHolder_PlanDetailTabContainer_PlanBenefitsPanel_BenefitsMonthlyPlanPremiumText",
            //    "ctl00_ctl00_ctl00_MCGMainContentPlaceHolder_ToolContentPlaceHolder_PlanFinderContentPlaceHolder_PlanDetailTabContainer_PlanBenefitsPanel_BenefitsMonthlyDrugPremiumText"
            //};

            //foreach (string  x in tags)
            //{
            //    var y = page.Html.CssSelect("#" + x).ToArray();
            //    if (y.Length > 0)
            //    {
            //        Console.WriteLine(y.First().InnerHtml);
            //    }
            //    else
            //    {
            //        Console.WriteLine("Empty");
            //    }

            //}

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

            // Add in the method library //
            enviro.SetMethodLibrary(new Libraries.FileMethodLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.TableMethodLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.MSOfficeLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.SystemMethodLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.WebMathodLibrary(enviro, web));
            enviro.SetMethodLibrary(new Libraries.ExchangeLibrary(enviro));

            // Add the function libraries //
            enviro.SetFunctionLibrary(new Libraries.FileFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.TableFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.FinanceFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.SystemFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.ExchangeFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.WebFunctionLibrary(enviro, web));
            

            // Create a script process //
            RyeScriptProcessor runner = new RyeScriptProcessor(enviro);

            // Run the script //
            runner.Execute(script);

            // Close down the kernel space //
            enviro.Kernel.BaseIO = enviro.IO;
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
            enviro.SetMethodLibrary(new Libraries.FileMethodLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.TableMethodLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.MSOfficeLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.SystemMethodLibrary(enviro));
            enviro.SetMethodLibrary(new Libraries.WebMathodLibrary(enviro, web));
            enviro.SetMethodLibrary(new Libraries.ExchangeLibrary(enviro));

            // Add the function libraries //
            enviro.SetFunctionLibrary(new Libraries.FileFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.TableFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.FinanceFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.SystemFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.ExchangeFunctionLibrary(enviro));
            enviro.SetFunctionLibrary(new Libraries.WebFunctionLibrary(enviro, web));
            
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
