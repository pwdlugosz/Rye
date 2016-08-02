using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Methods;
using Rye.Expressions;
using Rye.Data;
using System.IO;

namespace Rye.Structures
{

    public class FileStructure : MemoryStructure
    {

        public const string STRUCT_NAME = "FILE";

        public FileStructure()
            : base(STRUCT_NAME)
        {
            this._functions = new FileFunctionLibrary();
            this._procedures = new FileProcedureLibrary(this);
        }

        public sealed class FileFunctionLibrary : FunctionLibrary
        {

            public const string READ_ALL_TEXT = "READ_ALL_TEXT";
            public const string READ_ALL_BYTES = "READ_ALL_BYTES";
            public const string SIZE = "SIZE";
            public const string EXISTS = "EXISTS";

            private static string[] _FunctionNames = new string[]
            {
                READ_ALL_TEXT,
                READ_ALL_BYTES,
                SIZE,
                EXISTS
            };

            public FileFunctionLibrary()
                : base()
            {
            }

            public override CellFunction RenderFunction(string Name)
            {

                switch (Name)
                {
                    case FileFunctionLibrary.READ_ALL_TEXT: return this.LambdaReadAllText();
                    case FileFunctionLibrary.READ_ALL_BYTES: return this.LambdaReadAllBytes();
                    case FileFunctionLibrary.SIZE: return this.LambdaFileSize();
                    case FileFunctionLibrary.EXISTS: return this.LambdaFileExists();
                }

                throw new ArgumentException(string.Format("Function '{0}' does not exist", Name));

            }

            public override string[] Names
            {
                get { return FileFunctionLibrary._FunctionNames; }
            }

            private CellFunction LambdaReadAllText()
            {

                Func<Cell[], Cell> lambda = (x) =>
                {
                    string file_name = x[0].valueSTRING;
                    Cell c = new Cell(File.ReadAllText(file_name));
                    return c;
                };
                return new CellFunctionFixedShell("READ_ALL_TEXT", 1, CellAffinity.STRING, lambda);

            }

            private CellFunction LambdaReadAllBytes()
            {

                Func<Cell[], Cell> lambda = (x) =>
                {
                    string file_name = x[0].valueSTRING;
                    Cell c = new Cell(File.ReadAllBytes(file_name));
                    return c;
                };
                return new CellFunctionFixedShell("READ_ALL_BYTES", 1, CellAffinity.BLOB, lambda);

            }

            private CellFunction LambdaFileSize()
            {

                Func<Cell[], Cell> lambda = (x) =>
                {
                    string file_name = x[0].valueSTRING;
                    Cell c = new Cell(new FileInfo(file_name).Length);
                    return c;
                };
                return new CellFunctionFixedShell("LENGTH", 1, CellAffinity.INT, lambda);

            }

            private CellFunction LambdaFileExists()
            {

                Func<Cell[], Cell> lambda = (x) =>
                {
                    string file_name = x[0].valueSTRING;
                    Cell c = new Cell(File.Exists(file_name));
                    return c;
                };
                return new CellFunctionFixedShell("EXISTS", 1, CellAffinity.BOOL, lambda);

            }

        }

        public sealed class FileProcedureLibrary : ProcedureLibrary
        {

            public const string ZIP = "ZIP";
            public const string UNZIP = "UNZIP";
            public const string WRITE_ALL_TEXT = "WRITE_ALL_TEXT";
            public const string WRITE_ALL_BYTES = "WRITE_ALL_BYTES";
            public const string APPEND_ALL_TEXT = "APPEND_ALL_TEXT";
            public const string APPEND_ALL_BYTES = "APPEND_ALL_BYTES";
            public const string DELETE = "DELETE";
            public const string MOVE = "MOVE";
            public const string COPY = "COPY";
            public const string IMPORT = "IMPORT";
            public const string EXPORT = "EXPORT";
            public const string DOWNLOAD = "DOWNLOAD";

            private static string[] _BaseNames = new string[]
            {
                ZIP,
                UNZIP,
                WRITE_ALL_TEXT,
                WRITE_ALL_BYTES,
                APPEND_ALL_TEXT,
                APPEND_ALL_BYTES,
                DELETE,
                MOVE,
                COPY,
                IMPORT,
                EXPORT,
                DOWNLOAD
            };

            private Heap2<string, string> _CompressedSig;

            /*
             * Name | Description | Affinity | Can Be Null?
             * 
             * Affinity can be:
             * E: expression
             * V: vector
             * M: matrix expression
             * T: table (or extent)
             * 
             */

            public FileProcedureLibrary(FileStructure Caller)
                :base(Caller)
            {
                this._Caller = Caller;
                this._CompressedSig = new Heap2<string,string>();

                this._CompressedSig.Allocate(FileProcedureLibrary.ZIP, "Zips a file", "IN_PATH|The path of the file to zip|E|false;OUT_PATH|The path to the zipped file|E|false");
                this._CompressedSig.Allocate(FileProcedureLibrary.UNZIP, "Unzips a file", "IN_PATH|The path of the file to unzip (*.zip)|E|false;OUT_PATH|The path to directory|E|false");
                this._CompressedSig.Allocate(FileProcedureLibrary.WRITE_ALL_TEXT, "Writes text to a file; if the file already exists, it will be overwritten", "PATH|The path of the file to dump text to|E|false;TEXT|Text to write|E|false");
                this._CompressedSig.Allocate(FileProcedureLibrary.WRITE_ALL_BYTES, "Writes bytes to a file; if the file already exists, it will be overwritten", "PATH|The path of the file to dump text to|E|false;OUT_PATH|Bytes to write|E|false");
                this._CompressedSig.Allocate(FileProcedureLibrary.APPEND_ALL_TEXT, "Writes text to a file; if the file exists, it will append the text to the end", "PATH|The path of the file to dump text to|E|false;TEXT|Text to write|E|false");
                this._CompressedSig.Allocate(FileProcedureLibrary.APPEND_ALL_BYTES, "Writes bytes to a file; if the file exists, it will append the bytes to the end", "PATH|The path of the file to dump text to|E|false;OUT_PATH|Bytes to write|E|false");
                this._CompressedSig.Allocate(FileProcedureLibrary.DELETE, "Deletes a file", "PATH|The file to delete|E|false");
                this._CompressedSig.Allocate(FileProcedureLibrary.MOVE, "Moves a file to another location", "FROM_PATH|The original file to move|E|false;TO_PATH|The new location to move to|E|false");
                this._CompressedSig.Allocate(FileProcedureLibrary.COPY, "Copies a file to another location", "FROM_PATH|The original file to copy|E|false;TO_PATH|The path to put the copy in|E|false");
                this._CompressedSig.Allocate(FileProcedureLibrary.IMPORT, "Loads a file into an existing table", "DATA|The table to load|T|false;PATH|The flat file location|E|false;DELIM|The column delimitor|E|false;ESCAPE|The escape sequence character|E|true;SKIP|The number of lines to skip|E|true");
                this._CompressedSig.Allocate(FileProcedureLibrary.EXPORT, "Exports a table into a new file", "DATA|The table to export|T|false;PATH|The path to the exported file|E|false;DELIM|The column delimitor|E|false");
                this._CompressedSig.Allocate(FileProcedureLibrary.DOWNLOAD, "Downloads a url to a file", "URL|The URL to download|E|false;PATH|The path to the exported file|E|false");
                
            }

            public override Method RenderMethod(Method Parent, string Name, ParameterCollection Parameters)
            {

                switch (Name.ToUpper())
                {

                    case FileProcedureLibrary.ZIP: 
                        return this.KappaZip(Parent, Parameters);
                    case FileProcedureLibrary.UNZIP: 
                        return this.KappaUnzip(Parent, Parameters);
                    case FileProcedureLibrary.WRITE_ALL_TEXT: 
                        return this.KappaWriteAllText(Parent, Parameters);
                    case FileProcedureLibrary.WRITE_ALL_BYTES: 
                        return this.KappaWriteAllBytes(Parent, Parameters);
                    case FileProcedureLibrary.APPEND_ALL_TEXT: 
                        return this.KappaAppendAllText(Parent, Parameters);
                    case FileProcedureLibrary.APPEND_ALL_BYTES: 
                        return this.KappaAppendAllBytes(Parent, Parameters);
                    case FileProcedureLibrary.DELETE: 
                        return this.KappaDelete(Parent, Parameters);
                    case FileProcedureLibrary.MOVE: 
                        return this.KappaMove(Parent, Parameters);
                    case FileProcedureLibrary.COPY: 
                        return this.KappaCopy(Parent, Parameters);
                    case FileProcedureLibrary.IMPORT:
                        return this.KappaImport(Parent, Parameters);
                    case FileProcedureLibrary.EXPORT:
                        return this.KappaExport(Parent, Parameters);
                    case FileProcedureLibrary.DOWNLOAD:
                        return this.KappaDownload(Parent, Parameters);
                }
                throw new ArgumentException(string.Format("Method '{0}' does not exist in '{1}'", Name, this._Caller.Name));

            }

            public override ParameterCollectionSigniture RenderSigniture(string Name)
            {

                if (this._CompressedSig.Exists(Name))
                    return ParameterCollectionSigniture.Parse(Name, this._CompressedSig[Name].Item1, this._CompressedSig[Name].Item2);
                throw new ArgumentException(string.Format("Method '{0}' does not exist in '{1}'", Name, this._Caller.Name));

            }

            public override string[] Names
            {
                get { return FileProcedureLibrary._BaseNames; }
            }

            // Method support //
            private DynamicStructureMethod KappaZip(Method Parent, ParameterCollection Parameters)
            {

                Action<ParameterCollection> kappa = (x) =>
                {

                    string infile = x.Expressions["IN_PATH"].Evaluate().valueSTRING;
                    string outfile = x.Expressions["OUT_PATH"].Evaluate().valueSTRING;
                    if (File.Exists(outfile))
                    {
                        File.Delete(outfile);
                    }
                    System.IO.Compression.ZipFile.CreateFromDirectory(infile, outfile);

                };

                return new DynamicStructureMethod(Parent, this._Caller, ZIP, Parameters, false, kappa);

            }

            private DynamicStructureMethod KappaUnzip(Method Parent, ParameterCollection Parameters)
            {

                Action<ParameterCollection> kappa = (x) =>
                {

                    string infile = x.Expressions["IN_PATH"].Evaluate().valueSTRING;
                    string outfile = x.Expressions["OUT_PATH"].Evaluate().valueSTRING;
                    if (Directory.Exists(outfile))
                    {
                        DirectoryInfo di = new DirectoryInfo(outfile);
                        foreach (FileInfo fi in di.GetFiles())
                        {
                            fi.Delete();
                        }
                        di.Delete(true);
                    }
                    System.IO.Compression.ZipFile.ExtractToDirectory(infile, outfile);

                };
                return new DynamicStructureMethod(Parent, this._Caller, UNZIP, Parameters, false, kappa);

            }

            private DynamicStructureMethod KappaWriteAllText(Method Parent, ParameterCollection Parameters)
            {

                Action<ParameterCollection> kappa = (x) =>
                {

                    string file_path = x.Expressions["PATH"].Evaluate().valueSTRING;
                    if (File.Exists(file_path))
                    {
                        File.Delete(file_path);
                    }
                    File.WriteAllText(file_path, x.Expressions["TEXT"].Evaluate().valueSTRING);

                };
                return new DynamicStructureMethod(Parent, this._Caller, WRITE_ALL_TEXT, Parameters, false, kappa);

            }

            private DynamicStructureMethod KappaWriteAllBytes(Method Parent, ParameterCollection Parameters)
            {

                Action<ParameterCollection> kappa = (x) =>
                {

                    string file_path = x.Expressions["PATH"].Evaluate().valueSTRING;
                    if (File.Exists(file_path))
                    {
                        File.Delete(file_path);
                    }
                    File.WriteAllBytes(file_path, x.Expressions["BYTES"].Evaluate().valueBLOB);

                };
                return new DynamicStructureMethod(Parent, this._Caller, WRITE_ALL_BYTES, Parameters, false, kappa);


            }

            private DynamicStructureMethod KappaAppendAllText(Method Parent, ParameterCollection Parameters)
            {

                Action<ParameterCollection> kappa = (x) =>
                {

                    string file_path = x.Expressions["PATH"].Evaluate().valueSTRING;
                    File.AppendAllText(file_path, x.Expressions["TEXT"].Evaluate().valueSTRING);

                };
                return new DynamicStructureMethod(Parent, this._Caller, APPEND_ALL_TEXT, Parameters, false, kappa);

            }

            private DynamicStructureMethod KappaAppendAllBytes(Method Parent, ParameterCollection Parameters)
            {

                Action<ParameterCollection> kappa = (x) =>
                {

                    string file_path = x.Expressions["PATH"].Evaluate().valueSTRING;
                    using (FileStream fs = File.Open(file_path, FileMode.Append))
                    {
                        byte[] b = x.Expressions["BYTES"].Evaluate().valueBLOB;
                        fs.Write(b, 0, b.Length);
                    }

                };
                return new DynamicStructureMethod(Parent, this._Caller, APPEND_ALL_BYTES, Parameters, false, kappa);

            }

            private DynamicStructureMethod KappaDelete(Method Parent, ParameterCollection Parameters)
            {

                Action<ParameterCollection> kappa = (x) =>
                {

                    string file_path = x.Expressions["PATH"].Evaluate().valueSTRING;
                    if (File.Exists(file_path))
                        File.Delete(file_path);

                };
                return new DynamicStructureMethod(Parent, this._Caller, DELETE, Parameters, false, kappa);

            }

            private DynamicStructureMethod KappaMove(Method Parent, ParameterCollection Parameters)
            {

                Action<ParameterCollection> kappa = (x) =>
                {

                    string FromPath = x.Expressions["FROM_PATH"].Evaluate().valueSTRING;
                    string ToPath = x.Expressions["TO_PATH"].Evaluate().valueSTRING;
                    File.Move(FromPath, ToPath);

                };
                return new DynamicStructureMethod(Parent, this._Caller, MOVE, Parameters, false, kappa);

            }

            private DynamicStructureMethod KappaCopy(Method Parent, ParameterCollection Parameters)
            {

                Action<ParameterCollection> kappa = (x) =>
                {

                    string FromPath = x.Expressions["FROM_PATH"].Evaluate().valueSTRING;
                    string ToPath = x.Expressions["TO_PATH"].Evaluate().valueSTRING;
                    File.Copy(FromPath, ToPath);

                };
                return new DynamicStructureMethod(Parent, this._Caller, COPY, Parameters, false, kappa);

            }

            private DynamicStructureMethod KappaExport(Method Parent, ParameterCollection Parameters)
            {

                Action<ParameterCollection> kappa = (x) =>
                {

                    DataSet Data = x.Tables["DATA"];
                    string Path = x.Expressions["PATH"].Evaluate().valueSTRING;
                    char Delim = (x.Expressions["DELIM"] == null ? '\t' : x.Expressions["DELIM"].Evaluate().valueSTRING.First());
                    Kernel.TextDump(Data, Path, Delim); 

                };
                return new DynamicStructureMethod(Parent, this._Caller, EXPORT, Parameters, false, kappa);

            }

            private DynamicStructureMethod KappaImport(Method Parent, ParameterCollection Parameters)
            {

                Action<ParameterCollection> kappa = (x) =>
                {

                    DataSet Data = x.Tables["DATA"];
                    string Path = x.Expressions["PATH"].Evaluate().valueSTRING;
                    char[] Delim = x.Expressions["DELIM"].Evaluate().valueSTRING.ToCharArray();
                    char Escape = (x.Expressions["ESCAPE"] != null ? x.Expressions["ESCAPE"].Evaluate().valueSTRING.First() : char.MaxValue);
                    int Skip = (x.Expressions["SKIP"] != null ? (int)x.Expressions["SKIP"].Evaluate().valueINT : 0);
                    Kernel.TextPop(Data, Path, Delim, Escape, Skip);

                };
                return new DynamicStructureMethod(Parent, this._Caller, EXPORT, Parameters, false, kappa);

            }

            private DynamicStructureMethod KappaDownload(Method Parent, ParameterCollection Parameters)
            {

                Action<ParameterCollection> kappa = (x) =>
                {

                    string url = Parameters.Expressions["URL"].Evaluate().valueSTRING;
                    string path = Parameters.Expressions["PATH"].Evaluate().valueSTRING;

                    using (Stream writter = File.Create(path))
                    {

                        System.Net.WebRequest req = System.Net.HttpWebRequest.Create(url);

                        using (Stream reader = req.GetResponse().GetResponseStream())
                        {
                            reader.CopyTo(writter);
                        }

                    }

                };
                return new DynamicStructureMethod(Parent, this._Caller, EXPORT, Parameters, false, kappa);

            }

        }

    }


}
