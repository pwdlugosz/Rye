using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Methods;
using Rye.Expressions;
using Rye.Data;
using Rye.Structures;
using System.IO;

namespace Rye.Libraries
{

    public sealed class FileFunctionLibrary : FunctionLibrary
    {

        public const string LIBRARY_NAME = "FILE";

        public FileFunctionLibrary(Session Session)
            : base(Session)
        {
            this.LibName = LIBRARY_NAME;
        }

        // Functions //
        public const string READ_ALL_TEXT = "READ_ALL_TEXT";
        public const string READ_ALL_BYTES = "READ_ALL_BYTES";
        public const string SIZE = "SIZE";
        public const string EXISTS = "EXISTS";
        public const string ISDIR = "ISDIR";
        public const string MYDOC = "MYDOC";

        private static string[] _FunctionNames = new string[]
        {
            READ_ALL_TEXT,
            READ_ALL_BYTES,
            SIZE,
            EXISTS,
            ISDIR,
            MYDOC
        };

        public override CellFunction RenderFunction(string Name)
        {

            switch (Name)
            {

                case FileFunctionLibrary.READ_ALL_TEXT: 
                    return this.Function_ReadAllText();

                case FileFunctionLibrary.READ_ALL_BYTES: 
                    return this.Function_ReadAllBytes();

                case FileFunctionLibrary.SIZE: 
                    return this.Function_FileSize();

                case FileFunctionLibrary.EXISTS: 
                    return this.Function_FileExists();

                case FileFunctionLibrary.ISDIR: 
                    return this.Function_FileIsDir();

                case FileFunctionLibrary.MYDOC:
                    return this.Function_FileMyDoc();

            }

            throw new ArgumentException(string.Format("Function '{0}' does not exist", Name));

        }

        public override string[] Names
        {
            get { return FileFunctionLibrary._FunctionNames; }
        }

        private CellFunction Function_ReadAllText()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                string file_name = x[0].valueSTRING;
                Cell c = new Cell(File.ReadAllText(file_name));
                return c;
            };
            return new CellFunctionFixedShell(READ_ALL_TEXT, 1, CellAffinity.STRING, lambda);

        }

        private CellFunction Function_ReadAllBytes()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                string file_name = x[0].valueSTRING;
                Cell c = new Cell(File.ReadAllBytes(file_name));
                return c;
            };
            return new CellFunctionFixedShell(READ_ALL_BYTES, 1, CellAffinity.BLOB, lambda);

        }

        private CellFunction Function_FileSize()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {

                string path = x[0].valueSTRING;

                if (File.Exists(path))
                    return new Cell(new FileInfo(path).Length);

                if (Directory.Exists(path))
                    return new Cell(DirSize(new DirectoryInfo(path), 0));

                return Cell.NULL_INT;

            };
            return new CellFunctionFixedShell(SIZE, 1, CellAffinity.INT, lambda);

        }

        private CellFunction Function_FileExists()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                string file_name = x[0].valueSTRING;
                Cell c = new Cell(File.Exists(file_name) || Directory.Exists(file_name));
                return c;
            };
            return new CellFunctionFixedShell(EXISTS, 1, CellAffinity.BOOL, lambda);

        }

        private CellFunction Function_FileIsDir()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                string file_name = x[0].valueSTRING;
                Cell c = new Cell(Directory.Exists(file_name));
                return c;
            };
            return new CellFunctionFixedShell(ISDIR, 1, CellAffinity.BOOL, lambda);

        }

        private CellFunction Function_FileMyDoc()
        {

            Func<Cell[], Cell> lambda = (x) =>
            {
                return new Cell(Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments, Environment.SpecialFolderOption.DoNotVerify));
            };
            return new CellFunctionFixedShell(MYDOC, 0, CellAffinity.STRING, lambda);

        }

        public static bool IsDir(string Path)
        {
            return Path.Last() == '\\';
        }

        public static bool IsFile(string Path)
        {
            return File.Exists(Path);
        }

        public static long DirSize(DirectoryInfo Dir, int Level)
        {

            long ticks = 0;
            foreach (FileInfo fi in Dir.GetFiles())
            {
                ticks += fi.Length;
            }

            if (Level >= 128)
                return ticks;

            foreach (DirectoryInfo di in Dir.GetDirectories())
            {
                ticks += DirSize(di, Level++);
            }

            return ticks;

        }

        public static string SpecialDir()
        {
            return Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments, Environment.SpecialFolderOption.DoNotVerify);
        }

    }

    public sealed class FileMethodLibrary : MethodLibrary
    {

        public const string LIBRARY_NAME = "FILE";

        public FileMethodLibrary(Session Session)
            : base(Session)
        {
            this.Build();
            this.LibName = LIBRARY_NAME;
        }

        // Methods //
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
        public const string MAKE = "MAKE";
        public const string BUILD_FT = "BUILD_FT";

        private static string[] _MethodNames = new string[]
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
            DOWNLOAD,
            MAKE,
            BUILD_FT
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

        private void Build()
        {

            this._CompressedSig = new Heap2<string, string>();

            this._CompressedSig.Allocate(FileMethodLibrary.ZIP, "Zips a file", "IN_PATH|The path of the file to zip|E|false;OUT_PATH|The path to the zipped file|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.UNZIP, "Unzips a file", "IN_PATH|The path of the file to unzip (*.zip)|E|false;OUT_PATH|The path to directory|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.WRITE_ALL_TEXT, "Writes text to a file; if the file already exists, it will be overwritten", "PATH|The path of the file to dump text to|E|false;TEXT|Text to write|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.WRITE_ALL_BYTES, "Writes bytes to a file; if the file already exists, it will be overwritten", "PATH|The path of the file to dump text to|E|false;OUT_PATH|Bytes to write|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.APPEND_ALL_TEXT, "Writes text to a file; if the file exists, it will append the text to the end", "PATH|The path of the file to dump text to|E|false;TEXT|Text to write|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.APPEND_ALL_BYTES, "Writes bytes to a file; if the file exists, it will append the bytes to the end", "PATH|The path of the file to dump text to|E|false;OUT_PATH|Bytes to write|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.DELETE, "Deletes a file", "PATH|The file to delete|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.MOVE, "Moves a file to another location", "FROM_PATH|The original file to move|E|false;TO_PATH|The new location to move to|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.COPY, "Copies a file to another location", "FROM_PATH|The original file to copy|E|false;TO_PATH|The path to put the copy in|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.IMPORT, "Loads a file into an existing table", "DATA|The table to load|T|false;PATH|The flat file location|E|false;DELIM|The column delimitor|E|false;ESCAPE|The escape sequence character|E|true;SKIP|The number of lines to skip|E|true");
            this._CompressedSig.Allocate(FileMethodLibrary.EXPORT, "Exports a table into a new file", "DATA|The table to export|T|false;PATH|The path to the exported file|E|false;DELIM|The column delimitor|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.DOWNLOAD, "Downloads a url to a file", "URL|The URL to download|E|false;PATH|The path to the exported file|E|false;POST|The post string|E|true");
            this._CompressedSig.Allocate(FileMethodLibrary.MAKE, "Creates a file or directory", "PATH|The path to be created|E|false");
            this._CompressedSig.Allocate(FileMethodLibrary.BUILD_FT, "Creates a file table", "PATH|The path to the directory being traversed|E|false;NAME|The table create and load with data|E|false");

        }

        public override Method RenderMethod(Method Parent, string Name, ParameterCollection Parameters)
        {

            switch (Name.ToUpper())
            {

                case FileMethodLibrary.ZIP:
                    return this.Method_Zip(Parent, Parameters);
                case FileMethodLibrary.UNZIP:
                    return this.Method_Unzip(Parent, Parameters);
                case FileMethodLibrary.WRITE_ALL_TEXT:
                    return this.Method_WriteAllText(Parent, Parameters);
                case FileMethodLibrary.WRITE_ALL_BYTES:
                    return this.Method_WriteAllBytes(Parent, Parameters);
                case FileMethodLibrary.APPEND_ALL_TEXT:
                    return this.Method_AppendAllText(Parent, Parameters);
                case FileMethodLibrary.APPEND_ALL_BYTES:
                    return this.Method_AppendAllBytes(Parent, Parameters);
                case FileMethodLibrary.DELETE:
                    return this.Method_Delete(Parent, Parameters);
                case FileMethodLibrary.MOVE:
                    return this.Method_Move(Parent, Parameters);
                case FileMethodLibrary.COPY:
                    return this.Method_Copy(Parent, Parameters);
                case FileMethodLibrary.IMPORT:
                    return this.Method_Import(Parent, Parameters);
                case FileMethodLibrary.EXPORT:
                    return this.Method_Export(Parent, Parameters);
                case FileMethodLibrary.DOWNLOAD:
                    return this.Method_Download(Parent, Parameters);
                case FileMethodLibrary.MAKE:
                    return this.Method_Make(Parent, Parameters);
                case FileMethodLibrary.BUILD_FT:
                    return this.Method_FileTable(Parent, Parameters);

            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override ParameterCollectionSigniture RenderSigniture(string Name)
        {

            if (this._CompressedSig.Exists(Name))
                return ParameterCollectionSigniture.Parse(Name, this._CompressedSig[Name].Item1, this._CompressedSig[Name].Item2);
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override string[] Names
        {
            get { return FileMethodLibrary._MethodNames; }
        }

        // Method support //
        private Method Method_Zip(Method Parent, ParameterCollection Parameters)
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

            return new LibraryMethod(Parent, ZIP, Parameters, false, kappa);

        }

        private Method Method_Unzip(Method Parent, ParameterCollection Parameters)
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
            return new LibraryMethod(Parent, UNZIP, Parameters, false, kappa);

        }

        private Method Method_WriteAllText(Method Parent, ParameterCollection Parameters)
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
            return new LibraryMethod(Parent, WRITE_ALL_TEXT, Parameters, false, kappa);

        }

        private Method Method_WriteAllBytes(Method Parent, ParameterCollection Parameters)
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
            return new LibraryMethod(Parent, WRITE_ALL_BYTES, Parameters, false, kappa);


        }

        private Method Method_AppendAllText(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string file_path = x.Expressions["PATH"].Evaluate().valueSTRING;
                File.AppendAllText(file_path, x.Expressions["TEXT"].Evaluate().valueSTRING);

            };
            return new LibraryMethod(Parent, APPEND_ALL_TEXT, Parameters, false, kappa);

        }

        private Method Method_AppendAllBytes(Method Parent, ParameterCollection Parameters)
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
            return new LibraryMethod(Parent, APPEND_ALL_BYTES, Parameters, false, kappa);

        }

        private Method Method_Delete(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string file_path = x.Expressions["PATH"].Evaluate().valueSTRING;

                if (File.Exists(file_path))
                {
                    File.Delete(file_path);
                    return;
                }

                if (Directory.Exists(file_path))
                {
                    Directory.Delete(file_path);
                    return;
                }

            };
            return new LibraryMethod(Parent, DELETE, Parameters, false, kappa);

        }

        private Method Method_Move(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string FromPath = x.Expressions["FROM_PATH"].Evaluate().valueSTRING;
                string ToPath = x.Expressions["TO_PATH"].Evaluate().valueSTRING;

                if (FileFunctionLibrary.IsDir(FromPath) != FileFunctionLibrary.IsDir(ToPath))
                {
                    throw new ArgumentException(string.Format("Both a directory and file path were passed; both paths must be either directories or both files; \n{0} \n{1}", FromPath, ToPath));
                }

                if (File.Exists(FromPath))
                {
                    File.Move(FromPath, ToPath);
                    return;
                }

                if (Directory.Exists(FromPath))
                {
                    File.Move(FromPath, ToPath);
                    return;
                }

            };
            return new LibraryMethod(Parent, MOVE, Parameters, false, kappa);

        }

        private Method Method_Copy(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string FromPath = x.Expressions["FROM_PATH"].Evaluate().valueSTRING;
                string ToPath = x.Expressions["TO_PATH"].Evaluate().valueSTRING;

                if (FileFunctionLibrary.IsDir(FromPath) != FileFunctionLibrary.IsDir(ToPath))
                {
                    throw new ArgumentException(string.Format("Both a directory and file path were passed; both paths must be either directories or both files; \n{0} \n{1}", FromPath, ToPath));
                }

                if (File.Exists(FromPath))
                {
                    File.Copy(FromPath, ToPath);
                    return;
                }

                if (Directory.Exists(FromPath))
                {
                    File.Copy(FromPath, ToPath);
                    return;
                }

            };
            return new LibraryMethod(Parent, COPY, Parameters, false, kappa);

        }

        private Method Method_Export(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData Data = x.Tables["DATA"];
                string Path = x.Expressions["PATH"].Evaluate().valueSTRING;
                char Delim = (x.Expressions["DELIM"] == null ? '\t' : x.Expressions["DELIM"].Evaluate().valueSTRING.First());
                char Escape = (!x.Expressions.Exists("ESCAPE") ? char.MinValue : x.Expressions["ESCAPE"].Evaluate().valueSTRING.First());
                this._Session.Kernel.TextDump(Data, Path, Delim, Escape);

            };
            return new LibraryMethod(Parent, EXPORT, Parameters, false, kappa);

        }

        private Method Method_Import(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                TabularData Data = x.Tables["DATA"];
                string Path = x.Expressions["PATH"].Evaluate().valueSTRING;
                char[] Delim = x.Expressions["DELIM"].Evaluate().valueSTRING.ToCharArray();
                char Escape = (x.Expressions["ESCAPE"] != null ? x.Expressions["ESCAPE"].Evaluate().valueSTRING.First() : char.MaxValue);
                int Skip = (x.Expressions["SKIP"] != null ? (int)x.Expressions["SKIP"].Evaluate().valueINT : 0);
                this._Session.Kernel.TextPop(Data, Path, Delim, Escape, Skip);

            };
            return new LibraryMethod(Parent, EXPORT, Parameters, false, kappa);

        }

        private Method Method_Download(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string url = Parameters.Expressions["URL"].Evaluate().valueSTRING;
                string path = Parameters.Expressions["PATH"].Evaluate().valueSTRING;
                string post = Parameters.Exists("POST") ? Parameters.Expressions["POST"].Evaluate().valueSTRING : null;

                if (post == null)
                {
                    WebSupport.HTTP_Request_Get(url, path);
                }
                else
                {
                    WebSupport.HTTP_Request_Post(url, path, post);
                }

            };
            return new LibraryMethod(Parent, EXPORT, Parameters, false, kappa);

        }

        private Method Method_Make(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                string path = Parameters.Expressions["PATH"].Evaluate().valueSTRING;
                if (File.Exists(path) || Directory.Exists(path))
                {
                    return;
                }

                if (FileFunctionLibrary.IsDir(path))
                {
                    Directory.CreateDirectory(path);
                }

                if (!FileFunctionLibrary.IsDir(path))
                {
                    File.Create(path);
                }

            };
            return new LibraryMethod(Parent, EXPORT, Parameters, false, kappa);

        }

        private Method Method_FileTable(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (x) =>
            {

                // Get the data parameters //
                string Path = Parameters.Expressions["PATH"].Evaluate().valueSTRING;
                string FullTableName = Parameters.Expressions["NAME"].Evaluate().valueSTRING;

                // Create the table and open a writer //
                TabularData t = this._Session.CreateTabularData(FullTableName, FileMethodLibrary.FTSchema);
                RecordWriter w = t.OpenUncheckedWriter(t.Columns.GetHashCode());
                
                // Build the call stack //
                Quack<DirectoryInfo> Heap = new Quack<DirectoryInfo>(Quack<DirectoryInfo>.QuackState.FIFO);
                
                // Add the parameter sent //
                Heap.Allocate(new DirectoryInfo(Path));
                while (!Heap.IsEmpty)
                {

                    FileMethodLibrary.AppendFileTable(Heap.Deallocate(), w, Heap);

                }

                // Close out the stream //
                w.Close();

            };
            return new LibraryMethod(Parent, BUILD_FT, Parameters, false, kappa);

        }

        // File Table Support //
        private static Schema FTSchema
        {
            get
            {

                Schema s = new Schema();
                s.Add("PATH", CellAffinity.STRING, 256);
                s.Add("DIR", CellAffinity.STRING, 128);
                s.Add("NAME", CellAffinity.STRING, 128);
                s.Add("EXTENSION", CellAffinity.STRING, 16);
                s.Add("CREATE_DATE", CellAffinity.DATE_TIME);
                s.Add("LAST_ACCESS_DATE", CellAffinity.DATE_TIME);
                s.Add("LAST_WRITE_DATE", CellAffinity.DATE_TIME);
                s.Add("SIZE", CellAffinity.INT);
                s.Add("IS_READ_ONLY", CellAffinity.BOOL);

                return s;

            }
        }

        private static long AppendFileTable(DirectoryInfo LeafNode, RecordWriter Stream, Quack<DirectoryInfo> Roots)
        {

            long ticks = 0;
            
            // Go through each directory in the node, only if the 'Roots' variable is not null //
            if (Roots != null)
            {

                foreach (DirectoryInfo d in LeafNode.GetDirectories())
                {
                    Roots.Allocate(d);
                }

            }

            // Go through each file in the directory leaf //
            foreach (FileInfo f in LeafNode.GetFiles())
            {

                RecordBuilder rb = new RecordBuilder();
                rb.Add(f.FullName);
                rb.Add(f.Directory.FullName);
                rb.Add(f.Name.Replace(f.Extension,""));
                rb.Add(f.Extension.Replace(".", ""));
                rb.Add(f.CreationTime);
                rb.Add(f.LastAccessTime);
                rb.Add(f.LastWriteTime);
                rb.Add(f.Length);
                rb.Add(f.IsReadOnly);

                Stream.Insert(rb.ToRecord());

                ticks++;

            }

            return ticks;

        }

    }

    public static class WebSupport
    {

        public static void HTTP_Request_Get(string URL, string Path)
        {

            try
            {
                    
                // Open a writer
                using (Stream writter = File.Create(Path))
                {

                    // Create the web request
                    System.Net.WebRequest req = System.Net.HttpWebRequest.Create(URL);

                    // Dump to the output writer
                    using (Stream reader = req.GetResponse().GetResponseStream())
                    {
                        reader.CopyTo(writter);
                    }


                }

            }
            catch
            {

            }

            

        }

        public static void HTTP_Request_Post(string URL, string Path, string PostString)
        {


            try
            {

                // Open a writer
                using (Stream writter = File.Create(Path))
                {

                    // Create the web request
                    System.Net.WebRequest req = System.Net.HttpWebRequest.Create(URL);

                    // Set the posting attributes //
                    req.Method = "POST";
                    req.ContentType = "application/x-www-form-urlencoded";

                    // Set the posting variables //
                    byte[] hash = System.Text.Encoding.ASCII.GetBytes(PostString);
                    req.ContentLength = hash.Length;

                    // Write the post data to a stream //
                    using (Stream post = req.GetRequestStream())
                    {
                        post.Write(hash, 0, hash.Length);
                    }

                    // Dump to the output writer
                    using (Stream reader = req.GetResponse().GetResponseStream())
                    {
                        reader.CopyTo(writter);
                    }

                }

            }
            catch
            {

            }



        }


    }




}
