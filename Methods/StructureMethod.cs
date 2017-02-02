using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.MatrixExpressions;
using Rye.Structures;

namespace Rye.Methods
{

    public enum ParameterAffinity
    {
        Expression = 0, // Value, Nu
        ExpressionVector = 1, // V, NuV
        Table = 2, // T, Tau
        Matrix = 3 // M, Mu
    }

    public static class ParameterAffinityHelper
    {

        private static ParameterAffinity[] Enums = new ParameterAffinity[]
        {
            ParameterAffinity.Expression,
            ParameterAffinity.ExpressionVector,
            ParameterAffinity.Matrix,
            ParameterAffinity.Table
        };

        private static string[] ProperNames = new string[]
        {
            ParameterAffinity.Expression.ToString(),
            ParameterAffinity.ExpressionVector.ToString(),
            ParameterAffinity.Matrix.ToString(),
            ParameterAffinity.Table.ToString()
        };

        private static string[] ShortNames = new string[]
        {
            "Value",
            "V",
            "M",
            "T"
        };

        private static string[] GreekNames = new string[]
        {
            "ALPHA",
            "GAMMA",
            "MU",
            "TAU"
        };

        private static string[] IntNames = new string[]
        {
            "0",
            "1",
            "2",
            "3"
        };

        public static ParameterAffinity Parse(string Text)
        {

            StringComparer comp = StringComparer.OrdinalIgnoreCase;
            string trimmed = Text.Trim();

            for (int i = 0; i < Enums.Length; i++)
            {
                
                if (comp.Compare(trimmed, ProperNames[i]) == 0)
                    return Enums[i];
                if (comp.Compare(trimmed, ShortNames[i]) == 0)
                    return Enums[i];
                if (comp.Compare(trimmed, GreekNames[i]) == 0)
                    return Enums[i];
                if (comp.Compare(trimmed, IntNames[i]) == 0)
                    return Enums[i];

            }

            throw new ArgumentException(string.Format("Text '{0}' does not correlate to valid ParameterAffinity", Text));

        }

        public static string ToProperName(ParameterAffinity Affinity)
        {
            return ProperNames[(int)Affinity];
        }

        public static string ToShortName(ParameterAffinity Affinity)
        {
            return ShortNames[(int)Affinity];
        }

        public static string ToGreekName(ParameterAffinity Affinity)
        {
            return GreekNames[(int)Affinity];
        }

        public static string ToIntName(ParameterAffinity Affinity)
        {
            return IntNames[(int)Affinity];
        }

    }

    public sealed class ParameterCollection 
    {

        private Heap<ExpressionCollection> _vCache;
        private Heap<Expression> _eCache;
        private Heap<TabularData> _tCache;
        private Heap<MatrixExpression> _mCache;
        private Heap2<ParameterAffinity, bool> _MetaData; // ParameterAffinty = object type, bool == is null variable, to help quickly figure out if something is null

        public ParameterCollection()
        {
            this._vCache = new Heap<ExpressionCollection>();
            this._eCache = new Heap<Expression>();
            this._tCache = new Heap<TabularData>();
            this._mCache = new Heap<MatrixExpression>();
            this._MetaData = new Heap2<ParameterAffinity, bool>();
        }

        // Vector Functions //
        public Heap<Expression> Expressions
        {
            get { return this._eCache; }
        }

        public Heap<ExpressionCollection> ExpressionVectors
        {
            get { return this._vCache; }
        }

        public Heap<TabularData> Tables
        {
            get { return this._tCache; }
        }

        public Heap<MatrixExpression> Matricies
        {
            get { return this._mCache; }
        }

        public bool IsNull(string Name)
        {
            return this._MetaData[Name].Item2;
        }

        public bool IsNull(int Index)
        {
            return this._MetaData[Index].Item2;
        }

        public ParameterAffinity Affinity(string Name)
        {
            return this._MetaData[Name].Item1;
        }

        public ParameterAffinity Affinity(int Index)
        {
            return this._MetaData[Index].Item1;
        }

        // Global Methods //
        public int Count
        {
            get { return this._MetaData.Count; }
        }

        public bool Exists(string Name)
        {
            return this._MetaData.Exists(Name);
        }

        public void Add(string Name, ExpressionCollection Value)
        {
            if (this.Exists(Name))
                throw new ArgumentException(string.Format("Value '{0}' already exists", Name));
            this._vCache.Allocate(Name, Value);
            this._MetaData.Allocate(Name, new Tuple<ParameterAffinity, bool>(ParameterAffinity.ExpressionVector, Value == null ? true : false));
        }

        public void Add(string Name, Expression Value)
        {
            if (this.Exists(Name))
                throw new ArgumentException(string.Format("Value '{0}' already exists", Name));
            this._eCache.Allocate(Name, Value);
            this._MetaData.Allocate(Name, new Tuple<ParameterAffinity, bool>(ParameterAffinity.Expression, Value == null ? true : false));
        }

        public void Add(string Name, TabularData Value)
        {
            if (this.Exists(Name))
                throw new ArgumentException(string.Format("Value '{0}' already exists", Name));
            this._tCache.Allocate(Name, Value);
            this._MetaData.Allocate(Name, new Tuple<ParameterAffinity, bool>(ParameterAffinity.Table, Value == null ? true : false));
        }

        public void Add(string Name, MatrixExpression Value)
        {
            if (this.Exists(Name))
                throw new ArgumentException(string.Format("Value '{0}' already exists", Name));
            this._mCache.Allocate(Name, Value);
            this._MetaData.Allocate(Name, new Tuple<ParameterAffinity, bool>(ParameterAffinity.Matrix, Value == null ? true : false));
        }

        public void AddNull(string Name, ParameterAffinity Affinity)
        {

            if (this.Exists(Name))
                throw new ArgumentException(string.Format("Value '{0}' already exists", Name));

            switch (Affinity)
            {

                case ParameterAffinity.Expression:
                    this._eCache.Allocate(Name, null);
                    break;
                case ParameterAffinity.ExpressionVector:
                    this._vCache.Allocate(Name, null);
                    break;
                case ParameterAffinity.Matrix:
                    this._mCache.Allocate(Name, null);
                    break;
                case ParameterAffinity.Table:
                    this._tCache.Allocate(Name, null);
                    break;

            }

        }

        public ParameterCollection CloneOfMe()
        {

            ParameterCollection pc = new ParameterCollection();

            // clone expressions //
            foreach (KeyValuePair<string,Expression> x in this._eCache.Entries)
            {
                pc.Add(x.Key, x.Value.CloneOfMe());
            }

            // clone expression vectors //
            foreach (KeyValuePair<string, ExpressionCollection> x in this._vCache.Entries)
            {
                pc.Add(x.Key, x.Value.CloneOfMe());
            }

            // clone matrix expressions //
            foreach (KeyValuePair<string, MatrixExpression> x in this._mCache.Entries)
            {
                pc.Add(x.Key, x.Value.CloneOfMe());
            }

            // add tables, do not clone //
            foreach (KeyValuePair<string, TabularData> x in this._tCache.Entries)
            {
                pc.Add(x.Key, x.Value);
            }

            return pc;

        }

    }

    public sealed class ParameterCollectionSigniture
    {

        public const string ZERO_PARAMETER = "ZERO";

        private Heap3<string, ParameterAffinity, bool> _BaseMap;
        private string _MethodName;
        private string _MethodDesc;
        
        public ParameterCollectionSigniture(string MethodName, string MethodDesc)
        {
            this._BaseMap = new Heap3<string, ParameterAffinity, bool>();
            this._MethodName = MethodName;
            this._MethodDesc = MethodDesc;
        }

        public string MethodName
        {
            get { return this._MethodName; }
        }

        public string Description
        {
            get { return this._MethodDesc; }
        }

        public int Count
        {
            get { return this._BaseMap.Count; }
        }

        public bool Exists(string Name)
        {
            return this._BaseMap.Exists(Name);
        }

        public bool CanBeNull(string Name)
        {
            return this._BaseMap[Name].Item3;
        }

        public bool CanBeNull(int Index)
        {
            return this._BaseMap[Index].Item3;
        }

        public ParameterAffinity ParameterAffinity(string Name)
        {
            return this._BaseMap[Name].Item2;
        }

        public ParameterAffinity ParameterAffinity(int Index)
        {
            return this._BaseMap[Index].Item2;
        }

        public string ParameterDescription(string Name)
        {
            return this._BaseMap[Name].Item1;
        }

        public string ParameterDescription(int Index)
        {
            return this._BaseMap[Index].Item1;
        }

        public string Name(int Index)
        {
            return this._BaseMap.Name(Index);
        }

        public void AddElement(string Name, string Description, ParameterAffinity Affinity, bool CanBeNull)
        {

            this._BaseMap.Allocate(Name, new Tuple<string, ParameterAffinity, bool>(Description, Affinity, CanBeNull));

        }

        public void AddElement(string Text)
        {

            // Handle an empty element //
            if (Text == ZERO_PARAMETER)
                return;

            // Expects: Name|Description|Affinty|Bool
            string[] t = Text.Split('|');
            if (t.Length != 4)
                throw new ArgumentException(string.Format("Expecting the form 'Name|Description|Affinity|CanBeNull'; recieved '{0}'", Text));
            string Name = t[0];
            string Description = t[1];
            ParameterAffinity Affinity = ParameterAffinityHelper.Parse(t[2]);
            bool CanBeNull = Helpers.BooleanParserHelper.Parse(t[3].Trim());

            this.AddElement(Name, Description, Affinity, CanBeNull);

        }

        public void Check(ParameterCollection Parameters)
        {

            for (int i = 0; i < this.Count; i++)
            {

                string name = this._BaseMap.Name(i);
                
                // If it exists, then check the affinity and nullness //
                if (Parameters.Exists(name))
                {

                    // Check nullness //
                    if (Parameters.IsNull(name) && !this.CanBeNull(name))
                        throw new ArgumentException(string.Format("Variable '{0}' cannot be null", name));

                    // Check affinity //
                    if (Parameters.Affinity(name) != this.ParameterAffinity(name))
                        throw new ArgumentException(string.Format("Variable passed has affinity '{0}' but '{1}' must be '{2}'", Parameters.Affinity(name), name, this.ParameterAffinity(name)));

                }
                // Otherwise, fix the paramter collection by adding a null Value //
                else if (this.CanBeNull(i))
                {

                    Parameters.AddNull(name, this.ParameterAffinity(name));

                }
                // Finally, we must be missing a critical element //
                else
                {

                    throw new ArgumentException(string.Format("Missing critical element '{0}'", name));

                }

            }


        }

        public static ParameterCollectionSigniture Parse(string Name, string Description, string Text)
        {

            ParameterCollectionSigniture sig = new ParameterCollectionSigniture(Name, Description);
            if (Text != null)
            {
                string[] text = Text.Split(';');
                foreach (string t in text)
                {
                    sig.AddElement(t);
                }
            }

            return sig;

        }

    }

    /*
    public abstract class StructureMethod : Method
    {

        private bool _CanBeAsync = false;

        public StructureMethod(Method Storage, MemoryStructure NameSpace, string Name, ParameterCollection Parameters, bool CanBeAsync)
            : base(Storage, NameSpace)
        {
            this._CanBeAsync = CanBeAsync;
            this.Parameters = Parameters;
        }

        public string Name
        {
            get;
            protected set;
        }
    
        public ParameterCollection Parameters
        {
            get;
            protected set;
        }

        public override bool CanBeAsync
        {
	        get 
	        { 
		         return this._CanBeAsync;
	        }
        }

        public override string Message()
        {
            return string.Format("Structure Method: {0}", this.Name);
        }

    }

    public sealed class DynamicStructureMethod : StructureMethod
    {

        private Action<ParameterCollection> _BeginInvoke;
        private Action<ParameterCollection> _Invoke;
        private Action<ParameterCollection> _EndInvoke;

        public DynamicStructureMethod(Method Storage, MemoryStructure NameSpace, string Name, ParameterCollection Parameters, bool CanBeAsync, 
            Action<ParameterCollection> Initial, Action<ParameterCollection> Main, Action<ParameterCollection> Finish)
            :base(Storage, NameSpace, Name, Parameters, CanBeAsync)
        {
            this._BeginInvoke = Initial;
            this._Invoke = Main;
            this._EndInvoke = Finish;
        }

        public DynamicStructureMethod(Method Storage, MemoryStructure NameSpace, string Name, ParameterCollection Parameters, bool CanBeAsync,
            Action<ParameterCollection> Main)
            : this(Storage, NameSpace, Name, Parameters, CanBeAsync, (OriginalNode) => { }, Main, (OriginalNode) => { })
        {
        }
     
        public override void BeginInvoke()
        {
            if (this._BeginInvoke != null)
                this._BeginInvoke(this.Parameters);
        }

        public override void Invoke()
        {
            this._Invoke(this.Parameters);
        }

        public override void EndInvoke()
        {
            if (this._EndInvoke != null)
                this._EndInvoke(this.Parameters);
        }

        public override Method CloneOfMe()
        {
            return new DynamicStructureMethod(this._Parent, this._Heap, this.Name, this.Parameters, this.CanBeAsync, this._BeginInvoke, this._Invoke, this._EndInvoke);
        }

    }
    */

    public sealed class LibraryMethod : Method
    {

        private Action<ParameterCollection> _BeginInvoke;
        private Action<ParameterCollection> _Invoke;
        private Action<ParameterCollection> _EndInvoke;
        private bool _CanBeAsync = false;
        private ParameterCollection _Parameters;
        private string _Name;

        public LibraryMethod(Method Parent, string Name, ParameterCollection Parameters, bool CanBeAsync, 
            Action<ParameterCollection> Initial, Action<ParameterCollection> Main, Action<ParameterCollection> Finish)
            :base(Parent)
        {

            this._CanBeAsync = CanBeAsync;
            this._Parameters = Parameters;
            this._Name = Name;

            this._BeginInvoke = Initial;
            this._Invoke = Main;
            this._EndInvoke = Finish;

        }

        public LibraryMethod(Method Parent, string Name, ParameterCollection Parameters, bool CanBeAsync,
            Action<ParameterCollection> Main)
            : this(Parent, Name, Parameters, CanBeAsync, (x) => { }, Main, (x) => { })
        {
        }
     
        public override void BeginInvoke()
        {
            if (this._BeginInvoke != null)
                this._BeginInvoke(this._Parameters);
        }

        public override void Invoke()
        {
            this._Invoke(this._Parameters);
        }

        public override void EndInvoke()
        {
            if (this._EndInvoke != null)
                this._EndInvoke(this._Parameters);
        }

        public override Method CloneOfMe()
        {
            return new LibraryMethod(this._Parent, this._Name, this._Parameters, this.CanBeAsync, this._BeginInvoke, this._Invoke, this._EndInvoke);
        }

        public override bool CanBeAsync
        {
            get
            {
                return this._CanBeAsync;
            }
        }

        public override List<Expression> InnerExpressions()
        {

            List<Expression> val = new List<Expression>();

            // Expression Collections //
            foreach (ExpressionCollection x in this._Parameters.ExpressionVectors.Values)
            {

                foreach (Expression y in x.Nodes)
                {
                    val.Add(y);
                }

            }

            // Expressions //
            foreach (Expression y in this._Parameters.Expressions.Values)
            {
                val.Add(y);
            }

            return val;

        }

    }

}
