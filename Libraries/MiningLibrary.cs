using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Structures;
using Rye.Expressions;
using Rye.Methods;
using Rye.Mining;
using Rye.Mining.Regression;

namespace Rye.Libraries
{


    // Mining Library //
    public sealed class MiningLibrary : Library
    {

        // Methods //
        public const string OLS = "OLS";
        public const string GLM = "GLM";
        public const string POISSON = "POISSON";
        public const string LOGISTIC = "LOGISTIC";
        public const string NLIN = "NLIN";

        private static string[] _MethodNames = new string[]
        {
            OLS,
            GLM,
            NLIN,
            POISSON,
            LOGISTIC
        };

        public MiningLibrary(Session Session)
            : base(Session, "MINER")
        {
        }

        public override Method GetMethod(Method Parent, string Name, ParameterCollection Parameters)
        {

            switch (Name.ToUpper())
            {

                case OLS:
                    return this.Method_OLS(Parent, Parameters);
                case GLM:
                    return this.Method_GLM(Parent, Parameters);
                case POISSON:
                    return this.Method_POISSON(Parent, Parameters);
                case LOGISTIC:
                    return this.Method_LOGISTIC(Parent, Parameters);

            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override ParameterCollectionSigniture GetMethodSigniture(string Name)
        {

            switch (Name.ToUpper())
            {

                case OLS:
                    return ParameterCollectionSigniture.Parse(OLS, "Performs ordinary least squares",
                "DATA|The dataset used to fit the model|T|false;INPUT|The input values|V|false;OUTPUT|The prediction values|V|false;WEIGHT|The regression weights|Value|true;FILTER|A filter on the data|Value|true");
                case GLM:
                    return ParameterCollectionSigniture.Parse(GLM, "Fits a generalized linear model using itterively reweighted least squares",
                "DATA|The dataset used to fit the model|T|false;INPUT|The input values|V|false;OUTPUT|The prediction values|V|false;WEIGHT|The regression weights|Value|true;FILTER|A filter on the data|Value|true;LINK|The link function|Value|false");
                case POISSON:
                    return ParameterCollectionSigniture.Parse(GLM, "Fits a poisson regression model; this is faster than using GLM",
                "DATA|The dataset used to fit the model|T|false;INPUT|The input values|V|false;OUTPUT|The prediction values|V|false;WEIGHT|The regression weights|Value|true;FILTER|A filter on the data|Value|true");
                case LOGISTIC:
                    return ParameterCollectionSigniture.Parse(GLM, "Fits a logistic regression model; this is faster than using GLM",
                "DATA|The dataset used to fit the model|T|false;INPUT|The input values|V|false;OUTPUT|The prediction values|V|false;WEIGHT|The regression weights|Value|true;FILTER|A filter on the data|Value|true");

            }
            throw new ArgumentException(string.Format("Method '{0}' does not exist", Name));

        }

        public override string[] MethodNames
        {
            get { return _MethodNames; }
        }

        public override CellFunction GetFunction(string Name)
        {
            throw new NotImplementedException();
        }

        public override string[] FunctionNames
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        // Methods //
        private Method Method_OLS(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (k) =>
            {

                // Extract the parameters //
                TabularData t = k.Tables["DATA"];
                ExpressionCollection x = k.ExpressionVectors["INPUT"];
                ExpressionCollection y = k.ExpressionVectors["OUTPUT"];
                Expression w = (k.Expressions.Exists("WEIGHT") ? k.Expressions["WEIGHT"] : new ExpressionValue(null, new Cell(1D)));
                Filter f = (k.Expressions.Exists("FILTER") ? new Filter(k.Expressions["FILTER"] ?? Filter.TrueForAll.Node) : Filter.TrueForAll);

                // Create the source input //
                Mining.MiningModelSource source = new Mining.MiningModelSource(this._Session, t, x, y, w, f);

                // Create the model //
                Mining.Regression.LinearRegressionModel model = new Mining.Regression.LinearRegressionModel(this._Session, source);

                // Run the beta piece //
                model.RenderModel();

                // Run the error piece //
                model.RenderError();

                // Output the info string //
                this._Session.IO.WriteLine(model.MetaData);

            };

            return new LibraryMethod(Parent, OLS, Parameters, false, kappa);

        }

        private Method Method_GLM(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (k) =>
            {

                // Extract the parameters //
                TabularData t = k.Tables["DATA"];
                ExpressionCollection x = k.ExpressionVectors["INPUT"];
                ExpressionCollection y = k.ExpressionVectors["OUTPUT"];
                Expression w = (k.Expressions.Exists("WEIGHT") ? k.Expressions["WEIGHT"] : new ExpressionValue(null, new Cell(1D)));
                Filter f = (k.Expressions.Exists("FILTER") ? new Filter(k.Expressions["FILTER"] ?? Filter.TrueForAll.Node) : Filter.TrueForAll);
                Lambda l = this._Session.GetLambda(k.Expressions["LINK"].Evaluate().valueSTRING);

                // Create the source input //
                Mining.MiningModelSource source = new Mining.MiningModelSource(this._Session, t, x, y, w, f);

                // Create the model //
                Mining.Regression.GeneralizedLinearModel model = new Mining.Regression.GeneralizedLinearModel(this._Session, source, l);

                // Run the beta piece //
                model.RenderModel();

                // Run the error piece //
                if (model.InnerParameters != null)
                {

                    model.RenderError();

                    // Output the info string //
                    this._Session.IO.WriteLine(model.MetaData);

                }
                else
                {

                    // Output the info string //
                    this._Session.IO.WriteLine("Model failed to converge");

                }

            };

            return new LibraryMethod(Parent, OLS, Parameters, false, kappa);

        }

        private Method Method_POISSON(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (k) =>
            {

                // Extract the parameters //
                TabularData t = k.Tables["DATA"];
                ExpressionCollection x = k.ExpressionVectors["INPUT"];
                ExpressionCollection y = k.ExpressionVectors["OUTPUT"];
                Expression w = (k.Expressions.Exists("WEIGHT") ? k.Expressions["WEIGHT"] : new ExpressionValue(null, new Cell(1D)));
                Filter f = (k.Expressions.Exists("FILTER") ? new Filter(k.Expressions["FILTER"] ?? Filter.TrueForAll.Node) : Filter.TrueForAll);
                
                // Create the source input //
                Mining.MiningModelSource source = new Mining.MiningModelSource(this._Session, t, x, y, w, f);

                // Create the model //
                Mining.Regression.PoissonRegression model = new Mining.Regression.PoissonRegression(this._Session, source);

                // Run the beta piece //
                model.RenderModel();

                // Run the error piece //
                if (model.InnerParameters != null)
                {

                    model.RenderError();

                    // Output the info string //
                    this._Session.IO.WriteLine(model.MetaData);

                }
                else
                {

                    // Output the info string //
                    this._Session.IO.WriteLine("Model failed to converge");

                }

            };

            return new LibraryMethod(Parent, OLS, Parameters, false, kappa);

        }

        private Method Method_LOGISTIC(Method Parent, ParameterCollection Parameters)
        {

            Action<ParameterCollection> kappa = (k) =>
            {

                // Extract the parameters //
                TabularData t = k.Tables["DATA"];
                ExpressionCollection x = k.ExpressionVectors["INPUT"];
                ExpressionCollection y = k.ExpressionVectors["OUTPUT"];
                Expression w = (k.Expressions.Exists("WEIGHT") ? k.Expressions["WEIGHT"] : new ExpressionValue(null, new Cell(1D)));
                Filter f = (k.Expressions.Exists("FILTER") ? new Filter(k.Expressions["FILTER"] ?? Filter.TrueForAll.Node) : Filter.TrueForAll);

                // Create the source input //
                Mining.MiningModelSource source = new Mining.MiningModelSource(this._Session, t, x, y, w, f);

                // Create the model //
                Mining.Regression.LogisticRegression model = new Mining.Regression.LogisticRegression(this._Session, source);

                // Run the beta piece //
                model.RenderModel();

                // Run the error piece //
                if (model.InnerParameters != null)
                {

                    model.RenderError();

                    // Output the info string //
                    this._Session.IO.WriteLine(model.MetaData);

                }
                else
                {

                    // Output the info string //
                    this._Session.IO.WriteLine("Model failed to converge");

                }

            };

            return new LibraryMethod(Parent, OLS, Parameters, false, kappa);

        }

    }


}
