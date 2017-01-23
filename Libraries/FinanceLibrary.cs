using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Methods;

namespace Rye.Libraries
{


    // Finance Library //
    public sealed class FinanceLibrary : Library
    {

        public const string F_BS_CALL = "F_BS_CALL";
        public const string F_BS_CALL_DELTA = "F_BS_CALL_DELTA";
        public const string F_BS_CALL_GAMMA = "F_BS_CALL_GAMMA";
        public const string F_BS_CALL_THETA = "F_BS_CALL_THETA";
        public const string F_BS_CALL_VEGA = "BS_CALL_VEGA";
        public const string F_BS_CALL_RHO = "BS_CALL_RHO";
        public const string F_BS_CALL_PSI = "BS_CALL_PSI";
        public const string F_BS_CALL_VOL = "BS_CALL_VOL";

        public const string F_BS_PUT = "BS_PUT";
        public const string F_BS_PUT_DELTA = "BS_PUT_DELTA";
        public const string F_BS_PUT_GAMMA = "BS_PUT_GAMMA";
        public const string F_BS_PUT_THETA = "BS_PUT_THETA";
        public const string F_BS_PUT_VEGA = "BS_PUT_VEGA";
        public const string F_BS_PUT_RHO = "BS_PUT_RHO";
        public const string F_BS_PUT_PSI = "BS_PUT_PSI";
        public const string F_BS_PUT_VOL = "BS_PUT_VOL";

        public const string F_BS_PR_LT = "BS_PR_LT";
        public const string F_BS_PR_GT = "BS_PR_GT";
        public const string F_BS_AVG_LT = "BS_AVG_LT";
        public const string F_BS_AVG_GT = "BS_AVG_GT";
        public const string F_BS_RAND = "BS_RAND";

        public const string F_LN_AVG_LT = "LN_AVG_LT";
        public const string F_LN_AVG_GT = "LN_AVG_GT";
        public const string F_LN_RAND = "LN_RAND";

        private string[] _FunctionNames = new string[]
        {
            F_BS_CALL,
            F_BS_CALL_DELTA,
            F_BS_CALL_GAMMA,
            F_BS_CALL_THETA,
            F_BS_CALL_VEGA,
            F_BS_CALL_RHO,
            F_BS_CALL_PSI,
            F_BS_CALL_VOL,

            F_BS_PUT,
            F_BS_PUT_DELTA,
            F_BS_PUT_GAMMA,
            F_BS_PUT_THETA,
            F_BS_PUT_VEGA,
            F_BS_PUT_RHO,
            F_BS_PUT_PSI,
            F_BS_PUT_VOL,

            F_BS_PR_LT,
            F_BS_PR_GT,
            F_BS_AVG_LT,
            F_BS_AVG_GT,
            F_BS_RAND,

            F_LN_AVG_LT,
            F_LN_AVG_GT,
            F_LN_RAND

        };


        public FinanceLibrary(Session Session)
            : base(Session, "FIN")
        {
        }

        public override Method GetMethod(Method Parent, string Name, ParameterCollection Parameters)
        {
            throw new NotImplementedException();
        }

        public override ParameterCollectionSigniture GetMethodSigniture(string Name)
        {
            throw new NotImplementedException();
        }

        public override string[] MethodNames
        {
            get { throw new NotImplementedException(); }
        }

        public override CellFunction GetFunction(string Name)
        {

            switch (Name.ToUpper())
            {

                case F_BS_CALL:
                case F_BS_CALL_DELTA:
                case F_BS_CALL_GAMMA:
                case F_BS_CALL_PSI:
                case F_BS_CALL_RHO:
                case F_BS_CALL_THETA:
                case F_BS_CALL_VEGA:
                case F_BS_CALL_VOL:

                case F_BS_PUT:
                case F_BS_PUT_DELTA:
                case F_BS_PUT_GAMMA:
                case F_BS_PUT_PSI:
                case F_BS_PUT_RHO:
                case F_BS_PUT_THETA:
                case F_BS_PUT_VEGA:
                case F_BS_PUT_VOL:

                case F_BS_PR_GT:
                case F_BS_PR_LT:
                case F_BS_AVG_GT:
                case F_BS_AVG_LT:
                    return new Expressions.CellFunctionFixedShell(Name, 6, CellAffinity.DOUBLE, (x) => { return Wrapper(Name, x); });

                case F_BS_RAND:
                    return new Expressions.CellFunctionFixedShell(F_BS_RAND, 5, CellAffinity.DOUBLE, (x) => { return new Cell(BSRandomAsset(x[0].valueDOUBLE, x[1].valueDOUBLE, x[2].valueDOUBLE, x[3].valueDOUBLE, x[4].valueDOUBLE, this._Session.BaseGenerator.NextDoubleGauss().DOUBLE)); });

                case F_LN_AVG_LT:
                    return new Expressions.CellFunctionFixedShell(F_LN_AVG_LT, 3, CellAffinity.DOUBLE, (x) => { return new Cell(LogNormalExpectedLT(x[0].valueDOUBLE, x[1].valueDOUBLE, x[2].valueDOUBLE)); });
                case F_LN_AVG_GT:
                    return new Expressions.CellFunctionFixedShell(F_LN_AVG_GT, 3, CellAffinity.DOUBLE, (x) => { return new Cell(LogNormalExpectedGT(x[0].valueDOUBLE, x[1].valueDOUBLE, x[2].valueDOUBLE)); });
                case F_LN_RAND:
                    return new Expressions.CellFunctionFixedShell(F_LN_RAND, 2, CellAffinity.DOUBLE, (x) => { return new Cell(LogNormalRandom(x[0].valueDOUBLE, x[1].valueDOUBLE, this._Session.BaseGenerator.NextDoubleGauss().DOUBLE)); });

            }

            throw new ArgumentException(string.Format("Function '{0}' does not exist", Name));

        }

        public override string[] FunctionNames
        {
            get
            {
                return _FunctionNames;
            }
        }

        // Cell Wrapper //
        public static Cell Wrapper(string Name, Cell[] Values)
        {

            if (Values.Length != 6)
                throw new ArgumentException("Options functions require six arguments");

            string n = Name.ToUpper();

            switch (n)
            {

                case F_BS_CALL:
                    return new Cell(BSCallPrice(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_CALL_DELTA:
                    return new Cell(BSCallDelta(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_CALL_GAMMA:
                    return new Cell(BSCallGamma(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_CALL_THETA:
                    return new Cell(BSCallTheta(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_CALL_VEGA:
                    return new Cell(BSCallVega(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_CALL_RHO:
                    return new Cell(BSCallRho(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_CALL_PSI:
                    return new Cell(BSCallPsi(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_CALL_VOL:
                    return new Cell(BSCallImpliedVolatility(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));

                case F_BS_PUT:
                    return new Cell(BSPutPrice(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_PUT_DELTA:
                    return new Cell(BSPutDelta(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_PUT_GAMMA:
                    return new Cell(BSPutGamma(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_PUT_THETA:
                    return new Cell(BSPutTheta(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_PUT_VEGA:
                    return new Cell(BSPutVega(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_PUT_RHO:
                    return new Cell(BSPutRho(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_PUT_PSI:
                    return new Cell(BSPutPsi(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_PUT_VOL:
                    return new Cell(BSPutImpliedVolatility(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));

                case F_BS_AVG_GT:
                    return new Cell(BSExpectedGreaterThan(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_AVG_LT:
                    return new Cell(BSExpectedLessThan(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_PR_GT:
                    return new Cell(BSProbabilityGreaterThan(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case F_BS_PR_LT:
                    return new Cell(BSProbabilityLessThan(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));

            }

            throw new ArgumentException(string.Format("Function '{0}' does not exist", Name));

        }

        // Calls //
        /// <summary>
        /// Black-Scholes call price
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSCallPrice(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            double d2 = d1 - Sigma * Math.Sqrt(T);
            return S * Math.Exp(-D * T) * SpecialFunction.ProbabilityDistributions.NormalCDF(d1) - K * Math.Exp(-R * T) * SpecialFunction.ProbabilityDistributions.NormalCDF(d2);
        }

        /// <summary>
        /// Black-Scholes call price change per a $1 change in asset price
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSCallDelta(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            return Math.Exp(-D * T) * SpecialFunction.ProbabilityDistributions.NormalCDF(d1);
        }

        /// <summary>
        /// Black-Scholes call price derivative with respect to price
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSCallGamma(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            return Math.Exp(-D * T) * SpecialFunction.ProbabilityDistributions.NormalPDF(d1) / (S * Sigma * Math.Sqrt(T));
        }

        /// <summary>
        /// Black-Scholes call price derivative with respect to time
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSCallTheta(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            double d2 = d1 - Sigma * Math.Sqrt(T);
            return D * S * Math.Exp(-D * T) * SpecialFunction.ProbabilityDistributions.NormalCDF(d1) - R * K * Math.Exp(-R * T) * SpecialFunction.ProbabilityDistributions.NormalCDF(d2) - K * Math.Exp(-R * T) * SpecialFunction.ProbabilityDistributions.NormalPDF(d2) * Sigma / (2 * Math.Sqrt(T));
        }

        /// <summary>
        /// Black-Scholes call price derivative with respect to volatility
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSCallVega(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            return S * Math.Exp(-D * T) * SpecialFunction.ProbabilityDistributions.NormalPDF(d1) * Math.Sqrt(T);
        }

        /// <summary>
        /// Black-Scholes call price derivative with respect to the risk free rate
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSCallRho(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            double d2 = d1 - Sigma * Math.Sqrt(T);
            return K * Math.Exp(-R * T) * SpecialFunction.ProbabilityDistributions.NormalCDF(d2) * T;
        }

        /// <summary>
        /// Black-Scholes call price derivative with respect to the dividend yield
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSCallPsi(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            return -T * S * Math.Exp(-D * T) * SpecialFunction.ProbabilityDistributions.NormalCDF(d1);
        }

        /// <summary>
        /// Computes implied volatility given a call option
        /// </summary>
        /// <param name="Price">The option price</param>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSCallImpliedVolatility(double Price, double S, double K, double R, double D, double T)
        {

            double sigma = 0.25;

            for (int i = 0; i < 20; i++)
            {

                double xprice = BSCallPrice(S, K, R, D, sigma, T);
                double delta = Price - xprice;
                if (Math.Abs(delta) <= 0.00025)
                    break;

                double gradient = (BSCallPrice(S, K, R, D, sigma + 0.01, T) - xprice) / 0.01;

                sigma += delta / gradient;

            }

            return sigma;

        }

        // Puts //
        /// <summary>
        /// Black-Scholes put price
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSPutPrice(double S, double K, double R, double D, double Sigma, double T)
        {

            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            double d2 = d1 - Sigma * Math.Sqrt(T);
            return K * Math.Exp(-R * T) * (1 - SpecialFunction.ProbabilityDistributions.NormalCDF(d2)) - S * Math.Exp(-D * T) * (1 - SpecialFunction.ProbabilityDistributions.NormalCDF(d1));

        }

        /// <summary>
        /// Black-Scholes put price change per a $1 change in asset price
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSPutDelta(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            return -Math.Exp(-D * T) * SpecialFunction.ProbabilityDistributions.NormalCDF(-d1);
        }

        /// <summary>
        /// Black-Scholes put price derivative with respect to price
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSPutGamma(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            return Math.Exp(-D * T) * SpecialFunction.ProbabilityDistributions.NormalPDF(d1) / (S * Sigma * Math.Sqrt(T));
        }

        /// <summary>
        /// Black-Scholes put price derivative with respect to time
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSPutTheta(double S, double K, double R, double D, double Sigma, double T)
        {
            return BSCallTheta(S, K, R, D, Sigma, T) + R * K * Math.Exp(-R * T) - D * S * Math.Exp(-D * T);
        }

        /// <summary>
        /// Black-Scholes put price derivative with respect to volatility
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSPutVega(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            return S * Math.Exp(-D * T) * SpecialFunction.ProbabilityDistributions.NormalPDF(d1) * Math.Sqrt(T);
        }

        /// <summary>
        /// Black-Scholes put price derivative with respect to the risk free rate
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSPutRho(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            double d2 = d1 - Sigma * Math.Sqrt(T);
            return -K * Math.Exp(-R * T) * SpecialFunction.ProbabilityDistributions.NormalCDF(-d2) * T;
        }

        /// <summary>
        /// Black-Scholes put price derivative with respect to the dividend yield
        /// </summary>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="Sigma">Volaility (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSPutPsi(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            return -T * S * Math.Exp(-D * T) * SpecialFunction.ProbabilityDistributions.NormalCDF(-d1);
        }

        /// <summary>
        /// Computes implied volatility given a put option
        /// </summary>
        /// <param name="Price">The option price</param>
        /// <param name="S">Stock price</param>
        /// <param name="K">Strike price</param>
        /// <param name="R">Risk free rate (annual)</param>
        /// <param name="D">Dividend yield (annual)</param>
        /// <param name="T">Days to expiration (years)</param>
        /// <returns></returns>
        public static double BSPutImpliedVolatility(double Price, double S, double K, double R, double D, double T)
        {

            double sigma = 0.25;

            for (int i = 0; i < 20; i++)
            {

                Console.WriteLine(sigma);
                double xprice = BSPutPrice(S, K, R, D, sigma, T);
                double delta = Price - xprice;
                if (Math.Abs(delta) <= 0.00025)
                    break;

                double gradient = (BSPutPrice(S, K, R, D, sigma + 0.01, T) - xprice) / 0.01;

                sigma += delta / gradient;

            }

            return sigma;

        }

        // Supplemental //
        public static double BSExpectedGreaterThan(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            double d2 = d1 - Sigma * Math.Sqrt(T);
            return S * Math.Exp(-D * T) * SpecialFunction.ProbabilityDistributions.NormalCDF(d1) / SpecialFunction.ProbabilityDistributions.NormalCDF(d2);
        }

        public static double BSExpectedLessThan(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            double d2 = d1 - Sigma * Math.Sqrt(T);
            return S * Math.Exp(-D * T) * SpecialFunction.ProbabilityDistributions.NormalCDF(-d1) / SpecialFunction.ProbabilityDistributions.NormalCDF(-d2);
        }

        public static double BSProbabilityGreaterThan(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            double d2 = d1 - Sigma * Math.Sqrt(T);
            return SpecialFunction.ProbabilityDistributions.NormalCDF(d2);
        }

        public static double BSProbabilityLessThan(double S, double K, double R, double D, double Sigma, double T)
        {
            double d1 = (Math.Log(S / K) + (R - D + 0.5 * Sigma * Sigma) * T) / (Sigma * Math.Sqrt(T));
            double d2 = d1 - Sigma * Math.Sqrt(T);
            return SpecialFunction.ProbabilityDistributions.NormalCDF(-d2);
        }

        public static double BSRandomAsset(double S, double R, double D, double Sigma, double T, double Z)
        {
            double x = (R - D - 0.5 * Sigma * Sigma) * T + Sigma * Math.Sqrt(T) * Z;
            return S * Math.Exp(x);
        }

        // Log-Normal //
        public static double LogNormalExpectedLT(double x, double mu, double sigma)
        {
            double a1 = (x - mu - sigma * sigma) / sigma;
            double a2 = (x - mu) / sigma;
            double z1 = SpecialFunction.ProbabilityDistributions.NormalCDF(a1);
            double z2 = SpecialFunction.ProbabilityDistributions.NormalCDF(a2);
            return Math.Exp(mu + 0.5 * sigma * sigma) * z1 / z2;
        }

        public static double LogNormalExpectedGT(double x, double mu, double sigma)
        {
            double a1 = (x - mu - sigma * sigma) / sigma;
            double a2 = (x - mu) / sigma;
            double z1 = 1 - SpecialFunction.ProbabilityDistributions.NormalCDF(a1);
            double z2 = 1 - SpecialFunction.ProbabilityDistributions.NormalCDF(a2);
            return Math.Exp(mu + 0.5 * sigma * sigma) * z1 / z2;
        }

        public static double LogNormalRandom(double mu, double sigma, double Z)
        {
            double x = (mu - 0.5 * sigma * sigma) + sigma * Z;
            return Math.Exp(x);
        }


    }


}
