using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Libraries
{
    
    public sealed class FinanceFunctionLibrary : FunctionLibrary
    {

        public const string BS_CALL = "BS_CALL";
        public const string BS_CALL_DELTA = "BS_CALL_DELTA";
        public const string BS_CALL_GAMMA = "BS_CALL_GAMMA";
        public const string BS_CALL_THETA = "BS_CALL_THETA";
        public const string BS_CALL_VEGA = "BS_CALL_VEGA";
        public const string BS_CALL_RHO = "BS_CALL_RHO";
        public const string BS_CALL_PSI = "BS_CALL_PSI";
        public const string BS_CALL_VOL = "BS_CALL_VOL";

        public const string BS_PUT = "BS_PUT";
        public const string BS_PUT_DELTA = "BS_PUT_DELTA";
        public const string BS_PUT_GAMMA = "BS_PUT_GAMMA";
        public const string BS_PUT_THETA = "BS_PUT_THETA";
        public const string BS_PUT_VEGA = "BS_PUT_VEGA";
        public const string BS_PUT_RHO = "BS_PUT_RHO";
        public const string BS_PUT_PSI = "BS_PUT_PSI";
        public const string BS_PUT_VOL = "BS_PUT_VOL";

        private string[] _Names = new string[]
        {
            BS_CALL,
            BS_CALL_DELTA,
            BS_CALL_GAMMA,
            BS_CALL_THETA,
            BS_CALL_VEGA,
            BS_CALL_RHO,
            BS_CALL_PSI,
            BS_CALL_VOL,

            BS_PUT,
            BS_PUT_DELTA,
            BS_PUT_GAMMA,
            BS_PUT_THETA,
            BS_PUT_VEGA,
            BS_PUT_RHO,
            BS_PUT_PSI,
            BS_PUT_VOL

        };

        public FinanceFunctionLibrary(Session Session)
            : base(Session)
        {
            this.LibName = "FIN";
        }

        public override Expressions.CellFunction RenderFunction(string Name)
        {

            switch (Name.ToUpper())
            {

                case BS_CALL:
                case BS_CALL_DELTA:
                case BS_CALL_GAMMA:
                case BS_CALL_PSI:
                case BS_CALL_RHO:
                case BS_CALL_THETA:
                case BS_CALL_VEGA:
                case BS_CALL_VOL:
                case BS_PUT:
                case BS_PUT_DELTA:
                case BS_PUT_GAMMA:
                case BS_PUT_PSI:
                case BS_PUT_RHO:
                case BS_PUT_THETA:
                case BS_PUT_VEGA:
                case BS_PUT_VOL:
                    return new Expressions.CellFunctionFixedShell(Name, 6, CellAffinity.DOUBLE, (x) => { return Wrapper(Name, x); });
            }

            throw new ArgumentException(string.Format("Function '{0}' does not exist", Name));

        }

        public override string[] Names
        {
            get { return this._Names; }
        }

        // Cell Wrapper //
        public static Cell Wrapper(string Name, Cell[] Values)
        {

            if (Values.Length != 6)
                throw new ArgumentException("Options functions require six arguments");

            string n = Name.ToUpper();

            switch (n)
            {

                case BS_CALL:
                    return new Cell(BSCallPrice(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_CALL_DELTA:
                    return new Cell(BSCallDelta(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_CALL_GAMMA:
                    return new Cell(BSCallGamma(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_CALL_THETA:
                    return new Cell(BSCallTheta(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_CALL_VEGA:
                    return new Cell(BSCallVega(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_CALL_RHO:
                    return new Cell(BSCallRho(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_CALL_PSI:
                    return new Cell(BSCallPsi(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_CALL_VOL:
                    return new Cell(BSCallImpliedVolatility(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));

                case BS_PUT:
                    return new Cell(BSPutPrice(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_PUT_DELTA:
                    return new Cell(BSPutDelta(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_PUT_GAMMA:
                    return new Cell(BSPutGamma(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_PUT_THETA:
                    return new Cell(BSPutTheta(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_PUT_VEGA:
                    return new Cell(BSPutVega(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_PUT_RHO:
                    return new Cell(BSPutRho(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_PUT_PSI:
                    return new Cell(BSPutPsi(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
                case BS_PUT_VOL:
                    return new Cell(BSPutImpliedVolatility(Values[0].valueDOUBLE, Values[1].valueDOUBLE, Values[2].valueDOUBLE, Values[3].valueDOUBLE, Values[4].valueDOUBLE, Values[5].valueDOUBLE));
            
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
            return S * Math.Exp(-D * T) * NormalCDF(d1) - K * Math.Exp(-R * T) * NormalCDF(d2);
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
            return Math.Exp(-D * T) * NormalCDF(d1);
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
            return Math.Exp(-D * T) * NormalPDF(d1) / (S * Sigma * Math.Sqrt(T));
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
            return D * S * Math.Exp(-D * T) * NormalCDF(d1) - R * K * Math.Exp(-R * T) * NormalCDF(d2) - K * Math.Exp(-R * T) * NormalPDF(d2) * Sigma / (2 * Math.Sqrt(T));
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
            return S * Math.Exp(-D * T) * NormalPDF(d1) * Math.Sqrt(T);
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
            return K * Math.Exp(-R * T) * NormalCDF(d2) * T;
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
            return -T * S * Math.Exp(-D * T) * NormalCDF(d1);
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
                double delta = Price - xprice ;
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
            return K * Math.Exp(-R * T) * NormalCDF(-d2) - S * Math.Exp(-D * T) * NormalCDF(-d1);

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
            return -Math.Exp(-D * T) * NormalCDF(-d1);
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
            return Math.Exp(-D * T) * NormalPDF(d1) / (S * Sigma * Math.Sqrt(T));
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
            return S * Math.Exp(-D * T) * NormalPDF(d1) * Math.Sqrt(T);
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
            return -K * Math.Exp(-R * T) * NormalCDF(-d2) * T;
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
            return -T * S * Math.Exp(-D * T) * NormalCDF(-d1);
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

        // Normal //
        /// <summary>
        /// Computes the normal distribution probability density function
        /// </summary>
        /// <param name="x">The value of the normal distribution</param>
        /// <returns></returns>
        public static double NormalPDF(double x)
        {

            // Variables //
            double t = Math.Exp(-x * x * 0.50);
            t = t / Math.Sqrt(2 * Math.PI);

            // Return //
            return t;

        }

        /// <summary>
        /// Computes the normal distribution cummulative distribution function
        /// </summary>
        /// <param name="x">The value of the normal distribution</param>
        /// <returns></returns>
        public static double NormalCDF(double x)
        {

            // Variables //
            double[] b = { 0.2316419, 0.319381530, -0.356563782, 1.781477937, -1.821255978, 1.330274429 };
            double t = 1 / (1 + b[0] * x);

            // Set c //
            return 1 - NormalPDF(x) * (b[1] * t + b[2] * t * t + b[3] * t * t * t + b[4] * t * t * t * t + b[5] * t * t * t * t * t);

        }

        /// <summary>
        /// Computes the inverse of the normal cummulative distribution function
        /// </summary>
        /// <param name="p">The probability value</param>
        /// <returns></returns>
        public static double NormalINV(double p)
        {

            // Handle out of bounds //
            if (p >= 1) return double.PositiveInfinity;
            if (p <= 0) return double.NegativeInfinity;

            // Variables //
            double x = 0;
            double dx = 0;
            double ep = 0;
            double e = 0.0001;
            int maxitter = 10;

            for (int i = 0; i < maxitter; i++)
            {
                dx = NormalPDF(x);
                ep = (p - NormalCDF(x));
                if (Math.Abs(ep) <= e) break;
                x += (ep) / (dx);

            }

            return x;

        }


    }



}
