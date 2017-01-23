using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rye.SpecialFunction
{

    public static class ProbabilityDistributions
    {

        // Normal //
        public static double NormalPDF(double Value)
        {
            return Math.Exp(-0.5 * Value * Value) / SpecialFunction.SQTPI;
        }

        public static double NormalCDF(double Value)
        {

            // Variables //
            bool Inv = (Value < 0);
            Value = Math.Abs(Value);
            double[] b = { 0.2316419, 0.319381530, -0.356563782, 1.781477937, -1.821255978, 1.330274429 };
            double t = 1 / (1 + b[0] * Value);

            // Set c //
            double z = 1 - NormalPDF(Value) * (b[1] * t + b[2] * t * t + b[3] * t * t * t + b[4] * t * t * t * t + b[5] * t * t * t * t * t);
            return (Inv ? 1 - z : z);

        }

        public static double NormalINV(double PValue)
        {

            if (PValue < 0 || PValue > 1)
                return double.NaN;
            if (PValue == 0)
                return double.NegativeInfinity;
            if (PValue == 1)
                return double.PositiveInfinity;

            //if (PValue < 0.5)
            //    PValue = PValue + 0.5;

            return SpecialFunction.ierf(2D * PValue - 1) * Math.Sqrt(2D);

        }

        // Log normal //
        public static double LogNormalPDF(double Value)
        {
            double x = Math.Log(Value);
            return Math.Exp(-0.5 * x * x) / (SpecialFunction.SQTPI * Value);
        }

        public static double LogNormalCDF(double Value)
        {

            Value = Math.Log(Value);

            // Variables //
            bool Inv = (Value < 0);
            Value = Math.Abs(Value);
            double[] b = { 0.2316419, 0.319381530, -0.356563782, 1.781477937, -1.821255978, 1.330274429 };
            double t = 1 / (1 + b[0] * Value);

            // Set c //
            double z = 1 - NormalPDF(Value) * (b[1] * t + b[2] * t * t + b[3] * t * t * t + b[4] * t * t * t * t + b[5] * t * t * t * t * t);
            return (Inv ? 1 - z : z);

        }

        public static double LogNormalINV(double PValue)
        {

            if (PValue < 0 || PValue > 1)
                return double.NaN;
            if (PValue == 0)
                return double.NegativeInfinity;
            if (PValue == 1)
                return double.PositiveInfinity;

            return Math.Exp(SpecialFunction.erfc(2D * PValue - 1D) * Math.Sqrt(2D));

        }

        // StudentsT //
        public static double StudentsTPDF(double Value, double DF)
        {

            double x, y, z;
            x = SpecialFunction.gamma(0.5 * (1 + DF));
            y = SpecialFunction.gamma(0.5 * DF) * Math.Sqrt(DF * Math.PI);
            z = Math.Pow(1D + Value * Value / DF, -(DF + 1) * 0.5);
            return z * x / y;

        }

        public static double StudentsTCDF(double Value, double DF)
        {
            double x = (DF) / (DF + Value * Value);
            return 1D - 0.5 * SpecialFunction.ibeta(0.5 * DF, 0.5, x);

        }

        public static double StudentsTINV(double PValue, double DF)
        {

            // Handle out of bounds //
            if (PValue >= 1) return double.PositiveInfinity;
            if (PValue <= 0) return double.NegativeInfinity;

            // Variables //
            double x = 0;
            double dx = 0;
            double ep = 0;
            double e = 0.0001;
            int maxitter = 10;

            for (int i = 0; i < maxitter; i++)
            {
                dx = StudentsTPDF(x, DF);
                ep = (PValue - StudentsTCDF(x, DF));
                if (Math.Abs(ep) <= e) break;
                x += (ep) / (dx);

            }

            return x;

        }

        // Gamma //
        public static double GammaPDF(double Value, double Alpha, double Beta)
        {
            return Math.Pow(Beta, Alpha) / SpecialFunction.gamma(Alpha) * Math.Pow(Value, Alpha - 1) * Math.Exp(-Beta * Value);
        }

        public static double GammaCDF(double Value, double Alpha, double Beta)
        {
            return SpecialFunction.igam(Alpha, Value * Beta);
        }

        public static double GammaINV(double PValue, double Alpha, double Beta)
        {

            // Handle out of bounds //
            if (PValue >= 1) return double.PositiveInfinity;
            if (PValue <= 0) return double.NegativeInfinity;

            // Variables //
            double x = 0.5;
            double dx = 0;
            double ep = 0;
            double e = 0.0001;
            int maxitter = 10;

            for (int i = 0; i < maxitter; i++)
            {
                dx = GammaPDF(x, Alpha, Beta);
                ep = (PValue - GammaCDF(x, Alpha, Beta));
                if (Math.Abs(ep) <= e) break;
                x += (ep) / (dx);
            }

            return x;

        }

        // Exponential //
        public static double ExponetialPDF(double Value, double Lambda)
        {
            return Lambda * Math.Exp(-Value * Lambda);
        }

        public static double ExponetialCDF(double Value, double Lambda)
        {
            return 1 - Math.Exp(-Value * Lambda);
        }

        public static double ExponetialINV(double PValue, double Lambda)
        {
            return -Math.Log(1 - PValue) / Lambda;
        }

        // Chi-Square //
        public static double ChiSquarePDF(double Value, double DF)
        {
            return Math.Pow(2, DF / 2) / SpecialFunction.gamma(DF / 2) * Math.Pow(Value, DF / 2 - 1) * Math.Exp(-2 * Value);
        }

        public static double ChiSquareCDF(double Value, double DF)
        {
            return SpecialFunction.igam(DF / 2, 2 * Value);
        }

        public static double ChiSquareINV(double PValue, double DF)
        {

            // Handle out of bounds //
            if (PValue >= 1) return double.PositiveInfinity;
            if (PValue <= 0) return double.NegativeInfinity;

            // Variables //
            double x = 0.5;
            double dx = 0;
            double ep = 0;
            double e = 0.0001;
            int maxitter = 10;

            for (int i = 0; i < maxitter; i++)
            {

                dx = ChiSquarePDF(x, DF);
                ep = (PValue - ChiSquareCDF(x, DF));
                if (Math.Abs(ep) <= e) break;
                x += (ep) / (dx);

            }
            return x;

        }

        // Poisson //
        public static double PoissonPMF(double Value, double Lambda)
        {
            return Math.Pow(Lambda, Value) * Math.Exp(-Lambda) / SpecialFunction.fac(Value);
        }

        public static double PoissonCDF(double Value, double Lambda)
        {

            double p = 0;
            for (int i = 0; i <= (int)Value; i++)
            {
                p += PoissonPMF(i, Lambda);
            }
            return p;

        }

        public static double PoissonINV(double PValue, double Lambda)
        {

            double x = -1, p = 0;
            while (p < PValue)
            {

                x += 1;
                p += PoissonPMF(x, Lambda);

            }

            return x;

        }

        // Binomial //
        public static double BinomialPMF(double Value, double P, double N)
        {
            double x = SpecialFunction.bincoeff((long)Value, (long)N);
            return x * Math.Pow(P, Value) * Math.Pow(P, N - Value);
        }

        public static double BinomialCDF(double Value, double P, double N)
        {

            double p = 0;
            for (int i = 0; i <= (int)Value; i++)
            {
                p += BinomialPMF(i, P, N);
            }
            return p;

        }

        public static double BinomialINV(double PValue, double P, double N)
        {

            double x = -1, p = 0;
            while (p < PValue)
            {

                x += 1;
                p += BinomialPMF(x, P, N);

            }

            return x;

        }

        // Negative Binomial //
        public static double NegativeBinomialPMF(double Value, double P, double R)
        {
            double x = SpecialFunction.bincoeff((long)Value, (long)(Value + R - 1));
            return x * Math.Pow(P, Value) * Math.Pow(1 - P, R);
        }

        public static double NegativeBinomialCDF(double Value, double P, double R)
        {

            double p = 0;
            for (int i = 0; i <= (int)Value; i++)
            {
                p += NegativeBinomialPMF(i, P, R);
            }
            return p;

        }

        public static double NegativeBinomialINV(double PValue, double P, double R)
        {

            double x = -1, p = 0;
            while (p < PValue)
            {

                x += 1;
                p += NegativeBinomialPMF(x, P, R);

            }

            return x;

        }

    }

}
