using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Methods;
using Rye.SpecialFunction;

namespace Rye.Libraries
{

    public sealed class StatLibrary : Library
    {

        public const string NORMAL_PDF = "NORMAL_PDF";
        public const string NORMAL_CDF = "NORMAL_CDF";
        public const string NORMAL_INV = "NORMAL_INV";
        
        public const string T_PDF = "T_PDF";
        public const string T_CDF = "T_CDF";
        public const string T_INV = "T_INV";

        public const string GAMMA_PDF = "GAMMA_PDF";
        public const string GAMMA_CDF = "GAMMA_CDF";
        public const string GAMMA_INV = "GAMMA_INV";

        public const string EXP_PDF = "EXP_PDF";
        public const string EXP_CDF = "EXP_CDF";
        public const string EXP_INV = "EXP_INV";

        public const string CHISQ_PDF = "CHISQ_PDF";
        public const string CHISQ_CDF = "CHISQ_CDF";
        public const string CHISQ_INV = "CHISQ_INV";


        public StatLibrary(Session Session)
            : base(Session, "STAT")
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

                case NORMAL_PDF: 
                    return new CellFunctionFixedShell(NORMAL_PDF, 1, CellAffinity.DOUBLE, (x) => { return new Cell(ProbabilityDistributions.NormalPDF(x[0].valueDOUBLE)); });
                case NORMAL_CDF:
                    return new CellFunctionFixedShell(NORMAL_CDF, 1, CellAffinity.DOUBLE, (x) => { return new Cell(ProbabilityDistributions.NormalCDF(x[0].valueDOUBLE)); });
                case NORMAL_INV:
                    return new CellFunctionFixedShell(NORMAL_INV, 1, CellAffinity.DOUBLE, (x) => { return new Cell(ProbabilityDistributions.NormalINV(x[0].valueDOUBLE)); });

                case T_PDF:
                    return new CellFunctionFixedShell(NORMAL_PDF, 2, CellAffinity.DOUBLE, (x) => { return new Cell(ProbabilityDistributions.StudentsTPDF(x[0].valueDOUBLE, x[1].valueDOUBLE)); });
                case T_CDF:
                    return new CellFunctionFixedShell(NORMAL_CDF, 2, CellAffinity.DOUBLE, (x) => { return new Cell(ProbabilityDistributions.StudentsTCDF(x[0].valueDOUBLE, x[1].valueDOUBLE)); });
                case T_INV:
                    return new CellFunctionFixedShell(NORMAL_INV, 2, CellAffinity.DOUBLE, (x) => { return new Cell(ProbabilityDistributions.StudentsTINV(x[0].valueDOUBLE, x[1].valueDOUBLE)); });


            }

            throw new MissingMemberException(string.Format("Function '{0}' does not exist in the STAT library", Name.ToUpper()));

        }

        public override string[] FunctionNames
        {
            get 
            {

                return new string[]
                {
                    NORMAL_PDF, NORMAL_CDF, NORMAL_INV,
                    T_PDF, T_CDF, T_INV,
                    GAMMA_PDF, GAMMA_CDF, GAMMA_INV,
                    CHISQ_PDF, CHISQ_CDF, CHISQ_INV,
                    EXP_PDF, EXP_CDF, EXP_INV,

                };

            }
        }
    
    
    
    
    }

}
