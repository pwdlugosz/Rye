using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Expressions
{

    public class ExpressionComparer : IEqualityComparer<Expression>
    {

        bool IEqualityComparer<Expression>.Equals(Expression T1, Expression T2)
        {

            if (T1.Affinity != T2.Affinity)
                return false;

            if (T1.Affinity == ExpressionAffinity.Field)
                return (T1 as ExpressionFieldRef).Index == (T2 as ExpressionFieldRef).Index;

            if (T1.Affinity == ExpressionAffinity.Pointer)
                return (T1 as ExpressionPointer).PointerName == (T2 as ExpressionPointer).PointerName;

            //if (T1.Affinity == ExpressionAffinity.HeapReExpression)
            //    return (T1 as ExpressionHeapRef).HeapRef == (T2 as ExpressionHeapRef).HeapRef && (T1 as ExpressionHeapRef).Pointer == (T2 as ExpressionHeapRef).Pointer;

            if (T1.Affinity == ExpressionAffinity.Value)
                return (T1 as ExpressionValue).InnerValue == (T2 as ExpressionValue).InnerValue;

            return T1.NodeID == T2.NodeID;

        }

        int IEqualityComparer<Expression>.GetHashCode(Expression T)
        {

            if (T.Affinity == ExpressionAffinity.Field)
                return (T as ExpressionFieldRef).Index;

            if (T.Affinity == ExpressionAffinity.Pointer)
                return (T as ExpressionPointer).PointerName.GetHashCode();

            //if (T.Affinity == ExpressionAffinity.HeapReExpression)
            //    return (T as ExpressionHeapRef).HeapRef.Columns.GetHashCode() ^ (T as ExpressionHeapRef).Pointer;

            if (T.Affinity == ExpressionAffinity.Value)
                return (T as ExpressionValue).InnerValue.GetHashCode();

            return T.NodeID.GetHashCode();

        }

    }

}
