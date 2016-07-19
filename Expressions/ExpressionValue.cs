using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Expressions
{

    public class ExpressionValue : Expression
    {

        private Cell _value;
        
        public ExpressionValue(Expression Parent, Cell Value)
            : base(Parent, ExpressionAffinity.Value)
        {
            this._value = Value;
        }

        public override Cell Evaluate()
        {
            return _value;
        }

        public override CellAffinity ReturnAffinity()
        {
            return _value.Affinity;
        }

        public override string ToString()
        {
            return this.Affinity.ToString() + " : " + _value.ToString();
        }

        public override int GetHashCode()
        {
            return this._value.GetHashCode() ^ Expression.HashCode(this._Cache);
        }

        public override string Unparse(Schema S)
        {
            return this._value.ToString();
        }

        public override Expression CloneOfMe()
        {
            return new ExpressionValue(this.ParentNode, this._value);
        }

        public override int DataSize()
        {
            return this._value.DataCost;
        }

        public Cell InnerValue
        {
            get { return this._value; }
        }

    }

    public class ExpressionNull : ExpressionValue
    {

        public ExpressionNull(Expression Parent)
            : base(Parent, Cell.NULL_INT)
        {
        }

        public static ExpressionNull NullValue
        {
            get { return new ExpressionNull(null); }
        }

    }

}
