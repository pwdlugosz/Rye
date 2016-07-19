using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Expressions
{

    public class ExpressionPointer : Expression
    {

        private string _NameID;
        private CellAffinity _Type;
        private int _Size;
    
        public ExpressionPointer(Expression Parent, string RefName, CellAffinity Type, int Size)
            : base(Parent, ExpressionAffinity.Pointer)
        {
            this._Type = Type;
            this._NameID = RefName;
            this._name = RefName;
            this._Size = Schema.FixSize(Type, Size);
        }

        public override Cell Evaluate()
        {
            throw new Exception(string.Format("Cannot evaluate pointer nodes; Name '{0}'", _NameID));
        }

        public override CellAffinity ReturnAffinity()
        {
            return this._Type;
        }

        public override string ToString()
        {
            return this.Affinity.ToString() + " : " + _NameID;
        }

        public override int GetHashCode()
        {
            return this._NameID.GetHashCode() ^ Expression.HashCode(this._Cache);
        }

        public override string Unparse(Schema S)
        {
            return this._Type.ToString().ToUpper() + "." + this._NameID.ToString();
        }

        public override Expression CloneOfMe()
        {
            return new ExpressionPointer(this.ParentNode, this._NameID, this._Type, this._Size);
        }

        public override int DataSize()
        {
            return this._Size;
        }

        public string PointerName
        {
            get { return _NameID; }
        }

        // Statics //
        private static string _DefName = "@@@@NULL@@@@";

        public static Expression DefExpression
        {
            get { return new ExpressionPointer(null, _DefName, CellAffinity.BOOL, 8); }
        }

        public static bool IsDefExpression(Expression Node)
        {
            if (Node.Affinity != ExpressionAffinity.Pointer)
                return false;

            ExpressionPointer t = Node as ExpressionPointer;
            if (t.ReturnAffinity() == CellAffinity.BOOL && t.PointerName == _DefName)
                return true;
            return false;

        }

    }


}
