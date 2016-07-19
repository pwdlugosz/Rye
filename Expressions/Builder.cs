using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Expressions
{

    public static class Builder
    {

        // Value //
        public static Expression Value(bool Value)
        {
            return new ExpressionValue(null, new Cell(Value));
        }

        public static Expression Value(long Value)
        {
            return new ExpressionValue(null, new Cell(Value));
        }

        public static Expression Value(double Value)
        {
            return new ExpressionValue(null, new Cell(Value));
        }

        public static Expression Value(DateTime Value)
        {
            return new ExpressionValue(null, new Cell(Value));
        }

        public static Expression Value(string Value)
        {
            return new ExpressionValue(null, new Cell(Value));
        }

        public static Expression Value(byte[] Value)
        {
            return new ExpressionValue(null, new Cell(Value));
        }

        public static Expression Value(int Value)
        {
            return new ExpressionValue(null, new Cell(Value));
        }

        public static Expression Value(CellAffinity Value)
        {
            return new ExpressionValue(null, new Cell(Value));
        }

        public static Expression Value(Cell Value)
        {
            return new ExpressionValue(null, Value);
        }

        // Fields //
        public static Expression Field(int Index, CellAffinity Type, int Size, Register Memory)
        {
            return new ExpressionFieldRef(null, Index, Type, Size, Memory);
        }

        public static Expression Field(int Index, CellAffinity Type)
        {
            return Field(Index, Type, Schema.FixSize(Type, -1), null);
        }

        public static Expression Field(Schema Columns, string Name, Register Memory)
        {
            int idx = Columns.ColumnIndex(Name);
            return Field(idx, Columns.ColumnAffinity(idx), Columns.ColumnSize(idx), Memory);
        }

        public static Expression Field(Schema Columns, string Name)
        {
            return Field(Columns, Name, null);
        }

        // Functions //
        public static Expression Add(Expression Left, Expression Right)
        {
            Expression t = new ExpressionResult(Left.ParentNode, SystemFunctionLibrary.LookUp(SystemFunctionLibrary.OP_ADD));
            t.AddChildren(Left, Right);
            return t;
        }

        public static Expression Subtract(Expression Left, Expression Right)
        {
            Expression t = new ExpressionResult(Left.ParentNode, SystemFunctionLibrary.LookUp(SystemFunctionLibrary.OP_SUB));
            t.AddChildren(Left, Right);
            return t;
        }

        public static Expression Multiply(Expression Left, Expression Right)
        {
            Expression t = new ExpressionResult(Left.ParentNode, SystemFunctionLibrary.LookUp(SystemFunctionLibrary.OP_MUL));
            t.AddChildren(Left, Right);
            return t;
        }

        public static Expression Divide(Expression Left, Expression Right)
        {
            Expression t = new ExpressionResult(Left.ParentNode, SystemFunctionLibrary.LookUp(SystemFunctionLibrary.OP_DIV));
            t.AddChildren(Left, Right);
            return t;
        }

        public static Expression Modulo(Expression Left, Expression Right)
        {
            Expression t = new ExpressionResult(Left.ParentNode, SystemFunctionLibrary.LookUp(SystemFunctionLibrary.OP_MOD));
            t.AddChildren(Left, Right);
            return t;
        }

        public static Expression StichtAnd(IEnumerable<Expression> Nodes)
        {

            if (Nodes.Count() == 0) return null;

            if (Nodes.Count() == 1) return Nodes.First();

            Expression node = Nodes.First();

            for (int i = 1; i < Nodes.Count(); i++)
                node = LinkAnd(node, Nodes.ElementAt(i));
            
            return node;

        }

        public static Expression LinkAnd(Expression Left, Expression Right)
        {

            ExpressionResult node = new ExpressionResult(null, SystemFunctionLibrary.LookUp(SystemFunctionLibrary.FUNC_AND));
            node.AddChildNode(Left);
            node.AddChildNode(Right);
            return node;

        }

    }

}
