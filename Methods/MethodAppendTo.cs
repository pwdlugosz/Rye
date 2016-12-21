using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;

namespace Rye.Methods
{

    /// <summary>
    /// Append to class
    /// </summary>
    public sealed class MethodAppendTo : Method
    {

        private RecordWriter _writer;
        private ExpressionCollection _Fields;
        private long _writes = 0;

        public MethodAppendTo(Method Parent, RecordWriter Writer, ExpressionCollection Output)
            : base(Parent)
        {

            // Check that the column count is the same; we dont care about the schema //
            if (Writer.Columns.Count != Output.Count)
                throw new Exception("Attempting to write a different number of recors to a stream");

            this._writer = Writer;
            this._Fields = Output;

        }

        public override void BeginInvoke()
        {
            this._writes = 0;
        }

        public override void Invoke()
        {
            Record r = this._Fields.Evaluate();
            this._writer.Insert(r);
            this._writes++;
        }

        public override void EndInvoke()
        {
            this._writer.Close();
            base.EndInvoke();
        }

        public override string Message()
        {
            return string.Format("Append: {0}", this._writes);
        }

        public override Method CloneOfMe()
        {
            return new MethodAppendTo(this.Parent, this._writer, this._Fields.CloneOfMe());
        }

        public override List<Expression> InnerExpressions()
        {
            return this._Fields.Nodes.ToList();
        }

    }

}
