using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;
using Rye.Expressions;
using Rye.Structures;

namespace Rye.Methods
{

    /// <summary>
    /// Assign ID: 0 == assign, 1 == increment, 2 == decrement, 3 == auto increment, 4 == auto decrement
    /// </summary>
    public sealed class MethodAssignScalar : Method
    {

        private Expression _Mapping;
        private int _Index;
        private int _AssignID; // 0 == assign, 1 == increment, 2 == decrement, 3 == auto increment, 4 == auto decrement

        /// <summary>
        /// Create an assignment action
        /// </summary>
        /// <param name="Parent">Parent node</param>
        /// <param name="Heap">The memory heap to work off of</param>
        /// <param name="Index">The direct location of the variable in the heap</param>
        /// <param name="Map">The expression to assign to</param>
        /// <param name="AssignID">0 == assign, 1 == increment, 2 == decrement, 3 == auto increment, 4 == auto decrement</param>
        public MethodAssignScalar(Method Parent, MemoryStructure Heap, int Index, Expression Map, int AssignID)
            : base(Parent)
        {
            this._Heap = Heap;
            this._Mapping = Map;
            this._Index = Index;
            this._AssignID = AssignID;
        }

        public override void Invoke()
        {
            switch (this._AssignID)
            {

                case 0:
                    this._Heap.Scalars[this._Index] = this._Mapping.Evaluate();
                    return;

                case 1:
                    this._Heap.Scalars[this._Index] += this._Mapping.Evaluate();
                    return;

                case 2:
                    this._Heap.Scalars[this._Index] -= this._Mapping.Evaluate();
                    return;

                case 3:
                    this._Heap.Scalars[this._Index]++;
                    return;

                case 4:
                    this._Heap.Scalars[this._Index]--;
                    return;

            }

        }

        public override string Message()
        {
            
            switch (this._AssignID)
            {
                //case 0:
                //    return string.Format("Assign '{0}' to '{1}'", this._Heap.Scalars.Name(this._Index), this._Mapping.Evaluate().valueSTRING);
                //case 1:
                //    return string.Format("Increment '{0}' by '{1}'", this._Heap.Scalars.Name(this._Index), this._Mapping.Evaluate().valueSTRING);
                //case 2:
                //    return string.Format("Decrement '{0}' by '{1}'", this._Heap.Scalars.Name(this._Index), this._Mapping.Evaluate().valueSTRING);
                //case 3:
                //    return string.Format("Auto-Increment '{0}'", this._Heap.Scalars.Name(this._Index));
                //case 4:
                //    return string.Format("Auto-Decrement '{0}'", this._Heap.Scalars.Name(this._Index));

                case 0:
                    return string.Format("Assign '{0}'", this._Heap.Scalars.Name(this._Index));
                case 1:
                    return string.Format("Increment '{0}'", this._Heap.Scalars.Name(this._Index));
                case 2:
                    return string.Format("Decrement '{0}'", this._Heap.Scalars.Name(this._Index));
                case 3:
                    return string.Format("Auto-Increment '{0}'", this._Heap.Scalars.Name(this._Index));
                case 4:
                    return string.Format("Auto-Decrement '{0}'", this._Heap.Scalars.Name(this._Index));

            }

            return "TNode.Assign";

        }

        public override Method CloneOfMe()
        {
            return new MethodAssignScalar(this.Parent, this._Heap, this._Index, this._Mapping.CloneOfMe(), this._AssignID);
        }

    }

}
