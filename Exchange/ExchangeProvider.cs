using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rye.Data;

namespace Rye.Exchange
{

    public abstract class ExchangeProvider
    {

        public abstract void Render(string Path, TabularData Data);

        public abstract void Raze(string Path, RecordWriter Output);

    }
    
}
