using Castle.Core.Internal;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nostreets.Extensions.DataControl.Enums
{
    public enum State
    {
        Success,
        Error,
        Warning,
        Info,
        Question
    }

    public enum FileFormat
    {
        Csv,
        Excel,
        Xml
    }
}
