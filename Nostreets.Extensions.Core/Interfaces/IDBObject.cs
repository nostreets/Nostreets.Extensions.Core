using Nostreets.Extensions.DataControl.Classes;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nostreets.Extensions.Core.Interfaces
{
    public interface IDBObject<T>
    {
        public T Id { get; set; }
        public DateTime DateCreated { get; set; }
        public string? CreatedById { get; set; }
        public string? CreatedBy { get; set; }
        public DateTime DateModified { get; set; }
        public string? ModifiedBy { get; set; }
        public string? ModifiedById { get; set; }
        public bool IsArchived { get; set; }
    }
    
    public interface IDBObject : IDBObject<string> { }
}
