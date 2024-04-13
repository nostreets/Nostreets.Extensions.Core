using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nostreets.Extensions.Core.Models.Requests
{
    public class PagedListRequest<T>
    {
        public int PageIndex { get; set; }
        public int PageSize { get; set; }
        public IEnumerable<Func<T, bool>> Filters { get; set; }

        public string OrderByKey { get; set; }
        public bool OrderByDesc { get; set; }
        public IComparer<object> OrderByComparer { get; set; }
    }
}
