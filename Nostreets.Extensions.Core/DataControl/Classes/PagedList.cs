using Nostreets.Extensions.Extend.Basic;

namespace Nostreets.Extensions.Core.DataControl.Classes
{
    public class PagedList<T>
    {
        public PagedList(List<T> data, int pageIndex, int pageSize, int totalCount)
        {
            PageIndex = pageIndex;
            PageSize = pageSize;
            
            Items = data;
            TotalCount = totalCount;
        }

        public int PageIndex { get; private set; }


        public int PageSize { get; private set; }


        public int TotalCount { get; private set; }


        public int TotalPages { get => (int)Math.Ceiling(TotalCount / (double)PageSize); }


        public List<T> Items { get; private set; }

        public bool HasPreviousPage
        {
            get { return PageIndex != 0; }
        }


        public bool HasNextPage
        {
            get { return PageIndex + 1 < TotalPages; }
        }
    }
}
