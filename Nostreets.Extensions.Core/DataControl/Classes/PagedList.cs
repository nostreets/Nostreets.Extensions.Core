using Nostreets.Extensions.Extend.Basic;

namespace Nostreets.Extensions.Core.DataControl.Classes
{
    public class PagedList<T>
    {
        public PagedList(List<T> data, int pageIndex, int pageSize)
        {
            PageIndex = pageIndex;
            PageSize = pageSize;
            
            TotalItems = data;
            TotalCount = data.Count;
        }

        public int PageIndex { get; private set; }


        public int PageSize { get; private set; }


        public int TotalCount { get; private set; }


        public int TotalPages { get => (int)Math.Ceiling(TotalCount / (double)PageSize); }


        public List<T> TotalItems { get; private set; }


        public bool HasPreviousPage
        {
            get { return PageIndex > 0; }
        }


        public bool HasNextPage
        {
            get { return PageIndex + 1 < TotalPages; }
        }

        public void ReorderItems(string key, bool desc = false, IComparer<object> comparer = null) 
        {
            if (!string.IsNullOrEmpty(key) && typeof(T).HasProperty(key))
            {
                List<T> orderedList = new List<T>();
                if (comparer != null)
                {
                    if (desc)
                        orderedList = TotalItems.OrderByDescending(a => a.GetPropertyValue(key), comparer).ToList();
                    else
                        orderedList = TotalItems.OrderBy(a => a.GetPropertyValue(key), comparer).ToList();
                }
                else
                {
                    if (desc)
                        orderedList = TotalItems.OrderByDescending(a => a.GetPropertyValue(key)).ToList();
                    else
                        orderedList = TotalItems.OrderBy(a => a.GetPropertyValue(key)).ToList();
                }

                TotalItems = orderedList;
            }
        }

        public List<T> GetPagedItems(int? pageIndex = null, int? pageSize = null)
        {
            var result = new List<T>();

            if (pageIndex != null)
                PageIndex = pageIndex.Value;

            if (pageSize != null)
                PageSize = pageSize.Value;

            int startIndex = PageIndex * PageSize;
            int endIndex = startIndex + PageSize;
            endIndex = Math.Min(endIndex, TotalItems.Count);

            if (startIndex < endIndex)
                result = TotalItems.GetRange(startIndex, endIndex - startIndex);

            return result;
        }
    }
}
