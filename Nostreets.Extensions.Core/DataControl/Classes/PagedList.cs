using Nostreets.Extensions.Extend.Basic;

namespace Nostreets.Extensions.Core.DataControl.Classes
{
    /// <summary>
    /// One page of results plus the totals a pager needs.
    ///
    /// <para>
    /// 🔴 <b>This type must round-trip over the wire, and that is not automatic.</b> Its constructor
    /// parameter is named <c>items</c> ON PURPOSE: both Newtonsoft and System.Text.Json deserialize a
    /// type with no parameterless constructor by matching JSON property names to CONSTRUCTOR PARAMETER
    /// names. The parameter used to be called <c>data</c>, which matches no property — so the serializer
    /// silently passed <b>null</b> and every deserialized page came back with <c>Items == null</c>.
    /// </para>
    ///
    /// <para>
    /// ⚠️ That failure is silent and lands far from its cause. Nothing throws at deserialization; the
    /// page simply arrives empty-but-not-empty, and the crash surfaces later as
    /// <c>ArgumentNullException: Value cannot be null. (Parameter 'source')</c> from whatever LINQ or
    /// grid renders it. It broke the User Management page on dev on 2026-08-20 and read as a UI bug.
    /// Pinned by <c>PagedListWireContractTests</c>.
    /// </para>
    ///
    /// <para>
    /// 🔑 <b>Rename these parameters and you break the wire contract again</b>, with a clean build and
    /// green unit tests. Keep every constructor parameter name equal to the property it fills.
    /// </para>
    /// </summary>
    public class PagedList<T>
    {
        public PagedList(List<T> items, int pageIndex, int pageSize, int totalCount)
        {
            PageIndex = pageIndex;
            PageSize = pageSize;

            // Never null, even from a malformed payload: a null Items is what turns a bad response into
            // a render-time crash in the consumer rather than an empty page.
            Items = items ?? new List<T>();
            TotalCount = totalCount;
        }

        public int PageIndex { get; private set; }


        public int PageSize { get; private set; }


        public int TotalCount { get; private set; }


        public int TotalPages { get => PageSize <= 0 ? 0 : (int)Math.Ceiling(TotalCount / (double)PageSize); }


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
