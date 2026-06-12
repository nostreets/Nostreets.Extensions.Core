using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using Nostreets.Extensions.Core.Filtering;

namespace Nostreets.Extensions.Core.Models.Requests
{
    public class PagedListRequest<T>
    {
        public int PageIndex { get; set; }
        public int PageSize { get; set; }

        /// <summary>
        /// In-process predicate filters (delegates). NOT serialized over the wire — for HTTP use
        /// <see cref="SerializedFilters"/> (populate it via <see cref="AddFilter"/>); the receiving service
        /// deserializes + validates + compiles those into this list at the boundary.
        /// </summary>
        [System.Text.Json.Serialization.JsonIgnore]
        [Newtonsoft.Json.JsonIgnore]
        public IEnumerable<Func<T, bool>> Filters { get; set; }

        /// <summary>
        /// Serialize.Linq-encoded predicate expressions — the wire-safe form of <see cref="Filters"/>. Built
        /// via <see cref="AddFilter"/>. A service receiving this request compiles them into <see cref="Filters"/>
        /// via <c>PagedFilterCodec.CompileValidated</c> (strict allow-list) before use.
        /// </summary>
        public List<string> SerializedFilters { get; set; }

        public string OrderByKey { get; set; }
        public bool OrderByDesc { get; set; } = true;

        /// <summary>
        /// Custom in-process sort comparer. NOT serialized over the wire — HTTP callers sort via
        /// <see cref="OrderByKey"/> / <see cref="OrderByDesc"/> only.
        /// </summary>
        [System.Text.Json.Serialization.JsonIgnore]
        [Newtonsoft.Json.JsonIgnore]
        public IComparer<object> OrderByComparer { get; set; }

        /// <summary>
        /// Adds a predicate filter that works BOTH in-process and over HTTP: it serializes the expression into
        /// <see cref="SerializedFilters"/> (the wire form) AND compiles it into <see cref="Filters"/> (the
        /// in-process form). Compiling client-side is safe — the caller owns the expression; the strict
        /// allow-list only gates INBOUND untrusted expressions on the server.
        /// </summary>
        public PagedListRequest<T> AddFilter(Expression<Func<T, bool>> filter)
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));

            (SerializedFilters ??= new List<string>()).Add(PagedFilterCodec.Serialize(filter));

            var compiled = Filters as List<Func<T, bool>> ?? Filters?.ToList() ?? new List<Func<T, bool>>();
            compiled.Add(filter.Compile());
            Filters = compiled;

            return this;
        }
    }
}
