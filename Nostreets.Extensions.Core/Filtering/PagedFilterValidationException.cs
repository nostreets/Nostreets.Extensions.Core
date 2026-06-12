using System;

namespace Nostreets.Extensions.Core.Filtering
{
    /// <summary>
    /// Thrown when an inbound serialized filter expression is malformed or contains a node the strict
    /// <see cref="PagedFilterValidator"/> allow-list rejects. Callers (service controllers) map it to a
    /// <c>ServiceResponse.Error</c> — the expression is never compiled or invoked when this is thrown.
    /// </summary>
    public sealed class PagedFilterValidationException : Exception
    {
        public PagedFilterValidationException(string message) : base(message) { }

        public PagedFilterValidationException(string message, Exception innerException)
            : base(message, innerException) { }
    }
}
