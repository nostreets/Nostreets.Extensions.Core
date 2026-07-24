using System;
using System.Collections.Generic;

namespace Nostreets.Extensions.Core.Models.Responses
{
    /// <summary>
    /// Safe-consume helpers for <see cref="ServiceResponse"/> / <see cref="ServiceResponse{T}"/> — the single
    /// vocabulary for "did this fail?" and for pulling <c>Data</c> out without a NullReferenceException.
    /// Collapses the idioms scattered across consumers (<c>IsSuccessful ? Data ?? new() : new()</c>,
    /// <c>!IsSuccessful || Data == null</c>, <c>IsSuccessful &amp;&amp; Data != null</c>, <c>IsSuccessful ? Data : null</c>)
    /// into <see cref="HasError(ServiceResponse)"/> / <see cref="IsUsable(ServiceResponse)"/> /
    /// <see cref="GetDataOr{T}"/> / <see cref="TryGetData{T}"/>.
    ///
    /// Lives in <c>ServiceResponse</c>'s own namespace so a file that already imports the response type gets
    /// these for free. ⚠ That is NOT the same as "no <c>using</c> needed": C# resolves extension methods by
    /// IMPORTED NAMESPACE, not by the receiver's type. A consumer that only ever holds a response in a
    /// <c>var</c> returned from another namespace's method never has to name <see cref="ServiceResponse{T}"/>,
    /// so it often has NO <c>using Nostreets.Extensions.Core.Models.Responses;</c> — and then these methods
    /// simply do not resolve (CS1061 "does not contain a definition for 'IsUsable'"). If that happens, add the
    /// using; do not hand-roll <c>!IsSuccessful || Data == null</c> in its place.
    /// (Observed 2026-07-23 in <c>OS.Blazor.Base.Applications\Domian\AppAuthorizeRouteView.cs</c>, which
    /// consumed responses via <c>var</c> and had no such import.)
    ///
    /// The <c>&lt;T&gt;</c> overloads are more specific than the base ones, so a
    /// <see cref="ServiceResponse{T}"/> automatically resolves to the <c>Data</c>-aware version — meaning
    /// <see cref="IsUsable{T}(ServiceResponse{T})"/> ALSO treats a null <c>Data</c> as unusable, which the
    /// non-generic overload does not. Receivers are nullable — these are deliberately null-safe (an extension
    /// can be invoked on a null reference), so <c>response.IsUsable()</c> is safe even when <c>response</c>
    /// itself is null and needs no <c>?.</c>.
    /// </summary>
    public static class ServiceResponseExtensions
    {
        /// <summary>True when the response is null, not successful, or carries errors.</summary>
        public static bool HasError(this ServiceResponse? response)
            => response is null || !response.IsSuccessful || response.Errors?.Count > 0;

        /// <summary>
        /// True when the response is null, not successful, carries errors, OR its <c>Data</c> is null — the
        /// "successful but empty" case consumers already treat as unusable (e.g. a not-found lookup).
        /// </summary>
        public static bool HasError<T>(this ServiceResponse<T>? response)
            => response is null || !response.IsSuccessful || response.Data is null || response.Errors?.Count > 0;

        /// <summary>Inverse of <see cref="HasError(ServiceResponse)"/> — the request succeeded and carries no errors.</summary>
        public static bool IsUsable(this ServiceResponse? response) => !response.HasError();

        /// <summary>Inverse of <see cref="HasError{T}(ServiceResponse{T})"/> — safe to read <c>Data</c>.</summary>
        public static bool IsUsable<T>(this ServiceResponse<T>? response) => !response.HasError();

        /// <summary><c>Data</c> when usable, otherwise <paramref name="fallback"/> (e.g. an empty collection).</summary>
        public static T GetDataOr<T>(this ServiceResponse<T>? response, T fallback)
            => response.IsUsable() ? response!.Data : fallback;

        /// <summary><c>Data</c> when usable, otherwise <c>default</c> (null for reference types).</summary>
        public static T? GetDataOrDefault<T>(this ServiceResponse<T>? response)
            => response.IsUsable() ? response!.Data : default;

        /// <summary>
        /// Guard pattern: sets <paramref name="data"/> to <c>Data</c> and returns true when usable; otherwise
        /// sets <c>default</c> and returns false. Mirrors <c>IDictionary.TryGetValue</c>.
        /// </summary>
        public static bool TryGetData<T>(this ServiceResponse<T>? response, out T data)
        {
            if (response.IsUsable())
            {
                data = response!.Data;
                return true;
            }

            data = default!;
            return false;
        }

        /// <summary>
        /// Flattens <see cref="ServiceResponse.Errors"/> into a single readable string for logging / the
        /// error-page redirect — skips the noisy <c>"Traces"</c> key. Returns a generic message when there is no detail.
        /// </summary>
        public static string ErrorSummary(this ServiceResponse? response)
        {
            if (response is null)
                return "No response.";

            if (response.Errors is null || response.Errors.Count == 0)
                return response.IsSuccessful ? "OK." : "The request was not successful.";

            var parts = new List<string>();
            foreach (var kv in response.Errors)
            {
                if (kv.Key == "Traces")
                    continue;

                parts.Add(kv.Key + ": " + string.Join(", ", kv.Value ?? Array.Empty<string>()));
            }

            return parts.Count > 0 ? string.Join("; ", parts) : "The request was not successful.";
        }
    }
}
