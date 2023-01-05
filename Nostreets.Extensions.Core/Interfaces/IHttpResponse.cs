using System;
using System.Net;

namespace Nostreets.Extensions.Interfaces
{
    public interface IHttpResponse
    {
        bool SupportsHeaders { get; }
        Version ProtocolVersion { get; }
        string StatusDescription { get; }
        HttpStatusCode StatusCode { get; }
        DateTime LastModified { get; }
        string Server { get; }
        string CharacterSet { get; }
        string ContentType { get; }
        string ContentEncoding { get; }
        string Method { get; }
        Uri ResponseUri { get; }
        WebHeaderCollection Headers { get; }
        CookieCollection Cookies { get; set; }
        bool IsMutuallyAuthenticated { get; }
        long ContentLength { get; }
    }

    public interface IHttpResponse<T> : IHttpResponse
    {
        T Data { get; set; }
        
    }
}
