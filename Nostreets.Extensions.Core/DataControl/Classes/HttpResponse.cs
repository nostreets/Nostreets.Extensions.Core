using System;
using System.Collections.Generic;
using System.Net;
using Newtonsoft.Json;
using Nostreets.Extensions.Extend.Web;
using Nostreets.Extensions.Interfaces;

namespace Nostreets.Extensions.DataControl.Classes
{
    public class HttpResponse<T> : WebResponse, IHttpResponse<T>
    {
        public HttpResponse(HttpWebRequest request)
        {
            using (HttpWebResponse responseStream = (HttpWebResponse)request.GetResponse())
            {
                Data = responseStream.GetHttpResponseData<T>(out string responseString);
                Cookies = responseStream.Cookies;
                ProtocolVersion = responseStream.ProtocolVersion;
                StatusDescription = responseStream.StatusDescription;
                StatusCode = responseStream.StatusCode;
                LastModified = responseStream.LastModified;
                Server = responseStream.Server;
                CharacterSet = responseStream.CharacterSet;
                ContentEncoding = responseStream.ContentEncoding;
                Method = responseStream.Method;
                SerializedResponse = responseString;
            }
        }

        public HttpResponse(string url, string method = "GET", object data = null, string contentType = "application/json", Dictionary<string, string> headers = null, JsonConverter[] converters = null)
        {
            HttpWebRequest requestStream = url.GetRequestStream(method, data, contentType, headers);

            using (HttpWebResponse responseStream = (HttpWebResponse)requestStream.GetResponseAsync().Result)
            {
                Data = responseStream.GetHttpResponseData<T>(out string responseString, converters);
                Cookies = responseStream.Cookies;
                ProtocolVersion = responseStream.ProtocolVersion;
                StatusDescription = responseStream.StatusDescription;
                StatusCode = responseStream.StatusCode;
                LastModified = responseStream.LastModified;
                Server = responseStream.Server;
                CharacterSet = responseStream.CharacterSet;
                ContentEncoding = responseStream.ContentEncoding;
                Method = responseStream.Method;
                SerializedResponse = responseString;
            }
        }

        public T Data { get; set; } = default(T);
        public Version ProtocolVersion { get; } = null;
        public string StatusDescription { get; } = null;
        public HttpStatusCode StatusCode { get; } = default(HttpStatusCode);
        public DateTime LastModified { get; } = default(DateTime);
        public string Server { get; } = null;
        public string CharacterSet { get; } = null;
        public string ContentEncoding { get; } = null;
        public string Method { get; } = null;
        public CookieCollection Cookies { get; set; } = null; 
        public string SerializedResponse { get; set; } = null;
    }

   
}
