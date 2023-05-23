using Hangfire;

using Microsoft.AspNetCore.Builder.Extensions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Extensions;

using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

using Nostreets.Extensions.DataControl.Classes;
using Nostreets.Extensions.Extend.Basic;
using Nostreets.Extensions.Interfaces;
using Nostreets.Extensions.Utilities;

using Owin;

using RestSharp;
using RestSharp.Authenticators;

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using System.Web.Http;
using System.Web.Http.Results;
using System.Web.Http.Routing;
using System.Web.Mvc;
using System.Web.Optimization;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Serialization;
using System.Net.Http.Formatting;

namespace Nostreets.Extensions.Extend.Web
{
    public static class Web
    {
        #region Static
        /// <summary>
        /// Creates the HTTP response message.
        /// </summary>
        /// <param name="content">The content.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <param name="httpStatusCode">The HTTP status code.</param>
        /// <returns></returns>
        /// <exception cref="System.NotImplementedException"></exception>
        public static HttpResponseMessage CreateHttpResponseMessage(object content, string contentType, HttpStatusCode httpStatusCode)
        {
            throw new NotImplementedException();

            string serializedContent = content as string ?? JsonConvert.SerializeObject(content);
            HttpResponseMessage result = new HttpResponseMessage(httpStatusCode);
            result.Content = new StringContent((string)content, Encoding.UTF8, contentType);
        }

        public static string GetValueFromWebConfig(string key)
        {
            string result = null;
            // Get the configuration.
            var appSettings = ConfigurationManager.AppSettings;
            bool doesKeyExist = false;

            if (appSettings != null)
            {
                if (appSettings.AllKeys?.Contains(key) ?? false)
                    doesKeyExist = true;

                if (doesKeyExist)
                    result = appSettings[key]; 
            }

            return result;
        }

        /// <summary>
        /// Updates the web configuration.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public static void UpdateWebConfig(string key, string value)
        {
            // Get the configuration.
            var appSettings = ConfigurationManager.AppSettings;
            bool doesKeyExist = false;

            if (appSettings != null)
            {
                if (appSettings.AllKeys?.Contains(key) ?? false)
                    doesKeyExist = true;

                if (!doesKeyExist)
                    appSettings.Add(key, value);
                else
                    appSettings[key] = value;
            }
        }

        public static HttpResponseMessage CreateResponse(HttpStatusCode statusCode)
        {
            HttpResponseMessage response = new HttpResponseMessage();
            response.StatusCode = statusCode;
            return response;
        }


        public static HttpResponseMessage CreateResponse<T>(HttpStatusCode statusCode, T value, MediaTypeFormatter formater = null, string contentType = "application/json")
        {
            if (formater == null)
                formater = new JsonMediaTypeFormatter();

            var response = CreateResponse(statusCode);

            response.Content = new ObjectContent<T>(value, formater, contentType);

            return response;
        }
        #endregion

        #region Extensions
        public static string GetSitemap(this HttpContext context, string rootUrl, params string[] urls)
        {
            if (rootUrl == null)
                throw new ArgumentNullException("rootUrl");

            if (!rootUrl.IsValidUrl())
                throw new Exception("rootUrl is not a valid url...");


            context.Response.StatusCode = 200;
            context.Response.ContentType = "application/xml";

            string sitemapContent = "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">";


            foreach (string url in urls)
            {
                sitemapContent += "<url>";

                if (url.IsValidUrl())
                    sitemapContent += string.Format("<loc>{0}</loc>", url);
                else
                    sitemapContent += string.Format("<loc>{0}/{1}</loc>", rootUrl, url);

                sitemapContent += string.Format("<lastmod>{0}</lastmod>", DateTime.UtcNow.ToString("yyyy-MM-dd"));
                sitemapContent += "</url>";
            }

            sitemapContent += "</urlset>";

            return sitemapContent;
        }


        public static string GetSitemap(this HttpContext context, string rootUrl, params object[] urlsObjects)
        {
            if (rootUrl == null)
                throw new ArgumentNullException("rootUrl");

            if (!rootUrl.IsValidUrl())
                throw new Exception("rootUrl is not a valid url...");


            context.Response.StatusCode = 200;
            context.Response.ContentType = "application/xml";

            string sitemapContent = "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">";


            foreach (object data in urlsObjects)
            {
                string url = null,
                       changefreq = null;
                double priority = 0.5;

                if (data.HasProperty("url"))
                    url = (string)data.GetPropertyValue("url");
                else if (data.HasProperty("Url"))
                    url = (string)data.GetPropertyValue("Url");
                if (data.HasProperty("changefreq"))
                    changefreq = (string)data.GetPropertyValue("changefreq");
                else if (data.HasProperty("ChangeFreq"))
                    changefreq = (string)data.GetPropertyValue("ChangeFreq");
                if (data.HasProperty("priority"))
                    priority = (double)data.GetPropertyValue("priority");
                else if (data.HasProperty("Priority"))
                    priority = (double)data.GetPropertyValue("Priority");

                if (url != null)
                {
                    sitemapContent += "<url>";

                    if (url.IsValidUrl())
                        sitemapContent += string.Format("<loc>{0}</loc>", url);
                    else
                        sitemapContent += string.Format("<loc>{0}/{1}</loc>", rootUrl, url);

                    if (changefreq != null)
                        sitemapContent += string.Format("<changefreq>{0}</changefreq>", changefreq);


                    sitemapContent += string.Format("<priority>{0}</priority>", priority);
                    sitemapContent += string.Format("<lastmod>{0}</lastmod>", DateTime.UtcNow.ToString("yyyy-MM-dd"));
                    sitemapContent += "</url>";
                }
            }

            sitemapContent += "</urlset>";

            return sitemapContent;
        }


        /// <summary>
        /// Creates the response.
        /// </summary>
        /// <param name="app">The application.</param>
        /// <param name="statusCode">The status code.</param>
        /// <param name="obj">The object.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <param name="resolver">The resolver.</param>
        /// <param name="encoding">The encoding.</param>
        public static void CreateResponse(this HttpContext app
                                            , HttpStatusCode statusCode
                                            , object obj
                                            , string contentType = "application/json"
                                            , IContractResolver resolver = null
                                            , Encoding encoding = null)
        {
            if (encoding == null) { encoding = Encoding.UTF8; }
            if (resolver == null) { resolver = new CamelCasePropertyNamesContractResolver(); }

            JsonSerializerSettings settings = new JsonSerializerSettings()
            {
                ContractResolver = resolver
            };

            string jsonObj = JsonConvert.SerializeObject(obj, settings);
            HttpResponseMessage response = new HttpResponseMessage(statusCode)
            {
                Content = new StringContent(jsonObj, encoding, contentType)
            };

            app.Response.ContentType = contentType;
            app.Response.StatusCode = (int)statusCode;
            app.Response.WriteAsync(jsonObj);

        }

        /// <summary>
        /// Decodes the URL.
        /// </summary>
        /// <param name="url">The URL.</param>
        /// <returns></returns>
        public static string DecodeUrl(this string url)
        {
            return HttpUtility.UrlDecode(url);
        }

        /// <summary>
        /// Encodes the URL.
        /// </summary>
        /// <param name="url">The URL.</param>
        /// <returns></returns>
        public static string EncodeUrl(this string url)
        {
            return (url.IsValidUrl()) ? HttpUtility.UrlEncode(url) : null;
        }

        /// <summary>
        /// Gets the cookie.
        /// </summary>
        /// <param name="request">The request.</param>
        /// <param name="cookieName">Name of the cookie.</param>
        /// <returns></returns>
        public static string GetCookie(this HttpRequestMessage request, string cookieName)
        {
            CookieHeaderValue cookie = request.Headers.GetCookies(cookieName).FirstOrDefault() ?? default(CookieHeaderValue);

            return cookie[cookieName].Value;
        }

        /// <summary>
        /// Gets the cookie.
        /// </summary>
        /// <param name="request">The request.</param>
        /// <param name="cookieName">Name of the cookie.</param>
        /// <returns></returns>
        public static string GetCookie(this HttpContext context, string cookieName)
        {
            string result = null;
            string cookie = context.Request.Cookies[cookieName];
            if (cookie != null)
                result = cookie;

            return result;
        }

        /// <summary>
        /// Gets the header.
        /// </summary>
        /// <param name="request">The request.</param>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static string GetHeader(this HttpRequestMessage request, string key)
        {
            IEnumerable<string> keys = null;
            if (!request.Headers.TryGetValues(key, out keys))
                return null;

            return keys.First();
        }

        /// <summary>
        /// Gets the HTTP response data.
        /// </summary>
        /// <param name="responseStream">The response stream.</param>
        /// <param name="responseType">Type of the response.</param>
        /// <returns></returns>
        public static object GetHttpResponseData(this HttpWebResponse responseStream, Type responseType = null)
        {
            try
            {
                string responseString = null;

                using (Stream stream = responseStream.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(stream);
                    responseString = reader.ReadToEnd();
                }

                object responseData;

                if (responseString.IsJson())
                    responseData = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(responseString);
                else if (responseString.IsXml())
                {
                    object deserializedJson;
                    string json;

                    if (responseString.IsHtml())
                    {
                        XDocument doc = XDocument.Parse(responseString);
                        json = JsonConvert.SerializeXNode(doc);
                        deserializedJson = JsonConvert.DeserializeObject(json);
                    }
                    else
                    {
                        //XmlSerializer serial = new XmlSerializer(data.GetType());
                        //StringReader reader = new StringReader(responseString);
                        //responseData = serial.Deserialize(reader);
                        XmlDocument doc = new XmlDocument();
                        doc.Load(responseString);
                        json = JsonConvert.SerializeXmlNode(doc);
                        deserializedJson = JsonConvert.DeserializeObject(json);
                    }

                    if (responseType != null && deserializedJson.TryCast(responseType, out object obj))
                        responseData = obj;
                    else
                        responseData = deserializedJson;
                }
                else
                    responseData = null;

                return responseData;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Gets the HTTP response data.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="responseStream">The response stream.</param>
        /// <param name="responseString">The response string.</param>
        /// <returns></returns>
        public static T GetHttpResponseData<T>(this HttpWebResponse responseStream, out string responseString, JsonConverter[] converters = null)
        {
            try
            {
                responseString = null;

                using (Stream stream = responseStream.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(stream);
                    responseString = reader.ReadToEnd();
                }

                T responseData = default(T);

                if (responseString.IsJson())
                {
                    try
                    {
                        responseData = JsonConvert.DeserializeObject<T>(responseString, converters);
                    }
                    catch (Exception)
                    {
                        try
                        {
                            var responseDataCollection = JsonConvert.DeserializeObject<List<T>>(responseString);
                            if (responseDataCollection != null && responseDataCollection.Count == 1)
                                responseData = responseDataCollection[0];
                        }
                        catch (Exception)
                        {
                            throw;
                        }
                    }

                }
                else if (responseString.IsXml())
                {
                    object deserializedJson;
                    string json;

                    if (responseString.IsHtml())
                    {
                        XDocument doc = XDocument.Parse(responseString);
                        json = JsonConvert.SerializeXNode(doc);
                        deserializedJson = JsonConvert.DeserializeObject(json);
                    }
                    else
                    {
                        //XmlSerializer serial = new XmlSerializer(data.GetType());
                        //StringReader reader = new StringReader(responseString);
                        //responseData = serial.Deserialize(reader);

                        XmlDocument doc = new XmlDocument();
                        doc.Load(responseString);
                        json = JsonConvert.SerializeXmlNode(doc);
                        deserializedJson = JsonConvert.DeserializeObject(json);
                    }

                    if (deserializedJson.TryCast(out T convertedObj))
                        responseData = convertedObj;
                    else
                        responseData = default(T);
                    //    throw new InvalidCastException("Unable to cast response data to type of " + typeof(T).Name, new Exception(responseString));
                }

                return responseData;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Gets the i p4 address.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <returns></returns>
        public static string GetIP4Address(this HttpContext context)
        {
            return context.Connection.RemoteIpAddress.MapToIPv4().ToString();
        }

        /// <summary>
        /// Gets the request ip address.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <returns></returns>
        public static string GetIPAddress(this HttpContext context)
        {
            string remoteIpAddress = context.Session.GetString("IPAddress");
            if (remoteIpAddress == null)
            {
                remoteIpAddress = context.Connection.RemoteIpAddress.ToString();
                context.Session.SetString("IPAddress", remoteIpAddress);
            }
            return remoteIpAddress;
        }

        /// <summary>
        /// Gets the request ip address.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <returns></returns>
        public static IPAddress GetIPAddressData(this HttpContext context)
        {
            return context.Connection.RemoteIpAddress;
        }

        /// <summary>
        /// Gets the local ip addresses.
        /// </summary>
        /// <returns></returns>
        public static string GetIPAddressViaDns(this HttpContext context)
        {
            string hostName = Dns.GetHostName();
            IPHostEntry ipEntry = Dns.GetHostEntry(hostName);
            string result = ipEntry.AddressList[ipEntry.AddressList.Length - 1].ToString();

            return result;
        }

        /// <summary>
        /// Gets the query string.
        /// </summary>
        /// <param name="request">The request.</param>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static string GetQueryString(this HttpRequestMessage request, string key)
        {
            // IEnumerable<KeyValuePair<string,string>> - right!
            var queryStrings = request.GetQueryNameValuePairs();
            if (queryStrings == null)
                return null;

            var match = queryStrings.FirstOrDefault(kv => string.Compare(kv.Key, key, true) == 0);
            if (string.IsNullOrEmpty(match.Value))
                return null;

            return match.Value;
        }

        /// <summary>
        /// Gets the query strings.
        /// </summary>
        /// <param name="request">The request.</param>
        /// <returns></returns>
        public static Dictionary<string, string> GetQueryStrings(this HttpRequestMessage request)
        {
            return request.GetQueryNameValuePairs()
                          .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Gets the request stream.
        /// </summary>
        /// <param name="url">The URL.</param>
        /// <param name="method">The method.</param>
        /// <param name="data">The data.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <param name="headers">The headers.</param>
        /// <returns></returns>
        /// <exception cref="System.Exception">url to has to be valid url string to be able to HitEndpoint...</exception>
        public static HttpWebRequest GetRequestStream(this string url, string method = "GET", object data = null, string contentType = "application/json", Dictionary<string, string> headers = null)
        {
            try
            {
                if (!url.IsValidUrl())
                    throw new Exception("url to has to be valid url string to be able to HitEndpoint...");

                HttpWebRequest requestStream = (HttpWebRequest)WebRequest.Create(url);
                string requestString = null;
                byte[] bytes = null;

                if (headers == null) { headers = new Dictionary<string, string>(); }

                requestStream.ContentType = contentType;
                requestStream.Method = method;

                foreach (KeyValuePair<string, string> head in headers)
                {
                    requestStream.Headers.Add(head.Key, head.Value);
                }

                if (data != null)
                {
                    if (method == "POST" || method == "PUT" || method == "PATCH")
                    {
                        if (contentType == "application/json" && !(data.GetType() == typeof(string) && ((string)data).IsJson()))
                            requestString = JsonConvert.SerializeObject(data);
                        else if (contentType == "text/xml; encoding='utf-8'")
                        {
                            XmlSerializer serial = new XmlSerializer(data.GetType());
                            StringWriter writer = new StringWriter();
                            serial.Serialize(writer, data);
                            requestString = "XML=" + writer.ToString();
                            writer.Close();
                        }
                        else if (contentType == "text/plain" && data.GetType() == typeof(string))
                            requestString = (string)data;
                    }

                    using (Stream stream = requestStream.GetRequestStream())
                    {
                        StreamWriter writer = new StreamWriter(stream);
                        if (requestString != null) { writer.Write(requestString); }
                        else if (bytes != null) { stream.Write(bytes, 0, bytes.Length); }
                        writer.Close();
                    }
                }

                return requestStream;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Hits the endpoint.
        /// </summary>
        /// <param name="url">The URL.</param>
        /// <param name="method">The method.</param>
        /// <param name="data">The data.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <param name="headers">The headers.</param>
        /// <param name="responseType">Type of the response.</param>
        /// <returns></returns>
        /// <exception cref="Exception">url to has to be valid url string to be able to HitEndpoint...</exception>
        public static IHttpResponse<object> HitEndpoint(this string url, string method = "GET", object data = null, string contentType = "application/json", Dictionary<string, string> headers = null, Type responseType = null)
        {
            try
            {
                return new HttpResponse<object>(url, method, data, contentType, headers);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Hits the endpoint.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="url">The URL.</param>
        /// <param name="method">The method.</param>
        /// <param name="data">The data.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <param name="headers">The headers.</param>
        /// <returns></returns>
        /// <exception cref="Exception">url to has to be valid url string to be able to HitEndpoint...
        /// or</exception>
        public static IHttpResponse<T> HitEndpoint<T>(this string url, string method = "GET", object data = null, string contentType = "application/json", Dictionary<string, string> headers = null, JsonConverter[] converters = null)
        {
            try
            {
                return new HttpResponse<T>(url, method, data, contentType, headers, converters);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Applies the CssRewriteUrlTransform to every path in the array.
        /// </summary>
        /// <param name="bundle">The bundle.</param>
        /// <param name="virtualPaths">The virtual paths.</param>
        /// <returns></returns>
        public static Bundle IncludeWithCssRewriteUrlTransform(this StyleBundle bundle, params string[] virtualPaths)
        {
            //Ensure we add CssRewriteUrlTransform to turn relative paths (to images, etc.) in the CSS files into absolute paths.
            //Otherwise, you end up with 404s as the bundle paths will cause the relative paths to be off and not reach the static files.

            if ((virtualPaths != null) && (virtualPaths.Any()))
            {
                virtualPaths.ToList().ForEach(path =>
                {
                    bundle.Include(path, new CssRewriteUrlTransform());
                });
            }

            return bundle;
        }

        /// <summary>
        /// Determines whether this instance is HTML.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <returns>
        ///   <c>true</c> if the specified input is HTML; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsHtml(this string input)
        {
            return input != HttpUtility.HtmlEncode(input) && input.Contains("DOCTYPE html");
        }

        /// <summary>
        /// Registers the API external route.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <param name="namespace">The namespace.</param>
        /// <param name="url">The URL.</param>
        /// <param name="defaults">The defaults.</param>
        /// <exception cref="System.ArgumentNullException">@namespace</exception>
        public static void RegisterApiExternalRoute(this HttpConfiguration config, string @namespace, string url = "api/{controller}/{id}", object defaults = null)
        {
            if (@namespace == null || @namespace == "")
                throw new ArgumentNullException("@namespace");

            // config.Routes.MapHttpRoute(
            //    name: @namespace + "Default",
            //    routeTemplate: url,
            //    defaults: defaults ?? new { controller = "Home", action = "Index", id = UrlParameter.Optional },
            //    constraints: new[] {
            //        new HttpRouteValueDictionary(
            //            new
            //            {
            //                Namespace = new[] { @namespace }
            //                //namespaces = new[] { @namespace }
            //            }
            //        )
            //    }
            //);

            HttpRoute externalRoute = new HttpRoute(url,
                new HttpRouteValueDictionary(
                   new
                   {
                       Namespace = new[] { @namespace }
                       //namespaces = new[] { @namespace }
                   })
               );

            config.Routes.Add(@namespace + "Route", externalRoute);
        }

        /// <summary>
        /// Hits the endpoint.
        /// </summary>
        /// <param name="url">The URL.</param>
        /// <param name="method">The method.</param>
        /// <param name="data">The data.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <param name="headers">The headers.</param>
        /// <returns></returns>
        /// <exception cref="System.Exception">URL is not defined!</exception>
        /// <exception cref="Exception">url to has to be valid url string to be able to HitEndpoint...
        /// or</exception>
        public static RestResponse<object> RestSharpEndpoint(this string url, string method = "GET", object data = null, string contentType = "application/json", Dictionary<string, string> headers = null)
        {
            #region Client

            RestResponse<object> result = null;
            RestClient rest = null;
            if (url != null)
            {
                rest = new RestClient(url);
            }
            else { throw new Exception("URL is not defined!"); }

            #endregion Client

            #region Request

            RestRequest request = new RestRequest();
            switch (method)
            {
                case "GET":
                    request.Method = Method.Get;
                    break;

                case "POST":
                    request.Method = Method.Post;
                    break;

                case "PATCH":
                    request.Method = Method.Patch;
                    break;

                case "PUT":
                    request.Method = Method.Put;
                    break;

                case "DELETE":
                    request.Method = Method.Delete;
                    break;

                default:
                    request.Method = Method.Get;
                    break;
            };
            request.RequestFormat = DataFormat.Json;
            request.AddBody(data);
            if (headers != null)
            {
                foreach (var item in headers)
                {
                    if (item.Key.Contains("auth"))
                    {
                        rest.Authenticator = new HttpBasicAuthenticator("username", item.Value);
                    }
                    else if (item.Key == "contentType")
                    {
                        request.AddParameter(Parameter.CreateParameter("ContentType", item.Value, ParameterType.HttpHeader));
                    }
                    else
                    {
                        request.AddParameter(Parameter.CreateParameter(item.Key, item.Value, ParameterType.HttpHeader));
                    }
                }
            }

            #endregion Request

            result = rest.Execute<object>(request);
            return result;
        }

        /// <summary>
        /// Hits the endpoint.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="url">The URL.</param>
        /// <param name="method">The method.</param>
        /// <param name="data">The data.</param>
        /// <param name="contentType">Type of the content.</param>
        /// <param name="headers">The headers.</param>
        /// <returns></returns>
        /// <exception cref="System.Exception">URL is not defined!</exception>
        /// <exception cref="Exception">url to has to be valid url string to be able to HitEndpoint...
        /// or</exception>
        public static RestResponse<T> RestSharpEndpoint<T>(this string url, string method = "GET", object data = null, string contentType = "application/json", Dictionary<string, string> headers = null) where T : new()
        {
            #region Client

            RestResponse<T> result = null;
            RestClient rest = null;
            if (url != null)
                rest = new RestClient(url);

            else
                throw new Exception("URL is not defined!");

            #endregion Client

            #region Request

            RestRequest request = new RestRequest();
            switch (method)
            {
                case "GET":
                    request.Method = Method.Get;
                    break;

                case "POST":
                    request.Method = Method.Post;
                    break;

                case "PATCH":
                    request.Method = Method.Patch;
                    break;

                case "PUT":
                    request.Method = Method.Put;
                    break;

                case "DELETE":
                    request.Method = Method.Delete;
                    break;

                default:
                    request.Method = Method.Get;
                    break;
            };
            request.RequestFormat = DataFormat.Json;
            request.AddBody(data);
            if (headers != null)
            {
                foreach (var item in headers)
                {
                    if (item.Key.Contains("auth"))
                        rest.Authenticator = new HttpBasicAuthenticator("username", item.Value);

                    else if (item.Key == "contentType")
                    {
                        request.AddParameter(Parameter.CreateParameter("ContentType", item.Value, ParameterType.HttpHeader));
                    }
                    else
                    {
                        request.AddParameter(Parameter.CreateParameter(item.Key, item.Value, ParameterType.HttpHeader));
                    }
                }
            }

            #endregion Request

            result = rest.Execute<T>(request);
            return result;
        }

        /// <summary>
        /// Sets the cookie.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="cookieName">Name of the cookie.</param>
        /// <param name="value">The value.</param>
        /// <param name="expires">The expires.</param>
        public static void SetCookie(this HttpContext context, string cookieName, string value)
        {
            try
            {
                context.Response.Cookies.Append(cookieName, value);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Sets the cookie.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="cookieName">Name of the cookie.</param>
        /// <param name="values">The values.</param>
        /// <param name="expires">The expires.</param>
        public static void SetCookie(this HttpContext context, string cookieName, Dictionary<string, string> values)
        {
            try
            {
                string value = null;
                if (values != null && values.Count > 0)
                {
                    int i = 0;
                    foreach (KeyValuePair<string, string> val in values)
                    {
                        value += String.Format((i == values.Count - 1) ? "{0}={1}" : "{0}={1}, ", val.Key, val.Value); i++;
                    }
                }

                context.Response.Cookies.Append(cookieName, value);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static bool ValidUrl(this string s, out Uri resultURI)
        {
            resultURI = null;
            if (s != null)
            {
                if (!Regex.IsMatch(s, @"^https?:\/\/", RegexOptions.IgnoreCase))
                    s = "http://" + s;

                if (Uri.TryCreate(s, UriKind.Absolute, out resultURI))
                    return (resultURI.Scheme == Uri.UriSchemeHttp ||
                            resultURI.Scheme == Uri.UriSchemeHttps);
            }

            return false;
        }

        public static Uri RequestUrl(this HttpRequest request) 
        {
            return new Uri(request.GetDisplayUrl());
        }

        #endregion
    }
}