using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;

using Newtonsoft.Json;

using Nostreets.Extensions.Extend.Basic;

namespace Nostreets.Extensions.DataControl.Classes
{
    public class Error : DBObject
    {
        public Error() { }

        public Error(Exception ex)
        {
            ErrorMessage = CombinedMessage(ex);
            DateCreated = DateTime.Now;
            Source = ex.Source;
            HelpLink = ex.HelpLink;
            Trace = ex.StackTraceToDictionary();
            // These reads MUST be defensive, and the reason is not theoretical.
            // StackTraceToDictionary returns an EMPTY dictionary (never null) when its regex finds
            // no "in <file>:line <n>" frame -- which is every exception thrown inside a NuGet-packaged
            // assembly, because those ship without PDBs. Under the NugetRef profile that is MOST of
            // the code. Trace?["class"] then threw KeyNotFoundException from inside the error handler,
            // so the ORIGINAL exception was destroyed and the caller got a bare 500 with no
            // diagnosable cause -- that is how a login-blocking failure reached dev wearing the wrong
            // error. int.Parse on a missing key, and ex.TargetSite (null for a rethrown or
            // reflection-invoked exception), are the same trap.
            Class = TryTrace("class") ?? ex.TargetSite?.DeclaringType?.FullName;
            Line = int.TryParse(TryTrace("line"), out int parsedLine) ? parsedLine : 0;
            Method = ex.TargetSite?.NameWithParams();
        }

        public Error(Exception ex, string data)
        {
            ErrorMessage = CombinedMessage(ex);
            DateCreated = DateTime.Now;
            Source = ex.Source;
            HelpLink = ex.HelpLink;
            Trace = ex.StackTraceToDictionary();
            // These reads MUST be defensive, and the reason is not theoretical.
            // StackTraceToDictionary returns an EMPTY dictionary (never null) when its regex finds
            // no "in <file>:line <n>" frame -- which is every exception thrown inside a NuGet-packaged
            // assembly, because those ship without PDBs. Under the NugetRef profile that is MOST of
            // the code. Trace?["class"] then threw KeyNotFoundException from inside the error handler,
            // so the ORIGINAL exception was destroyed and the caller got a bare 500 with no
            // diagnosable cause -- that is how a login-blocking failure reached dev wearing the wrong
            // error. int.Parse on a missing key, and ex.TargetSite (null for a rethrown or
            // reflection-invoked exception), are the same trap.
            Class = TryTrace("class") ?? ex.TargetSite?.DeclaringType?.FullName;
            Line = int.TryParse(TryTrace("line"), out int parsedLine) ? parsedLine : 0;
            Method = ex.TargetSite?.NameWithParams();
            Data = data;
        }


        /// <summary>
        /// Reads a parsed stack-trace field, tolerating BOTH a null dictionary and a present-but-empty
        /// one. Never throws: an error object that cannot be constructed hides the error it describes.
        /// </summary>
        private string TryTrace(string key)
            => Trace != null && Trace.TryGetValue(key, out string value) ? value : null;

        public string? SessionKey { get; set; }
        public string? TransactionId { get; set; }
        public string? Data { get; set; }
        public string ErrorMessage { get; set; }
        public string Source { get; set; }
        public string Class { get; set; }
        public string Method { get; set; }
        public int Line { get; set; }
        public string SerializedTrace { get => Trace != null ? JsonConvert.SerializeObject(Trace) : null; }
        public string? HelpLink { get; set; }

        [NotMapped]
        public override bool IsArchived { get; set; }
        [NotMapped]
        public override DateTime DateModified { get; set; }
        [NotMapped]
        public Dictionary<string, string> Trace { get; set; }

        private string CombinedMessage(Exception ex)
        {
            if (ex == null)
                throw new ArgumentNullException("ex");


            string result = ex.Message;

            while (ex.InnerException != null)
            {
                ex = ex.InnerException;
                result += " --> " + ex.Message;
            }

            return result;
        }

    }

}
