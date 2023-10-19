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
            Class = Trace?["class"];
            Line = int.Parse(Trace?["line"]);
            Method = ex.TargetSite.NameWithParams();
        }

        public Error(Exception ex, string data)
        {
            ErrorMessage = CombinedMessage(ex);
            DateCreated = DateTime.Now;
            Source = ex.Source;
            HelpLink = ex.HelpLink;
            Trace = ex.StackTraceToDictionary();
            Class = Trace?["class"];
            Line = int.Parse(Trace?["line"]);
            Method = ex.TargetSite.NameWithParams();
            Data = data;
        }

        public string? SessionKey { get; set; }
        public string? Data { get; set; }
        public string ErrorMessage { get; set; }
        public string Source { get; set; }
        public string Class { get; set; }
        public string Method { get; set; }
        public int Line { get; set; }
        public string SerializedTrace { get => Trace != null ? JsonConvert.SerializeObject(Trace) : null; }

        
        
        public string HelpLink { get; set; }
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
