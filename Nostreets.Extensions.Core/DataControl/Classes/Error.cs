using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using Nostreets.Extensions.Extend.Basic;

namespace Nostreets.Extensions.DataControl.Classes
{

    public class Error : DBObject
    {
        public Error() { }

        public Error(Exception ex)
        {
            Message = CombinedMessage(ex);
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
            Message = CombinedMessage(ex);
            DateCreated = DateTime.Now;
            Source = ex.Source;
            HelpLink = ex.HelpLink;
            Trace = ex.StackTraceToDictionary();
            Class = Trace?["class"];
            Line = int.Parse(Trace?["line"]);
            Method = ex.TargetSite.NameWithParams();
            Data = data;
        }

        public string Data { get; set; }
        public string Message { get; set; }
        public string Source { get; set; }
        public string Class { get; set; }
        public string Method { get; set; }
        public int Line { get; set; }
        public Dictionary<string, string> Trace { get; set; }
        public string HelpLink { get; set; }
        [NotMapped]
        public override string ModifiedUserId { get; set; }
        [NotMapped]
        public override bool IsDeleted { get; set; }
        [NotMapped]
        public override DateTime? DateModified { get; set; }


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
