using Google.Apis.Auth.OAuth2;
using Google.Apis.Gmail.v1;
using Google.Apis.Gmail.v1.Data;
using Google.Apis.Services;
using Google.Apis.Util.Store;
using Microsoft.AspNetCore.Http;

using Nostreets.Extensions.Extend.Basic;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Web;

namespace Nostreets.Extensions.Extend.Google
{
    public static class Google
    {
        public static IList<Label> GetLabels(this GmailService service)
        {
            try
            {
                var request = service.Users.Labels.List("me");
                IList<Label> labels = request.Execute().Labels;
                return labels;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static IList<Message> GetMessages(this GmailService service, int limit = 0, DateTime? start = null, DateTime? end = null, string label = null)
        {
            List<Message> result = new List<Message>();

            try
            {
                var request = service.Users.Messages.List("me");

                do
                {
                    var response = request.Execute();


                    foreach (Message partial in response.Messages)
                    {
                        Message message = partial;
                        message = service.GetMessage(partial.Id);

                        if (isInDateRange(message))
                            result.Add(message);

                        if(doesLabelMatch(message))
                            result.Add(message);
#if DEBUG
                        message.Snippet.LogInDebug();
#endif
                    }

                    request.PageToken = response.NextPageToken;


                } while (isAtLimit(request));


                return result;
            }
            catch (Exception ex)
            {
                throw ex;
            }


            bool doesLabelMatch(Message m)
            {
                return label == null ? true : m.LabelIds != null ? m.LabelIds.Any(b => b == label) : false;
            }

            bool isAtLimit(UsersResource.MessagesResource.ListRequest r)
            {
                bool isTrue = string.IsNullOrEmpty(r.PageToken); ;

                if (limit > 0)
                    isTrue = limit <= result.Count;


                return isTrue;
            }

            bool isInDateRange(Message m)
            {

                DateTime emailDate = m.InternalDate.HasValue ? DateTimeOffset.FromUnixTimeMilliseconds(m.InternalDate.Value).DateTime : default(DateTime);
                bool isTrue = false;

                if (start != null && end != null && emailDate == default(DateTime))
                    isTrue = true;
                else if (start != null && end != null)
                    isTrue = true;
                else if (emailDate == default(DateTime))
                    isTrue = false;
                else if (end != null)
                    isTrue = end >= emailDate;
                else if (start != null)
                    isTrue = emailDate >= start;
                else if (start != null && end != null)
                    isTrue = emailDate >= start && end >= emailDate;


                return isTrue;

            }

        }

        public static Message GetMessage(this GmailService service, string id)
        {

            try
            {
                var request = service.Users.Messages.Get("me", id);
                return request.Execute();
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }

    }
}
