using Nostreets.Extensions.Extend.Basic;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Nostreets.Extensions.Models.Responses
{
    public class ErrorResponse<T> : ErrorResponse
    {
        public ErrorResponse(string errMsg, T data) : base(errMsg)
        {
            Data = data;
        }

        public ErrorResponse(IEnumerable<string> errMsgs, T data) : base(errMsgs)
        {
            Data = data;
        }

        public ErrorResponse(Exception ex, T data) : base(ex)
        {
            Data = data;
        }


        public T Data { get; set; }
    }


    public class ErrorResponse : BaseResponse
    {
        public Dictionary<string, string[]> Errors { get; set; }

        public ErrorResponse(string errMsg)
        {
            Errors = new Dictionary<string, string[]>();

            Errors.Add("Message", new[] { errMsg });
            IsSuccessful = false;
        }

        public ErrorResponse(IEnumerable<string> errMsgs)
        {
            Errors = new Dictionary<string, string[]>();

            Errors.Add("Messages", errMsgs.ToArray());
            IsSuccessful = false;
        }

        public ErrorResponse(Exception ex)
        {
            Errors = new Dictionary<string, string[]>();

            Errors.Add("Message", new[] { ex.Message });

            if (ex.InnerException != null)
            {
                Errors.Add("InnerMessage", new[] { ex.InnerException.Message });
                string[] traces = ex.InnerException.StackTrace.Split("  ");
                Errors.Add("Traces", traces);
            }
            else
            {
                string[] traces = ex.StackTrace.Split("  ");
                Errors.Add("Traces", traces);
            }

            IsSuccessful = false;
        }
    }
}