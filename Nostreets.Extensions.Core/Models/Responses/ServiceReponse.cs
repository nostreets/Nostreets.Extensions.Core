using Nostreets.Extensions.Models.Responses;

namespace Nostreets.Extensions.Core.Models.Responses
{
    public class ServiceResponse 
    {
        internal ServiceResponse() { }

        public bool IsSuccessful { get; set; }
        public string TransactionId { get; set; } = Guid.NewGuid().ToString();
        public Dictionary<string, string[]> Errors { get; set; }


        public static ServiceResponse Error(string errMsg)
        {
            var result = new ServiceResponse();

            result.IsSuccessful = false;
            result.Errors = new Dictionary<string, string[]>
            {
                { "Message", new[] { errMsg } }
            };

            return result;
        }

        public static ServiceResponse Error(IEnumerable<string> errMsgs)
        {
            var result = new ServiceResponse();

            result.IsSuccessful = false;
            result.Errors = new Dictionary<string, string[]>
            {
                { "Messages", errMsgs.ToArray() }
            };

            return result;
        }

        public static ServiceResponse Error(Exception ex)
        {
            var result = new ServiceResponse();

            result.IsSuccessful = false;
            result.Errors = new Dictionary<string, string[]>
            {
                { "Message", new[] { ex.Message } }
            };

            if (ex.InnerException != null)
            {
                result.Errors.Add("InnerMessage", new[] { ex.InnerException.Message });
                string[] traces = ex.InnerException.StackTrace.Split("  ");
                result.Errors.Add("Traces", traces);
            }
            else
            {
                string[] traces = ex.StackTrace.Split("  ");
                result.Errors.Add("Traces", traces);
            }

            return result;
        }

        public static ServiceResponse Success()
        {
            var result = new ServiceResponse();
            result.IsSuccessful = true;
            return result;
        }
       
    }

    public class ServiceResponse<T> : ServiceResponse
    {
        internal ServiceResponse() { }

        public T Data { get; set; }

        public static ServiceResponse<T> Error(string errMsg)
        {
            var result = new ServiceResponse<T>();

            result.IsSuccessful = false;
            result.Errors = new Dictionary<string, string[]>
            {
                { "Message", new[] { errMsg } }
            };

            return result;
        }

        public static ServiceResponse<T> Error(IEnumerable<string> errMsgs)
        {
            var result = new ServiceResponse<T>();

            result.IsSuccessful = false;
            result.Errors = new Dictionary<string, string[]>
            {
                { "Messages", errMsgs.ToArray() }
            };

            return result;
        }

        public static ServiceResponse<T> Error(Exception ex)
        {
            var result = new ServiceResponse<T>();

            result.IsSuccessful = false;
            result.Errors = new Dictionary<string, string[]>
            {
                { "Message", new[] { ex.Message } }
            };

            if (ex.InnerException != null)
            {
                result.Errors.Add("InnerMessage", new[] { ex.InnerException.Message });
                string[] traces = ex.InnerException.StackTrace.Split("  ");
                result.Errors.Add("Traces", traces);
            }
            else
            {
                string[] traces = ex.StackTrace.Split("  ");
                result.Errors.Add("Traces", traces);
            }

            return result;
        }

        public static ServiceResponse<T> Success(T data) 
        {
            var result = new ServiceResponse<T>();

            result.IsSuccessful = true;
            result.Data = data;

            return result;
        }
    }
}
