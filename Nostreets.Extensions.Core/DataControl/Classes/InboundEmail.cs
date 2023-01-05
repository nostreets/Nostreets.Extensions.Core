namespace Nostreets.Extensions.DataControl.Classes
{
    public class InboundEmail 
    {
        public string AttachmentCount { get; set; }
        public string BodyHtml { get; set; }
        public string BodyPlain { get; set; }
        public string FromEmail { get; set; }
        public string Recipient { get; set; }
        public string RecipientEmail { get; set; }
        public string Sender { get; set; }
        public string StrippedHtml { get; set; }
        public string StrippedSignature { get; set; }
        public string StrippedText { get; set; }
        public string Subject { get; set; }
        public string TimeStamp { get; set; }
    }
}