using System;

namespace Nostreets.Extensions.DataControl.Classes
{

    public class Token : DBObject<string>
    {
        public string Name { get; set; }

        public string Value { get; set; }

        public DateTime ExpirationDate { get; set; }

        public TokenPurpose Purpose { get; set; }

        public bool IsValidated { get; set; }
    }

    public class TokenRequest
    {
        public string TokenId { get; set; }

        public string Code { get; set; }
    }


    public enum TokenPurpose
    {
        TwoFactorAuth,
        EmailValidtion,
        PhoneValidtion,
        PasswordReset
    }
}
