using System;
using System.ComponentModel.DataAnnotations;

namespace Nostreets.Extensions.DataControl.Classes
{
    public abstract partial class UserDBObject : DBObject 
    { 
        public virtual string UserId { get; set; }
        
        public virtual string ModifiedUserId { get; set; }
    }

    public abstract partial class DBObject<T>
    {
        [Key]
        public virtual T Id { get; set; }

        public virtual DateTime? DateCreated { get; set; } = DateTime.Now;

        public virtual DateTime? DateModified { get; set; } = DateTime.Now;

        public virtual bool IsArchived { get; set; } = false;
    }

    public abstract class DBObject : DBObject<string>
    {
        public override string Id { get; set; } = Guid.NewGuid().ToString();
    }
}
