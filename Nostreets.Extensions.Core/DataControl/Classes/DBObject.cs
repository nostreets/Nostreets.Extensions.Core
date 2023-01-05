using System;
using System.ComponentModel.DataAnnotations;

namespace Nostreets.Extensions.DataControl.Classes
{
    public abstract partial class DBObject<T>
    {

        private DateTime _dateCreated = DateTime.Now;
        private DateTime _dateModified = DateTime.Now;

        [Key]
        public T Id { get; set; }

        public virtual string UserId { get; set; }

        public virtual DateTime? DateCreated { get => _dateCreated; set => _dateCreated = value.Value; }

        public virtual DateTime? DateModified { get => _dateModified; set => _dateModified = value.Value; }

        public virtual string ModifiedUserId { get; set; }

        public virtual bool IsDeleted { get; set; } = false;

    }

    public abstract class DBObject : DBObject<int>
    {

    }




}
