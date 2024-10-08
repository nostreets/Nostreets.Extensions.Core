﻿using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Nostreets.Extensions.DataControl.Classes
{
    public abstract partial class DBObject<T>
    {
        [Key]
        [Column(Order = 1)]
        public virtual T Id { get; set; }

        public virtual DateTime DateCreated { get; set; } = DateTime.Now;

        public virtual string? CreatedById { get; set; }

        public virtual string? CreatedBy { get; set; }

        public virtual DateTime DateModified { get; set; } = DateTime.Now;

        public virtual string? ModifiedBy { get; set; }

        public virtual string? ModifiedById { get; set; }

        public virtual bool IsArchived { get; set; } = false;
    }

    public abstract class DBObject : DBObject<string>
    {
        public override string Id { get; set; } = Guid.NewGuid().ToString();
    }
}
