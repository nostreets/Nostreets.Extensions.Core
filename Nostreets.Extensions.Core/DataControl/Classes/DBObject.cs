using Nostreets.Extensions.Core.Interfaces;
using Nostreets.Extensions.Extend.Basic;

using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Nostreets.Extensions.DataControl.Classes
{
    public abstract partial class DBObject<T> : IDBObject<T>
    {
        [Key]
        [Column(Order = 1)]
        public virtual T Id { get; set; }

        // UTC, never DateTime.Now. Every stored instant is UTC and is converted only for display
        // (operator ruling, 2026-09-25). This read Now for years and looked correct because the
        // Azure hosts run UTC, so the two were equal there - but a row written by a host in any
        // other zone (a local dev run) landed hours off, in the same column, with nothing to say
        // which kind it was. A mixed-kind column cannot be repaired by a reader.
        public virtual DateTime DateCreated { get; set; } = DateTime.UtcNow;

        public virtual string? CreatedById { get; set; }

        public virtual string? CreatedBy { get; set; }

        public virtual DateTime DateModified { get; set; } = DateTime.UtcNow;

        public virtual string? ModifiedBy { get; set; }

        public virtual string? ModifiedById { get; set; }

        public virtual bool IsArchived { get; set; } = false;
    }

    public abstract class DBObject : DBObject<string>, IDBObject
    {
        public override string Id { get; set; } = Guid.NewGuid().ToString();
    }
}
