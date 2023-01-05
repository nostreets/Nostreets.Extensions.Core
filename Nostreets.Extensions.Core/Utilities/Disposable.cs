using System;

namespace Nostreets.Extensions.Utilities
{
    public abstract class Disposable : IDisposable
    {
        bool _disposed;
        void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    //dispose managed resources
                }
            }
            //dispose unmanaged resources
            _disposed = true;
        }

        public virtual void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
