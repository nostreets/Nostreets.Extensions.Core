using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nostreets.Extensions.Utilities
{
    public class DebuggerWriter : TextWriter
    {
        /// <summary>
        /// A description of the importance of the messages.
        /// </summary>
        private readonly int level;

        /// <summary>
        /// The category of the messages.
        /// </summary>
        private readonly string category;

        /// <summary>
        /// Encoding Type
        /// </summary>
        private static UnicodeEncoding encoding;

        /// <summary>
        /// Flag used to determine if the disposed method was called
        /// </summary>
        private bool isOpen;

        /// <summary>
        /// Initializes a new instance of the <see cref="DebuggerWriter"/> class.
        /// </summary>
        public DebuggerWriter()
            : this(0, Debugger.DefaultCategory)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DebuggerWriter"/> class with the specified level and category.
        /// </summary>
        /// <param name="level">A description of the importance of the messages.</param>
        /// <param name="category">The category of the messages.</param>
        public DebuggerWriter(int level, string category)
            : this(level, category, CultureInfo.CurrentCulture)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DebuggerWriter"/> class with the specified level, category and format provider.
        /// </summary>
        /// <param name="level">A description of the importance of the messages.</param>
        /// <param name="category">The category of the messages.</param>
        /// <param name="formatProvider">An <see cref="IFormatProvider"/> object that controls formatting.</param>
        public DebuggerWriter(int level, string category, IFormatProvider formatProvider)
            : base(formatProvider)
        {
            this.level = level;
            this.category = category;
            this.isOpen = true;
        }

        /// <summary>
        /// IDisposable.Dispose method
        /// </summary>
        /// <param name="disposing">determines if disposing managed code</param>
        protected override void Dispose(bool disposing)
        {
            this.isOpen = false;
            base.Dispose(disposing);
        }

        /// <summary>
        /// Writes a character to the debugger
        /// </summary>
        /// <param name="value">Character Value</param>
        public override void Write(char value)
        {
            if (!this.isOpen)
            {
                throw new ObjectDisposedException(null);
            }

            Debugger.Log(this.level, this.category, value.ToString());
        }

        /// <summary>
        /// Writes a string to the debugger
        /// </summary>
        /// <param name="value">String Value</param>
        public override void Write(string value)
        {
            if (!this.isOpen)
            {
                throw new ObjectDisposedException(null);
            }

            if (value != null)
            {
                Debugger.Log(this.level, this.category, value);
            }
        }

        /// <summary>
        /// Write character array to the debugger
        /// </summary>
        /// <param name="buffer">buffer value</param>
        /// <param name="index">index value</param>
        /// <param name="count">count value</param>
        public override void Write(char[] buffer, int index, int count)
        {
            if (!this.isOpen)
            {
                throw new ObjectDisposedException(null);
            }

            if (buffer == null || index < 0 || count < 0 || buffer.Length - index < count)
            {
                base.Write(buffer, index, count); // delegate throw exception to base class
            }

            Debugger.Log(this.level, this.category, new string(buffer, index, count));
        }

        /// <summary>
        /// Gets Encoding
        /// </summary>
        public override Encoding Encoding
        {
            get
            {
                if (encoding == null)
                {
                    encoding = new UnicodeEncoding(false, false);
                }

                return encoding;
            }
        }

        /// <summary>
        /// Gets Level
        /// </summary>
        public int Level
        {
            get { return this.level; }
        }

        /// <summary>
        /// Gets Category
        /// </summary>
        public string Category
        {
            get { return this.category; }
        }
    }
}
