using Newtonsoft.Json;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http.Results;

using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Nostreets.Extensions.Core.DataControl.Classes
{
    public class SerializedList<T> : SerializedList<T, string> { }

    public class SerializedList<T, T2> : IList<T>
    {
        public Func<T2, T> TConverter { get; set; }
        public Func<T, T2> T2Converter { get; set; }

        public string Text
        {
            get
            {
                if (_text == null && _list != null)
                {
                    if (typeof(T) != typeof(T2) && T2Converter != null)
                    {
                        var convertedList = _list.Select(item => T2Converter(item)).ToList();
                        _text = JsonConvert.SerializeObject(convertedList);
                    }
                    else
                    {
                        _text = JsonConvert.SerializeObject(_list);
                    }
                }
                return _text;
            }
            set
            {
                _text = value;
                _list = GetDeserializedList();
            }
        }
        private string _text;

        public List<T> List
        {
            get
            {
                if (_list != null)
                    return _list;
                else
                {
                    var result = new List<T>();
                    if (!string.IsNullOrEmpty(_text))
                        result = GetDeserializedList();
                    _list = result; 
                    return _list;
                }
            }
            set
            {
                _list = value;
                _text = JsonConvert.SerializeObject(_list);
            }
        }
        private List<T> _list = new List<T>();

        public int Count => List.Count;

        public bool IsReadOnly => false;

        public T this[int index]
        {
            get => List[index];
            set
            {
                List[index] = value;
                UpdateSerializedList();
            }
        }

        public int IndexOf(T item)
        {
            return List.IndexOf(item);
        }

        public bool Contains(T item)
        {
            return List.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            List.CopyTo(array, arrayIndex);
            UpdateSerializedList();
        }

        public IEnumerator<T> GetEnumerator()
        {
            return List.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return List.GetEnumerator();
        }

        public bool All(Func<T, bool> predicate) 
        {
            return List.All(predicate);
        }

        public IEnumerable<T> Where(Func<T, bool> predicate)
        {
            return List.Where(predicate);
        }

        public int FindIndex(Predicate<T> match)
        {
            var size = List.Count;
            return FindIndex(0, size, match);
        }
        
        public int FindIndex(int startIndex, Predicate<T> match)
        {
            var size = List.Count;
            return FindIndex(startIndex, size - startIndex, match);
        }

        public int FindIndex(int startIndex, int count, Predicate<T> match)
        {
            return List.FindIndex(startIndex, count, match);
        }

        public bool Any(Func<T, bool> predicate)
        {
            return List.Any(predicate);
        }

        public T First(Func<T, bool> predicate)
        {
            return List.First(predicate);
        }

        public T FirstOrDefault(Func<T, bool> predicate)
        {
            return List.FirstOrDefault(predicate);
        }

        public void Add(T item)
        {
            List.Add(item);
            UpdateSerializedList();
        }

        public bool Remove(T item)
        {
            bool result = List.Remove(item);
            UpdateSerializedList();
            return result;
        }

        public void Clear()
        {
            List.Clear();
            UpdateSerializedList();
        }

        public void Insert(int index, T item)
        {
            List.Insert(index, item);
            UpdateSerializedList();
        }

        public void RemoveAt(int index)
        {
            List.RemoveAt(index);
            UpdateSerializedList();
        }

        public void Set(int index, T item)
        {
            if (index >= 0 && index < List.Count)
            {
                List[index] = item;
                UpdateSerializedList();
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");
            }
        }

        private List<T> GetDeserializedList()
        {
            var result = new List<T>();

            if (typeof(T) != typeof(T2) && TConverter != null)
            {
                var list = JsonConvert.DeserializeObject<List<T2>>(_text);
                foreach (var item in list)
                    result.Add(TConverter(item));
            }
            else
            {
                result = JsonConvert.DeserializeObject<List<T>>(_text);
            }

            return result;
        }

        private void UpdateSerializedList()
        {
            _text = null; // Clear the serialized list
            _text = Text; // Re-generate the serialized list
        }
    }
}
