
using Newtonsoft.Json;
using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace Nostreets.Extensions.Core.DataControl.Classes
{
    public class SerializedDictionary<TKey, TValue> : SerializedDictionary<TKey, TValue, string> { }

    public class SerializedDictionary<TKey, TValue, T2Value> : IDictionary<TKey, TValue>, IDictionary
    {
        public Func<T2Value, TValue> TConverter { get; set; }
        public Func<TValue, T2Value> T2Converter { get; set; }

        public string Text
        {
            get
            {
                if (_text == null && _dictionary != null)
                {
                    if (typeof(TValue) != typeof(T2Value) && T2Converter != null)
                    {
                        var convertedList = _dictionary.Select(item => T2Converter(item.Value)).ToList();
                        _text = JsonConvert.SerializeObject(convertedList);
                    }
                    else
                    {
                        _text = JsonConvert.SerializeObject(_dictionary);
                    }
                }
                return _text;
            }
            set
            {
                _text = value;
                _dictionary = GetDeserializedDictionary();
            }
        }
        private string _text;

        public Dictionary<TKey, TValue> Dictionary
        {
            get
            {
                if (_dictionary != null)
                    return _dictionary;
                else
                {
                    var result = new Dictionary<TKey, TValue>();
                    if (!string.IsNullOrEmpty(_text))
                        result = GetDeserializedDictionary();
                    _dictionary = result; 
                    return _dictionary;
                }
            }
            set
            {
                _dictionary = value;
                _text = JsonConvert.SerializeObject(_dictionary);
            }
        }
        private Dictionary<TKey, TValue> _dictionary = new Dictionary<TKey, TValue>();

        public ICollection<TKey> Keys => Dictionary.Keys;

        public ICollection<TValue> Values => Dictionary.Values;

        public int Count => Dictionary.Count;

        public bool IsReadOnly => false;

        public bool IsFixedSize => false;

        ICollection IDictionary.Keys => Dictionary.Keys;

        ICollection IDictionary.Values => Dictionary.Values;

        public bool IsSynchronized => false;

        public object SyncRoot => this;

        public object? this[object key]
        {
            get => Dictionary[(TKey)key];
            set
            {
                Dictionary[(TKey)key] = (TValue)value;
                UpdateSerializedDictionary();
            }
        }

        public TValue this[TKey key]
        {
            get => Dictionary[key];
            set
            {
                Dictionary[key] = value;
                UpdateSerializedDictionary();
            }
        }

        public void Add(TKey key, TValue value)
        {
            Dictionary.Add(key, value);
            UpdateSerializedDictionary();
        }

        public void Add(object key, object? value)
        {
            Dictionary.Add((TKey)key, (TValue)value);
            UpdateSerializedDictionary();
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Dictionary.Add(item.Key, item.Value);
            UpdateSerializedDictionary();
        }

        public bool ContainsKey(TKey key)
        {
            return Dictionary.ContainsKey(key);
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return Dictionary.Contains(item);
        }

        public bool Contains(object key)
        {
            return Dictionary.ContainsKey((TKey)key);
        }

        public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
        {
            var result = Dictionary.TryGetValue(key, out value);
            return result;
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            if (array == null) 
                throw new ArgumentNullException(nameof(array));

            if (arrayIndex < 0 || arrayIndex >= array.Length) 
                throw new ArgumentOutOfRangeException(nameof(arrayIndex));

            if (array.Length - arrayIndex < Count)
                throw new ArgumentException("The destination array is not long enough to copy all the items in the collection.");

            foreach (var kvp in _dictionary)
            {
                array[arrayIndex++] = kvp;
            }

            UpdateSerializedDictionary();
        }

        public void CopyTo(Array array, int index)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            if (index < 0 || index >= array.Length)
                throw new ArgumentOutOfRangeException(nameof(index));

            if (array.Length - index < Count)
                throw new ArgumentException("The destination array is not long enough to copy all the items in the collection.");

            // Ensure that the array is of the correct type
            if (array is KeyValuePair<TKey, TValue>[] kvpArray)
            {
                foreach (var kvp in _dictionary)
                {
                    kvpArray[index++] = kvp;
                }
            }
            else
            {
                throw new ArgumentException("Array type is not compatible with KeyValuePair<TKey, TValue>.");
            }
        }

        public bool Remove(TKey key)
        {
            var result = Dictionary.Remove(key);
            UpdateSerializedDictionary();
            return result;
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            var result = Dictionary.Remove(item.Key);
            UpdateSerializedDictionary();
            return result;
        }

        public void Remove(object key)
        {
            var result = Dictionary.Remove((TKey)key);
            UpdateSerializedDictionary();
        }

        public void Clear()
        {
            Dictionary.Clear();
            UpdateSerializedDictionary();
        }

        IDictionaryEnumerator IDictionary.GetEnumerator()
        {
            return Dictionary.GetEnumerator();
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return Dictionary.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return Dictionary.GetEnumerator();
        }

        private Dictionary<TKey, TValue> GetDeserializedDictionary()
        {
            var result = new Dictionary<TKey, TValue>();

            if (typeof(TValue) != typeof(T2Value) && TConverter != null)
            {
                var list = JsonConvert.DeserializeObject<Dictionary<TKey, T2Value>>(_text);
                foreach (var item in list)
                    result.Add(item.Key, TConverter(item.Value));
            }
            else
            {
                result = JsonConvert.DeserializeObject<Dictionary<TKey, TValue>>(_text);
            }

            return result;
        }

        private void UpdateSerializedDictionary()
        {
            _text = null; // Clear the serialized list
            _text = Text; // Re-generate the serialized list
        }

    }
}
