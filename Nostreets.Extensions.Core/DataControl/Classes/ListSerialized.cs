using Newtonsoft.Json;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http.Results;

namespace Nostreets.Extensions.Core.DataControl.Classes
{
    public class ListSerialized<T, T2>
    {
        public Func<T2, T> TConverter { get; set; }
        public Func<T, T2> T2Converter { get; set; }

        public string SerializedList
        {
            get
            {
                if (_serializedList == null && _list != null)
                {
                    if (typeof(T) != typeof(T2) && TConverter != null)
                    {
                        var convertedList = _list.Select(item => T2Converter(item)).ToList();
                        _serializedList = JsonConvert.SerializeObject(convertedList);
                    }
                    else
                    {
                        _serializedList = JsonConvert.SerializeObject(_list);
                    }
                }
                return _serializedList;
            }
            set
            {
                _serializedList = value;
                _list = GetDeserializedList();
            }
        }
        private string _serializedList;

        public List<T> List
        {
            get
            {
                if (_list != null)
                    return _list;
                else
                {
                    var result = new List<T>();
                    if (!string.IsNullOrEmpty(_serializedList))
                        result = GetDeserializedList();
                    _list = result; 
                    return _list;
                }
            }
            set
            {
                _list = value;
                _serializedList = JsonConvert.SerializeObject(_list);
            }
        }
        private List<T> _list = new List<T>();

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
                var list = JsonConvert.DeserializeObject<List<T2>>(_serializedList);
                foreach (var item in list)
                    result.Add(TConverter(item));
            }
            else
            {
                result = JsonConvert.DeserializeObject<List<T>>(_serializedList);
            }

            return result;
        }

        private void UpdateSerializedList()
        {
            _serializedList = null; // Clear the serialized list
            _serializedList = SerializedList; // Re-generate the serialized list
        }
    }
}
