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
    public class SerializedObject<T> : SerializedObject<T, string> where T : class { }

    public class SerializedObject<T, T2> where T : class
    {
        public Func<T2, T> TConverter { get; set; }
        public Func<T, T2> T2Converter { get; set; }

        public string Text
        {
            get
            {
                if (_text == null && _data != null)
                {
                    if (typeof(T) != typeof(T2) && T2Converter != null)
                    {
                        var convertedData = T2Converter(_data);
                        _text = JsonConvert.SerializeObject(convertedData);
                    }
                    else
                    {
                        _text = JsonConvert.SerializeObject(_data);
                    }
                }
                return _text;
            }
            set
            {
                _text = value;
                _data = GetDeserializedObject();
            }
        }
        private string _text;

        public T Data
        {
            get
            {
                if (_data != null)
                    return _data;
                else
                {
                    T result = default;
                    if (!string.IsNullOrEmpty(_text))
                        result = GetDeserializedObject();
                    _data = result;
                    return _data;
                }
            }
            set
            {
                _data = value;
                _text = JsonConvert.SerializeObject(_data);
            }
        }
        private T _data = default;

        private T GetDeserializedObject()
        {
            T result = default;

            if (typeof(T) != typeof(T2) && TConverter != null)
            {
                var data = JsonConvert.DeserializeObject<T2>(_text);
                result = TConverter(data);
            }
            else
            {
                result = JsonConvert.DeserializeObject<T>(_text);
            }

            return result;
        }

        private void UpdateSerializedObject()
        {
            _text = null; // Clear the serialized list
            _text = Text; // Re-generate the serialized list
        }
    }
}
