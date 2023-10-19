using Newtonsoft.Json;
using System;
using System.Collections.Generic;

namespace Nostreets.Extensions.Core.Helpers.Converter
{
    public class GenericDictionaryConverter<T, T2> : JsonConverter<Dictionary<T, T2>>
    {
        public override bool CanRead => true;
        public override bool CanWrite => true;

        public override Dictionary<T, T2> ReadJson(JsonReader reader, Type objectType, Dictionary<T, T2> existingValue, bool hasExistingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null)
                return null;

            if (reader.TokenType == JsonToken.StartObject)
            {
                var dictionary = new Dictionary<T, T2>();
                serializer.Populate(reader, dictionary);
                return dictionary;
            }

            throw new JsonSerializationException($"Unexpected token type: {reader.TokenType}");
        }

        public override void WriteJson(JsonWriter writer, Dictionary<T, T2> value, JsonSerializer serializer)
        {
            writer.WriteStartObject();
            foreach (var kvp in value)
            {
                // Write key
                writer.WritePropertyName(kvp.Key.ToString());

                // Serialize value
                serializer.Serialize(writer, kvp.Value);
            }
            writer.WriteEndObject();
        }
    }
}
