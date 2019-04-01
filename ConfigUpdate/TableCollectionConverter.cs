using Newtonsoft.Json;
using System;

namespace ConfigUpdate
{
    public class TableCollectionConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(ConfigTableCollection);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var tableCollection = new ConfigTableCollection();
            while (reader.Read())
            {
                switch (reader.TokenType)
                {
                    case JsonToken.EndObject:
                        return tableCollection;

                    case JsonToken.PropertyName:
                        var tableName = (string)reader.Value;
                        reader.Read();
                        var table = serializer.Deserialize<ConfigTable>(reader);
                        table.name = tableName;
                        tableCollection.Add(table);
                        break;
                }
            }

            return tableCollection;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var tableCollection = value as ConfigTableCollection;
            writer.WriteStartObject();

            if (tableCollection != null)
                foreach (var table in tableCollection)
                {
                    writer.WritePropertyName(table.name);
                    writer.WriteStartObject();
                    writer.WritePropertyName("columns");
                    serializer.Serialize(writer, table.columns);
                    if (!table.required)
                    {
                        writer.WritePropertyName("required");
                        writer.WriteValue(table.required);
                    }
                    writer.WriteEndObject();
                }
            writer.WriteEndObject();
        }
    }
}
