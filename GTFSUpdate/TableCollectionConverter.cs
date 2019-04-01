using Newtonsoft.Json;
using System;

namespace GTFS
{
    public class TableCollectionConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(GTFSTableCollection);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var tableCollection = new GTFSTableCollection();
            while (reader.Read())
            {
                switch (reader.TokenType)
                {
                    case JsonToken.EndObject:
                        return tableCollection;
                    case JsonToken.PropertyName:
                        var tableName = (string)reader.Value;
                        reader.Read();
                        var table = serializer.Deserialize<GTFSTable>(reader);
                        table.name = tableName;
                        tableCollection.Add(table);
                        break;

                }
            }
            return tableCollection;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var tableCollection = value as GTFSTableCollection;
            writer.WriteStartObject();

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
