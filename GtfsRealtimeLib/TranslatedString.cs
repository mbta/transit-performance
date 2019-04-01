using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GtfsRealtimeLib
{

    [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"TranslatedString")]
    public partial class TranslatedString : global::ProtoBuf.IExtensible
    {
        public TranslatedString() { }

        private readonly global::System.Collections.Generic.List<TranslatedString.Translation> _translation = new global::System.Collections.Generic.List<TranslatedString.Translation>();
        [global::ProtoBuf.ProtoMember(1, Name = @"translation", DataFormat = global::ProtoBuf.DataFormat.Default)]
        public global::System.Collections.Generic.List<TranslatedString.Translation> translation
        {
            get { return _translation; }
        }

        [global::System.Serializable, global::ProtoBuf.ProtoContract(Name = @"Translation")]
        public partial class Translation : global::ProtoBuf.IExtensible
        {
            public Translation() { }

            private string _text;
            [global::ProtoBuf.ProtoMember(1, IsRequired = true, Name = @"text", DataFormat = global::ProtoBuf.DataFormat.Default)]
            public string text
            {
                get { return _text; }
                set { _text = value; }
            }
            private string _language = "";
            [global::ProtoBuf.ProtoMember(2, IsRequired = false, Name = @"language", DataFormat = global::ProtoBuf.DataFormat.Default)]
            [global::System.ComponentModel.DefaultValue("")]
            public string language
            {
                get { return _language; }
                set { _language = value; }
            }
            private global::ProtoBuf.IExtension extensionObject;
            global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
            { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
        }

        private global::ProtoBuf.IExtension extensionObject;
        global::ProtoBuf.IExtension global::ProtoBuf.IExtensible.GetExtensionObject(bool createIfMissing)
        { return global::ProtoBuf.Extensible.GetExtensionObject(ref extensionObject, createIfMissing); }
    }
}
