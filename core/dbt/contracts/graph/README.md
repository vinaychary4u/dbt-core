# nodes.proto messages

For new fields in a node or node config to be included in the protobuf serialized output,
the messages in nodes.proto need to be updated.

Then proto.nodes need to be compiled: ```protoc --python_betterproto_out . nodes.proto```

In order to use optional fields (really necessary for nodes and configs) we had to use
a beta version of betterproto. This version has a bug in the way that it writes the
names of some generated classes, so we will have to update the name of the rpc node from
RpcNode to RPCNode in the generated file.

In addition, betterproto now always creates the generated python file as an __init__.py
file in a subdirectory. For now, I'm moving it up from proto_nodes/__init__.py to proto_nodes.py.

# updating nodes.py and model_config.py for nodes.proto changes

Protobuf python messages objects are created to "to_msg" methods. There is often a list
of attributes to set in a "msg_attributes" method, but this isn't entirely consistent.
If a class has a small number of additional attributes they are sometimes set directly.
Some attributes aren't handled well by the "get_msg_attribute_value" utility function,
in which case they are set directly. This is particularly true of lists or dictionaries
of objects, which need to be converted using a "to_msg" method.

The utility class "get_msg_attribute_value" does a couple of common conversions of
attribute values, such as getting the string value of an enum or converting dictionaries to
dictionaries of strings. A few common more elaborate conversions are also performed, such as
"columns".
