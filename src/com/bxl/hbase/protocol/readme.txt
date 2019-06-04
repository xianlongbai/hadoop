#生成一个java结构的文件包
#Protocol Buffers 是一种轻便高效的结构化数据存储格式，可以用于结构化数据串行化，或者说序列化。
#它很适合做数据存储或 RPC 数据交换格式。可用于通讯协议、数据存储等领域的语言无关、平台无关、可扩展的序列化结构数据格式。
#目前提供了 C++、Java、Python 三种语言的 API。
-proto_path=IMPORT_PATH 可以使用 -I=IMPORT_PATH 来简写

protoc -I=$SRC_DIR –java_out =$DST_DIR $SRC_DIR/addressbook.proto

例：
protoc -I=/user/bxl/ -–java_out =/user/bxl/outpath /user/bxl/addressbook.proto