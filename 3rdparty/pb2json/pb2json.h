/*
 * Original code from https://github.com/renenglish/pb2json
 */
#ifndef __PB2JSON_H_
#define __PB2JSON_H_

#include <string>

namespace google { namespace protobuf {
class Message;
class Reflection;
class FieldDescriptor;
}}

struct json_t;

class  Pb2Json {
 public:
  char *pb2json(const google::protobuf::Message &msg);
  char *pb2json(google::protobuf::Message *msg,const char *buf,int len);
 private:
  json_t *parse_msg(const google::protobuf::Message *msg);
  json_t *parse_repeated_field(const google::protobuf::Message *msg,
			       const google::protobuf::Reflection * ref,
			       const google::protobuf::FieldDescriptor *field);
  std::string hex_encode(const std::string& input);
};

#endif

