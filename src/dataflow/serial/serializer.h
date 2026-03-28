#pragma once

#include <memory>
#include <string>

#include "src/dataflow/core/table.h"

namespace dataflow {

enum class SerializationKind {
  ProtoLike = 0,
  ArrowLike = 1,
};

class ISerializer {
 public:
  virtual ~ISerializer() = default;
  virtual std::string name() const = 0;
  virtual std::string serialize(const Table& table) const = 0;
  virtual Table deserialize(const std::string& payload) const = 0;
};

class ProtoLikeSerializer : public ISerializer {
 public:
  std::string name() const override;
  std::string serialize(const Table& table) const override;
  Table deserialize(const std::string& payload) const override;
};

class ArrowLikeSerializer : public ISerializer {
 public:
  std::string name() const override;
  std::string serialize(const Table& table) const override;
  Table deserialize(const std::string& payload) const override;
};

std::unique_ptr<ISerializer> makeSerializer(SerializationKind kind);

}  // namespace dataflow
