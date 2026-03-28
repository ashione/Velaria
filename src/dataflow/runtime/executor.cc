#include "src/dataflow/runtime/executor.h"

#include <sstream>
#include <unordered_map>

namespace dataflow {

namespace {

struct StringHash {
  size_t operator()(const std::string& s) const {
    return std::hash<std::string>()(s);
  }
};

}  // namespace

Table LocalExecutor::execute(const PlanNodePtr& plan) const {
  if (!plan) return Table();

  switch (plan->kind) {
    case PlanKind::Source: {
      const auto* source = static_cast<SourcePlan*>(plan.get());
      return source->table;
    }
    case PlanKind::Select: {
      const auto* sel = static_cast<SelectPlan*>(plan.get());
      Table in = execute(sel->child);
      Table out;
      for (size_t i : sel->indices) {
        out.schema.fields.push_back(in.schema.fields[i]);
      }
      for (size_t i = 0; i < in.rows.size(); ++i) {
        Row row;
        row.reserve(sel->indices.size());
        for (size_t col : sel->indices) {
          row.push_back(in.rows[i][col]);
        }
        out.rows.push_back(std::move(row));
      }
      for (size_t i = 0; i < out.schema.fields.size(); ++i) out.schema.index[out.schema.fields[i]] = i;
      return out;
    }
    case PlanKind::Filter: {
      const auto* fil = static_cast<FilterPlan*>(plan.get());
      Table in = execute(fil->child);
      Table out(in.schema, {});
      for (const auto& row : in.rows) {
        if (fil->pred(row[fil->column_index], fil->value)) {
          out.rows.push_back(row);
        }
      }
      return out;
    }
    case PlanKind::WithColumn: {
      const auto* wc = static_cast<WithColumnPlan*>(plan.get());
      Table in = execute(wc->child);
      Table out = in;
      out.schema.fields.push_back(wc->added_column);
      out.schema.index[wc->added_column] = out.schema.fields.size() - 1;
      for (auto& row : out.rows) {
        row.push_back(row[wc->source_column_index]);
      }
      return out;
    }
    case PlanKind::Drop: {
      const auto* drop = static_cast<DropPlan*>(plan.get());
      Table in = execute(drop->child);
      Table out;
      for (size_t i : drop->keep_indices) out.schema.fields.push_back(in.schema.fields[i]);
      for (size_t i = 0; i < in.rows.size(); ++i) {
        Row row;
        row.reserve(drop->keep_indices.size());
        for (size_t col : drop->keep_indices) {
          row.push_back(in.rows[i][col]);
        }
        out.rows.push_back(std::move(row));
      }
      for (size_t i = 0; i < out.schema.fields.size(); ++i) out.schema.index[out.schema.fields[i]] = i;
      return out;
    }
    case PlanKind::Limit: {
      const auto* lim = static_cast<LimitPlan*>(plan.get());
      Table in = execute(lim->child);
      Table out = in;
      if (out.rows.size() > lim->n) out.rows.resize(lim->n);
      return out;
    }
    case PlanKind::GroupBySum: {
      const auto* g = static_cast<GroupBySumPlan*>(plan.get());
      Table in = execute(g->child);
      Table out;
      for (size_t idx : g->keys) out.schema.fields.push_back(in.schema.fields[idx]);
      out.schema.fields.push_back("sum");
      for (size_t i = 0; i < out.schema.fields.size(); ++i) out.schema.index[out.schema.fields[i]] = i;

      std::unordered_map<std::string, double> agg;
      for (const auto& row : in.rows) {
        std::string key;
        for (size_t i = 0; i < g->keys.size(); ++i) {
          if (i > 0) key.push_back('\x1f');
          key += row[g->keys[i]].toString();
        }
        double v = row[g->value_index].asDouble();
        agg[key] += v;
      }

      for (const auto& kv : agg) {
        Row row;
        std::istringstream keys(kv.first);
        std::string part;
        while (std::getline(keys, part, '\x1f')) {
          row.push_back(Value(part));
        }
        row.push_back(Value(kv.second));
        out.rows.push_back(std::move(row));
      }
      return out;
    }
    case PlanKind::Join: {
      const auto* j = static_cast<JoinPlan*>(plan.get());
      Table left = execute(j->left);
      Table right = execute(j->right);
      Table out;
      out.schema.fields = left.schema.fields;
      out.schema.fields.insert(out.schema.fields.end(), right.schema.fields.begin(), right.schema.fields.end());
      for (size_t i = 0; i < out.schema.fields.size(); ++i) out.schema.index[out.schema.fields[i]] = i;

      std::unordered_map<std::string, std::vector<Row>> rightMap;
      for (const auto& row : right.rows) {
        rightMap[row[j->right_key].toString()].push_back(row);
      }

      for (const auto& lrow : left.rows) {
        auto it = rightMap.find(lrow[j->left_key].toString());
        if (it != rightMap.end()) {
          for (const auto& rrow : it->second) {
            Row merged = lrow;
            merged.insert(merged.end(), rrow.begin(), rrow.end());
            out.rows.push_back(std::move(merged));
          }
        } else if (j->kind == JoinKind::Left) {
          Row merged = lrow;
          for (size_t i = 0; i < right.schema.fields.size(); ++i) merged.push_back(Value());
          out.rows.push_back(std::move(merged));
        }
      }
      return out;
    }
    default:
      return Table();
  }
}

}  // namespace dataflow
