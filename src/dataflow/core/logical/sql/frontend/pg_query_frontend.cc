#include "src/dataflow/core/logical/sql/frontend/pg_query_frontend.h"
#include "src/dataflow/core/logical/sql/frontend/pg_ast_lowerer.h"
#include "src/dataflow/core/logical/sql/frontend/pg_query_raii.h"
#include "src/dataflow/core/logical/sql/frontend/sql_feature_validator.h"

extern "C" {
#include "pg_query.h"
}

#include <algorithm>
#include <cctype>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace dataflow {
namespace sql {

struct PgQueryFrontend::Impl { SqlFeaturePolicy policy; };

namespace {

struct ExtensionToken {
  std::string text;
  std::string value;
  std::size_t start = 0;
  std::size_t end = 0;
  int depth = 0;
  bool quoted_identifier = false;
  bool string_literal = false;
  bool number = false;
};

struct Edit {
  std::size_t start = 0;
  std::size_t end = 0;
  std::string replacement;
};

struct ExtensionPreprocessResult {
  std::string normalized_sql;
  VelariaSqlExtensions extensions;
  std::vector<SqlDiagnostic> diagnostics;
};

std::string toUpperAscii(std::string value) {
  for (auto& ch : value) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return value;
}

bool isIdentifierStart(unsigned char c) {
  return std::isalpha(c) || c == '_' || c >= 0x80;
}

bool isIdentifierPart(unsigned char c) {
  return std::isalnum(c) || c == '_' || c == '$' || c >= 0x80;
}

std::string unescapeSingleQuoted(std::string_view text) {
  std::string out;
  if (text.size() < 2) return out;
  for (std::size_t i = 1; i + 1 < text.size(); ++i) {
    if (text[i] == '\'' && i + 1 < text.size() - 1 && text[i + 1] == '\'') {
      out.push_back('\'');
      ++i;
    } else {
      out.push_back(text[i]);
    }
  }
  return out;
}

std::string unescapeDoubleQuoted(std::string_view text) {
  std::string out;
  if (text.size() < 2) return out;
  for (std::size_t i = 1; i + 1 < text.size(); ++i) {
    if (text[i] == '"' && i + 1 < text.size() - 1 && text[i + 1] == '"') {
      out.push_back('"');
      ++i;
    } else {
      out.push_back(text[i]);
    }
  }
  return out;
}

std::vector<ExtensionToken> tokenizeExtensions(std::string_view sql) {
  std::vector<ExtensionToken> tokens;
  int depth = 0;
  std::size_t i = 0;
  while (i < sql.size()) {
    const unsigned char ch = static_cast<unsigned char>(sql[i]);
    if (std::isspace(ch)) {
      ++i;
      continue;
    }
    ExtensionToken tok;
    tok.start = i;
    tok.depth = depth;
    if (sql[i] == '\'') {
      ++i;
      while (i < sql.size()) {
        if (sql[i] == '\'' && i + 1 < sql.size() && sql[i + 1] == '\'') {
          i += 2;
          continue;
        }
        if (sql[i] == '\'') {
          ++i;
          break;
        }
        ++i;
      }
      tok.end = i;
      tok.text = std::string(sql.substr(tok.start, tok.end - tok.start));
      tok.value = unescapeSingleQuoted(tok.text);
      tok.string_literal = true;
      tokens.push_back(std::move(tok));
      continue;
    }
    if (sql[i] == '"') {
      ++i;
      while (i < sql.size()) {
        if (sql[i] == '"' && i + 1 < sql.size() && sql[i + 1] == '"') {
          i += 2;
          continue;
        }
        if (sql[i] == '"') {
          ++i;
          break;
        }
        ++i;
      }
      tok.end = i;
      tok.text = std::string(sql.substr(tok.start, tok.end - tok.start));
      tok.value = unescapeDoubleQuoted(tok.text);
      tok.quoted_identifier = true;
      tokens.push_back(std::move(tok));
      continue;
    }
    if (isIdentifierStart(ch)) {
      ++i;
      while (i < sql.size() && isIdentifierPart(static_cast<unsigned char>(sql[i]))) ++i;
      tok.end = i;
      tok.text = std::string(sql.substr(tok.start, tok.end - tok.start));
      tok.value = tok.text;
      tokens.push_back(std::move(tok));
      continue;
    }
    if (std::isdigit(ch) || (sql[i] == '.' && i + 1 < sql.size() &&
                             std::isdigit(static_cast<unsigned char>(sql[i + 1])))) {
      bool dot_seen = sql[i] == '.';
      ++i;
      while (i < sql.size()) {
        const unsigned char c = static_cast<unsigned char>(sql[i]);
        if (std::isdigit(c)) {
          ++i;
        } else if (sql[i] == '.' && !dot_seen) {
          dot_seen = true;
          ++i;
        } else {
          break;
        }
      }
      tok.end = i;
      tok.text = std::string(sql.substr(tok.start, tok.end - tok.start));
      tok.value = tok.text;
      tok.number = true;
      tokens.push_back(std::move(tok));
      continue;
    }
    tok.text = std::string(1, sql[i]);
    tok.value = tok.text;
    tok.end = i + 1;
    if (sql[i] == '(') {
      tokens.push_back(std::move(tok));
      ++depth;
      ++i;
      continue;
    }
    if (sql[i] == ')') {
      tok.depth = std::max(0, depth - 1);
      depth = std::max(0, depth - 1);
      tokens.push_back(std::move(tok));
      ++i;
      continue;
    }
    tokens.push_back(std::move(tok));
    ++i;
  }
  return tokens;
}

bool isWord(const ExtensionToken& tok, std::string_view word) {
  return !tok.quoted_identifier && !tok.string_literal && toUpperAscii(tok.text) == word;
}

bool isSymbol(const ExtensionToken& tok, std::string_view symbol) {
  return !tok.quoted_identifier && !tok.string_literal && tok.text == symbol;
}

SqlDiagnostic makePreprocessDiag(const std::string& type, const std::string& message,
                                 const std::string& hint = "") {
  SqlDiagnostic d;
  d.phase = SqlDiagnostic::Phase::Parse;
  d.error_type = type;
  d.message = message;
  d.hint = hint;
  return d;
}

ColumnRef parseExtensionColumn(const std::vector<ExtensionToken>& tokens,
                               std::size_t* index) {
  ColumnRef ref;
  if (!index || *index >= tokens.size()) return ref;
  ref.name = tokens[*index].value;
  ++(*index);
  if (*index + 1 < tokens.size() && isSymbol(tokens[*index], ".")) {
    ref.qualifier = ref.name;
    ++(*index);
    ref.name = tokens[*index].value;
    ++(*index);
  }
  return ref;
}

std::optional<std::size_t> parseSizeLiteral(const ExtensionToken& tok) {
  if (!tok.number) return std::nullopt;
  try {
    return static_cast<std::size_t>(std::stoull(tok.value));
  } catch (...) {
    return std::nullopt;
  }
}

std::optional<uint64_t> parseUint64Literal(const ExtensionToken& tok) {
  if (!tok.number) return std::nullopt;
  try {
    return static_cast<uint64_t>(std::stoull(tok.value));
  } catch (...) {
    return std::nullopt;
  }
}

bool isExtensionClauseStart(const std::vector<ExtensionToken>& tokens, std::size_t i) {
  if (i >= tokens.size() || tokens[i].depth != 0) return false;
  if (isWord(tokens[i], "WINDOW")) return i + 1 < tokens.size() && isWord(tokens[i + 1], "BY");
  if (isWord(tokens[i], "KEYWORD")) return i + 1 < tokens.size() && isWord(tokens[i + 1], "SEARCH");
  if (isWord(tokens[i], "HYBRID")) return i + 1 < tokens.size() && isWord(tokens[i + 1], "SEARCH");
  return false;
}

bool isClauseBoundary(const std::vector<ExtensionToken>& tokens, std::size_t i) {
  if (i >= tokens.size()) return true;
  if (tokens[i].depth != 0) return false;
  if (isExtensionClauseStart(tokens, i)) return true;
  if (isWord(tokens[i], "GROUP") || isWord(tokens[i], "HAVING") ||
      isWord(tokens[i], "ORDER") || isWord(tokens[i], "LIMIT") ||
      isWord(tokens[i], "UNION") || isWord(tokens[i], "INTERSECT") ||
      isWord(tokens[i], "EXCEPT")) {
    return true;
  }
  return isSymbol(tokens[i], ";");
}

std::size_t findClauseEnd(const std::vector<ExtensionToken>& tokens, std::size_t begin) {
  std::size_t i = begin + 1;
  while (i < tokens.size() && !isClauseBoundary(tokens, i)) ++i;
  return i;
}

void addRemoveEdit(const std::vector<ExtensionToken>& tokens, std::size_t begin,
                   std::size_t end, std::vector<Edit>* edits) {
  if (!edits || begin >= tokens.size() || end <= begin) return;
  edits->push_back(Edit{tokens[begin].start, tokens[end - 1].end, ""});
}

bool parseKeywordExtension(const std::vector<ExtensionToken>& tokens, std::size_t begin,
                           std::size_t end, VelariaSqlExtensions* extensions,
                           std::vector<SqlDiagnostic>* diagnostics) {
  std::size_t i = begin;
  KeywordSearchSpec spec;
  if (i + 2 >= end || !isWord(tokens[i], "KEYWORD") || !isWord(tokens[i + 1], "SEARCH") ||
      !isSymbol(tokens[i + 2], "(")) {
    diagnostics->push_back(makePreprocessDiag(
        "parse_error", "Invalid KEYWORD SEARCH clause.",
        "Use KEYWORD SEARCH(col, ...) QUERY 'text' TOP_K n."));
    return false;
  }
  i += 3;
  while (i < end && !isSymbol(tokens[i], ")")) {
    spec.columns.push_back(parseExtensionColumn(tokens, &i));
    if (i < end && isSymbol(tokens[i], ",")) ++i;
  }
  if (i >= end || !isSymbol(tokens[i], ")")) {
    diagnostics->push_back(makePreprocessDiag("parse_error",
        "KEYWORD SEARCH column list must end with ')'."));
    return false;
  }
  ++i;
  if (i + 1 >= end || !isWord(tokens[i], "QUERY") || !tokens[i + 1].string_literal) {
    diagnostics->push_back(makePreprocessDiag("parse_error",
        "KEYWORD SEARCH QUERY must be a quoted string literal."));
    return false;
  }
  spec.query_text = tokens[i + 1].value;
  i += 2;
  while (i < end) {
    if (isWord(tokens[i], "TOP_K") && i + 1 < end) {
      auto top_k = parseSizeLiteral(tokens[i + 1]);
      if (!top_k.has_value()) {
        diagnostics->push_back(makePreprocessDiag("parse_error",
            "KEYWORD SEARCH TOP_K must be numeric."));
        return false;
      }
      spec.top_k = *top_k;
      i += 2;
      continue;
    }
    diagnostics->push_back(makePreprocessDiag("parse_error",
        "Unexpected token in KEYWORD SEARCH clause: " + tokens[i].text));
    return false;
  }
  extensions->keyword_search = std::move(spec);
  return true;
}

bool parseHybridExtension(const std::vector<ExtensionToken>& tokens, std::size_t begin,
                          std::size_t end, VelariaSqlExtensions* extensions,
                          std::vector<SqlDiagnostic>* diagnostics) {
  std::size_t i = begin;
  HybridSearchSpec spec;
  if (i + 4 > end || !isWord(tokens[i], "HYBRID") || !isWord(tokens[i + 1], "SEARCH")) {
    diagnostics->push_back(makePreprocessDiag(
        "parse_error", "Invalid HYBRID SEARCH clause.",
        "Use HYBRID SEARCH col QUERY 'vector' [METRIC m] [TOP_K n]."));
    return false;
  }
  i += 2;
  spec.vector_column = parseExtensionColumn(tokens, &i);
  if (i + 1 >= end || !isWord(tokens[i], "QUERY") || !tokens[i + 1].string_literal) {
    diagnostics->push_back(makePreprocessDiag("parse_error",
        "HYBRID SEARCH QUERY must be a quoted vector literal."));
    return false;
  }
  spec.query_vector = tokens[i + 1].value;
  i += 2;
  while (i < end) {
    if (isWord(tokens[i], "METRIC") && i + 1 < end) {
      spec.metric = tokens[i + 1].value;
      i += 2;
      continue;
    }
    if (isWord(tokens[i], "TOP_K") && i + 1 < end) {
      auto top_k = parseSizeLiteral(tokens[i + 1]);
      if (!top_k.has_value()) {
        diagnostics->push_back(makePreprocessDiag("parse_error",
            "HYBRID SEARCH TOP_K must be numeric."));
        return false;
      }
      spec.top_k = *top_k;
      i += 2;
      continue;
    }
    if (isWord(tokens[i], "SCORE_THRESHOLD") && i + 1 < end) {
      if (!tokens[i + 1].number) {
        diagnostics->push_back(makePreprocessDiag("parse_error",
            "HYBRID SEARCH SCORE_THRESHOLD must be numeric."));
        return false;
      }
      try {
        spec.score_threshold = std::stod(tokens[i + 1].value);
      } catch (...) {
        diagnostics->push_back(makePreprocessDiag("parse_error",
            "Invalid HYBRID SEARCH SCORE_THRESHOLD: " + tokens[i + 1].value));
        return false;
      }
      i += 2;
      continue;
    }
    diagnostics->push_back(makePreprocessDiag("parse_error",
        "Unexpected token in HYBRID SEARCH clause: " + tokens[i].text));
    return false;
  }
  extensions->hybrid_search = std::move(spec);
  return true;
}

bool parseWindowExtension(const std::vector<ExtensionToken>& tokens, std::size_t begin,
                          std::size_t end, VelariaSqlExtensions* extensions,
                          std::vector<SqlDiagnostic>* diagnostics) {
  std::size_t i = begin;
  WindowSpec spec;
  if (i + 6 > end || !isWord(tokens[i], "WINDOW") || !isWord(tokens[i + 1], "BY")) {
    diagnostics->push_back(makePreprocessDiag(
        "parse_error", "Invalid WINDOW BY clause.",
        "Use WINDOW BY time_col EVERY window_ms [SLIDE slide_ms] AS output_col."));
    return false;
  }
  i += 2;
  spec.time_column = parseExtensionColumn(tokens, &i);
  if (i + 1 >= end || !isWord(tokens[i], "EVERY")) {
    diagnostics->push_back(makePreprocessDiag("parse_error",
        "WINDOW BY clause requires EVERY window_ms."));
    return false;
  }
  auto every = parseUint64Literal(tokens[i + 1]);
  if (!every.has_value()) {
    diagnostics->push_back(makePreprocessDiag("parse_error", "WINDOW EVERY must be numeric."));
    return false;
  }
  spec.every_ms = *every;
  spec.slide_ms = spec.every_ms;
  i += 2;
  if (i < end && isWord(tokens[i], "SLIDE")) {
    if (i + 1 >= end) {
      diagnostics->push_back(makePreprocessDiag("parse_error", "WINDOW SLIDE must be numeric."));
      return false;
    }
    auto slide = parseUint64Literal(tokens[i + 1]);
    if (!slide.has_value()) {
      diagnostics->push_back(makePreprocessDiag("parse_error", "WINDOW SLIDE must be numeric."));
      return false;
    }
    spec.slide_ms = *slide;
    i += 2;
  }
  if (i + 1 >= end || !isWord(tokens[i], "AS")) {
    diagnostics->push_back(makePreprocessDiag("parse_error",
        "WINDOW BY clause requires AS output_col."));
    return false;
  }
  spec.output_column = tokens[i + 1].value;
  i += 2;
  if (i != end) {
    diagnostics->push_back(makePreprocessDiag("parse_error",
        "Unexpected token in WINDOW BY clause: " + tokens[i].text));
    return false;
  }
  extensions->window = std::move(spec);
  return true;
}

void applyEdits(std::string_view sql, std::vector<Edit> edits, std::string* out) {
  std::sort(edits.begin(), edits.end(), [](const Edit& a, const Edit& b) {
    if (a.start != b.start) return a.start > b.start;
    return a.end > b.end;
  });
  std::string normalized(sql);
  for (const auto& edit : edits) {
    if (edit.start > normalized.size() || edit.end > normalized.size() || edit.start > edit.end) {
      continue;
    }
    normalized.replace(edit.start, edit.end - edit.start, edit.replacement);
  }
  *out = std::move(normalized);
}

void parseCreateExtensions(const std::vector<ExtensionToken>& tokens,
                           VelariaSqlExtensions* extensions,
                           std::vector<Edit>* edits,
                           std::vector<SqlDiagnostic>* diagnostics) {
  std::size_t create_idx = tokens.size();
  for (std::size_t i = 0; i < tokens.size(); ++i) {
    if (tokens[i].depth == 0 && isWord(tokens[i], "CREATE")) {
      create_idx = i;
      break;
    }
  }
  if (create_idx == tokens.size()) return;
  std::size_t table_idx = create_idx + 1;
  if (table_idx < tokens.size() && isWord(tokens[table_idx], "SOURCE")) {
    extensions->has_create_kind = true;
    extensions->create_kind = TableKind::Source;
    edits->push_back(Edit{tokens[table_idx].start, tokens[table_idx].end, ""});
    ++table_idx;
  } else if (table_idx < tokens.size() && isWord(tokens[table_idx], "SINK")) {
    extensions->has_create_kind = true;
    extensions->create_kind = TableKind::Sink;
    edits->push_back(Edit{tokens[table_idx].start, tokens[table_idx].end, ""});
    ++table_idx;
  }
  if (table_idx >= tokens.size() || !isWord(tokens[table_idx], "TABLE")) return;
  if (table_idx + 1 >= tokens.size()) {
    diagnostics->push_back(makePreprocessDiag("parse_error", "CREATE TABLE requires a table name."));
    return;
  }
  const std::size_t table_name_idx = table_idx + 1;
  bool has_columns = table_name_idx + 1 < tokens.size() && isSymbol(tokens[table_name_idx + 1], "(");
  for (std::size_t i = table_name_idx + 1; i < tokens.size(); ++i) {
    if (tokens[i].depth != 0) continue;
    if (isWord(tokens[i], "USING")) {
      if (i + 1 >= tokens.size()) {
        diagnostics->push_back(makePreprocessDiag("parse_error", "CREATE TABLE USING requires a provider."));
        return;
      }
      extensions->create_provider = tokens[i + 1].value;
      edits->push_back(Edit{tokens[i].start, tokens[i + 1].end, ""});
      ++i;
      continue;
    }
    if (isWord(tokens[i], "OPTIONS")) {
      if (i + 1 >= tokens.size() || !isSymbol(tokens[i + 1], "(")) {
        diagnostics->push_back(makePreprocessDiag("parse_error", "OPTIONS must use OPTIONS(key: value, ...)."));
        return;
      }
      const int option_depth = tokens[i + 1].depth;
      std::size_t j = i + 2;
      while (j < tokens.size() && !(isSymbol(tokens[j], ")") && tokens[j].depth == option_depth)) {
        const auto key = tokens[j].value;
        if (key.empty() || j + 2 >= tokens.size() || !isSymbol(tokens[j + 1], ":")) {
          diagnostics->push_back(makePreprocessDiag("parse_error",
              "OPTIONS entries must be key: value pairs."));
          return;
        }
        (*extensions).create_options[key] = tokens[j + 2].value;
        j += 3;
        if (j < tokens.size() && isSymbol(tokens[j], ",")) ++j;
      }
      if (j >= tokens.size()) {
        diagnostics->push_back(makePreprocessDiag("parse_error", "OPTIONS clause must end with ')'."));
        return;
      }
      edits->push_back(Edit{tokens[i].start, tokens[j].end, ""});
      i = j;
      continue;
    }
  }
  if (!has_columns) {
    edits->push_back(Edit{tokens[table_name_idx].end, tokens[table_name_idx].end, " ()"});
  }
}

void rewriteVelariaFunctionNames(const std::vector<ExtensionToken>& tokens,
                                 std::vector<Edit>* edits) {
  if (!edits) return;
  for (std::size_t i = 0; i + 1 < tokens.size(); ++i) {
    if (tokens[i].quoted_identifier || tokens[i].string_literal || !isSymbol(tokens[i + 1], "(")) {
      continue;
    }
    if (isWord(tokens[i], "LEFT")) {
      edits->push_back(Edit{tokens[i].start, tokens[i].end, "VELARIA_LEFT"});
    } else if (isWord(tokens[i], "RIGHT")) {
      edits->push_back(Edit{tokens[i].start, tokens[i].end, "VELARIA_RIGHT"});
    } else if (isWord(tokens[i], "POSITION")) {
      edits->push_back(Edit{tokens[i].start, tokens[i].end, "VELARIA_POSITION"});
    } else if (isWord(tokens[i], "CURRENT_TIMESTAMP")) {
      edits->push_back(Edit{tokens[i].start, tokens[i].end, "VELARIA_CURRENT_TIMESTAMP"});
    }
  }
}

void detectUnquotedNonAsciiIdentifiers(const std::vector<ExtensionToken>& tokens,
                                       std::vector<SqlDiagnostic>* diagnostics) {
  if (!diagnostics) return;
  for (const auto& tok : tokens) {
    if (tok.quoted_identifier || tok.string_literal) continue;
    for (unsigned char ch : tok.text) {
      if (ch >= 0x80) {
        std::ostringstream out;
        out << "invalid token byte 0x" << std::hex << static_cast<int>(ch);
        diagnostics->push_back(makePreprocessDiag(
            "parse_error", out.str(),
            "Quote non-ASCII identifiers with double quotes."));
        return;
      }
    }
  }
}

ExtensionPreprocessResult preprocessVelariaExtensions(std::string_view sql) {
  ExtensionPreprocessResult out;
  out.normalized_sql = std::string(sql);
  const auto tokens = tokenizeExtensions(sql);
  std::vector<Edit> edits;
  detectUnquotedNonAsciiIdentifiers(tokens, &out.diagnostics);
  rewriteVelariaFunctionNames(tokens, &edits);
  parseCreateExtensions(tokens, &out.extensions, &edits, &out.diagnostics);
  for (std::size_t i = 0; i < tokens.size(); ++i) {
    if (!isExtensionClauseStart(tokens, i)) continue;
    const std::size_t end = findClauseEnd(tokens, i);
    bool ok = true;
    if (isWord(tokens[i], "KEYWORD")) {
      ok = parseKeywordExtension(tokens, i, end, &out.extensions, &out.diagnostics);
    } else if (isWord(tokens[i], "HYBRID")) {
      ok = parseHybridExtension(tokens, i, end, &out.extensions, &out.diagnostics);
    } else if (isWord(tokens[i], "WINDOW")) {
      ok = parseWindowExtension(tokens, i, end, &out.extensions, &out.diagnostics);
    }
    if (ok) addRemoveEdit(tokens, i, end, &edits);
    if (end > i) i = end - 1;
  }
  applyEdits(sql, std::move(edits), &out.normalized_sql);
  return out;
}

}  // namespace

PgQueryFrontend::PgQueryFrontend() : impl_(std::make_unique<Impl>()) {}
PgQueryFrontend::~PgQueryFrontend() = default;

SqlFrontendResult PgQueryFrontend::process(std::string_view sql,
                                            const SqlFeaturePolicy& policy) {
  SqlFrontendResult result;
  result.frontend_name = "pg_query";
  result.frontend_version = std::to_string(PG_VERSION_NUM);
  auto preprocessed = preprocessVelariaExtensions(sql);
  result.diagnostics.insert(result.diagnostics.end(),
                            preprocessed.diagnostics.begin(),
                            preprocessed.diagnostics.end());
  if (!result.diagnostics.empty()) {
    return result;
  }

  // Phase 1: Parse via libpg_query protobuf API
  PgQueryParseResultHolder parse_result(preprocessed.normalized_sql.c_str());
  if (!parse_result.ok()) {
    result.diagnostics.push_back(parse_result.diagnostic(preprocessed.normalized_sql));
    return result;
  }

  // Phase 2: Validate + lower in a single protobuf tree walk.
  PgAstLowerer lowerer;
  UnboundPlan unbound;
  lowerer.lower(parse_result, policy, preprocessed.extensions, result.statement, unbound,
                result.diagnostics);
  return result;
}

std::string PgQueryFrontend::version() const {
  return std::to_string(PG_VERSION_NUM);
}

}  // namespace sql
}  // namespace dataflow
