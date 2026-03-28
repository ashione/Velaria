#include "src/dataflow/sql/sql_parser.h"

#include <cctype>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace dataflow {
namespace sql {

namespace {

struct Token {
  std::string text;
  bool is_string = false;
  bool is_number = false;

  Token() = default;
  Token(std::string t, bool s, bool n) : text(std::move(t)), is_string(s), is_number(n) {}
};

std::string toUpper(std::string value) {
  for (char& c : value) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  return value;
}

bool isKeyword(const std::string& value, const std::string& expected) {
  return toUpper(value) == expected;
}

bool isClauseKeyword(const std::string& value) {
  const auto u = toUpper(value);
  return u == "FROM" || u == "WHERE" || u == "GROUP" || u == "BY" || u == "HAVING" ||
         u == "LIMIT" || u == "JOIN" || u == "INNER" || u == "LEFT" || u == "ON" || u == "AS" ||
         u == "SELECT";
}

bool isJoinKeyword(const std::string& value) {
  const auto u = toUpper(value);
  return u == "JOIN" || u == "INNER" || u == "LEFT";
}

std::vector<Token> tokenize(const std::string& sql) {
  std::vector<Token> out;
  std::size_t i = 0;
  while (i < sql.size()) {
    char c = sql[i];
    if (std::isspace(static_cast<unsigned char>(c))) {
      ++i;
      continue;
    }
    if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
      std::size_t start = i++;
      while (i < sql.size()) {
        char p = sql[i];
        if (std::isalnum(static_cast<unsigned char>(p)) || p == '_') {
          ++i;
        } else {
          break;
        }
      }
      out.push_back(Token(sql.substr(start, i - start), false, false));
      continue;
    }
    if (std::isdigit(static_cast<unsigned char>(c)) || ((c == '+' || c == '-') &&
        i + 1 < sql.size() && std::isdigit(static_cast<unsigned char>(sql[i + 1])))) {
      std::size_t start = i++;
      bool hasDot = false;
      while (i < sql.size()) {
        char p = sql[i];
        if (std::isdigit(static_cast<unsigned char>(p))) {
          ++i;
          continue;
        }
        if (p == '.' && !hasDot) {
          hasDot = true;
          ++i;
          continue;
        }
        break;
      }
      out.push_back(Token(sql.substr(start, i - start), false, true));
      continue;
    }
    if (c == '\'' || c == '"') {
      char quote = c;
      std::size_t start = i + 1;
      ++i;
      while (i < sql.size() && sql[i] != quote) {
        if (sql[i] == '\\' && i + 1 < sql.size()) {
          i += 2;
        } else {
          ++i;
        }
      }
      if (i >= sql.size()) {
        throw SQLSyntaxError("unterminated string literal");
      }
      std::string content = sql.substr(start, i - start);
      ++i;
      out.push_back(Token(content, true, false));
      continue;
    }
    if (i + 1 < sql.size()) {
      std::string two = sql.substr(i, 2);
      if (two == ">=" || two == "<=" || two == "!=" || two == "<>") {
        out.push_back(Token(two, false, false));
        i += 2;
        continue;
      }
    }
    if (c == ',' || c == '(' || c == ')' || c == '.' || c == '*' || c == '=' || c == '>' || c == '<') {
      out.push_back(Token(std::string(1, c), false, false));
      ++i;
      continue;
    }
    throw SQLSyntaxError(std::string("invalid token: ") + c);
  }
  out.push_back(Token("", false, false));
  return out;
}

class ParseState {
 public:
  explicit ParseState(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

  bool isEnd() const { return pos_ >= tokens_.size() || tokens_[pos_].text.empty(); }
  const Token& peek() const { return tokens_[pos_]; }
  Token take() {
    if (isEnd()) throw SQLSyntaxError("unexpected end of query");
    return tokens_[pos_++];
  }
  bool consumeWord(const std::string& w) {
    if (!isEnd() && isKeyword(peek().text, w)) {
      ++pos_;
      return true;
    }
    return false;
  }
  bool consumeSymbol(const std::string& s) {
    if (!isEnd() && tokens_[pos_].text == s) {
      ++pos_;
      return true;
    }
    return false;
  }
  Token expectToken() { return take(); }
  void expectWord(const std::string& w) {
    if (!consumeWord(w)) {
      throw SQLSyntaxError("expected keyword: " + w);
    }
  }
  void expectSymbol(const std::string& s) {
    if (!consumeSymbol(s)) {
      throw SQLSyntaxError("expected symbol: " + s);
    }
  }
  bool isIdentifier() const {
    return !isEnd() && !peek().is_string && !peek().is_number;
  }

 private:
  std::vector<Token> tokens_;
  std::size_t pos_ = 0;
};

bool isAliasCandidate(const Token& token) {
  return !token.text.empty() && !token.is_number && !token.is_string && !isClauseKeyword(token.text) &&
         token.text != "," && token.text != "(" && token.text != ")" && token.text != "." && token.text != "*" &&
         token.text != "=" && token.text != "<" && token.text != ">" && token.text != "<=" &&
         token.text != ">=" && token.text != "!=" && token.text != "<>";
}

bool isJoinStart(const ParseState& state) {
  return !state.isEnd() && isJoinKeyword(state.peek().text);
}

Value parseValueLiteral(const Token& token) {
  if (token.is_number) {
    try {
      if (token.text.find('.') != std::string::npos) {
        return Value(std::stod(token.text));
      }
      return Value(std::stoll(token.text));
    } catch (...) {
      throw SQLSyntaxError("invalid numeric literal: " + token.text);
    }
  }
  if (token.is_string) return Value(token.text);
  if (isKeyword(token.text, "NULL")) return Value();
  return Value(token.text);
}

BinaryOperatorKind parseOperator(ParseState& state) {
  Token t = state.expectToken();
  if (t.text == "=") return BinaryOperatorKind::Eq;
  if (t.text == "!=" || t.text == "<>") return BinaryOperatorKind::Ne;
  if (t.text == "<") return BinaryOperatorKind::Lt;
  if (t.text == "<=") return BinaryOperatorKind::Lte;
  if (t.text == ">") return BinaryOperatorKind::Gt;
  if (t.text == ">=") return BinaryOperatorKind::Gte;
  throw SQLSyntaxError("unsupported operator: " + t.text);
}

ColumnRef parseColumnOrTableStar(ParseState& state, bool& tableStar, std::string& tableName) {
  tableStar = false;
  Token first = state.expectToken();
  if (first.text == "*" && !first.is_string && !first.is_number) {
    return ColumnRef{};
  }

  std::string left = first.text;
  if (state.consumeSymbol(".")) {
    Token second = state.expectToken();
    if (second.text == "*") {
      tableStar = true;
      tableName = left;
      return ColumnRef{left, "*"};
    }
    ColumnRef ref;
    ref.qualifier = left;
    ref.name = second.text;
    return ref;
  }

  ColumnRef ref;
  ref.name = left;
  return ref;
}

ColumnRef parseColumn(ParseState& state) {
  bool tableStar = false;
  std::string tbl;
  ColumnRef out = parseColumnOrTableStar(state, tableStar, tbl);
  if (tableStar) {
    throw SQLSyntaxError("expected column reference, got table.*");
  }
  return out;
}

FromItem parseFrom(ParseState& state) {
  FromItem out;
  out.name = state.expectToken().text;
  if (state.consumeWord("AS")) {
    out.alias = state.expectToken().text;
    return out;
  }
  if (!state.isEnd() && isAliasCandidate(state.peek())) {
    auto token = state.peek();
    if (!isKeyword(token.text, "ON") && !isKeyword(token.text, "WHERE") &&
        !isKeyword(token.text, "GROUP") && !isKeyword(token.text, "HAVING") &&
        !isKeyword(token.text, "LIMIT") && token.text != ",") {
      out.alias = state.expectToken().text;
    }
  }
  return out;
}

Predicate parsePredicate(ParseState& state) {
  Predicate out;
  out.lhs = parseColumn(state);
  out.op = parseOperator(state);
  Token rhs = state.expectToken();
  if (rhs.text == "(" || rhs.text == ")" || rhs.text == ",") {
    throw SQLSyntaxError("invalid predicate");
  }
  if (rhs.is_string) {
    out.rhs = Value(rhs.text);
  } else if (rhs.is_number) {
    out.rhs = parseValueLiteral(rhs);
  } else if (isKeyword(rhs.text, "NULL")) {
    out.rhs = Value();
  } else if (isClauseKeyword(rhs.text)) {
    throw SQLSyntaxError("unsupported predicate literal");
  } else {
    out.rhs = Value(rhs.text);
  }
  return out;
}

AggregateFunctionKind parseAggregateFunction(const std::string& name) {
  auto u = toUpper(name);
  if (u == "SUM") return AggregateFunctionKind::Sum;
  if (u == "COUNT") return AggregateFunctionKind::Count;
  if (u == "AVG") return AggregateFunctionKind::Avg;
  if (u == "MIN") return AggregateFunctionKind::Min;
  if (u == "MAX") return AggregateFunctionKind::Max;
  throw SQLSyntaxError("unsupported aggregate function: " + name);
}

void parseOptionalAlias(ParseState& state, std::string* alias) {
  if (state.consumeWord("AS")) {
    *alias = state.expectToken().text;
  } else if (!state.isEnd() && isAliasCandidate(state.peek())) {
    if (!isKeyword(state.peek().text, "FROM") && !isKeyword(state.peek().text, "WHERE") &&
        !isKeyword(state.peek().text, "GROUP") && !isKeyword(state.peek().text, "HAVING") &&
        !isKeyword(state.peek().text, "LIMIT") && state.peek().text != ",") {
      *alias = state.expectToken().text;
    }
  }
}

SelectItem parseSelectItem(ParseState& state) {
  SelectItem item;

  if (state.consumeSymbol("*")) {
    item.is_all = true;
    parseOptionalAlias(state, &item.alias);
    return item;
  }

  Token first = state.expectToken();
  if (first.is_number || first.is_string || isKeyword(first.text, "NULL")) {
    item.is_literal = true;
    item.literal = parseValueLiteral(first);
    parseOptionalAlias(state, &item.alias);
    return item;
  }
  if (state.consumeSymbol("(")) {
    AggregateExpr agg;
    agg.function = parseAggregateFunction(first.text);
    if (state.consumeSymbol("*")) {
      agg.count_all = true;
      state.expectSymbol(")");
    } else {
      agg.argument = parseColumn(state);
      state.expectSymbol(")");
    }
    item.is_aggregate = true;
    item.aggregate = agg;
    parseOptionalAlias(state, &item.alias);
    return item;
  }

  bool tableStar = false;
  std::string tableName;
  if (state.consumeSymbol(".")) {
    Token second = state.expectToken();
    if (second.text == "*") {
      tableStar = true;
      tableName = first.text;
    } else {
      item.column = ColumnRef{first.text, second.text};
    }
  } else {
    item.column = ColumnRef{"", first.text};
  }
  if (tableStar) {
    item.is_table_all = true;
    item.table_name_or_alias = tableName;
  }
  parseOptionalAlias(state, &item.alias);
  return item;
}

JoinItem parseJoin(ParseState& state) {
  JoinItem out;
  if (state.consumeWord("LEFT")) {
    out.is_left = true;
  } else {
    state.consumeWord("INNER");
  }
  state.expectWord("JOIN");
  out.right = parseFrom(state);
  state.expectWord("ON");
  out.left_key = parseColumn(state);
  if (!state.consumeSymbol("=") && !state.consumeSymbol("==")) {
    throw SQLSyntaxError("only equality join is supported");
  }
  out.right_key = parseColumn(state);
  return out;
}

}  // namespace

SqlQuery SqlParser::parse(const std::string& sql) {
  ParseState state(tokenize(sql));
  SqlQuery out;

  state.expectWord("SELECT");
  while (true) {
    out.select_items.push_back(parseSelectItem(state));
    if (state.consumeSymbol(",")) {
      if (state.isEnd()) {
        throw SQLSyntaxError("unexpected end while parsing select list");
      }
      continue;
    }
    if (state.consumeWord("FROM")) {
      out.has_from = true;
      break;
    }
    if (state.isEnd()) break;
    throw SQLSyntaxError("expect comma or FROM");
  }

  if (out.select_items.empty()) {
    throw SQLSyntaxError("SELECT list cannot be empty");
  }
  if (out.has_from) {
    out.from = parseFrom(state);
  }

  if (out.has_from && isJoinStart(state)) {
    out.join = parseJoin(state);
  }

  if (state.consumeWord("WHERE")) {
    out.where = parsePredicate(state);
  }

  if (state.consumeWord("GROUP")) {
    state.expectWord("BY");
    out.group_by.push_back(parseColumn(state));
    while (state.consumeSymbol(",")) {
      out.group_by.push_back(parseColumn(state));
    }
  }

  if (state.consumeWord("HAVING")) {
    out.having = parsePredicate(state);
  }

  if (state.consumeWord("LIMIT")) {
    Token t = state.expectToken();
    if (!t.is_number) {
      throw SQLSyntaxError("LIMIT must be numeric");
    }
    try {
      out.limit = std::stoul(t.text);
    } catch (...) {
      throw SQLSyntaxError("invalid LIMIT: " + t.text);
    }
  }

  if (!state.isEnd()) {
    throw SQLSyntaxError("unexpected token: " + state.peek().text);
  }
  return out;
}

}  // namespace sql
}  // namespace dataflow
