#include "src/dataflow/core/logical/sql/sql_parser.h"

#include <cctype>
#include <stdexcept>
#include <optional>
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
         u == "SELECT" || u == "CREATE" || u == "TABLE" || u == "INSERT" || u == "INTO" ||
         u == "VALUES" || u == "USING" || u == "OPTIONS" || u == "SOURCE" || u == "SINK" ||
         u == "WINDOW" || u == "EVERY";
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
    if (c == ',' || c == '(' || c == ')' || c == '.' || c == '*' || c == '=' || c == '>' ||
        c == '<') {
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
    if (!consumeWord(w)) throw SQLSyntaxError("expected keyword: " + w);
  }
  void expectSymbol(const std::string& s) {
    if (!consumeSymbol(s)) {
      throw SQLSyntaxError("expected symbol: " + s);
    }
  }
  bool isIdentifier() const { return !isEnd() && !peek().is_string && !peek().is_number; }

 private:
  std::vector<Token> tokens_;
  std::size_t pos_ = 0;
};

bool isAliasCandidate(const Token& token) {
  return !token.text.empty() && !token.is_number && !token.is_string && !isClauseKeyword(token.text) &&
         token.text != "," && token.text != "(" && token.text != ")" && token.text != "." &&
         token.text != "*" && token.text != "=" && token.text != "<" && token.text != ">" &&
         token.text != "<=" && token.text != ">=" && token.text != "!=" && token.text != "<>";
}

bool isJoinStart(const ParseState& state) {
  return !state.isEnd() && isJoinKeyword(state.peek().text);
}

std::optional<AggregateFunctionKind> tryParseAggregateFunction(const std::string& value) {
  auto u = toUpper(value);
  if (u == "SUM") return AggregateFunctionKind::Sum;
  if (u == "COUNT") return AggregateFunctionKind::Count;
  if (u == "AVG") return AggregateFunctionKind::Avg;
  if (u == "MIN") return AggregateFunctionKind::Min;
  if (u == "MAX") return AggregateFunctionKind::Max;
  return std::nullopt;
}

Value parseValueToken(const Token& token) {
  if (token.is_number) {
    try {
      if (token.text.find('.') != std::string::npos) return Value(std::stod(token.text));
      return Value(static_cast<int64_t>(std::stoll(token.text)));
    } catch (...) {
      throw SQLSyntaxError("invalid numeric literal: " + token.text);
    }
  }
  if (token.is_string) return Value(token.text);
  if (isKeyword(token.text, "NULL")) return Value();
  if (isKeyword(token.text, "TRUE")) return Value(static_cast<int64_t>(1));
  if (isKeyword(token.text, "FALSE")) return Value(static_cast<int64_t>(0));
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

ColumnRef parseColumnWithFirst(ParseState& state, const Token& first) {
  if (first.text == "*" && !first.is_string && !first.is_number) {
    throw SQLSyntaxError("expected column reference, got *");
  }
  if (state.consumeSymbol(".")) {
    Token second = state.expectToken();
    if (second.text == "*") {
      throw SQLSyntaxError("expected column reference, got table.*");
    }
    return ColumnRef{first.text, second.text};
  }
  return ColumnRef{"", first.text};
}

AggregateFunctionKind parseAggregateFunction(const std::string& name);

AggregateExpr parseAggregateExpr(ParseState& state, const std::string& function_name) {
  AggregateExpr agg;
  agg.function = parseAggregateFunction(function_name);
  if (state.consumeSymbol("*")) {
    agg.count_all = true;
    state.expectSymbol(")");
    return agg;
  }
  agg.argument = parseColumn(state);
  state.expectSymbol(")");
  return agg;
}

std::optional<StringFunctionKind> tryParseStringFunction(const std::string& value) {
  auto u = toUpper(value);
  if (u == "LENGTH") return StringFunctionKind::Length;
  if (u == "LEN" || u == "CHAR_LENGTH" || u == "CHARACTER_LENGTH") {
    return StringFunctionKind::Length;
  }
  if (u == "LOWER") return StringFunctionKind::Lower;
  if (u == "UPPER") return StringFunctionKind::Upper;
  if (u == "TRIM") return StringFunctionKind::Trim;
  if (u == "CONCAT") return StringFunctionKind::Concat;
  if (u == "REVERSE") return StringFunctionKind::Reverse;
  if (u == "CONCAT_WS") return StringFunctionKind::ConcatWs;
  if (u == "LEFT") return StringFunctionKind::Left;
  if (u == "RIGHT") return StringFunctionKind::Right;
  if (u == "SUBSTR" || u == "SUBSTRING") return StringFunctionKind::Substr;
  if (u == "LTRIM") return StringFunctionKind::Ltrim;
  if (u == "RTRIM") return StringFunctionKind::Rtrim;
  if (u == "POSITION") return StringFunctionKind::Position;
  if (u == "REPLACE") return StringFunctionKind::Replace;
  return std::nullopt;
}

StringFunctionArg parseFunctionArg(ParseState& state, const Token& first) {
  StringFunctionArg arg;
  if (first.is_string || first.is_number || isKeyword(first.text, "NULL") ||
      isKeyword(first.text, "TRUE") || isKeyword(first.text, "FALSE")) {
    arg.is_column = false;
    arg.literal = parseValueToken(first);
    return arg;
  }
  if (first.text == "*") {
    throw SQLSyntaxError("invalid function argument");
  }
  if (state.consumeSymbol(".")) {
    Token second = state.expectToken();
    if (second.text == "*" ) {
      throw SQLSyntaxError("invalid function argument");
    }
    arg.is_column = true;
    arg.column = ColumnRef{first.text, second.text};
    return arg;
  }
  if (!state.isEnd() && state.peek().text == "(") {
    throw SQLUnsupportedError("not supported in SQL v1: nested function calls");
  }
  arg.is_column = true;
  arg.column = ColumnRef{"", first.text};
  return arg;
}

StringFunctionExpr parseStringFunctionExpr(ParseState& state, StringFunctionKind function) {
  StringFunctionExpr expr;
  expr.function = function;
  if (state.consumeSymbol(")")) {
    return expr;
  }
  while (true) {
    Token token = state.expectToken();
    expr.args.push_back(parseFunctionArg(state, token));
    if (state.consumeSymbol(",")) {
      continue;
    }
    state.expectSymbol(")");
    return expr;
  }
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
        !isKeyword(token.text, "LIMIT") && token.text != "," && !isClauseKeyword(token.text)) {
      out.alias = state.expectToken().text;
    }
  }
  return out;
}

Predicate parsePredicate(ParseState& state) {
  Predicate out;
  bool left_paren = false;
  if (state.consumeSymbol("(")) {
    left_paren = true;
  }
  Token lhs = state.expectToken();
  if (lhs.text == ")" || lhs.text == "," || lhs.text == "*") {
    throw SQLSyntaxError("invalid predicate");
  }

  if (state.consumeSymbol("(")) {
    if (!tryParseAggregateFunction(lhs.text).has_value()) {
      throw SQLSyntaxError("unsupported aggregate function: " + lhs.text);
    }
    out.lhs_is_aggregate = true;
    out.lhs_aggregate = parseAggregateExpr(state, lhs.text);
    if (left_paren) {
      state.consumeSymbol(")");
      left_paren = false;
    }
  } else {
    out.lhs = parseColumnWithFirst(state, lhs);
  }

  out.op = parseOperator(state);
  Token rhs = state.expectToken();
  bool rhs_parenthesized = false;
  if (rhs.text == "(") {
    rhs_parenthesized = true;
    rhs = state.expectToken();
  }
  if (rhs.text == ")" || rhs.text == ",") throw SQLSyntaxError("invalid predicate");
  if (rhs.is_string) {
    out.rhs = Value(rhs.text);
  } else if (rhs.is_number) {
    out.rhs = parseValueToken(rhs);
  } else if (isClauseKeyword(rhs.text)) {
    throw SQLSyntaxError("unsupported predicate literal");
  } else {
    out.rhs = parseValueToken(rhs);
  }
  if (rhs_parenthesized) {
    state.expectSymbol(")");
  }
  if (left_paren) {
    state.expectSymbol(")");
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
  throw SQLUnsupportedError("not supported in SQL v1: aggregate function " + name);
}

void parseOptionalAlias(ParseState& state, std::string* alias) {
  if (state.consumeWord("AS")) {
    *alias = state.expectToken().text;
  } else if (!state.isEnd() && isAliasCandidate(state.peek())) {
    if (!isKeyword(state.peek().text, "FROM") && !isKeyword(state.peek().text, "WHERE") &&
        !isKeyword(state.peek().text, "GROUP") && !isKeyword(state.peek().text, "HAVING") &&
        !isKeyword(state.peek().text, "LIMIT") && state.peek().text != "," && state.peek().text != ")" &&
        !isClauseKeyword(state.peek().text)) {
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
    item.literal = parseValueToken(first);
    parseOptionalAlias(state, &item.alias);
    return item;
  }
  if (state.consumeSymbol("(")) {
    if (auto aggregate = tryParseAggregateFunction(first.text)) {
      AggregateExpr agg = parseAggregateExpr(state, first.text);
      item.is_aggregate = true;
      item.aggregate = agg;
      parseOptionalAlias(state, &item.alias);
      return item;
    }
    auto function = tryParseStringFunction(first.text);
    if (!function.has_value()) {
      throw SQLUnsupportedError("not supported in SQL v1: scalar function " + first.text);
    }
    item.is_string_function = true;
    item.string_function = parseStringFunctionExpr(state, *function);
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
  const bool parenthesized = state.consumeSymbol("(");
  out.left_key = parseColumn(state);
  if (!state.consumeSymbol("=") && !state.consumeSymbol("==")) {
    throw SQLSyntaxError("only equality join is supported");
  }
  out.right_key = parseColumn(state);
  if (parenthesized) {
    state.expectSymbol(")");
  }
  return out;
}

SqlQuery parseSelectQuery(ParseState& state, bool alreadyConsumedSelect) {
  SqlQuery out;
  if (!alreadyConsumedSelect) {
    state.expectWord("SELECT");
  }

  while (true) {
    out.select_items.push_back(parseSelectItem(state));
    if (state.consumeSymbol(",")) {
      if (state.isEnd()) throw SQLSyntaxError("unexpected end while parsing select list");
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

  if (state.consumeWord("WINDOW")) {
    WindowSpec window;
    state.expectWord("BY");
    window.time_column = parseColumn(state);
    state.expectWord("EVERY");
    Token window_ms = state.expectToken();
    if (!window_ms.is_number) {
      throw SQLSyntaxError("WINDOW EVERY must be numeric");
    }
    try {
      window.every_ms = static_cast<uint64_t>(std::stoull(window_ms.text));
    } catch (...) {
      throw SQLSyntaxError("invalid WINDOW EVERY value: " + window_ms.text);
    }
    state.expectWord("AS");
    window.output_column = state.expectToken().text;
    out.window = std::move(window);
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
    if (!t.is_number) throw SQLSyntaxError("LIMIT must be numeric");
    try {
      out.limit = std::stoul(t.text);
    } catch (...) {
      throw SQLSyntaxError("invalid LIMIT: " + t.text);
    }
  }

  return out;
}

SqlColumnDef parseCreateColumn(ParseState& state) {
  SqlColumnDef out;
  out.name = state.expectToken().text;
  out.type = state.expectToken().text;
  return out;
}

void parseCreateOptions(ParseState& state, SqlStatement& out) {
  if (state.consumeWord("USING")) {
    out.create.provider = state.expectToken().text;
  }
  if (state.consumeWord("OPTIONS")) {
    state.expectSymbol("(");
    if (!state.consumeSymbol(")")) {
      while (true) {
        const auto key = state.expectToken().text;
        state.consumeSymbol("=");
        const auto value = state.expectToken().text;
        out.create.options[key] = value;
        if (state.consumeSymbol(")")) {
          break;
        }
        state.expectSymbol(",");
      }
    }
  }
}

SqlStatement parseCreateTable(ParseState& state) {
  SqlStatement out;
  out.kind = SqlStatementKind::CreateTable;
  if (state.consumeWord("SOURCE")) {
    out.create.kind = sql::TableKind::Source;
  } else if (state.consumeWord("SINK")) {
    out.create.kind = sql::TableKind::Sink;
  }
  state.expectWord("TABLE");
  out.create.table = state.expectToken().text;
  state.expectSymbol("(");
  if (state.consumeSymbol(")")) {
    throw SQLSyntaxError("CREATE TABLE requires at least one column");
  }
  while (true) {
    out.create.columns.push_back(parseCreateColumn(state));
    if (state.consumeSymbol(")")) break;
    state.expectSymbol(",");
  }
  parseCreateOptions(state, out);
  return out;
}

std::vector<Value> parseValuesRow(ParseState& state) {
  std::vector<Value> row;
  row.push_back(parseValueToken(state.expectToken()));
  while (state.consumeSymbol(",")) {
    row.push_back(parseValueToken(state.expectToken()));
  }
  return row;
}

SqlStatement parseInsertStatement(ParseState& state) {
  SqlStatement out;
  out.insert.select_from = false;
  state.expectWord("INTO");
  out.insert.table = state.expectToken().text;

  if (state.consumeSymbol("(")) {
    if (state.consumeSymbol(")")) {
      throw SQLSyntaxError("INSERT INTO with empty column list");
    }
    out.insert.columns.push_back(state.expectToken().text);
    while (state.consumeSymbol(",")) {
      out.insert.columns.push_back(state.expectToken().text);
    }
    state.expectSymbol(")");
  }

  if (state.consumeWord("VALUES")) {
    out.kind = SqlStatementKind::InsertValues;
    do {
      state.expectSymbol("(");
      out.insert.values.push_back(parseValuesRow(state));
      state.expectSymbol(")");
    } while (state.consumeSymbol(","));
    return out;
  }

  if (state.consumeWord("SELECT")) {
    out.kind = SqlStatementKind::InsertSelect;
    out.insert.select_from = true;
    out.insert.query = parseSelectQuery(state, true);
    return out;
  }

  throw SQLSyntaxError("INSERT expected VALUES or SELECT");
}

}  // namespace

SqlStatement SqlParser::parse(const std::string& sql) {
  ParseState state(tokenize(sql));
  SqlStatement out;

  if (state.consumeWord("SELECT")) {
    out.kind = SqlStatementKind::Select;
    out.query = parseSelectQuery(state, true);
  } else if (state.consumeWord("CREATE")) {
    out = parseCreateTable(state);
  } else if (state.consumeWord("INSERT")) {
    out = parseInsertStatement(state);
  } else {
    throw SQLUnsupportedError("not supported in SQL v1: unsupported statement");
  }

  if (!state.isEnd()) {
    throw SQLSyntaxError("unexpected token: " + state.peek().text);
  }
  return out;
}

}  // namespace sql
}  // namespace dataflow
