use std::fmt;
use std::borrow::Cow;

use combine::{StreamOnce, Positioned};
use combine::error::{StreamError};
use combine::stream::{ResetStream};
use combine::easy::{Error, Errors};
use twoway::find_str;

use crate::position::Pos;


// Current max keyword length is 10, but we're reserving some space
const MAX_KEYWORD_LENGTH: usize = 16;


#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Kind {
    Assign,           // :=
    SubAssign,        // -=
    AddAssign,        // +=
    Arrow,            // ->
    Coalesce,         // ??
    Namespace,        // ::
    ForwardLink,      // .>
    BackwardLink,     // .<
    FloorDiv,         // //
    Concat,           // ++
    GreaterEq,        // >=
    LessEq,           // <=
    NotEq,            // !=
    NotDistinctFrom,  // ?=
    DistinctFrom,     // ?!=
    Comma,            // ,
    OpenParen,        // (
    CloseParen,       // )
    OpenBracket,      // [
    CloseBracket,     // ]
    OpenBrace,        // {
    CloseBrace,       // }
    Dot,              // .
    Semicolon,        // ;
    Colon,            // :
    Add,              // +
    Sub,              // -
    Mul,              // *
    Div,              // /
    Modulo,           // %
    Pow,              // ^
    Less,             // <
    Greater,          // >
    Eq,               // =
    Ampersand,        // &
    Pipe,             // |
    Dollar,           // $
    DecimalConst,
    FloatConst,
    IntConst,
    BigIntConst,
    BinStr,           // b"xx", b'xx'
    Str,              // "xx", 'xx', r"xx", r'xx', $$xx$$
    BacktickName,     // `xx`
    Keyword,
    Ident,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Token<'a> {
    pub kind: Kind,
    pub value: &'a str,
}

#[derive(Debug, PartialEq)]
pub struct TokenStream<'a> {
    buf: &'a str,
    position: Pos,
    off: usize,
    dot: bool,
    next_state: Option<(usize, Token<'a>, usize, Pos)>,
    keyword_buf: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Checkpoint {
    position: Pos,
    off: usize,
    dot: bool,
}

impl<'a> StreamOnce for TokenStream<'a> {
    type Token = Token<'a>;
    type Range = Token<'a>;
    type Position = Pos;
    type Error = Errors<Token<'a>, Token<'a>, Pos>;

    fn uncons(&mut self) -> Result<Self::Token, Error<Token<'a>, Token<'a>>> {
        // This quickly resets the stream one token back
        // (the most common reset that used quite often)
        if let Some((at, tok, off, pos)) = self.next_state {
            if at == self.off {
                self.off = off;
                self.position = pos;
                return Ok(tok);
            }
        }
        let old_pos = self.off;
        let (kind, len) = self.peek_token()?;

        // note we may want to get rid of "update_position" here as it's
        // faster to update 'as you go', but this is easier to get right first
        self.update_position(len);
        self.dot = kind == Kind::Dot;

        let value = &self.buf[self.off-len..self.off];
        self.skip_whitespace();
        let token = Token { kind, value };
        // This is for quick reset on token back
        self.next_state = Some((old_pos, token, self.off, self.position));
        Ok(token)
    }
}

impl<'a> Positioned for TokenStream<'a> {
    fn position(&self) -> Self::Position {
        self.position
    }
}

impl<'a> ResetStream for TokenStream<'a> {
    type Checkpoint = Checkpoint;
    fn checkpoint(&self) -> Self::Checkpoint {
        Checkpoint {
            position: self.position,
            off: self.off,
            dot: self.dot,
        }
    }
    fn reset(&mut self, checkpoint: Checkpoint) -> Result<(), Self::Error> {
        self.position = checkpoint.position;
        self.off = checkpoint.off;
        self.dot = checkpoint.dot;
        Ok(())
    }
}

impl<'a> TokenStream<'a> {
    pub fn new(s: &str) -> TokenStream {
        let mut me = TokenStream {
            buf: s,
            position: Pos { line: 1, column: 1, offset: 0 },
            off: 0,
            dot: false,
            next_state: None,
            // Current max keyword length is 10, but we're reserving some
            // space
            keyword_buf: String::with_capacity(MAX_KEYWORD_LENGTH),
        };
        me.skip_whitespace();
        me
    }

    /// Start stream a with a modified position
    ///
    /// Note: we assume that the current position is at the start of slice `s`
    pub fn new_at(s: &str, position: Pos) -> TokenStream {
        let mut me = TokenStream {
            buf: s,
            position: position,
            off: 0,
            dot: false,
            next_state: None,
            keyword_buf: String::with_capacity(MAX_KEYWORD_LENGTH),
        };
        me.skip_whitespace();
        me
    }

    fn peek_token(&mut self)
        -> Result<(Kind, usize), Error<Token<'a>, Token<'a>>>
    {
        use self::Kind::*;
        let mut iter = self.buf[self.off..].char_indices();
        let cur_char = match iter.next() {
            Some((_, x)) => x,
            None => return Err(Error::end_of_input()),
        };

        match cur_char {
            ':' => match iter.next() {
                Some((_, '=')) => return Ok((Assign, 2)),
                Some((_, ':')) => return Ok((Namespace, 2)),
                _ => return Ok((Colon, 1)),
            },
            '-' => match iter.next() {
                Some((_, '>')) => return Ok((Arrow, 2)),
                Some((_, '=')) => return Ok((SubAssign, 2)),
                Some((_, '0'..='9')) => self.parse_number(),
                _ => return Ok((Sub, 1)),
            },
            '>' => match iter.next() {
                Some((_, '=')) => return Ok((GreaterEq, 2)),
                _ => return Ok((Greater, 1)),
            },
            '<' => match iter.next() {
                Some((_, '=')) => return Ok((LessEq, 2)),
                _ => return Ok((Less, 1)),
            },
            '+' => match iter.next() {
                Some((_, '=')) => return Ok((AddAssign, 2)),
                Some((_, '+')) => return Ok((Concat, 2)),
                Some((_, '0'..='9')) => self.parse_number(),
                _ => return Ok((Add, 1)),
            },
            '/' => match iter.next() {
                Some((_, '/')) => return Ok((FloorDiv, 2)),
                _ => return Ok((Div, 1)),
            },
            '.' => match iter.next() {
                Some((_, '>')) => return Ok((ForwardLink, 2)),
                Some((_, '<')) => return Ok((BackwardLink, 2)),
                _ => return Ok((Dot, 1)),
            },
            '?' => match iter.next() {
                Some((_, '?')) => return Ok((Coalesce, 2)),
                Some((_, '=')) => return Ok((NotDistinctFrom, 2)),
                Some((_, '!')) => {
                    if let Some((_, '=')) = iter.next() {
                        return Ok((DistinctFrom, 3));
                    } else {
                        return Err(Error::unexpected_format(
                            format_args!("{}: `?!` is not an operator, \
                                did you mean `?!=` ?",
                                self.position)
                        ))
                    }
                }
                _ => {
                    return Err(Error::unexpected_format(
                        format_args!("{}: Bare `?` is not an operator, \
                            did you mean `?=` or `??` ?",
                            self.position)
                    ))
                }
            },
            '!' => match iter.next() {
                Some((_, '=')) => return Ok((NotEq, 2)),
                _ => {
                    return Err(Error::unexpected_format(
                        format_args!("{}: Bare `!` is not an operator, \
                            did you mean `!=`?",
                            self.position)
                    ))
                }
            },
            '"' | '\'' => self.parse_string(0, false),
            '`' => {
                for (idx, c) in iter {
                    if c == '`' { return Ok((BacktickName, idx+1)); }
                }
                return Err(Error::unexpected_format(
                    format_args!("{}: unclosed backtick name",
                        self.position)));
            }
            '=' => return Ok((Eq, 1)),
            ',' => return Ok((Comma, 1)),
            '(' => return Ok((OpenParen, 1)),
            ')' => return Ok((CloseParen, 1)),
            '[' => return Ok((OpenBracket, 1)),
            ']' => return Ok((CloseBracket, 1)),
            '{' => return Ok((OpenBrace, 1)),
            '}' => return Ok((CloseBrace, 1)),
            ';' => return Ok((Semicolon, 1)),
            '*' => return Ok((Mul, 1)),
            '%' => return Ok((Modulo, 1)),
            '^' => return Ok((Pow, 1)),
            '&' => return Ok((Ampersand, 1)),
            '|' => return Ok((Pipe, 1)),
            c if c == '_' || c.is_alphabetic() => {
                for (idx, c) in iter {
                    match c {
                        '"' | '\'' => {
                            let prefix = &self.buf[self.off..][..idx];
                            let binary = match prefix {
                                "r" => false,
                                "b" => true,
                                _ => return Err(Error::unexpected_format(
                                    format_args!("{}: Prefix {:?} \
                                    is not allowed for strings, \
                                    allowed: `b`, `r`",
                                    self.position, prefix))),
                            };
                            return self.parse_string(idx, binary);
                        }
                        '`' => {
                            let prefix = &self.buf[self.off..idx];
                            return Err(Error::unexpected_format(
                                format_args!("{}: Prefix {:?} is not \
                                allowed for field names, perhaps missing \
                                comma or dot?", self.position, prefix)));
                        }
                        c if c == '_' || c.is_alphanumeric() => continue,
                        _ => {
                            let val = &self.buf[self.off..self.off+idx];
                            if self.is_keyword(val) {
                                return Ok((Keyword, idx));
                            } else {
                                return Ok((Ident, idx));
                            }
                        }
                    }
                }
                let val = &self.buf[self.off..];
                let len = val.len();
                if self.is_keyword(val) {
                    return Ok((Keyword, len));
                } else {
                    return Ok((Ident, len));
                }
            }
            '0'..='9' => {
                if self.dot {
                    for (idx, c) in iter {
                        match c {
                            '0'..='9' => continue,
                            c if c.is_alphabetic() => {
                                return Err(Error::unexpected_format(
                                    format_args!("{}: unexpected char {:?} \
                                        only integers are allowed after dot \
                                        (for tuple access)",
                                        self.position, c)
                                ));
                            }
                            _ => return Ok((IntConst, idx)),
                        }
                    }
                    Ok((IntConst, self.buf.len() - self.off))
                } else {
                    self.parse_number()
                }
            }
            '$' => {
                if let Some((_, c)) = iter.next() {
                    match c {
                        '$' => {
                            if let Some(end) = find_str(
                                &self.buf[self.off+2..], "$$")
                            {
                                return Ok((Str, 2+end+2));
                            } else {
                                return Err(Error::unexpected_format(
                                    format_args!("{}: unclosed string started \
                                        with $$", self.position)));
                            }
                        }
                        'A'..='Z' | 'a'..='z' | '_' => { }
                        _ => return Ok((Dollar, 1)),
                    }
                }
                while let Some((end_idx, c)) = iter.next() {
                    match c {
                        '$' => {
                            let msize = end_idx+1;
                            let marker = &self.buf[self.off..][..msize];
                            if let Some(end) = find_str(
                                &self.buf[self.off+msize..],
                                &marker)
                            {
                                return Ok((Str, msize+end+msize));
                            } else {
                                return Err(Error::unexpected_format(
                                    format_args!("{}: unclosed string started \
                                        with {:?}", self.position, marker)));
                            }
                        }
                        'A'..='Z' | 'a'..='z' | '0'..='9' | '_' => continue,
                        _ => return Ok((Dollar, 1)),

                    }
                }
                return Ok((Dollar, 1));
            }
            _ => return Err(
                Error::unexpected_format(
                    format_args!("{}: unexpected character {:?}",
                        cur_char, self.position)
                )
            ),
        }
    }

    fn parse_string(&mut self, quote_off: usize, binary: bool)
        -> Result<(Kind, usize), Error<Token<'a>, Token<'a>>>
    {
        let mut iter = self.buf[self.off+quote_off..].char_indices();
        let open_quote = iter.next().unwrap().1;
        while let Some((idx, c)) = iter.next() {
            match c {
                '\\' => match iter.next() {
                    // skip any next char, even quote
                    Some((_, _)) => continue,
                    None => break,
                }
                c if c == open_quote => {
                    if binary {
                        return Ok((Kind::BinStr, quote_off+idx+1))
                    } else {
                        return Ok((Kind::Str, quote_off+idx+1))
                    }
                }
                _ => {}
            }
        }
        return Err(Error::unexpected_format(
            format_args!("{}: unclosed string, quoted by `{}`",
                self.position, open_quote)));
    }

    fn parse_number(&mut self)
        -> Result<(Kind, usize), Error<Token<'a>, Token<'a>>>
    {
        #[derive(PartialEq, PartialOrd)]
        enum Break {
            Dot,
            Exponent,
            Letter,
        }
        use self::Kind::*;
        let mut iter = self.buf[self.off+1..].char_indices();
        let mut suffix = None;
        let mut float = false;
        // decimal part
        let mut bstate = loop {
            match iter.next() {
                Some((_, '0'..='9')) => continue,
                Some((_, 'e')) => break Break::Exponent,
                Some((_, '.')) => break Break::Dot,
                Some((idx, c)) if c.is_alphabetic() => {
                    suffix = Some(idx+1);
                    break Break::Letter;
                }
                Some((idx, _)) => return Ok((IntConst, idx+1)),
                None => return Ok((IntConst, self.buf.len() - self.off)),
            }
        };
        if bstate == Break::Dot {
            float = true;
            bstate = loop {
                if let Some((idx, c)) = iter.next() {
                    match c {
                        '0'..='9' => continue,
                        'e' => break Break::Exponent,
                        '.' => return Err(Error::unexpected_format(
                            format_args!("{}: extra decimal dot in number",
                                self.position))),
                        c if c.is_alphabetic() => {
                            suffix = Some(idx+1);
                            break Break::Letter;
                        }
                        _ => return Ok((FloatConst, idx+1)),
                    }
                } else {
                    return Ok((FloatConst, self.buf.len() - self.off));
                }
            }
        }
        if bstate == Break::Exponent {
            float = true;
            match iter.next() {
                Some((_, '0'..='9')) => {},
                Some((_, '+')) | Some((_, '-'))=> {
                    match iter.next() {
                        Some((_, '0'..='9')) => {},
                        Some((_, '.')) => return Err(Error::unexpected_format(
                            format_args!("{}: extra decimal dot \
                                in number",
                                self.position))),
                        _ => return Err(Error::unexpected_format(
                            format_args!("{}: optional `+` or `-` \
                                followed by digits must \
                                follow `e` in float const",
                                self.position))),
                    }
                }
                _ => return Err(Error::unexpected_format(
                    format_args!("{}: optional `+` or `-` \
                        followed by digits must \
                        follow `e` in float const",
                        self.position))),
            }
            loop {
                match iter.next() {
                    Some((_, '0'..='9')) => continue,
                    Some((_, '.')) => return Err(Error::unexpected_format(
                        format_args!("{}: extra decimal dot in number",
                            self.position))),
                    Some((idx, c)) if c.is_alphabetic() => {
                        suffix = Some(idx+1);
                        break;
                    }
                    Some((idx, _)) => return Ok((FloatConst, idx+1)),
                    None => return Ok((FloatConst, self.buf.len() - self.off)),
                }
            }
        }
        let soff = suffix.expect("tokenizer integrity error");
        let end = loop {
            if let Some((idx, c)) = iter.next() {
                if c != '_' && !c.is_alphanumeric() {
                    break idx+1;
                }
            } else {
                break self.buf.len() - self.off;
            }
        };
        let suffix = &self.buf[self.off+soff..self.off+end];
        if suffix == "n" {
            if float {
                return Ok((DecimalConst, end));
            } else {
                return Ok((BigIntConst, end));
            }
        } else {
            let suffix = if suffix.len() > 8 {
                Cow::Owned(format!("{}...", &suffix[..8]))
            } else {
                Cow::Borrowed(suffix)
            };
            let val = if soff < 20 {
                &self.buf[self.off..soff]
            } else {
                "123"
            };
            if suffix.chars().next() == Some('O') {
                return Err(Error::unexpected_format(
                    format_args!("{}: suffix {:?} is invalid for \
                        numbers, perhaps mixed up letter `O` \
                        with zero `0`?",
                        self.position, suffix)));
            } else if float {
                return Err(Error::unexpected_format(
                    format_args!("{}: suffix {:?} is invalid for \
                        numbers, perhaps you wanted `{}n` (decimal)?",
                        self.position, suffix, val)));
            } else {
                return Err(Error::unexpected_format(
                    format_args!("{}: suffix {:?} is invalid for \
                        numbers, perhaps you wanted `{}n` (bigint)?",
                        self.position, suffix, val)));
            }
        }
    }

    fn skip_whitespace(&mut self) {
        let mut iter = self.buf[self.off..].char_indices();
        let idx = loop {
            let (idx, cur_char) = match iter.next() {
                Some(pair) => pair,
                None => break self.buf.len() - self.off,
            };
            match cur_char {
                '\u{feff}' | '\r' => continue,
                '\t' => self.position.column += 8,
                '\n' => {
                    self.position.column = 1;
                    self.position.line += 1;
                }
                // comma is also entirely ignored in spec
                ' ' => {
                    self.position.column += 1;
                    continue;
                }
                //comment
                '#' => {
                    while let Some((_, cur_char)) = iter.next() {
                        if cur_char == '\r' || cur_char == '\n' {
                            self.position.column = 1;
                            self.position.line += 1;
                            break;
                        }
                    }
                    continue;
                }
                _ => break idx,
            }
        };
        self.off += idx;
        self.position.offset += idx as u64;
    }

    fn update_position(&mut self, len: usize) {
        let val = &self.buf[self.off..][..len];
        self.off += len;
        let lines = val.as_bytes().iter().filter(|&&x| x == b'\n').count();
        self.position.line += lines;
        if lines > 0 {
            let line_offset = val.rfind('\n').unwrap()+1;
            let num = val[line_offset..].chars().count();
            self.position.column = num + 1;
        } else {
            let num = val.chars().count();
            self.position.column += num;
        }
        self.position.offset += len as u64;
    }

    fn is_keyword(&mut self, s: &str) -> bool {
        if s.len() > MAX_KEYWORD_LENGTH {
            return false;
        }
        self.keyword_buf.clear();
        self.keyword_buf.push_str(s);
        self.keyword_buf.make_ascii_lowercase();
        match &self.keyword_buf[..] {
            // Reserved keywords
            | "__source__"
            | "__subject__"
            | "__type__"
            | "alter"
            | "and"
            | "anytuple"
            | "anytype"
            | "commit"
            | "configure"
            | "create"
            | "declare"
            | "delete"
            | "describe"
            | "detached"
            | "distinct"
            | "drop"
            | "else"
            | "empty"
            | "exists"
            | "extending"
            | "false"
            | "filter"
            | "for"
            | "function"
            | "group"
            | "if"
            | "ilike"
            | "in"
            | "insert"
            | "introspect"
            | "is"
            | "like"
            | "limit"
            | "module"
            | "not"
            | "offset"
            | "optional"
            | "or"
            | "order"
            | "release"
            | "reset"
            | "rollback"
            | "select"
            | "set"
            | "start"
            | "true"
            | "typeof"
            | "update"
            | "union"
            | "variadic"
            | "with"
            // Future reserved keywords
            | "analyze"
            | "anyarray"
            | "begin"
            | "case"
            | "check"
            | "deallocate"
            | "discard"
            | "do"
            | "end"
            | "execute"
            | "explain"
            | "fetch"
            | "get"
            | "global"
            | "grant"
            | "import"
            | "listen"
            | "load"
            | "lock"
            | "match"
            | "move"
            | "notify"
            | "prepare"
            | "partition"
            | "policy"
            | "raise"
            | "refresh"
            | "reindex"
            | "revoke"
            | "over"
            | "when"
            | "window"
            => true,
            _ => false,
        }
    }
}

impl<'a> fmt::Display for Token<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}[{:?}]", self.value, self.kind)
    }
}
