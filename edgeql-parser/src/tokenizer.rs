use std::fmt;

use combine::{StreamOnce, Positioned};
use combine::error::{StreamError};
use combine::stream::{ResetStream};
use combine::easy::{Error, Errors};

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
    FloorDivision,    // //
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
    BinAnd,           // &
    BinOr,            // |
    DecimalConst,
    FloatConst,
    IntConst,
    BinStr,           // b"xx", b'xx'
    Str,              // "xx", 'xx', r"xx", r'xx', $$xx$$
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
    next_state: Option<(usize, Token<'a>, usize, Pos)>,
    keyword_buf: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Checkpoint {
    position: Pos,
    off: usize,
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
        }
    }
    fn reset(&mut self, checkpoint: Checkpoint) -> Result<(), Self::Error> {
        self.position = checkpoint.position;
        self.off = checkpoint.off;
        Ok(())
    }
}

impl<'a> TokenStream<'a> {
    pub fn new(s: &str) -> TokenStream {
        let mut me = TokenStream {
            buf: s,
            position: Pos { line: 1, column: 1, offset: 0 },
            off: 0,
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
                _ => return Ok((Sub, 1)),
            },
            '>' => match iter.next() {
                Some((_, '=')) => return Ok((GreaterEq, 2)),
                _ => return Ok((Greater, 1)),
            },
            '=' => return Ok((Eq, 1)),
            c if c == '_' || c.is_alphabetic() => {
                for (idx, c) in iter {
                    if c != '_' && !c.is_alphanumeric() {
                        let val = &self.buf[self.off..self.off+idx];
                        if self.is_keyword(val) {
                            return Ok((Keyword, idx));
                        } else {
                            return Ok((Ident, idx));
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
            _ => return Err(
                Error::unexpected_format(
                    format_args!("unexpected character {:?}", cur_char)
                )
            ),
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
