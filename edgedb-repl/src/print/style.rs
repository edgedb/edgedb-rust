use std::fmt::Write;
use std::collections::HashMap;
use std::sync::Arc;

use colorful::{Colorful, RGB, Style as TermStyle};


#[derive(Hash, PartialEq, Eq, Debug, Clone, Copy)]
pub enum Style {
    Decorator,
    Comment,
    String,
    Number,
    Constant,
    Punctuation,
    Keyword,
    DeclName,
    Tag,
    Attribute,
    BackslashCommand,
    Error,
}

#[derive(Debug)]
pub struct Styled<T>(T, Style);

#[derive(Debug)]
pub struct Item(RGB, Option<TermStyle>);

#[derive(Debug)]
pub struct Theme {
    items: HashMap<Style, Item>,
}

#[derive(Debug, Clone)]
pub struct Styler(Arc<Theme>);


trait AddStyle: Sized {
    fn decorator(self) -> Styled<Self>;
    fn comment(self) -> Styled<Self>;
    fn string(self) -> Styled<Self>;
    fn number(self) -> Styled<Self>;
    fn constant(self) -> Styled<Self>;
    fn punctuation(self) -> Styled<Self>;
    fn keyword(self) -> Styled<Self>;
    fn decl_name(self) -> Styled<Self>;
    fn tag(self) -> Styled<Self>;
    fn attribute(self) -> Styled<Self>;
}

impl<T: AsRef<str>> AddStyle for T {
    fn decorator(self) -> Styled<Self> {
        Styled(self, Style::Decorator)
    }
    fn comment(self) -> Styled<Self> {
        Styled(self, Style::Comment)
    }
    fn string(self) -> Styled<Self> {
        Styled(self, Style::String)
    }
    fn number(self) -> Styled<Self> {
        Styled(self, Style::Number)
    }
    fn constant(self) -> Styled<Self> {
        Styled(self, Style::Constant)
    }
    fn punctuation(self) -> Styled<Self> {
        Styled(self, Style::Punctuation)
    }
    fn keyword(self) -> Styled<Self> {
        Styled(self, Style::Keyword)
    }
    fn decl_name(self) -> Styled<Self> {
        Styled(self, Style::DeclName)
    }
    fn tag(self) -> Styled<Self> {
        Styled(self, Style::Tag)
    }
    fn attribute(self) -> Styled<Self> {
        Styled(self, Style::Attribute)
    }
}

impl Styler {
    pub fn dark_256() -> Styler {
        use self::Style::*;
        use colorful::Style::*;

        let mut t = HashMap::new();
        t.insert(Decorator, Item(RGB::new(0xaf, 0x5f, 0x00), None));
        t.insert(Comment, Item(RGB::new(0x56, 0x56, 0x56), Some(Bold)));
        t.insert(String, Item(RGB::new(0x4a, 0xa3, 0x36), None));
        t.insert(Number, Item(RGB::new(0xaf, 0x5f, 0x5f), None));
        t.insert(Constant, Item(RGB::new(0x1d, 0xbd, 0xd0), None));
        t.insert(Punctuation, Item(RGB::new(0xa7, 0xa9, 0x63), None));
        t.insert(Keyword, Item(RGB::new(0x1d, 0xbd, 0xd0), None));
        t.insert(DeclName, Item(RGB::new(0xbc, 0x74, 0xd7), Some(Bold)));
        t.insert(Tag, Item(RGB::new(0x1d, 0xbd, 0xd0), None));
        t.insert(Comment, Item(RGB::new(0x56, 0x56, 0x56), None));
        t.insert(BackslashCommand,
            Item(RGB::new(0xbc, 0x74, 0xd7), Some(Bold)));
        t.insert(Error,
            Item(RGB::new(0xff, 0x40, 0x40), Some(Bold)));

        return Styler(Arc::new(Theme {
            items: t,
        }));
    }
    pub fn apply(&self, style: Style, data: &str, buf: &mut String) {
        if let Some(Item(col, style)) = self.0.items.get(&style) {
            let mut c = data.color(*col);
            if let Some(s) = style {
                c = c.style(*s);
            }
            write!(buf, "{}", c).unwrap();
        } else {
            buf.push_str(data);
        }
    }
}
