#![allow(unused)]

mod py;


#[test]
fn simple_query() -> py::RunResult {
    py::run("\
        from edb.edgeql._edgeql_rust import tokenize\n\
        tokens = list(map(repr, tokenize('SELECT 1+ x;')))\n\
        assert tokens == [
            '<Token SELECT>',
            '<Token ICONST 1>',
            '<Token +>',
            '<Token IDENT \\'x\\'>',
            '<Token ;>',
        ], tokens
    ")
}

#[test]
fn tokenizer_error() -> py::RunResult {
    py::run("\
        from edb.edgeql._edgeql_rust import tokenize, TokenizerError\n\
        try:\n    \
            tokenize('$``')\n\
        except TokenizerError:\n    \
            pass\n\
        else:\n    \
            raise AssertionError('no tokenizer error')\n\
    ")
}

#[test]
fn token_methods() -> py::RunResult {
    py::run("\
        from edb.edgeql._edgeql_rust import tokenize\n\
        tokens = tokenize('SELECT 1+1;')\n\
        assert tokens[1].kind() == 'ICONST', tokens[1].kind()\n\
        assert tokens[1].text() == '1', tokens[1].text()\n\
        assert tokens[1].value() == 1, tokens[1].value()\n\
        assert tokens[1].start() == (1, 8, 7), tokens[1].start()\n\
        assert tokens[1].end() == (1, 9, 8), tokens[1].end()\n\
    ")
}
