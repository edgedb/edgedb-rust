mod py;

#[test]
fn pickle_tokenizer_error() -> py::RunResult {
    py::run("\
        import pickle\n\
        from edb._edgeql_rust import TokenizerError\n\
        pickle.loads(pickle.dumps(TokenizerError('error')))\n\
    ")
}

#[test]
fn pickle_token() -> py::RunResult {
    py::run("\
        import pickle\n\
        from edb._edgeql_rust import tokenize\n\
        pickle.loads(pickle.dumps(tokenize('SELECT 1+1;')))\n\
    ")
}
