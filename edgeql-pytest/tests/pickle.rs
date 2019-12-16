mod py;

#[test]
fn pickle_tokenizer_error() -> py::RunResult {
    py::run("\
        import pickle\n\
        from edb._edgeql_rust import TokenizerError\n\
        pickle.loads(pickle.dumps(TokenizerError('error')))\n\
    ")
}
