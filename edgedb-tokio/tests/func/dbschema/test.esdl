module test {
    type Counter {
        required property name -> str {
            constraint std::exclusive;
        }
        required property value -> int32 {
            default := 0;
        }
    }
}
