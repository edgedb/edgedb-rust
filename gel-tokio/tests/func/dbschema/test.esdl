using extension pgvector;

module test {
    scalar type State extending enum<'done', 'waiting', 'blocked'>;

    type Counter {
        required property name -> str {
            constraint std::exclusive;
        }
        required property value -> int32 {
            default := 0;
        }
    }

    global str_val -> str;
    global int_val -> int32;

    type OtpPhoneRequest {
        required phone: str;
        required otp: int32;
        required sent_at: datetime;
        confirmed_at: datetime;
    }
}
