#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let conn = edgedb_tokio::create_client().await?;

    let no_args: String = conn
        .query_required_single("select {'No args inside here'};", &())
        .await?;
    println!("No args:\n{no_args}");

    let arg = "Hi I'm arg";
    let one_arg: String = conn
        .query_required_single("select {'One arg inside here: ' ++ <str>$0};", &(arg,))
        .await?;
    // Note the comma to indicate this is a tuple:                                                   ^^^^^^^^
    println!("One arg:\n{one_arg}");

    let vec_of_numbers: Vec<i32> = conn
        .query("select {<int32>$0, <int32>$1}", &(10, 20))
        .await?;
    println!("Here are your numbers back: {vec_of_numbers:?}");

    let num_res: i32 = conn
        .query_required_single("select {<int32>$0 + <int32>$1}", &(10, 20))
        .await?;
    println!("Two args added together: {num_res}");

    let name = "Gadget";
    let title = "Inspector";
    let val = conn
        .query_required_single::<String, _>(
            "with person := 
        (name := <str>$0, title := <str>$1)
         select person.title ++ ' ' ++ person.name;",
            &(name, title),
        )
        .await?;
    println!("And person's name was... {}!", val);

    // Arguments must be positional ($0, $1, $2, etc.), not named
    let named_args_fail: Result<i32, _> = conn
        .query_required_single("select {<int32>$num1 + <int32>$num2}", &(10, 20))
        .await;
    assert!(format!("{named_args_fail:?}")
        .contains("expected positional arguments, got num1 instead of 0"));

    Ok(())
}
