#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let conn = edgedb_tokio::create_client().await?;

    let num_res: i32 = conn.query_required_single("select {<int32>$0 + <int32>$1}", &(10, 20)).await?;
    println!("{num_res}");

    let name = "Gadget";
    let title = "Inspector";
    let val = conn.query_required_single::<String, _>("with person := 
        (name := <str>$0, title := <str>$1)
         select person.title ++ ' ' ++ person.name;", &(name, title)).await?;
    println!("And person's name was... {}!", val);

    Ok(())
}
