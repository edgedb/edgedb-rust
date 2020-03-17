use edgeql_parser::helpers::{quote_string, quote_name};
use crate::commands::Options;
use crate::commands::helpers::print_result;
use crate::client::Client;
use crate::options::{Configure, ConfigStr, AuthParameter, PortParameter};

async fn set_string(cli: &mut Client<'_>, name: &str, value: &ConfigStr)
    -> Result<(), anyhow::Error>
{
    print_result(cli.execute(
        &format!("CONFIGURE SYSTEM SET {} := {}",
            name, quote_string(&value.value))
    ).await?);
    Ok(())
}

pub async fn configure(cli: &mut Client<'_>, _options: &Options,
    cfg: &Configure)
    -> Result<(), anyhow::Error>
{
    use crate::options::ConfigureCommand as C;
    use crate::options::ConfigureInsert as Ins;
    use crate::options::ConfigureReset as Res;
    use crate::options::ListParameter as I;
    use crate::options::ConfigureSet as Set;
    use crate::options::ValueParameter as S;
    match &cfg.command {
        C::Insert(Ins { parameter: I::Auth(param) }) => {
            let AuthParameter { users, comment, priority, method } = param;
            let mut props = vec![
                format!("priority := {}", priority),
                format!("method := (INSERT {})", quote_name(method)),
            ];
            let users = users.iter().map(|x| quote_string(x))
                .collect::<Vec<_>>().join(", ");
            if !users.is_empty() {
                props.push(format!("user := {{ {} }}", users))
            }
            if let Some(ref comment_text) = comment {
                props.push(format!(
                    "comment := {}", quote_string(comment_text)))
            }
            let result = cli.execute(&format!(r###"
                CONFIGURE SYSTEM INSERT Auth {{
                    {}
                }}
                "###,
                props.join(",\n")
            )).await?;
            print_result(result);
            Ok(())
        }
        C::Insert(Ins { parameter: I::Port(param) }) => {
            let PortParameter {
                addresses, port, protocol,
                database, user, concurrency,
            } = param;
            print_result(cli.execute(&format!(r###"
                    CONFIGURE SYSTEM INSERT Port {{
                        address := {{ {addresses} }},
                        port := {port},
                        protocol := {protocol},
                        database := {database},
                        user := {user},
                        concurrency := {concurrency},
                    }}
                "###,
                addresses=addresses.iter().map(|x| quote_string(x))
                    .collect::<Vec<_>>().join(", "),
                port=port,
                concurrency=concurrency,
                protocol=quote_string(protocol),
                database=quote_string(database),
                user=quote_string(user),
            )).await?);
            Ok(())
        }
        C::Set(Set { parameter: S::ListenAddresses(param) }) => {
            print_result(cli.execute(
                &format!("CONFIGURE SYSTEM SET listen_addresses := {{{}}}",
                param.address.iter().map(|x| quote_string(x))
                    .collect::<Vec<_>>().join(", "))
            ).await?);
            Ok(())
        }
        C::Set(Set { parameter: S::ListenPort(param) }) => {
            print_result(cli.execute(
                &format!("CONFIGURE SYSTEM SET listen_port := {}", param.port)
            ).await?);
            Ok(())
        }
        C::Set(Set { parameter: S::SharedBuffers(param) }) => {
            set_string(cli, "shared_buffers", param).await
        }
        C::Set(Set { parameter: S::QueryWorkMem(param) }) => {
            set_string(cli, "query_work_mem", param).await
        }
        C::Set(Set { parameter: S::EffectiveCacheSize(param) }) => {
            set_string(cli, "effective_cache_size", param).await
        }
        C::Set(Set { parameter: S::DefaultStatisticsTarget(param) }) => {
            set_string(cli, "default_statistics_target", param).await
        }
        C::Reset(Res { parameter }) => {
            use crate::options::ConfigParameter as C;
            let name = match parameter {
                C::ListenAddresses => "listen_addresses",
                C::ListenPort => "listen_port",
                C::Port => "Port",
                C::Auth => "Auth",
                C::SharedBuffers => "shared_buffers",
                C::QueryWorkMem => "query_work_mem",
                C::EffectiveCacheSize => "effective_cache_size",
                C::DefaultStatisticsTarget => "default_statistics_target",
            };
            print_result(cli.execute(
                &format!("CONFIGURE SYSTEM RESET {}", name)
            ).await?);
            Ok(())
        }
    }
}
