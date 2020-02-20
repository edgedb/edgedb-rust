use edgeql_parser::helpers::{quote_string, quote_name};
use crate::commands::Options;
use crate::commands::helpers::print_result;
use crate::client::Client;
use crate::options::{RoleParams};


fn process_params(options: &RoleParams) -> Result<Vec<String>, anyhow::Error> {
    let mut result = Vec::new();
    if options.password || options.password_from_stdin {
        let password = if options.password_from_stdin {
            rpassword::read_password()?
        } else {
            loop {
                let password = rpassword::read_password_from_tty(
                    Some(&format!("New password for '{}': ",
                                  options.role.escape_default())))?;
                let confirm = rpassword::read_password_from_tty(
                    Some(&format!("Confirm password for '{}': ",
                                  options.role.escape_default())))?;
                if password != confirm {
                    eprintln!("Password don't match");
                } else {
                    break password;
                }
            }
        };
        result.push(format!("SET password := {}", quote_string(&password)));
    }
    Ok(result)
}

pub async fn create_superuser(cli: &mut Client<'_>, _options: &Options,
    role: &RoleParams)
    -> Result<(), anyhow::Error>
{
    let params = process_params(role)?;
    if params.is_empty() {
        print_result(cli.execute(
            &format!("CREATE SUPERUSER ROLE {}", quote_name(&role.role))
        ).await?);
    } else {
        print_result(cli.execute(
            &format!(r###"
                CREATE SUPERUSER ROLE {name} {{
                    {params}
                }}"###,
                name=quote_name(&role.role),
                params=params.join(";\n"))
        ).await?);
    }
    Ok(())
}

pub async fn alter(cli: &mut Client<'_>, _options: &Options,
    role: &RoleParams)
    -> Result<(), anyhow::Error>
{
    let params = process_params(role)?;
    if params.is_empty() {
        return Err(anyhow::anyhow!("Please specify attribute to alter"));
    } else {
        print_result(cli.execute(
            &format!(r###"
                ALTER ROLE {name} {{
                    {params}
                }}"###,
                name=quote_name(&role.role),
                params=params.join(";\n"))
        ).await?);
    }
    Ok(())
}

pub async fn drop(cli: &mut Client<'_>, _options: &Options,
    name: &str)
    -> Result<(), anyhow::Error>
{
    print_result(cli.execute(
        &format!("DROP ROLE {}", quote_name(name))
    ).await?);
    Ok(())
}
