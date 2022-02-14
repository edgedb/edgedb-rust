use std::collections::{BTreeMap, BTreeSet};
use std::env::args;
use std::fs;

fn find_tag<'x>(template: &'x str, tag: &str) -> (usize, usize, &'x str) {
    let tag_line = format!("// <{}>\n", tag);
    let pos = template
        .find(&tag_line)
        .expect(&format!("missing tag <{}>", tag));
    let indent = template[..pos].rfind("\n").unwrap_or(0) + 1;
    (pos, pos + tag_line.len(), &template[indent..pos])
}

fn find_macro<'x>(template: &'x str, name: &str) -> &'x str {
    let macro_line = format!("macro_rules! {} {{", name);
    let pos = template
        .find(&macro_line)
        .map(|pos| pos + macro_line.len())
        .expect(&format!("missing macro {}", name));
    let body = template[pos..]
        .find("{")
        .map(|x| pos + x + 1)
        .and_then(|open| {
            let mut level = 0;
            for (idx, c) in template[open..].char_indices() {
                match c {
                    '}' if level == 0 => return Some((open, open + idx)),
                    '}' => level -= 1,
                    '{' => level += 1,
                    _ => {}
                }
            }
            None
        })
        .map(|(begin, end)| template[begin..end].trim())
        .expect("invalid macro");
    return body;
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filename = args().skip(1).next().expect("single argument");
    let mut all_errors = Vec::new();
    let mut all_tags = BTreeSet::<&str>::new();
    let data = fs::read_to_string(filename)?;
    for line in data.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let code = u32::from_str_radix(
            &parts
                .next()
                .expect("code always specified")
                .strip_prefix("0x")
                .expect("code contains 0x")
                .replace("_", ""),
            16,
        )
        .expect("code is valid hex");
        let name = parts.next().expect("name always specified");
        let tags: Vec<_> = parts
            .map(|x| x.strip_prefix('#'))
            .collect::<Option<_>>()
            .expect("tags must follow name");
        all_tags.extend(&tags);
        all_errors.push((code, name, tags));
    }

    let tag_masks = all_tags
        .iter()
        .enumerate()
        .map(|(bit, tag)| (tag, 1 << bit as u32))
        .collect::<BTreeMap<_, _>>();

    let tmp_errors = all_errors
        .into_iter()
        .map(|(code, name, tags)| {
            let tags = tags.iter().map(|t| *tag_masks.get(t).unwrap()).sum();
            (code, (name, tags))
        })
        .collect::<BTreeMap<_, _>>();

    let mut all_errors = BTreeMap::<u32, (_, u32)>::new();
    // propagate tags from error superclasses
    for (code, (name, mut tags)) in tmp_errors {
        for (&scode, (_, stags)) in all_errors.iter().rev() {
            let mask_bits = (scode.trailing_zeros() / 8) * 8;
            let mask = 0xFFFFFFFF_u32 << mask_bits;
            if code & mask == scode {
                tags |= stags;
            }
            if mask_bits == 24 {
                // first byte checked no more matches possible
                // (errors are sorted by code)
                break;
            }
        }
        all_errors.insert(code, (name, tags));
    }

    let outfile = "./edgedb-errors/src/kinds.rs";
    let template = fs::read_to_string(outfile)?;
    let mut out = String::with_capacity(template.len() + 100);

    let (_, def_start, indent) = find_tag(&template, "define_tag");
    out.push_str(&template[..def_start]);

    let define_tag = find_macro(&template, "define_tag");
    for (bit, tag) in all_tags.iter().enumerate() {
        out.push_str(indent);
        out.push_str(
            &define_tag
                .replace("$name", tag)
                .replace("$bit", &bit.to_string()),
        );
        out.push('\n');
    }

    let (def_end, _, _) = find_tag(&template, "/define_tag");
    let (_, err_start, indent) = find_tag(&template, "define_error");
    out.push_str(&template[def_end..err_start]);

    let define_err = find_macro(&template, "define_error");
    for (code, (name, tags)) in all_errors.iter() {
        out.push_str(indent);
        out.push_str(
            &define_err
                .replace("$name", name)
                .replace("$code", &format!("0x{:08X}u32", code))
                .replace("$tag_bits", &format!("0x{:08x}", tags)),
        );
        out.push('\n');
    }

    let (err_end, _, _) = find_tag(&template, "/define_error");
    out.push_str(indent);
    out.push_str(&template[err_end..]);

    fs::write(outfile, out)?;

    Ok(())
}
