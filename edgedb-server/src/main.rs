use structopt::StructOpt;

mod options;

use options::Options;

fn main() {
    let _options = Options::from_args();
}
