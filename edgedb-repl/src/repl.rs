use async_std::sync::Sender;

use crate::prompt;
use crate::print;


pub struct State {
    pub control: Sender<prompt::Control>,
    pub print: print::Config,
}
