use async_std::sync::{Sender, Receiver};

use crate::prompt;
use crate::print;


pub struct State {
    pub control: Sender<prompt::Control>,
    pub data: Receiver<prompt::Input>,
    pub print: print::Config,
}


impl State {
    pub async fn edgeql_input(&mut self, database: &str, initial: &str)
        -> prompt::Input
    {
        self.control.send(
            prompt::Control::EdgeqlInput {
                database: database.to_owned(),
                initial: initial.to_owned(),
            }
        ).await;
        match self.data.recv().await {
            None | Some(prompt::Input::Eof) => prompt::Input::Eof,
            Some(x) => x,
        }
    }
    pub async fn variable_input(&mut self,
        name: &str, type_name: &str, initial: &str)
        -> prompt::Input
    {
        self.control.send(
            prompt::Control::VariableInput {
                name: name.to_owned(),
                type_name: type_name.to_owned(),
                initial: initial.to_owned(),
            }
        ).await;
        match self.data.recv().await {
            None | Some(prompt::Input::Eof) => prompt::Input::Eof,
            Some(x) => x,
        }
    }
    pub async fn vi_mode(&self) {
        self.control.send(prompt::Control::ViMode).await;
    }
    pub async fn emacs_mode(&self) {
        self.control.send(prompt::Control::EmacsMode).await;
    }
}
