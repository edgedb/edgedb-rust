use async_std::sync::{Sender, Receiver};

use crate::prompt;
use crate::print;


pub struct State {
    pub control: Sender<prompt::Control>,
    pub data: Receiver<prompt::Input>,
    pub print: print::Config,
    pub verbose_errors: bool,
    pub last_error: Option<anyhow::Error>,
    pub database: String,
}


impl State {
    pub async fn edgeql_input(&mut self, initial: &str) -> prompt::Input {
        self.control.send(
            prompt::Control::EdgeqlInput {
                database: self.database.clone(),
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
    pub async fn show_history(&self) {
        self.control.send(prompt::Control::ShowHistory).await;
    }
    pub async fn spawn_editor(&self, entry: Option<isize>) -> prompt::Input {
        self.control.send(prompt::Control::SpawnEditor { entry }).await;
        match self.data.recv().await {
            None | Some(prompt::Input::Eof) => prompt::Input::Eof,
            Some(x) => x,
        }
    }
}
