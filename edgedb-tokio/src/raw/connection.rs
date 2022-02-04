use crate::raw::ConnInner;
use crate::{Builder, Error};

impl ConnInner {
    pub fn is_consistent(&self) -> bool {
        todo!();
    }
    pub async fn connect(config: &Builder) -> Result<Self, Error> {
        todo!();
    }
}
