use futures::future::{select_all, LocalBoxFuture};
use std::mem;

pub trait Handler<'h> {
    type OperationResult;

    fn first_operations(&mut self) -> Vec<LocalBoxFuture<'h, Self::OperationResult>>;
    fn next_operations(
        &mut self,
        last_operation_result: Self::OperationResult,
    ) -> Option<Vec<LocalBoxFuture<'h, Self::OperationResult>>>;
    fn finished(&self) -> bool;
}

pub struct Dispatcher<'d, H: Handler<'d>> {
    handler: H,
    ops: Vec<LocalBoxFuture<'d, H::OperationResult>>,
}

impl<'d, H: Handler<'d>> Dispatcher<'d, H> {
    pub fn new(mut handler: H) -> Self {
        let ops = handler.first_operations();
        Self { handler, ops }
    }

    pub async fn dispatch_one(&mut self) -> bool {
        if self.ops.is_empty() || self.handler.finished() {
            return false;
        }
        let current_ops = mem::take(&mut self.ops);
        let (finished_result, _finished_index, mut pending_ops) =
            select_all(current_ops.into_iter()).await;
        self.ops.append(&mut pending_ops);
        if let Some(mut next_ops) = self.handler.next_operations(finished_result) {
            self.ops.append(&mut next_ops);
        }
        true
    }
}
