pub trait Shared<T>: Clone {
    fn with<R, F>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R;
}

#[allow(dead_code)]
pub trait WeakShared<T>: Clone {
    fn with<R, F>(&mut self, f: F) -> R
    where
        F: FnOnce(Option<&mut T>) -> R;
}
