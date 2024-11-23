use crate::utils::shared::{Shared, WeakShared};
use std::cell::RefCell;
use std::rc::Rc;

pub struct SharedHandle<T> {
    inner: Rc<RefCell<T>>,
}

impl<T> SharedHandle<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Rc::new(RefCell::new(inner)),
        }
    }

    pub fn project<R, F>(&self, f: F) -> impl Shared<R>
    where
        F: Fn(&mut T) -> &mut R + Clone,
    {
        ProjectingHandle {
            inner: self.inner.clone(),
            proj: f,
        }
    }

    pub fn project_weak<R, F>(&self, f: F) -> impl WeakShared<R>
    where
        F: Fn(&mut T) -> Option<&mut R> + Clone,
    {
        ProjectingHandle {
            inner: self.inner.clone(),
            proj: f,
        }
    }
}

impl<T> Shared<T> for SharedHandle<T> {
    #[inline(always)]
    fn with<R, F>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        f(&mut self.inner.borrow_mut())
    }
}

pub struct ProjectingHandle<T, F> {
    inner: Rc<RefCell<T>>,
    proj: F,
}

impl<T, Proj, Op> Shared<Proj> for ProjectingHandle<T, Op>
where
    Op: Fn(&mut T) -> &mut Proj + Clone,
{
    #[inline(always)]
    fn with<R, F>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut Proj) -> R,
    {
        let mut borrowed = self.inner.borrow_mut();
        let project_fn = &self.proj;
        f(project_fn(&mut borrowed))
    }
}

impl<T, Proj, Op> WeakShared<Proj> for ProjectingHandle<T, Op>
where
    Op: Fn(&mut T) -> Option<&mut Proj> + Clone,
{
    #[inline(always)]
    fn with<R, F>(&mut self, f: F) -> R
    where
        F: FnOnce(Option<&mut Proj>) -> R,
    {
        let mut borrowed = self.inner.borrow_mut();
        let project_fn = &self.proj;
        f(project_fn(&mut borrowed))
    }
}

impl<T> Clone for SharedHandle<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T, F: Clone> Clone for ProjectingHandle<T, F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            proj: self.proj.clone(),
        }
    }
}
