//! What a hub calls when the relay sent something, so an app that draws frames only when asked
//! (egui) takes the message in now rather than on its next scheduled frame.
//!
//! [`crate::Hub`] stays polled: the callback only says "poll me". The browser hub fires it from
//! its socket handlers (`WebHub::set_wake`); a hub without one is polled on the app's own
//! schedule, as before.

use std::cell::RefCell;
use std::rc::Rc;

/// The callback a [`Wake`] runs.
type Callback = Rc<dyn Fn()>;

/// A shared, replaceable "something arrived" callback. Cloning shares it.
#[derive(Clone, Default)]
pub struct Wake(Rc<RefCell<Option<Callback>>>);

impl Wake {
    /// Call `f` from now on whenever [`Wake::fire`] runs, in place of any earlier callback.
    pub fn set(&self, f: impl Fn() + 'static) {
        *self.0.borrow_mut() = Some(Rc::new(f));
    }

    /// Stop calling back.
    pub fn clear(&self) {
        *self.0.borrow_mut() = None;
    }

    /// Whether a callback is set.
    pub fn is_set(&self) -> bool {
        self.0.borrow().is_some()
    }

    /// Run the callback, if one is set. The callback may set or clear this wake itself.
    pub fn fire(&self) {
        let f = self.0.borrow().clone();
        if let Some(f) = f {
            f();
        }
    }
}

impl std::fmt::Debug for Wake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wake").field("set", &self.is_set()).finish()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::Wake;

    #[test]
    fn fire_without_a_callback_does_nothing() {
        let w = Wake::default();
        assert!(!w.is_set());
        w.fire();
    }

    #[test]
    fn fire_calls_the_callback_each_time() {
        let n = Rc::new(Cell::new(0));
        let w = Wake::default();
        let m = Rc::clone(&n);
        w.set(move || m.set(m.get() + 1));
        w.fire();
        w.fire();
        assert_eq!(n.get(), 2);
    }

    #[test]
    fn a_clone_shares_the_callback_and_set_replaces_it() {
        let (a, b) = (Rc::new(Cell::new(0)), Rc::new(Cell::new(0)));
        let w = Wake::default();
        let shared = w.clone();
        let x = Rc::clone(&a);
        w.set(move || x.set(x.get() + 1));
        shared.fire();
        let y = Rc::clone(&b);
        shared.set(move || y.set(y.get() + 1));
        w.fire();
        assert_eq!((a.get(), b.get()), (1, 1));
        w.clear();
        shared.fire();
        assert_eq!((a.get(), b.get()), (1, 1));
    }

    #[test]
    fn the_callback_may_clear_itself_while_firing() {
        let n = Rc::new(Cell::new(0));
        let w = Wake::default();
        let (inner, m) = (w.clone(), Rc::clone(&n));
        w.set(move || {
            m.set(m.get() + 1);
            inner.clear();
        });
        w.fire();
        w.fire();
        assert_eq!(n.get(), 1);
    }
}
