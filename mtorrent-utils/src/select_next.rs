use futures_util::{Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Wait for the next item from any of the streams in a keyed container, e.g.
/// `HashMap<K, impl Stream + Unpin>`.
///
/// The returned [`SelectNext`] resolves to:
/// - `None` if the container is empty,
/// - `Some((key, Some(item)))` when the stream at `key` yields `item`,
/// - `Some((key, None))` when the stream at `key` has ended.
///
/// Streams are polled starting from a random position in the container's iteration order, so
/// that no stream is systematically prioritized. Note that this is not strictly fair: a ready
/// stream preceded by many pending streams is more likely to be selected than one preceded by
/// other ready streams.
///
/// The container is borrowed mutably for as long as the returned future exists, i.e. it can only
/// be modified after the future has completed or been dropped.
pub fn select_next<T>(container: &mut T) -> SelectNext<'_, T> {
    SelectNext(container)
}

/// Future (and stream) returned by [`select_next`].
///
/// As a [`Stream`] it yields the same items as the future, and ends when the container is empty.
#[derive(Debug)]
pub struct SelectNext<'a, T>(&'a mut T);

impl<'a, T, K, V> Future for SelectNext<'a, T>
where
    K: Clone,
    V: Stream + Unpin,
    for<'r> &'r mut T: IntoIterator<Item = (&'r K, &'r mut V)>,
    for<'r> <&'r mut T as IntoIterator>::IntoIter: ExactSizeIterator,
{
    type Output = Option<(K, Option<<V as Stream>::Item>)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        poll_select(self.0, cx)
    }
}

impl<'a, T, K, V> Stream for SelectNext<'a, T>
where
    K: Clone,
    V: Stream + Unpin,
    for<'r> &'r mut T: IntoIterator<Item = (&'r K, &'r mut V)>,
    for<'r> <&'r mut T as IntoIterator>::IntoIter: ExactSizeIterator,
{
    type Item = (K, Option<<V as Stream>::Item>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        poll_select(self.0, cx)
    }
}

fn poll_select<T, K, V>(
    container: &mut T,
    cx: &mut Context<'_>,
) -> Poll<Option<(K, Option<<V as Stream>::Item>)>>
where
    K: Clone,
    V: Stream + Unpin,
    for<'r> &'r mut T: IntoIterator<Item = (&'r K, &'r mut V)>,
    for<'r> <&'r mut T as IntoIterator>::IntoIter: ExactSizeIterator,
{
    let len = container.into_iter().len();
    if len == 0 {
        Poll::Ready(None)
    } else {
        let start_ind = rand::random_range(0..len);

        for (key, stream) in container.into_iter().skip(start_ind) {
            if let Poll::Ready(item) = stream.poll_next_unpin(cx) {
                return Poll::Ready(Some((key.clone(), item)));
            }
        }
        for (key, stream) in container.into_iter().take(start_ind) {
            if let Poll::Ready(item) = stream.poll_next_unpin(cx) {
                return Poll::Ready(Some((key.clone(), item)));
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use local_async_utils::prelude::*;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use tokio_test::task::spawn;
    use tokio_test::{assert_pending, assert_ready, assert_ready_eq};

    #[test]
    fn test_empty_container_yields_none() {
        let mut streams = HashMap::<u32, local_bounded::Receiver<usize>>::new();

        assert_ready_eq!(spawn(select_next(&mut streams)).poll(), None);
        assert_ready_eq!(spawn(select_next(&mut streams)).poll_next(), None);
    }

    #[test]
    fn test_yield_item_with_its_key() {
        let mut streams = HashMap::new();
        let (mut tx1, rx1) = local_bounded::channel::<usize>(1);
        let (_tx2, rx2) = local_bounded::channel::<usize>(1);
        streams.insert(1, rx1);
        streams.insert(2, rx2);

        tx1.try_send(42).unwrap();

        assert_ready_eq!(spawn(select_next(&mut streams)).poll(), Some((1, Some(42))));
        assert_pending!(spawn(select_next(&mut streams)).poll());
    }

    #[test]
    fn test_yield_none_item_for_ended_stream() {
        let mut streams = BTreeMap::new();
        let (tx, rx) = local_bounded::channel::<usize>(1);
        streams.insert("a", rx);

        drop(tx);

        assert_ready_eq!(spawn(select_next(&mut streams)).poll(), Some(("a", None)));
    }

    #[test]
    fn test_woken_when_pending_stream_becomes_ready() {
        let mut streams = HashMap::new();
        let (mut tx1, rx1) = local_bounded::channel::<usize>(1);
        let (_tx2, rx2) = local_bounded::channel::<usize>(1);
        streams.insert(1, rx1);
        streams.insert(2, rx2);

        let mut select = spawn(select_next(&mut streams));
        assert_pending!(select.poll());

        tx1.try_send(7).unwrap();
        assert!(select.is_woken());
        assert_ready_eq!(select.poll(), Some((1, Some(7))));
    }

    #[test]
    fn test_all_ready_streams_are_selected() {
        let mut streams = HashMap::new();
        let mut senders = Vec::new();
        for key in 0..10 {
            let (tx, rx) = local_bounded::channel::<usize>(1);
            streams.insert(key, rx);
            senders.push(tx);
        }
        for (key, tx) in senders.iter_mut().enumerate() {
            tx.try_send(key).unwrap();
        }

        let mut selected = HashSet::new();
        for _ in 0..10 {
            let (key, item) = assert_ready!(spawn(select_next(&mut streams)).poll()).unwrap();
            assert_eq!(item, Some(key));
            assert!(selected.insert(key), "stream {key} selected twice");
        }
        assert_pending!(spawn(select_next(&mut streams)).poll());
    }

    #[test]
    fn test_stream_yields_items_until_container_is_empty() {
        let mut streams = HashMap::new();
        let (mut tx, rx) = local_bounded::channel::<usize>(2);
        streams.insert(1, rx);

        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        drop(tx);

        let mut select = spawn(select_next(&mut streams));
        assert_ready_eq!(select.poll_next(), Some((1, Some(1))));
        assert_ready_eq!(select.poll_next(), Some((1, Some(2))));
        assert_ready_eq!(select.poll_next(), Some((1, None)));
    }
}
