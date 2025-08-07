use mtorrent_utils::connctrl::{ConnectControl, ConnectPermit, QuickConnectControl};
use std::net::SocketAddr;

pub fn connection_control<D>(
    connection_limit: usize,
    connection_data: D,
) -> (OutgoingConnectionControl<D>, IncomingConnectionControl<D>) {
    let cc = ConnectControl::new(connection_limit, connection_data);
    let icc = cc.split_off();
    (OutgoingConnectionControl(cc), IncomingConnectionControl(icc))
}

#[cfg_attr(test, derive(Debug))]
pub struct OutgoingConnectionPermit<D>(pub(super) ConnectPermit<D>);
#[cfg_attr(test, derive(Debug))]
pub struct IncomingConnectionPermit<D>(pub(super) ConnectPermit<D>);

pub struct OutgoingConnectionControl<D>(ConnectControl<D>);

impl<D> OutgoingConnectionControl<D> {
    /// Wait for available connection slot or return None if already connected to 'addr'
    pub async fn issue_permit(&mut self, addr: SocketAddr) -> Option<OutgoingConnectionPermit<D>> {
        self.0.acquire_permit(addr).await.map(OutgoingConnectionPermit)
    }
}

pub struct IncomingConnectionControl<D>(QuickConnectControl<D>);

impl<D> IncomingConnectionControl<D> {
    /// Issue immediate permit if a connection slot is available
    pub fn issue_permit(&mut self, addr: SocketAddr) -> Option<IncomingConnectionPermit<D>> {
        self.0.try_acquire_permit(addr).map(IncomingConnectionPermit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use tokio_test::task::spawn;
    use tokio_test::{assert_pending, assert_ready};
    macro_rules! addr {
        ($addr:literal) => {
            SocketAddr::from_str($addr).unwrap()
        };
    }

    #[test]
    fn test_incoming_control_respects_budget() {
        let (_out_ctrl, mut in_ctrl) = connection_control(2, ());

        // when
        let permit1 = in_ctrl.issue_permit(addr!("1.2.3.4:1111"));
        assert!(permit1.is_some());
        let permit2 = in_ctrl.issue_permit(addr!("1.2.3.4:1112"));
        assert!(permit2.is_some());

        // then
        let permit3 = in_ctrl.issue_permit(addr!("1.2.3.4:1113"));
        assert!(permit3.is_none());

        // when
        drop(permit2);

        // then
        let permit3 = in_ctrl.issue_permit(addr!("1.2.3.4:1113"));
        assert!(permit3.is_some());
    }

    #[test]
    fn test_notify_when_outgoing_connection_drops() {
        let (mut out_ctrl, _in_ctrl) = connection_control(1, ());

        let permit1 = {
            let mut fut1 = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:1111")));
            assert_ready!(fut1.poll())
        };
        assert!(permit1.is_some());

        let mut fut2 = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:2222")));
        assert_pending!(fut2.poll());

        drop(permit1);
        assert!(fut2.is_woken());
        let permit2 = assert_ready!(fut2.poll());
        assert!(permit2.is_some(), "permit denied");
    }

    #[test]
    fn test_notify_when_incoming_connection_drops() {
        let (mut out_ctrl, mut in_ctrl) = connection_control(1, ());
        let permit1 = in_ctrl.issue_permit(addr!("1.2.3.4:1112"));
        assert!(permit1.is_some());

        let mut fut2 = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:1111")));
        assert_pending!(fut2.poll());

        drop(permit1);
        assert!(fut2.is_woken());
        let permit2 = assert_ready!(fut2.poll());
        assert!(permit2.is_some(), "permit denied");
    }

    #[test]
    fn test_outgoing_control_respects_uniqueness() {
        let (mut out_ctrl, mut in_ctrl) = connection_control(10, ());

        // when
        let permit1 = {
            let mut fut1 = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:1111")));
            assert_ready!(fut1.poll())
        };
        assert!(permit1.is_some());

        // then
        let permit1_dup = {
            let mut fut1 = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:1111")));
            assert_ready!(fut1.poll())
        };
        assert!(permit1_dup.is_none());

        // when
        let permit2 = {
            let mut fut2 = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:2222")));
            assert_ready!(fut2.poll())
        };
        assert!(permit2.is_some());

        // then
        let permit2_dup = {
            let mut fut2_dup = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:2222")));
            assert_ready!(fut2_dup.poll())
        };
        assert!(permit2_dup.is_none());

        // when
        let permit3 = in_ctrl.issue_permit(addr!("1.2.3.4:3333"));
        assert!(permit3.is_some());

        // then
        let permit3_dup = {
            let mut fut3_dup = spawn(out_ctrl.issue_permit(addr!("1.2.3.4:3333")));
            assert_ready!(fut3_dup.poll())
        };
        assert!(permit3_dup.is_none());
    }
}
