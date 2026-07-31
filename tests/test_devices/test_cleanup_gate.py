"""Reproduce pyinsteon#83: nothing may be transmitted to a device while it is
running its all-link cleanup process.

When a battery device is triggered it stays awake through a cleanup sequence:
    1. all-link group broadcast
    2. (optional) all-link cleanup direct to the modem
    3. all-link cleanup *status report* broadcast  <- cleanup is finished here
Per the Insteon Developers Guide, any transmission during that window aborts the
device's cleanup, so the device ignores it. Today pyinsteon fires the battery
keep-awake immediately on the group broadcast (step 1), the device ignores it,
and the queued on-wake commands never run.

Desired behavior: an outbound command is deferred until the cleanup status report
(or a timeout), then sent once.
"""
import asyncio
import unittest

from pyinsteon import pub
from pyinsteon.device_types.ipdb import GeneralController_MiniRemote_4

from tests import set_log_levels
from tests.utils import (
    TopicItem,
    async_case,
    async_protocol_manager,
    cmd_kwargs,
    random_address,
    send_topics,
)


class TestNoSendDuringCleanup(unittest.TestCase):
    """A device must not be sent anything mid all-link cleanup."""

    def setUp(self):
        """Set up the test."""
        self.sent = []
        set_log_levels(logger="info", logger_pyinsteon="info",
                       logger_messages="info", logger_topics=False)

    def tearDown(self):
        """Tear down the test."""
        pub.unsubAll("send")

    def _record_send(self, **kwargs):
        """Record any outbound transmission (bubbles up from send.* topics)."""
        self.sent.append(kwargs)

    @async_case
    async def test_outbound_deferred_until_cleanup_report(self):
        """Keep-awake sent during cleanup is held until the cleanup report."""
        async with async_protocol_manager():
            addr = random_address()
            device = GeneralController_MiniRemote_4(
                address=addr, cat=0x00, subcat=0x10, description="Mini Remote"
            )
            await asyncio.sleep(0.1)
            pub.subscribe(self._record_send, "send")

            # 1. device wakes / enters cleanup with a group-1 ON broadcast
            broadcast = TopicItem(
                f"{addr.id}.1.on.all_link_broadcast",
                cmd_kwargs(0x11, 0x00, None, target="000001", hops_left=3),
                0,
            )
            send_topics([broadcast])
            await asyncio.sleep(0.1)

            # 2. try to talk to the device WHILE it is in cleanup
            task = asyncio.ensure_future(device.async_keep_awake())
            await asyncio.sleep(0.3)
            assert not self.sent, f"transmitted during cleanup (aborts it): {self.sent}"

            # 3. device finishes cleanup -> status report broadcast
            report = TopicItem(
                f"{addr.id}.all_link_cleanup_status_report.all_link_broadcast", {}, 0
            )
            send_topics([report])
            await asyncio.sleep(0.3)
            assert self.sent, "command was not transmitted after cleanup completed"

            task.cancel()

    @async_case
    async def test_battery_keepawake_deferred_via_wake_path(self):
        """The real #83 path: a battery on-wake command is queued, the device wakes
        with its group broadcast (which both starts cleanup and triggers the battery
        keep-awake), and the keep-awake must be deferred until the cleanup report.

        This exercises the ordering: the broadcast dispatches to both the battery's
        ``_device_awake`` (which schedules ``_ensure_commands`` -> keep-awake) and the
        CleanUpManager; the event must be cleared synchronously so the deferred
        keep-awake is gated.
        """
        from pyinsteon.constants import ResponseStatus

        async with async_protocol_manager():
            addr = random_address()
            device = GeneralController_MiniRemote_4(
                address=addr, cat=0x00, subcat=0x10, description="Mini Remote"
            )
            await asyncio.sleep(0.1)
            pub.subscribe(self._record_send, "send")

            # queue an on-wake command, exactly as ALDBBattery.async_load does
            async def queued():
                return ResponseStatus.SUCCESS

            device._run_on_wake(queued)

            # device wakes: group broadcast -> starts cleanup AND fires _device_awake
            send_topics([TopicItem(
                f"{addr.id}.1.on.all_link_broadcast",
                cmd_kwargs(0x11, 0x00, None, target="000001", hops_left=3), 0)])
            await asyncio.sleep(0.3)
            keepawake = [s for s in self.sent if s.get("data2") == 0x04]
            assert not keepawake, f"battery keep-awake sent during cleanup: {keepawake}"

            # cleanup finishes -> keep-awake may now be transmitted
            send_topics([TopicItem(
                f"{addr.id}.all_link_cleanup_status_report.all_link_broadcast", {}, 0)])
            await asyncio.sleep(0.3)
            keepawake = [s for s in self.sent if s.get("data2") == 0x04]
            assert keepawake, "battery keep-awake not sent after cleanup completed"


if __name__ == "__main__":
    unittest.main()
