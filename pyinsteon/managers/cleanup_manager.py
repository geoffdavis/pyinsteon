"""Track a device's All-Link cleanup so nothing is transmitted mid-cleanup.

When an Insteon device is triggered (button press, motion, etc.) it stays awake
through an All-Link cleanup sequence:

    1. All-Link group broadcast
    2. (optional) All-Link cleanup direct to the modem
    3. All-Link cleanup *status report* broadcast   <- cleanup is finished here

Per the Insteon Developers Guide, any transmission during that window aborts the
device's cleanup, so the device ignores it. For battery devices this is why the
keep-awake -- fired immediately on the group broadcast -- never lands and the
queued on-wake commands never run (pyinsteon#83).

This manager holds a per-device ``asyncio.Event`` that is *set* except while the
device is running its cleanup. Outbound handlers await that event before sending,
so nothing is transmitted to a device mid-cleanup. The event is cleared on the
group broadcast (step 1) and set on the cleanup status report (step 3); a timeout
sets it as a fallback in case that final broadcast is lost (broadcasts do not
repeat).
"""
import asyncio
import logging
from typing import Dict, Optional

from pubsub import pub

from ..address import Address
from ..constants import MessageFlagType
from ..topics import ALL_LINK_CLEANUP_STATUS_REPORT
from ..utils import subscribe_topic, unsubscribe_topic

_LOGGER = logging.getLogger(__name__)

# Fallback timeout in case the cleanup status report broadcast is lost. Each linked
# responder adds a cleanup-direct round trip, so scale modestly with the ALDB.
CLEANUP_BASE_TIMEOUT = 2.5
CLEANUP_PER_LINK = 0.5
CLEANUP_MAX_TIMEOUT = 8.0

_BROADCAST_SUFFIX = str(MessageFlagType.ALL_LINK_BROADCAST).lower()  # "all_link_broadcast"

_cleanup_events: Dict[Address, asyncio.Event] = {}


def get_cleanup_event(address) -> Optional[asyncio.Event]:
    """Return the cleanup Event for a device address, or None if unknown.

    Non-Insteon addresses (e.g. X10) never have a cleanup event.
    """
    try:
        key = Address(address)
    except ValueError:
        return None
    return _cleanup_events.get(key)


class CleanUpManager:
    """Track the All-Link cleanup state of a single device."""

    def __init__(self, device):
        """Init the CleanUpManager for a device."""
        self._device = device
        self._address = device.address
        self._cleanup_done = asyncio.Event()
        self._cleanup_done.set()  # not in cleanup initially
        self._timeout_handle = None
        _cleanup_events[self._address] = self._cleanup_done
        subscribe_topic(self._device_message, self._address.id)

    @property
    def cleanup_done(self) -> asyncio.Event:
        """Event that is set unless the device is running its All-Link cleanup."""
        return self._cleanup_done

    def close(self):
        """Unsubscribe and drop the registered cleanup event."""
        unsubscribe_topic(self._device_message, self._address.id)
        self._cancel_timeout()
        _cleanup_events.pop(self._address, None)

    def _device_message(self, topic=pub.AUTO_TOPIC, **kwargs):
        """Watch the device's inbound messages for cleanup start/end."""
        name = topic.getName() if hasattr(topic, "getName") else str(topic)
        if ALL_LINK_CLEANUP_STATUS_REPORT in name:
            self._end_cleanup()
        elif name.endswith(_BROADCAST_SUFFIX):
            self._start_cleanup()

    def _start_cleanup(self):
        """Enter the cleanup window: block outbound and arm the fallback timeout."""
        self._cleanup_done.clear()
        self._cancel_timeout()
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            return
        self._timeout_handle = loop.call_later(self._timeout(), self._end_cleanup)

    def _end_cleanup(self):
        """Exit the cleanup window: allow outbound again."""
        self._cancel_timeout()
        self._cleanup_done.set()

    def _cancel_timeout(self):
        if self._timeout_handle is not None:
            self._timeout_handle.cancel()
            self._timeout_handle = None

    def _timeout(self) -> float:
        """Fallback cleanup duration, scaled by the device's controller links."""
        num_links = 0
        try:
            aldb = self._device.aldb
            num_links = sum(1 for mem in aldb if aldb[mem].is_controller)
        except Exception:  # noqa: BLE001  # ALDB may be empty / not yet loaded
            num_links = 0
        return min(CLEANUP_BASE_TIMEOUT + CLEANUP_PER_LINK * num_links, CLEANUP_MAX_TIMEOUT)
