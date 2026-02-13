"""
Async Market Data Queue Manager

This module defines asyncio queues for real-time market data feeds. These queues
are used to decouple data producers from consumers in an asynchronous event-driven architecture.

Queues:
- trade_queue: Holds trade updates from market feeds.
- quote_queue: Holds quote updates from market feeds.
- ref_px_queue: Holds reference price updates.

Producers push normalized market data into these queues, and consolidators
consume them to buffer, deduplicate, and store data efficiently.
"""

import asyncio

trade_queue = asyncio.Queue()
quote_queue = asyncio.Queue()
ref_px_queue = asyncio.Queue()

