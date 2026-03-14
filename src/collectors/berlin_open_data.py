"""Backward-compatible import path. Use src.collectors.germany.berlin_open_data instead."""

from .germany.berlin_open_data import BerlinOpenDataCollector, BerlinOpenDataCollectorSync

__all__ = ["BerlinOpenDataCollector", "BerlinOpenDataCollectorSync"]
