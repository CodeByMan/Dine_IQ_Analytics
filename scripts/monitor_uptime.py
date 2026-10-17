from dineiq.operations.monitor_uptime import availability_percent, main, probe, run

__all__ = ["availability_percent", "main", "probe", "run"]


if __name__ == "__main__":
    raise SystemExit(main())
