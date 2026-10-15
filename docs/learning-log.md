# Learning log

One entry per week. Each entry is raw material for a romano.io post.

## Template

### Week N — <topic>
- **Built:**
- **Surprised me (vs. SQL Server / .NET):**
- **Broke and how I fixed it:**
- **Numbers:** (rows, runtime, cost)
- **Post angle:**

---

### Week 1 — Python for the .NET developer
- **Built:** 2026-10-15: `Reading`, `interpolate_at`, `floor_15`, `ceil_15`, and `interpolate_angle` in `src/river_nav/transforms.py`, with `tests/test_interpolate.py`. Nine tests, each added only after the previous one was green. `make check`: 27 passed, ruff clean. No Spark yet. The gold pipeline still only floors an observed minute into `time_key`; mirroring these rules there is the next session.
- **Surprised me (vs. SQL Server / .NET):** `bisect_right` on the sorted timestamps is `SELECT TOP 1 … WHERE observed_at <= @t ORDER BY observed_at DESC`. The index it returns is the last equal timestamp, so the next neighbor is always strictly later. That is the procedure's `p2.time > p1.time` guard. `bisect_left` would stop on the first duplicate and divide by a zero-length span. A point before the first reading (`bisect_right - 1 < 0`) is the `WHERE p1.Id IS NOT NULL` filter, and it was already true once the halfway branch existed, so that test never went red. Same for duplicates. Python's `%` on a negative number is what makes the angle wrap work: `(-165) % 360` is `195`, not `-165`. Null arithmetic is the other shock. In C#, `float?` lifts the operators, so `(float?)null - 8f` is `null` and does not throw. `value: float | None` only tells the type checker; at runtime `8.0 - None` is a `TypeError`. The `is None` guard before the blend is the check `float?` was doing for free.
- **Broke and how I fixed it:** `ceil_15` always added 15 minutes, so 10:15:00 became 10:30:00. Floor first, and add `timedelta(minutes=15)` only when the floor is earlier. `timedelta` also rolls 10:45:30 to 11:00:00; `replace(minute=60)` would throw. A `None` on either side of a blend raised `TypeError` on `8.0 - None`. Guard before the arithmetic and return `None`. A straight average of 355° and 10° is 182.5°, the long way around. The short arc is +15°, so halfway is 2.5°. That stays in `interpolate_angle`. Stage in feet does not wrap.
- **Numbers:** Nine new tests, about 0.08s for the whole suite. Synthetic readings only (10:00 = 4.0, 10:30 = 8.0, grid point 10:15 = 6.0). The Corps Locks traffic file in `data/raw` is 211 objects with string times like `"09/20/26 16:12"`. Nothing there is a gauge reading, so it never entered these tests.
- **Post angle:** C# ↔ Python cheat sheet for senior .NET devs. Today's example is porting a `TOP 1` interpolation procedure: one failing assertion, then the smallest branch that turns it green. `bisect_right` replaces the `ORDER BY`, `float?` versus `float | None` is the null row, and two tests that never fail are the lesson that the guard from the previous rule was already the next rule.
