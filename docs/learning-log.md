# Learning Log

This is the raw record behind my romano.io lakehouse series. I write down what I
built, what broke, and what surprised me. The numbers matter. They keep the story
honest.

## Entry Format

### Week N: Topic

**What I built**

**What surprised me**

**What broke**

**Numbers**

**Possible post**

## Week 1: Python for a .NET Developer

Date: October 15, 2026

### Pass 1: Port the interpolation rules

**What I built**

I started with the old SQL Server interpolation logic. I wrote a pure Python
version before I touched Spark.

The first functions were `Reading`, `interpolate_at`, `floor_15`, `ceil_15`, and
`interpolate_angle`. They live in `src/river_nav/transforms.py`.

I wrote one test at a time. I watched each new test fail before I changed the
code. The suite ended at 27 passing tests. Ruff was clean.

Gold does not use these rules yet. It still puts each observed reading into a
15-minute bucket. That is not interpolation.

**What surprised me**

`bisect_right` is Python's version of this SQL:

```sql
SELECT TOP 1 ...
WHERE observed_at <= @t
ORDER BY observed_at DESC
```

It returns the last matching timestamp. The next item is always later. That
matches the old stored procedure's `p2.time > p1.time` check.

`bisect_left` would return the first duplicate. That can give the code two
readings with the same timestamp. The time span would be zero.

Python handles null math very differently from C#. In C#, `float?` carries the
null through the calculation. In Python, `8.0 - None` throws a `TypeError`. A
type hint does not change runtime behavior. I had to check for `None` before the
calculation.

Python's remainder operator also helped with angles. `(-165) % 360` returns
`195`. That makes the shortest path between 355 degrees and 10 degrees work.

**What broke**

My first `ceil_15` always added 15 minutes. A time of 10:15 became 10:30.

The fix was simple. Floor the time first. Add 15 minutes only when the original
time is later than the floor. `timedelta` also handles the hour change. A time of
10:45:30 becomes 11:00:00.

The first null test also failed. A missing value on either side of the line
caused a `TypeError`. I added a guard and returned `None`.

The first angle formula used a normal average. Halfway between 355 and 10 became
182.5 degrees. That is the long path around the circle. The correct halfway
value is 2.5 degrees.

**Numbers**

I added nine tests. The full suite ran in about 0.08 seconds.

The sample was small on purpose:

- 10:00 = 4.0
- 10:30 = 8.0
- 10:15 = 6.0

I did not use the Corps Locks file for these tests. That file has 211 traffic
records. It does not contain gauge readings.

**Possible post**

Port one `TOP 1` interpolation procedure from T-SQL to Python. Use one failing
test for each rule. Compare `float?` with `float | None`. Show where the same
idea has a different runtime behavior.

### Pass 2: Write the same rules in pandas

**What I built**

Next I wrote `interpolate_frame`. It is the pandas version of `interpolate_at`.
The two functions must return the same values.

I added eight tests in `tests/test_interpolate_asof.py`. Each test runs the same
readings through both functions and compares every result.

I then ran the code against four live USGS gauges:

- Grafton
- St. Louis
- Chester
- Valley City

The script put them on one 15-minute grid. The run covered October 13 at 20:00
UTC through October 15 at 19:30 UTC.

The suite ended at 35 passing tests. Ruff was clean. The run did not use Spark.

**What surprised me**

`merge_asof` does not interpolate. It only finds a nearby row.

The backward join finds the last reading at or before the grid time. This is the
pandas version of `bisect_right - 1`.

The forward join finds the first reading after the grid time. This is the pandas
version of the next item in the sorted list.

I expected `by="gauge_id"` to handle the sort for each gauge. It does not. The
time column must still be in order across the full frame. Sorting by gauge and
then by time makes the clock move backward at every gauge boundary.

Pandas stores missing numbers as `NaN`. Math with `NaN` does not throw. It
quietly returns another `NaN`. That can hide a bug that pure Python exposes at
once.

**What broke**

My first halfway value was 4.0 instead of 6.0. I had found only the reading on
the left. I needed a second join to find the reading on the right.

A grid time before the first reading stayed in the result as a null row. The
pure Python function skips that row. I dropped rows with no left timestamp.

A time after the last reading returned null. The rule at that point was to carry
the last value forward. I kept the left value when no right reading existed.

Two gauges caused this error:

```text
ValueError: right keys must be sorted
```

I fixed it by sorting on time with a stable sort. The `by` argument still keeps
each gauge in its own series.

The live report then failed on `min(readings)`. A Python dataclass does not sort
itself. I gave `min` and `max` the timestamp as the key.

**Numbers**

The full suite passed 35 tests in 0.44 seconds.

Each gauge had about 96 readings over two days. The median gap was 30 minutes.
The shared grid had 191 slots. For each gauge, 96 slots were exact readings and
95 were interpolated.

Every pandas result matched the pure Python result.

At St. Louis, the reading was 12.88 feet at 20:00 and 12.86 feet at 20:30. The
20:15 value was 12.87 feet.

Duplicate timestamps exposed an important detail. A stable sort keeps the input
order. The backward join takes the last duplicate. The forward join takes the
first later row. That is input order, not revision time.

**Possible post**

Show why one as-of join is not interpolation. One join finds the left value. A
second join finds the right value. The math comes after both joins. Also show
why `PARTITION BY` logic does not remove the need for a correct sort.

### Pass 3: Land 400 days of USGS data

**What I built**

I moved `land` out of the collector notebook and into
`src/river_nav/collect.py`.

The function now accepts its base folder. Local scripts pass `data/raw`.
Databricks passes the landing Volume. The notebook and local scripts use the
same function.

I added a USGS backfill for four gauges and two parameters:

- Discharge, parameter `00060`
- Gage height, parameter `00065`

That creates eight API requests for a 400-day load.

The suite ended at 40 passing tests. Ruff was clean.

**What surprised me**

The notebook version of `land` read values from the notebook around it. In C#
terms, it acted like a method that reads fields instead of parameters. Passing
the base path made the function easy to use in both places.

Eight API requests did not create eight files. Each series had about 17,000
readings. The USGS API returns at most 10,000 features per page. Each request
needed two pages.

`Path.write_text` uses UTF-8. The older `open(path, "w")` code used the machine's
default encoding.

The USGS client reads its API key from the process environment. The backfill
script loads `.env` only when the shell has not already set the key.

**What broke**

An early draft called `usgs_backfill_calls(end, days=0)` inside `collect_usgs`.
The helper rejects a zero-day window. The loop also did no useful work. I
removed the call before the live run.

The raw response still lands as received. The collector adds only its source
URL and collection time.

**Numbers**

The backfill covered 400 days and ended on October 15, 2026 at 19:54 UTC.

- 8 API requests
- 16 pages
- 140,548 features
- 63 seconds
- 70 MB of local raw data

Counts by gauge:

- Grafton: 17,648 discharge and 17,833 stage readings
- St. Louis: 17,425 discharge and 17,407 stage readings
- Chester: 17,306 discharge and 17,306 stage readings
- Valley City: 17,194 discharge and 18,429 stage readings

The `data` folder stays out of Git.

**Possible post**

One API request is not one file. Pagination changes the landing design. The
other useful point is dependency injection in its smallest form: pass the base
folder to the function instead of reading it from the notebook.

### Pass 4: Stop inventing data during outages

**What I built**

The first interpolation code would draw a line across any gap. It also carried
the last reading forward forever. The old stored procedure had the same flaw.

I added a two-hour limit to both interpolation functions. A gap longer than two
hours now returns `None`. A value after the last reading also returns `None`
when the reading is more than two hours old.

I added the failing pure Python test first. One hour into a two-day outage, the
old code returned about 4.08. Three hours after the final reading, it still
returned 8.0. Both values are now missing.

I also moved the USGS statistic rule into `transforms.py`. Statistic `00011` is
an instantaneous reading. Other statistic types do not belong in the same
series. The silver pipeline now applies the same rule.

The interpolation docstrings now tell callers to remove duplicate revisions
first. A stable sort resolves equal timestamps by input order. It does not know
which row is the newest revision.

The backfill now uses every tuple returned by `usgs_backfill_calls`. It no
longer takes the first start date and loops over the gauges a second time.

Finally, `make check` now runs Ruff against `scripts` as well as `src` and
`tests`.

**What surprised me**

The dangerous result looked reasonable. A smooth line across a two-day outage
does not look broken on a chart. It looks cleaner than the truth.

This is the same kind of bug I have seen in SQL reporting code. The query is
valid. The numbers are valid. The business rule is wrong.

**What broke**

The three new tests failed for the right reasons.

- The two-day gap returned an interpolated value.
- A daily statistic was parsed as an instantaneous reading.
- Backfill ignored the request plan and walked all gauges again.

I fixed the pure functions first. Then I copied the statistic rule into the
silver pipeline.

**Numbers**

The full suite now has 48 passing tests. It runs in about 0.64 seconds. Ruff is
clean across `src`, `tests`, and `scripts`.

**Possible post**

The chart looked better when it was wrong. Show how a stored procedure and a
Python port can share the same bad business rule. Then show the test that turns
two days of smooth, invented readings into an honest gap.
