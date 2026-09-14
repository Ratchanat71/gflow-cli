# The video path polls on a fixed 5.00 s client timer — nothing signals it

**Date:** 2026-09-14 · **Cost:** **one real Veo generation, ~10 credits** (`veo_3_1_lite`,
t2v, x1). The only credit-spending measurement in this line of work.
**Script:** [`spike_generation_wire_survey.py`](../../../scripts/dev/spike_generation_wire_survey.py)
`--mode video` — the video arm was pre-registered and committed in `8d4502bb`, **before
any video data existed**, so the ordering is checkable this time.
**Evidence:** `scripts/dev/_spike_out/generation_wire_video_ffroliva_20260914_145524.json`
(gitignored)
**Design:** profile `ffroliva`, project `c5550ed7-…`, **1 run**, full submit→completion
lifecycle instrumented. One run because each further one costs credits.

## The gap this closes

[Survey #2](2026-09-14-generation-wire-no-push-channel.md) settled the **image** path —
`ogiZ0b` dispatched once, answered 19.7 s later with the finished images, a held response
with neither push nor poll — and then **corrected its own premise**:

> "The image path does not poll at all, so it says much less about video than assumed. …
> Images use a **held response**; video uses a **poll loop** (`jwpduf`, `as29s`). Those are
> different mechanisms, so an absence on one is not an absence on the other."

It named the question worth paying for: *does anything arrive between `jwpduf` polls, and
are the polls a client timer or a reaction?* This answers exactly that.

## Observed

| | video run 1 |
|---|---|
| generation completed | ✅ `done=True`, status 3, 1,273,409 bytes, `flow-content.google` |
| **WebSocket events** | **0** |
| streaming content-types | 0 |
| `jwpduf` polls | 8 |
| **`jwpduf` DISPATCH gaps** | 5.002, 5.009, 5.011, 5.005, 5.008, 5.001, 5.004 s |
| — mean / stdev / spread | **5.006 / 0.0035 / 0.010 s** |
| `jwpduf` ARRIVAL gaps | mean 5.007, stdev **0.178**, spread **0.641** s |
| `jwpduf` in flight | 0.441–0.809 s, mean 0.538 |
| `YhhmEf` (submit) in flight | 4.301 s |
| `as29s` (result) in flight | 2.541 s |
| `as29s` dispatched | **0.034 s** after the last `jwpduf` reply |
| total wall clock | 56.61 s |

### 1. It is a fixed 5.00 s client timer, and only dispatch timing can show that

Seven consecutive inter-dispatch gaps span **10 milliseconds** end to end (stdev 3.5 ms).
That is a `setInterval`, not a reaction to anything.

The **arrival** gaps of the same eight polls have a stdev of 178 ms and a spread of 641 ms —
**fifty times wider**, and purely network jitter. An arrival-only instrument would have
reported "roughly every 5 s, a bit ragged" and could not have separated a timer from a
reactive cadence. Survey #2 rebuilt its instrument mid-spike to record request dispatch for
exactly this reason; this is the second finding that turns on it, and the first where the
two readings disagree in character rather than only in precision.

### 2. The cadence does not collapse near completion

The pre-registered alternative was that gaps shorten at the end, which would mean something
told the client. They do not: the **last** inter-dispatch gap (5.004 s) is indistinguishable
from the **first** (5.002 s).

What happens instead is fully accounted for by the poll itself. The generation finished
during the poll dispatched at t=53.554; that poll's own reply (t=54.021) carried status 3
and the byte count, and the app dispatched `as29s` **34 ms later**. The client reacted to
the answer it had asked for. Nothing arrived out of band to prompt it.

### 3. Nothing arrives between polls that could be the signal

Across the whole submit→completion window the only non-`batchexecute` responses are:

| t | host | what |
|---|---|---|
| 15.549–15.562 | `region1.google-analytics.com/g/collect` ×4 | GA4 beacons fired at the submit click. **204 No Content** — no body to carry anything |
| 31.907 | `play.google.com/log?hasfast=true&auth=SAPISIDHASH…` | Google's shared client-logging sink. Outbound telemetry |

Both are **requests the page sent**, not channels it was pushed on, and a request the client
makes cannot be the thing that told it. The stronger argument is the cadence: the
`play.google.com` hit sits inside the **longest** arrival gap (5.379 s) and between two
dispatches **5.011 s** apart — within the 10 ms spread of every other pair. Whatever it
carried, the timer did not notice it.

One extra Flow rpcid, **`WuwhI`**, fires once mid-poll (t=30.32 → 30.572, 4,925-byte
request). It is a client-sent `batchexecute` call like the rest, on the same host, and is
not a push.

### 4. `jwpduf` is a poll, not a long-poll

Every one of the eight is answered in under 0.81 s (mean 0.538). Compare the image path's
`ogiZ0b`, **held open 19.7 s**. So the two paths really do use different mechanisms — the
distinction survey #2 drew is confirmed rather than assumed — and neither of them is push.

### 5. No WebSocket, no streaming, no Flow protobuf — now measured *during* a video run

Zero WebSocket events, zero `text/event-stream` / `grpc` / `x-protobuf` / NDJSON /
multipart. The single `application/json+protobuf` is **`ogads-pa.clients6.google.com`** at
t=0.53 — the OneGoogle account bar, before submit, not Flow. That is the third time it has
shown up and the third time it is not Flow; it is named here so the next reader does not
have to re-derive it.

Every `batchexecute` 200 again arrives as `application/json` with **no `content-length`**.
Ordinary chunked transfer of the batchexecute envelope — the same non-finding as the image
arm, recorded so it is not mistaken for streaming a second time.

## What this settles

**There is no push channel on the video path either — and that is now measured, not
generalised.** The three surveys together cover Flow's hosts at idle (#1), during an image
generation (#2), and during a video generation (this one):

| path | mechanism | measured in |
|---|---|---|
| image | one **held response** (`ogiZ0b`, ~19.7 s), no poll | survey #2 |
| video | **fixed 5.00 s client-timer poll** (`jwpduf`) → `as29s` | this spike |

The polling in `migrated_composer.py` is the real mechanism, not a fallback, and the driver
is right to observe rather than drive it: **the 5 s cadence is the page's, and gflow adds no
traffic to it** (`submit_and_observe` waits on futures resolved by the page's own responses;
`_await_terminal` issues nothing). Any future "we could subscribe instead of polling"
proposal for video is refuted by measurement, as it already was for images.

It also puts a floor under observed generation latency that is **not ours**: a finished video
is seen up to one poll interval late. Shortening that would mean issuing our own status
request rather than reading Flow's — a different design, with its own cost.

## NOT measured — leads, not conclusions

- **One run, one account, one shape.** `ffroliva`, one project, t2v, `veo_3_1_lite`, x1, a
  generation that finished in ~35 s of polling. Each further run costs credits.
- **Whether the 5 s cadence holds for a long or queued generation.** This one never sat in a
  queue (`status=2` throughout, then 3). A multi-minute generation could back the timer off
  and this run could not see it. *What would settle it:* the same script on a longer
  generation — another ~10 credits.
- **Whether the cadence varies by model, duration, count or cohort.** Unmeasured.
- **i2v / r2v**, which add upload and attach traffic before submit.
- **`labs.google`'s generation path** — nothing here is served it ([survey #1](2026-09-14-two-domain-protocol-survey.md)).
- **Per-request HTTP version in this run** — see the instrument note below. No h2/h3 claim is
  made from this capture.

## Instrument notes

**The url-keyed dispatch dict would have destroyed this result, and the fix was made before
the run.** Request dispatch times were keyed by `url[:200]`. Measured here: **all 8 `jwpduf`
URLs are byte-identical at that truncation** — "1 distinct of 8". Every poll would have
overwritten the previous one's dispatch time, and `in_flight_s` — the discriminator for
long-polling — would have been wrong for seven of eight. The key is now the Playwright
`Request` object. Survey #2's image arm was unaffected only because `ogiZ0b` fired once.

**Per-request HTTP version is unattributed in this capture, and that is a bug this run
found.** `protocols` was keyed on `url[:300]` while events carry `url[:200]`, so the lookup
missed every long URL — 62 of this run's responses, including all eight polls. Fixed
afterwards (both sides now `[:200]`); this run's h2/h3 split is therefore **not reportable**
and nothing above rests on it. **Survey #1 is not affected** — it truncates both sides at
300, so its "34–171 requests per run negotiate h3" stands.

## Applying the pre-registered reading to this data

The script's video arm listed five outcomes. Stating which one fired, because the previous
spike in this series wrote a reading and then did not apply it to its own numbers:

| pre-registered outcome | fired? |
|---|---|
| gaps UNIFORM → a client timer we own; latency is ours to tune | ✅ **this one** |
| gaps COLLAPSE near completion → something signalled the client | ❌ last gap == first gap |
| non-`batchexecute` traffic between polls is the candidate signal | ⚠️ 5 hits found, all **outbound** telemetry, cadence untouched — reported above rather than dismissed |
| a poll HELD IN FLIGHT for many seconds → long-poll, push by another name | ❌ max 0.809 s |
| submit refused / timed out → **UNMEASURED** | ❌ n/a, the run completed |
