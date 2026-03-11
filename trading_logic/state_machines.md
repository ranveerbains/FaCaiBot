# State Machines & Transitions (Section 13)

---

## OrderState (Leg 1)

```
         evaluate()
None ----------------> Posted -----------------> Filled
  ^                    |                      |
  |   OrderFailed /    |   UserWS/advanceSim  |
  |   SustainCancel /  |   or sync fill       |
  |   WhipsawCancel    |                      |
  +--------------------+                      |
  ^                                           |
  |         on_trade_complete() / rotation     |
  +-------------------------------------------+
```

Note: Sustain cancel and whipsaw cancel go through `CancelLeg1Order` -> fire-and-confirm. Cancel confirmed -> `None`. Cancel NOT confirmed -> stays `Posted` (may fill via User WS).

**Triggers:**
- `None -> Posted`: Engine emits signal on `BuildupConfirmed`, executor posts maker order
- `Posted -> Filled`: User WS fill (live) or `advance_simulation()` (sim) or sync fill (`already_filled=true`)
- `Posted -> None`: CLOB rejection/error OR sustain cancel (flow fade / timeout) OR whipsaw cancel
- `Filled -> None`: Trade completion (both legs done) or MarketRotation

## HedgeState

```
                  init_leg2()
   None --------------------------> Phase1 (emergency=false)
                                  |
              +-------------------+-------------------+
              v                   v                   v
        Phase1 breach    Flow reversal/collapse   Phase1 timeout
        (immediate FOK)  (immediate FOK)          OR Flow weakening
              |                   |                   |
              |                   |                   v
              |                   |           Phase2 (ask-1tick)
              |                   |                   |
              |                   |        +----------+----------+
              |                   |        v          v          v
              |                   |  P2 breach   P2 timeout  Maker fill
              |                   | (immed FOK) (immed FOK)     |
              |                   |        |          |          |
              v                   v        v          v          v
           emergency_submitted = true             Leg 2 Filled
           exit_reason = Some(...)                      |
              |                                         |
              v                                         v
           Leg 2 fill detected                on_trade_complete()
              |                                 hedge = None
              v
     on_trade_complete()
         hedge = None
```

## SimPosition status

```
record_leg1_fill()                record_leg2_fill() / record_emergency_*()
      None ------> Open --------------------------> Hedged
                     |                         |
                     | lock_for_resolution()    | close_trade()
                     v                         v
              AwaitingResolution          [closed_trades]
```

## Full trade lifecycle

```
1. BuildupConfirmed -> buildup_detected = true

2. evaluate() passes all guards -> Leg 1 signal emitted (maker post-only)
   buildup_detected = false, leg1_state = Posted

3. [Sustain monitoring] On each event while Leg 1 is Posted:
   - Composite score < cancel_threshold -> CancelLeg1Order -> STOP (if confirmed)
   - cancel_window_ms elapsed -> CancelLeg1Order -> STOP (if confirmed)
   - Cancel NOT confirmed -> keep Posted, wait for User WS fill
   - Whipsaw (opposite direction signal) -> CancelLeg1Order -> STOP (if confirmed)

4a. [SIM] advance_simulation() fill check passes -> leg1_state = Filled
    init_leg2() (Phase B repricing), emit confirmed fill signal

4b. [LIVE] Executor posts maker post-only GTC
    OrderPosted feedback -> engine stores order_id
    User WS fill (MATCHED event) -> leg1_state = Filled, init_leg2() (Phase B repricing)

5. evaluate_leg2() runs on each event:
   - Phase 1: post at profit target, hold until fill or timeout
   - Flow monitoring: reversal -> FOK, collapse -> FOK, weakening -> Phase 2
   - Phase 2: post at ask-1tick alongside Phase 1 (no reposts -- preserve FIFO)
   - Emergency triggers (breach/timeout/whipsaw/flow): immediate FOK taker at ask

6a. [SIM] advance_simulation() Leg 2 fill detected
    leg2_state = Filled, emit confirmed fill signal

6b. [LIVE] User WS fill for Leg 2 (or sync FOK fill)
    leg2_state = Filled, main loop detects both filled -> on_trade_complete()

7. State reset -> ready for next signal
```
