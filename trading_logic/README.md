# Trading Logic Reference

This directory contains FaCaiBot's complete trading logic documentation, split by domain.

## File Map

| File | Contents | Read when working on... |
|------|----------|------------------------|
| [`signal_and_entry.md`](signal_and_entry.md) | Core trade model, BuildupDetector (6-metric composite), entry validation guards, repricing model & allocation, Leg 1 execution & sustain | Signal detection, entry guards, repricing formula, Leg 1 maker orders, buildup metrics/weights |
| [`leg2_hedge.md`](leg2_hedge.md) | 2-phase hedge system, emergency exits (breach/timeout/whipsaw/flow), favorable exits (try-maker-first) | Hedge phases, Phase 1/Phase 2 logic, emergency FOK, favorable maker, flow-based graduated response |
| [`lifecycle.md`](lifecycle.md) | Trade completion & PnL, market rotation, cutoff window, quiet period, trade cooldown, capital management | Rotation, state reset, PnL computation, cooldown/quiet periods, capital allocation |
| [`state_machines.md`](state_machines.md) | OrderState (Leg 1), HedgeState, SimPosition diagrams, full trade lifecycle flowchart | State transitions, debugging state bugs, understanding the full trade flow |
| [`edge_cases.md`](edge_cases.md) | Race conditions, edge cases, complete trade example walkthrough, simulation vs live differences | Debugging live issues, understanding cancel races, partial fills, balance exhaustion, sim assumptions |

## Source File Cross-Reference

| Topic | Doc file | Source files |
|-------|----------|-------------|
| BuildupDetector | `signal_and_entry.md` | `engine/buildup/detector.rs`, `engine/buildup/metrics.rs` |
| Entry evaluation | `signal_and_entry.md` | `engine/evaluator.rs`, `engine/confidence.rs` |
| Leg 1 execution | `signal_and_entry.md` | `engine/strategy.rs`, `executor/live.rs` |
| Leg 2 hedge | `leg2_hedge.md` | `engine/erosion.rs`, `engine/evaluator.rs`, `engine/strategy.rs` |
| Emergency/favorable exits | `leg2_hedge.md` | `executor/live.rs`, `engine/evaluator.rs` |
| Trade completion | `lifecycle.md` | `engine/strategy.rs` |
| Market rotation | `lifecycle.md` | `gateway/polymarket/rotation.rs`, `engine/strategy.rs` |
| State machines | `state_machines.md` | `types/order.rs`, `engine/erosion.rs`, `engine/strategy.rs` |
| Fill detection (live) | `edge_cases.md` | `gateway/polymarket/user_ws.rs`, `engine/strategy.rs` |
