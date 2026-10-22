## Write code that requires minimal human attention 

- Fact: every line of code requires human review, so write readable code not just working code.

- Build reusable, trustworthy components that has deep logic but thin interface, so that human only need to review the complexity once (reuse the attention).

- Follow Rust best practices to save human attention from reviewing for common mistakes.

- Before implementing a new feature, identify similar existing features, and check if it's worth to refactor existing code so it's reusable.

## Engineering

- Prefer explicit props and function arguments so ownership and data flow remain visible. Use context only when a dependency must cross at least two component layers and the intermediate parent does not use it. Keep state owned by the component that creates it.

- use descriptive, intention-revealing names for variables and functions, don't use generic names like `Built`, `Common`, `View`.

- do not use free form functions, functions should have a clear owner, e.g., struct, enum; unless it is fully justified stateless function.
  Example 1: `fn build_chart(model: &DataModel) -> Built`, instead use a member method like: `fn build_chart(&self) -> Built`. Even better, use a `From` trait to build the chart from the data model.
  Example 2: `fn toggle_expand(code: crate::views::codemap::CodeState, key: (u32, u32))` instead use a member method like: `fn toggle_expand(&self, key: (u32, u32))`.

- try to use minimal visibility for functions and variables, e.g., a pub function under a private mod is essentially private, but it is confusing, prefer to make it private. 

- try to avoid public fields, prefer to use getters and setters, unless the field is truly public.

- organize the code by features, not by technical buckets, e.g., don't have `api.rs`, `views.rs`, `data.rs`, etc. Instead, do `settings.rs`, `data_panel.rs`, `code_map.rs`, etc.

- No static variables, except a binary's `#[global_allocator]`, which Rust takes only as a static.

## Performance reasoning

- Reason then measure. Don't start with blind sweeping, or brainless micro/premature optimizations. Always first think about what is the bottleneck by profiling and measurements, reason the cost model. Benchmark is the last step, to verify that your reasoning is correct, if not, correct your model and repeat; benchmark is NOT a tool to guide the optimization.

- Never do explicit prefetching: no software prefetch instructions or intrinsics (`_mm_prefetch`, `core::intrinsics::prefetch_*`, inline `prefetcht0`, ...), not in the library, benchmarks, prototypes or A/B variants, and not behind a feature flag. Fix memory latency with layout instead: put what an operation needs in the cache lines it already loads (D46).
  Exception: the batch lanes (doc 12, D53) prefetch, only through `Prefetcher` in `src/tree/batch.rs`, the line each lane reads next, the query window included. Never in single-key paths, and never whole keys or unchosen edges.

## Verification

- Never run Miri (`cargo miri test`, any mode) in a coding session or a subagent. Miri runs in CI only. Local checks are `cargo fmt --check`, `cargo clippy -- -D warnings` and `cargo test`.
- Miri runs only the tests `.github/miri-nextest.toml` names. Don't mark tests `#[cfg_attr(miri, ignore)]`; to run a test under Miri, name it there.

## Benchmark rule

- A benchmark is useless if it takes more than 5 minutes to run.
- Never run full benchmark sweep without explicit approval.
- It's meaningless to repeat a benchmark, if the run to run variance is high, either the machine is not stable -- then wait for it to stablize, or the benchmark/implementation is broken -- then fix it. Repeating and taking average/median is for losers to cover benchmark flaw.
- Only run a carefully selected subset of relevant benchmarks.


## Verus 

1. Always ask before using `external_body`.
2. Make sure the verus version in cargo is consistent with the verus version in flake.nix.
3. Keep verus proof code small, avoid reduant proofs.

