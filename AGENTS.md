## Engineering
1. parse, do not validate.
2. Whenever you add a if statement, you need to think: 1. why we need to check this? 2. can we move this check to the caller? 3. can we use type system to make invalid state unrepresentable?
3. Do right thing. Break any api if it make things better. Compatibility is for losers.
4. Think about zero-overhead abstraction.

## Verus 

1. Always ask before using `external_body`.
