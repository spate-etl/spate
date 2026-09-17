**Leaner duplicate-key guard** (`spate-json`) — the structural pass behind
`reject_duplicate_keys` no longer allocates a second copy of every object key
it checks: keys are held as borrows of the input document, so scanning a
document whose keys need no unescaping allocates nothing for the guard itself.
Rejection behavior, including the key named in the error, is unchanged.
