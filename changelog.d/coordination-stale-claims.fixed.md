**A worker no longer claims a split that finished or ran out of attempts while its view lagged** (`spate-coordination`)

A worker that takes over a split after its lease vanishes re-reads the split's
record first, and now acts on what it reads. It leaves a completed or
quarantined split alone, and parks one that has reached `max_attempts` instead
of claiming it. In previous versions it could claim such a split anyway when the
record changed faster than its watch delivered it, emitting `Gained` for finished
work or running a split past its attempt limit. A takeover whose first write
loses to the previous owner's late commit now counts its delivery attempt on
the retry, which previous versions dropped. A worker also ignores leader and
presence updates older than ones it already holds, and a plan record older than
the one it read at startup.
