**A commit after a commit whose reply was lost stores its own state** (`spate-coordination`)

When a commit applies but its reply is lost, the next commit for the split may
carry the same watermark with different resume state. The worker now stores
that commit's state before it returns success, and a completing commit also
releases the lease. In previous versions, when both commits completed the
split or neither did, and the next commit ran before the worker saw the
earlier one, the next commit returned success while the store kept the earlier
commit's state. A split that was not complete then resumed from that state
under its next owner. Sources whose resume state is fully determined by the
watermark were not affected.
