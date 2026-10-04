**A new leader does not run the planner over a final plan**
(`spate-coordination`)

A worker that takes leadership after the plan is final now fences the plan
record and assigns the existing splits without running the planner. A
leadership change therefore adds no splits to a final plan. A new leader over
an open plan runs the planner as before.

In previous versions every new leader ran the planner once, including over a
final plan. That run repeated the source's enumeration: for the S3 source, a
full listing of the prefix. If objects had been added under the prefix since
the plan became final, the run could also add a split that grouped them with
objects already read. The job could then report completion with that split
unread. When the listing was unchanged, `spate_coordination_replans_total`
recorded a `noop` run.
