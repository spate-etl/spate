**A pipeline thread no longer allocates a list of its lanes on each poll** (`spate-core`)

A pipeline thread lists the IDs of its lanes only when backpressure pauses or resumes them. Previously it built that list before every poll, including polls that returned nothing, and a pause or resume then copied it again. The saving per poll grows with the number of lanes a thread holds. The lanes a pause or resume names are unchanged.
