**A source lane can report that it has finished** (`spate-core`)

`SourceLane::finished` lets a lane say it has reached the end of its input. The pipeline thread then stops polling it and waits on its control channel until the lane is stopped, dropped or handed new work. Lanes that do not implement it are polled as before.
