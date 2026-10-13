**A gauge for how busy the I/O runtime is** (`spate-core`)

Each pipeline exports `spate_pipeline_io_busy_ratio`, the fraction of time the I/O runtime's async workers spent running tasks, from 0 to 1, sampled every 5 seconds. Previously no metric showed when those workers were the limit, so a saturated I/O runtime looked like a slow sink. When the ratio stays at 0.9 or above for 60 seconds, the pipeline logs one warning that names `pipeline.io_threads`. The gauge does not count the blocking thread pool.
