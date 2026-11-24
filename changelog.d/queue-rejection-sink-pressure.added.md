**`WatermarkController::on_queue_rejected` and `on_push_done`** (`spate-core`)

`WatermarkController::on_queue_rejected` records a shard-queue rejection and reads the `InflightBudget` it is given to class it. While a shard worker sharing that budget waits for a write permit, the rejection is sink pressure, as `on_send_rejected` records it. Otherwise it pauses only once rejections have lasted `min_pause`, and `WatermarkController::on_push_done` ends the run. A custom poll loop that calls `on_send_rejected` keeps the previous behavior.
