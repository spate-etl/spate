**Breaking:** **The SQS feed refuses a URL carrying a user or password, and an endpoint that is not http or https** (`spate-sqs`)

`queue_url` and `endpoint` must each be an `http` or `https` URL with no user or password, and a value that is not fails at load, such as `feed.sqs.endpoint: carries userinfo; credentials come from the AWS SDK's default provider chain`. In previous versions a `queue_url` or `endpoint` carrying a user or password loaded, and the SDK logged the endpoint at debug level and quoted it in its errors. An `endpoint` with any scheme also loaded.

A `queue_url` with another scheme reads `feed.sqs.queue_url: must be a URL with scheme http or https; found ftp`. In previous versions it read ``must be an http or https URL; got scheme `ftp` ``.
