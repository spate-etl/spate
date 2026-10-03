**Breaking:** **Registry URL credentials** (`spate-avro`)

Registry URLs with credentials fail at configuration load, including URLs
used without separate credential fields. Remove credentials from `registry.url`
and set `registry.username` and `registry.password`. Use decoded credential
text in these fields if the URL used percent encoding. Previously, URL
credentials were accepted and could produce duplicate Authorization headers
when separate credentials were also configured.
