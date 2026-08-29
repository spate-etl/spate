**The start reports the admin server's address** (`spate-core`) — a pipeline
whose `admin.listen` names a port of `0` asks the kernel to pick one, and
nothing said which port it picked, so `/metrics`, `/healthz` and `/readyz` were
served at an address no one could learn. The bound address is logged at `INFO`
under the message `admin server listening`. A deployment running at `INFO` sees
one new line per start.
