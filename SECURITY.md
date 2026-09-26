# Security Policy

## Reporting a vulnerability

Please **do not open a public issue** for security problems.

Report vulnerabilities privately via GitHub's [private vulnerability
reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
on this repository. If that is unavailable to you, open a regular issue that
contains **no exploit details and no secrets**, and ask the maintainer for a
private channel.

Include, as far as you can:

- what the issue is and which file or command is involved
- steps to reproduce, using dummy credentials only
- the impact you believe it has

You can expect an acknowledgement within a week. Fixes land as soon as they are
ready, and you are welcome to be credited in the advisory unless you prefer
otherwise.

## Handling your Discord token

This project authenticates with a **Discord user token**, which is a complete
credential for your account. Treat it like a password.

- **Never** commit it. `.env` and `.env.*` are ignored by `.gitignore`, and
  only `.env.example` is meant to be tracked.
- **Never** paste it into an issue, a pull request, a commit message, a
  screenshot, or a log. Anyone who reads it can act as you.
- The token is read from `DISCORD_USER_TOKEN` in the environment or `.env`. If
  you export it in your shell, prefer a per-command prefix or a secret manager
  rather than a value that ends up in your shell history.
- `.dockerignore` keeps `.env` and `data/` out of the Docker build context, so
  they cannot be baked into an image layer by accident. Keep it that way if you
  change the `Dockerfile`.
- Anyone running the tool ends up with an archive of other people's messages in
  `./data/`. That directory is ignored by git. It is still private data, and
  distributing it may violate both Discord's terms and the rights of the people
  whose messages it contains.

If you think your token has leaked, revoke it in Discord immediately
(**Settings → Devices → Log Out**), then obtain a new one.

## Scope

This is a client for Discord's own API surfaces, using your own credentials
against servers you are already a member of. Reports that are in scope:

- a credential or archive being written somewhere it should not be
- a path traversal or arbitrary file write in the archive writer
- a crash or resource exhaustion reachable from untrusted archive input
- extraction returning data from a channel the account cannot read

Out of scope:

- account termination or rate limiting imposed by Discord
- anything requiring you to supply your own token to attack yourself
- missing rate limiting that merely makes the tool slower
- social engineering of project maintainers
