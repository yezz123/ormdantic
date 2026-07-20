# Prepare the Todo application for production

Use this checklist after the example works locally. It turns the demonstration
defaults into explicit operational decisions.

## Database and migrations

- Use a dedicated PostgreSQL database and least-privileged application role.
- Store `DATABASE_URL` in a secret manager; never log the raw value.
- Require TLS according to your provider's PostgreSQL URL options.
- Generate artifacts for the destination dialect and keep revisions immutable.
- Preview SQL, test apply and rollback on a recent backup, then apply once from a
  controlled migration job.
- Treat repair and destructive rollback as incident operations, not startup
  conveniences.
- Back up the database and verify restore procedures before destructive changes.

## Application lifecycle

- Let FastAPI lifespan initialize Ormdantic before the process becomes ready.
- Keep readiness responses credential-free and use a separate liveness policy if
  your platform needs one.
- Run the container as a non-root user with a read-only filesystem where possible.
- Set worker count, connection capacity, timeouts, and shutdown grace periods from
  measured load rather than copying development defaults.
- Send structured errors to observability systems, but return only domain-safe
  messages to clients.

## API and data behavior

- Keep request and persistence schemas separate.
- Bound pagination and free-text search inputs.
- Load relationships explicitly to prevent hidden query growth.
- Wrap dependent writes in transactions and test rollback paths.
- Review cascade behavior before exposing Project deletion.
- Add authentication, authorization, rate limits, and audit events for your domain;
  the reference app intentionally does not choose these policies for you.

## Release verification

Run the unit, SQLite, PostgreSQL, migration, documentation, lint, and package smoke
tests against the exact artifact you will deploy. Confirm `/health`, `/openapi.json`,
one write transaction, and migration history after deployment.

Return to the [tutorial overview](index.md), consult the
[documentation matrix](../development/documentation-matrix.md), or use
[Troubleshooting](../troubleshooting.md) when a driver or migration fails.
