# Releasing

All four modules (`clickhouse-native-client`, `clickhouse-native-client-jdbc`,
`clickhouse-native-client-kotlin`, `clickhouse-native-client-adbc`) are published
to Maven Central in lockstep, always with the same version number.

## Versioning

Versions are inferred from the repo — nobody ever types one:

- **major.minor** lives in the [`VERSION`](VERSION) file (e.g. `0.0`).
- **patch** = the number of commits on `main` since `VERSION` last changed.

So with `VERSION` = `0.0` and 14 commits since it was last touched, the next
publish is `0.0.14`. Editing `VERSION` resets the patch counter to 0.

Local builds (`./gradlew build`) don't consult git and use
`<major.minor>.0-SNAPSHOT`. CI passes the real computed version via
`-PbuildVersion=`; the root build rejects a `buildVersion` whose major.minor
disagrees with the checkout's `VERSION` file.

## Snapshots (automatic)

Every push to `main` that passes CI publishes `<major.minor>.<patch>-SNAPSHOT`
of all four modules to the Central Portal snapshots repository
(see the `publish-snapshot` job in [.github/workflows/ci.yml](.github/workflows/ci.yml)).

Consumers opt in by adding the snapshots repo:

```kotlin
repositories {
    maven("https://central.sonatype.com/repository/maven-snapshots/")
}
```

Snapshots are mutable, not searchable on central.sonatype.com, and are cleaned
up by Sonatype after ~90 days. Don't depend on one from anything you ship.

## Cutting a release

1. Make sure `main` is green.
2. GitHub → **Actions** → **Release** → **Run workflow** (branch: `main`).

That's it. The workflow computes the version (same number as the latest
snapshot from that commit, minus the `-SNAPSHOT` suffix), runs the build,
publishes to Maven Central via the auto-promoting Portal upload, and only after
a successful publish tags `vX.Y.Z` and creates a GitHub Release with
auto-generated notes. No commit is pushed back to `main`.

Release notes are generated from merged PR titles — good PR titles are the
changelog.

### Failure modes

- **Publish fails** (Central outage, bad secrets): no tag was created — fix the
  cause and re-dispatch the workflow.
- **"Tag already exists"**: that commit was already released; there's nothing
  new to publish. Merge something first.
- **Signature validation fails on a brand-new GPG key**: keyserver propagation
  can lag a few minutes — wait and re-dispatch, don't regenerate the key.

## Bumping major or minor

Edit `VERSION` in a normal PR (e.g. `0.0` → `0.1`). Once merged, the patch
counter restarts at 0, so the first release on the new line is `0.1.N` where N
counts commits since that merge. Avoid releasing the bump commit itself if you
don't want to publish a `.0`.

## One-time setup (already done, recorded for posterity)

- Namespace `io.github.danielbunting` registered and verified on
  [central.sonatype.com](https://central.sonatype.com), with SNAPSHOT
  publishing enabled for the namespace.
- GPG signing key published to `keyserver.ubuntu.com`.
- Four GitHub Actions repository secrets:

  | Secret | Contents |
  |---|---|
  | `MAVEN_CENTRAL_USERNAME` | Central Portal user-token name |
  | `MAVEN_CENTRAL_PASSWORD` | Central Portal user-token password |
  | `SIGNING_KEY` | ASCII-armored GPG private key (whole `BEGIN/END` block) |
  | `SIGNING_KEY_PASSWORD` | passphrase for that key |

  The workflows expose these to Gradle as `ORG_GRADLE_PROJECT_*` variables read
  by the [vanniktech maven-publish plugin](https://vanniktech.github.io/gradle-maven-publish-plugin/).
