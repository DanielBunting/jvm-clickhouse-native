# Authentication & TLS

The client supports three credential families — password, access token, and client certificate — plus TLS transport with optional mutual TLS.

- [How the auth method is chosen](#how-the-auth-method-is-chosen)
- [Username and password](#username-and-password)
- [Access tokens](#access-tokens)
- [TLS](#tls)
- [Mutual TLS (mTLS)](#mutual-tls-mtls)
- [URL equivalents](#url-equivalents)

## How the auth method is chosen

`ClickHouseConfig.build()` derives an `AuthMethod` from the configured credentials and enforces that **at most one credential family is set**:

| Configuration | Effective `AuthMethod` |
|---|---|
| Nothing set (or password only — empty password is valid) | `PASSWORD` |
| `accessToken(...)` set | `ACCESS_TOKEN` |
| `clientCertPath(...)` **and** `clientKeyPath(...)` set | `CERT` |
| More than one family set | `ConfigurationException` |
| Only one of cert path / key path set | `ConfigurationException` |

## Username and password

The default. An empty password is allowed (and is the default for the `default` user on a fresh ClickHouse install).

#### Java

```java
ClickHouseConfig config = ClickHouseConfig.builder()
    .host("ch.example.com")
    .username("app")
    .password("secret")
    .build();
```

#### Kotlin

```kotlin
val config = clickHouseConfig {
    host = "ch.example.com"
    username = "app"
    password = "secret"
}
```

## Access tokens

For servers configured with token/JWT authentication, pass the bearer credential via `accessToken`. It is mutually exclusive with a non-empty password and with client certificates.

#### Java

```java
ClickHouseConfig config = ClickHouseConfig.builder()
    .host("ch.example.com")
    .username("app")
    .accessToken(System.getenv("CH_TOKEN"))
    .build();
```

#### Kotlin

```kotlin
val config = clickHouseConfig {
    host = "ch.example.com"
    username = "app"
    accessToken = System.getenv("CH_TOKEN")
}
```

## TLS

Enable TLS with `tls(true)`. ClickHouse conventionally serves TLS-native traffic on port **9440** — the port is not switched automatically, so set it explicitly.

#### Java

```java
ClickHouseConfig config = ClickHouseConfig.builder()
    .host("ch.example.com")
    .port(9440)
    .tls(true)
    // optional: custom CA instead of the platform default trust store
    .trustStorePath(Paths.get("/etc/pki/ch-truststore.p12"))
    .trustStorePassword("changeit")
    .build();
```

#### Kotlin

```kotlin
val config = clickHouseConfig {
    host = "ch.example.com"
    port = 9440
    tls = true
}
```

Trust behavior:

- **Trust store** — `trustStorePath` accepts a **JKS or PKCS12** store (format auto-detected). When unset, the JVM's platform default trust material is used.
- **Hostname verification** — on by default (RFC 2818). `verifyHostname(false)` keeps certificate-chain validation but skips matching the certificate against the connect host.
- **`insecureSkipVerify(true)`** — trusts any certificate **and** skips hostname verification. No MITM protection. Dev/test only; never use in production.

## Mutual TLS (mTLS)

Two pieces are involved, and they are configured separately:

1. **Transport identity** — the client certificate and key presented during the TLS handshake come from a **JKS/PKCS12 key store** via `keyStorePath` / `keyStorePassword`.
2. **Credential declaration** — `clientCertPath` / `clientKeyPath` (PEM paths) declare certificate authentication as the credential family, which selects `AuthMethod.CERT` (instead of password/token).

#### Java

```java
ClickHouseConfig config = ClickHouseConfig.builder()
    .host("ch.example.com")
    .port(9440)
    .tls(true)
    .trustStorePath(Paths.get("/etc/pki/ch-truststore.p12"))
    .keyStorePath(Paths.get("/etc/pki/app-identity.p12"))   // client cert + key (transport)
    .keyStorePassword("changeit")
    .clientCertPath("/etc/pki/app-cert.pem")                 // credential declaration
    .clientKeyPath("/etc/pki/app-key.pem")
    .build();
```

Both `clientCertPath` and `clientKeyPath` must be set together; setting only one fails validation.

## URL equivalents

TLS options are available on `chnative://` URLs (see [configuration.md](configuration.md#url-format) for the full parameter list):

```
chnative://app:secret@ch.example.com:9440/analytics?sslmode=strict
chnative://app@ch.example.com:9440/analytics?ssl=true
chnative://dev@localhost:9440/default?sslmode=insecure   # dev only: TLS without verification
```

| `sslmode` value | Effect |
|---|---|
| `none`, `disable` | TLS off |
| `strict`, `verify-full`, `require`, `true` | TLS on, full verification |
| `none-verify`, `insecure` | TLS on, **no certificate or hostname verification** |

## See also

- [Configuration](configuration.md)
- [Quickstart](quickstart.md)
