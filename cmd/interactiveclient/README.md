# interactiveclient

A test application you control over MQTT. It is the first tool built on the client's supervisor,
`Client.Run` (phase 4). The supervisor keeps the connection up, reconnects with backoff, and
re-subscribes from `OnConnect` after every reconnect.

## What it does

The app subscribes to two control topics under `-root` (default `interactiveclient`):

| Topic | Payload | Effect |
|---|---|---|
| `<root>/on` | a non-zero number, `true`, `on` or `yes` | publishing on |
| `<root>/on` | `0`, `false`, `off`, `no`, or empty | publishing off (the default at start) |
| `<root>/reset` | anything, including empty | sine and step go back to 0; if publishing is on, values for 0 are published at once |

While publishing is on, it publishes every `-interval` (default 1 s):

| Topic | Value |
|---|---|
| `<root>/sin` | `amplitude × sin(2π · t / period)` with 6 decimals; `t` is the time since the last reset |
| `<root>/step` | `0, 1, 2, …`, one more each interval |

Details:
- **Retained `on` messages work:** the broker delivers a retained value when the app subscribes,
  so `mosquitto_pub -r -t interactiveclient/on -m 1` keeps it on across restarts. The value
  arrives again after every reconnect.
- **Retained `reset` messages are ignored,** with a log line. Otherwise they would reset the
  values on every reconnect.
- **Values freeze while off:** turning it on again continues from where it stopped.
- **Unknown payloads on `on`,** such as `banana`, are logged and ignored.
- **Nothing is published while disconnected.** The app doesn't queue a backlog of old values;
  publishing resumes once the connection is back.

## Build

From the repository root:

```sh
go build -o build/interactiveclient ./cmd/interactiveclient
```

## Flags

The connection flags are the same as in the other tools: `-server`, `-v`, `-id`, `-clean`,
`-session-expiry`, `-keepalive`, `-u`, `-P`, `-timeout`, `-debug`, TLS and Will. See the
[mqttpub README](../mqttpub/README.md#flags).

Reconnect flags. These map to the `ReconnectDelay`, `ReconnectDelayMax`, `ReconnectExponential`
and `ConnectTimeout` options:

| Flag | Default | Meaning |
|---|---|---|
| `-reconnect-delay` | `1s` | Wait before the first reconnect attempt |
| `-reconnect-delay-max` | `30s` | Longest wait. Wait *n* (from 0) is `delay × (n+1)`, capped here. A successful connection starts again from 0. |
| `-reconnect-exponential` | `false` | Use `delay × (n+1)²` instead, as libmosquitto's "exponential" backoff does |
| `-connect-timeout` | `30s` | Limit for each attempt, from dialling to CONNACK |

Application flags:

| Flag | Default | Meaning |
|---|---|---|
| `-root` | `interactiveclient` | Topic root for `on`, `reset`, `sin` and `step` |
| `-q` | `1` | QoS of the `on` and `reset` subscriptions |
| `-pub-qos` | `0` | QoS of `sin` and `step` |
| `-retain` | `false` | Publish `sin` and `step` retained |
| `-interval` | `1s` | Time between publications; the step grows by 1 each interval |
| `-period` | `1m` | Period of the sine wave |
| `-amplitude` | `1` | Amplitude of the sine wave |
| `-quiet` | `false` | Don't print every publication |

Ctrl-C disconnects cleanly, so the broker doesn't publish the Will, and the app exits with 0. It
exits with 1 if `Run` stops on a permanent error, such as bad credentials, and with 2 for invalid
arguments.

## How to test

These examples assume a mosquitto broker on `localhost:1883` and are run from the repository
root.

Terminal 1, the app:

```sh
build/interactiveclient
```

Terminal 2, a watcher:

```sh
mosquitto_sub -t 'interactiveclient/#' -v
```

Terminal 3, the controls (`build/mqttpub` works too, for example
`build/mqttpub -t interactiveclient/on -m 1`):

```sh
mosquitto_pub -t interactiveclient/on -m 1          # starts: sin 0.000000, step 0, 1, 2, ...
mosquitto_pub -t interactiveclient/reset -m x       # back to 0, published at once
mosquitto_pub -t interactiveclient/on -m 0          # stops; values freeze
mosquitto_pub -t interactiveclient/on -m true -r    # retained: on again, and on at the next start
mosquitto_pub -t interactiveclient/on -r -n         # clear the retained value
```

To see a whole sine period quickly, use `-interval 200ms -period 2s`.

### Reconnecting

Run a private broker so the system one keeps running:

```sh
printf 'listener 18883 127.0.0.1\nallow_anonymous true\n' > /tmp/m.conf
/usr/sbin/mosquitto -c /tmp/m.conf &
build/interactiveclient -server mqtt://127.0.0.1:18883 -v 5 -keepalive 5 -interval 300ms \
    -reconnect-delay 200ms -reconnect-delay-max 1s
mosquitto_pub -p 18883 -t interactiveclient/on -m 1
kill -9 %1                                   # broker gone: OnDisconnect, then OnConnectError each attempt
/usr/sbin/mosquitto -c /tmp/m.conf &         # back: OnConnect, SUBACK again, publishing resumes
```

Expected output, shortened:

```
OnDisconnect err=mqttclient: connection lost: EOF
OnConnectError ... connect: connection refused      (after 200ms, 400ms, 600ms, then every 1s)
not connected; not publishing until reconnected
OnConnect reason=0x00 session_present=false
SUBACK interactiveclient/on=QoS 1 interactiveclient/reset=QoS 1
sin=0.094108 step=3
```

The killed broker loses the retained `on` value, but the app remembers that publishing is on.
After a restart of the app itself, publish `on` again, or use a broker with persistence.

With `-clean=false -id <id>` (and `-session-expiry` for MQTT 5), the broker keeps the
subscriptions across reconnects and CONNACK reports `session_present=true`. The app subscribes
again anyway, which does no harm.
