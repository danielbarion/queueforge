import Link from "next/link";
import { BROKERS, featureGroups, level, note, standardCount, tally, type Cell, type Level } from "./features";

const LABEL: Record<Level, string> = { y: "Yes", p: "Partial", n: "No" };

function Mark({ value }: { value: Level }) {
  return (
    <svg className={`mark-icon is-${value}`} viewBox="0 0 24 24" aria-hidden="true">
      <circle cx="12" cy="12" r="12" />
      {value === "y" && <path d="M7.5 12.5l3 3 6-6.5" />}
      {value === "p" && <path d="M8 12h8" />}
      {value === "n" && <path d="M9 9l6 6M15 9l-6 6" />}
    </svg>
  );
}

function Support({ cell }: { cell: Cell }) {
  const value = level(cell);
  const text = note(cell);
  return (
    <td className={`support is-${value}`}>
      <Mark value={value} />
      <span className="sr-only">{LABEL[value]}</span>
      {text && <span className="support-note">{text}</span>}
    </td>
  );
}

export function FeatureTable() {
  return (
    <>
    <div className="panel table-panel feature-panel">
      <div className="scroll">
        <table className="feature-table">
          <thead>
            <tr>
              <th scope="col">Feature</th>
              {BROKERS.map((broker) => (
                <th key={broker.key} scope="col" className={`tone-${broker.key}`}>
                  <span className="app">
                    <span className="dot" aria-hidden="true" />
                    <span>
                      {broker.name}
                      {(broker.key === "bun" || broker.key === "php") && <sup>*</sup>}
                    </span>
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          {featureGroups.map((group) => (
            <tbody key={group.title}>
              <tr className="group">
                <th scope="rowgroup" colSpan={BROKERS.length + 1}>
                  {group.title}
                </th>
              </tr>
              {group.rows.map((row) => (
                <tr key={row.name}>
                  <th scope="row">
                    <span className="feature-name">{row.name}</span>
                    <span className="feature-detail">
                      {row.detail}
                      {row.code && (
                        <span className="feature-code">
                          {row.code.map((token) => (
                            <code key={token}>{token}</code>
                          ))}
                        </span>
                      )}
                    </span>
                  </th>
                  {BROKERS.map((broker) => (
                    <Support key={broker.key} cell={row.cells[broker.key]} />
                  ))}
                </tr>
              ))}
            </tbody>
          ))}
        </table>
      </div>
    </div>
    <p className="caption" id="process-note">
      <sup>*</sup> Bun and PHP are not one process that uses every core. When the process is allowed
      more than one core, a parent starts one child per core. The parent accepts the connection and
      hands the socket to a child. It does not read messages. The child keeps the messages from the
      connections it was given. PHP does not start those children when TLS is on. One connection
      then confirms and delivers nothing, because the publisher and the consumer are handed to
      different children. Sixteen publishers opened before sixteen consumers still deliver, because
      both ends of a queue land on the same child.
    </p>
    </>
  );
}

export function Tallies() {
  return (
    <>
    <ul className="tallies" aria-label="Support counts">
      {BROKERS.map((broker) => {
        const { y, p, total } = tally(broker.key);
        return (
          <li key={broker.key} className={`panel tone-${broker.key}`}>
            <span className="tally-name">
              <span className="dot" aria-hidden="true" />
              {broker.name}
            </span>
            <strong>
              {y}
              <small> / {total}</small>
            </strong>
            <span className="tally-sub">core features, {p} partial</span>
          </li>
        );
      })}
    </ul>
    <p className="caption">
      Counts cover the {standardCount()} rows RabbitMQ ships in its core. Rows that need a RabbitMQ
      plugin are listed in the table but not counted.
    </p>
    </>
  );
}

export function FeatureTease() {
  return (
    <section className="rule">
      <div className="wrap">
        <p className="kicker">Features</p>
        <h2>What it does, and what it does not.</h2>
        <p className="section-lead">
          A checked list of RabbitMQ features, read from each broker&apos;s source. Not every
          RabbitMQ 4.3 feature. Bun covers every core row; Rust has gaps in replication and
          storage. LDAP, OAuth, MQTT 5 and the other plugin rows are partial on both.
        </p>
        <Tallies />
        <p>
          <Link href="/features">See the full comparison</Link>
        </p>
      </div>
    </section>
  );
}

export function Legend() {
  return (
    <ul className="legend" aria-label="Legend">
      {(["y", "p", "n"] as const).map((value) => (
        <li key={value}>
          <Mark value={value} />
          {value === "y" ? "Supported" : value === "p" ? "Partial, see the note" : "Not supported"}
        </li>
      ))}
    </ul>
  );
}
