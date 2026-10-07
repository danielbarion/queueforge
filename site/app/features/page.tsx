import type { Metadata } from "next";
import { FeatureTable, Legend, Tallies } from "../feature-table";
import { AUDITED, featureGroups } from "../features";

export const metadata: Metadata = {
  title: "Features",
  description:
    "RabbitMQ features checked against the Rust, Bun, and PHP brokers. Not a complete RabbitMQ 4.3 list.",
};

export default function FeaturesPage() {
  const count = featureGroups.reduce((sum, group) => sum + group.rows.length, 0);
  return (
    <main id="content">
      <section className="page-intro">
        <div className="wrap">
          <p className="kicker">Features · read from source {AUDITED}</p>
          <h1>Next to RabbitMQ, line by line.</h1>
          <p className="lede">
            {count} RabbitMQ features, checked against each broker&apos;s source. This is not every
            RabbitMQ 4.3 feature. Quorum priorities, delayed retry, consumer timeouts, and stream
            filters have no row here. Partial means the feature works with a gap, and the note names
            the gap.
          </p>
        </div>
      </section>

      <section className="score-section">
        <div className="wrap">
          <Tallies />
          <Legend />
          <FeatureTable />
          <p className="caption">
            QueueForge cells come from each broker&apos;s source and tests, not its README.
            RabbitMQ cells marked plugin need a plugin that ships with RabbitMQ, except delayed
            messages, which is a community plugin. The QueueForge extra protocols are shims: they
            accept the frames a basic client sends, not the whole specification.
          </p>
        </div>
      </section>
    </main>
  );
}
