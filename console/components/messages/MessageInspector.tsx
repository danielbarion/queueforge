"use client";

import { useEffect, useId, useRef, useState } from "react";
import { X, Download, Search } from "lucide-react";
import { Modal } from "@/components/ui/Modal";
import { callBroker } from "@/lib/client";
import { isDemo } from "@/lib/demo";
import { deadLetterDetails, inspectionBody, messageBytes, parseMessages, payloadText, replayAvailability, type MessageSnapshot } from "@/lib/messages";
import type { BrokerTarget } from "@/stores/broker";

function download(value: Blob, name: string) {
  const url = URL.createObjectURL(value); const link = document.createElement("a");
  link.href = url; link.download = name; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function MessageInspector({ broker, queue, onClose, deadLetter = false }: { broker: BrokerTarget; queue: { name: string; type?: string }; onClose: () => void; deadLetter?: boolean }) {
  const id = useId();
  const [messages, setMessages] = useState<MessageSnapshot[]>([]);
  const [selected, setSelected] = useState(0);
  const [count, setCount] = useState(1);
  const [filter, setFilter] = useState("");
  const [raw, setRaw] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [status, setStatus] = useState("");
  const [exchange, setExchange] = useState("");
  const [routingKey, setRoutingKey] = useState("");
  const [confirm, setConfirm] = useState(false);
  const generation = useRef(0);
  const working = useRef(false);
  useEffect(() => { generation.current++; setMessages([]); setSelected(0); setError(""); setStatus(""); setConfirm(false); working.current = false; setBusy(false); return () => { generation.current++; }; }, [broker.id, queue.name]);
  const message = messages[selected];
  const demo = isDemo(broker.url);
  const history = message ? deadLetterDetails(message) : [];
  const replay = message ? replayAvailability(broker.kind, demo, message) : { allowed: false, reason: "Fetch a complete message snapshot first." };
  const stream = queue.type === "stream";
  function pick(index: number) {
    setSelected(index); setConfirm(false);
    const item = messages[index]; const death = item && deadLetterDetails(item)[0];
    setExchange(death?.exchange ?? item?.exchange ?? ""); setRoutingKey(death?.routingKeys?.[0] ?? item?.routingKey ?? "");
  }
  async function fetchMessages() {
    if (working.current || stream) return;
    working.current = true; setBusy(true); setError(""); setStatus(""); setConfirm(false);
    const token = generation.current;
    try {
      const response = await callBroker(broker.url, "POST", `/api/queues/%2F/${encodeURIComponent(queue.name)}/get`, inspectionBody(broker.kind, count));
      if (token !== generation.current) return;
      if (response.status !== 200) throw new Error(`Inspection refused (HTTP ${response.status}).`);
      const snapshots = parseMessages(response.body);
      if (snapshots === null) throw new Error("The response could not be decoded safely. No complete snapshot is available.");
      setMessages(snapshots); setSelected(0);
      const first = snapshots[0];
      setExchange(first ? deadLetterDetails(first)[0]?.exchange ?? first.exchange : ""); setRoutingKey(first ? deadLetterDetails(first)[0]?.routingKeys?.[0] ?? first.routingKey : "");
      setStatus(snapshots.length ? `${snapshots.length} snapshot${snapshots.length === 1 ? "" : "s"} fetched${demo ? " from sample data" : " with requeue requested"}.` : "No ready messages returned.");
    } catch (caught) { if (token === generation.current) setError(caught instanceof Error ? caught.message : "Inspection failed. A fetch may already have affected delivery state; check the broker before retrying."); }
    finally { if (token === generation.current) { working.current = false; setBusy(false); } }
  }
  async function publishCopy() {
    if (!message || !replay.allowed || working.current || !confirm) return;
    working.current = true; setBusy(true); setError(""); setStatus(""); const token = generation.current;
    try {
      const response = await callBroker(broker.url, "POST", `/api/exchanges/%2F/${encodeURIComponent(exchange || "amq.default")}/publish`, { routing_key: routingKey, payload: message.payload, payload_encoding: message.encoding, properties: message.properties });
      if (token !== generation.current) return;
      if (response.status >= 400) throw new Error(`Copy refused (HTTP ${response.status}).`);
      const body = response.body as { routed?: boolean } | null;
      if (body?.routed === false) throw new Error("The copy was not routed to any queue. Check the exchange and routing key.");
      if (body?.routed !== true) throw new Error("The API did not confirm routing. Check the broker before trying again; a copy may have been published.");
      setStatus("Copy routed. The inspected original remains queued. Routing does not confirm consumer processing."); setConfirm(false);
    } catch (caught) { if (token === generation.current) setError(`${caught instanceof Error ? caught.message : "Publish failed."} If the request reached the broker, a copy may exist. Check before retrying.`); }
    finally { if (token === generation.current) { working.current = false; setBusy(false); } }
  }
  const matches = messages.map((item, index) => ({ item, index })).filter(({ item }) => `${item.routingKey} ${item.payload} ${payloadText(item)} ${JSON.stringify(item.properties)}`.toLowerCase().includes(filter.toLowerCase()));
  return <Modal labelId={id} onClose={onClose} drawer>
    <header className="qf-inspector-header"><div><span className="qf-eyebrow">{deadLetter ? "Dead-letter inspection" : "Message inspector"} · {demo ? "sample data" : broker.name}</span><h2 id={id} className="mt-2 break-all text-xl font-semibold">{queue.name}</h2></div><button className="btn btn-ghost btn-sm btn-square" aria-label="Close message inspector" onClick={onClose}><X className="size-5" /></button></header>
    <div className="qf-inspector-body">
      <p className="text-sm text-warning">{demo ? "Sample snapshots only. This action does not contact a broker." : "Fetching consumes messages temporarily and requests requeue. It can change ordering, redelivery state and delivery-limit behavior; it is not a passive peek."}</p>
      {broker.kind === "rust" && <p className="text-xs text-muted">Rust may return the same head message repeatedly when fetching a batch. Headers are omitted by its management API.</p>}
      {broker.kind === "php" && <p className="text-xs text-muted">PHP omits message properties and may return text without preserving binary bytes.</p>}
      {stream && <p className="text-sm text-warning">Stream queues do not support this basic.get inspection workflow.</p>}
      <div className="flex flex-wrap items-end gap-3"><label className="qf-field"><span>Maximum messages</span><select className="select select-bordered" value={count} disabled={busy} onChange={(event) => setCount(Number(event.target.value))}>{[1, 5, 10, 20].map((n) => <option key={n}>{n}</option>)}</select></label><button className="btn btn-primary" disabled={busy || stream} onClick={() => void fetchMessages()}>{busy ? "Working…" : demo ? "Load sample" : "Fetch and requeue"}</button></div>
      {error && <p className="text-sm text-error" role="alert">{error}</p>}{status && <p className="text-sm text-muted" role="status">{status}</p>}
      {messages.length > 0 && <>
        <div className="flex flex-wrap gap-3"><label className="qf-field flex-1"><span className="flex items-center gap-2"><Search className="size-3" />Filter snapshots, headers or failure reasons</span><input className="input input-bordered w-full" value={filter} onChange={(event) => setFilter(event.target.value)} /></label><button className="btn btn-ghost self-end" onClick={() => download(new Blob([JSON.stringify(messages, null, 2)], { type: "application/json" }), "message-snapshots.json")}><Download className="size-4" />Export snapshots</button></div>
        <label className="qf-field"><span>Snapshot ({matches.length} matching)</span><select className="select select-bordered w-full" disabled={busy} value={matches.some(({ index }) => index === selected) ? selected : ""} onChange={(event) => pick(Number(event.target.value))}><option value="" disabled>Select a matching snapshot</option>{matches.map(({ item, index }) => <option key={index} value={index}>#{index + 1} · {item.routingKey || "(empty key)"} · {item.payloadBytes} bytes</option>)}</select></label>
      </>}
      {message && <>
        <dl className="grid grid-cols-2 gap-3 text-sm">{[["Exchange", message.exchange || "(default)"], ["Routing key", message.routingKey || "(empty)"], ["Size", `${message.payloadBytes} bytes`], ["Redelivered", message.redelivered ? "Yes" : "No"], ["Expiration", message.properties.expiration ?? "Unavailable"], ["Properties", message.propertiesAvailable ? "Returned by API" : "Omitted by API"]].map(([label, value]) => <div key={String(label)}><dt className="text-xs text-muted">{String(label)}</dt><dd className="break-all font-mono">{String(value)}</dd></div>)}</dl>
        {!message.complete && <p className="text-sm text-warning">The returned payload length does not match the reported size. Replay is disabled.</p>}
        <div className="flex flex-wrap items-center justify-between gap-2"><h3 className="font-semibold">Payload</h3><div className="flex gap-2"><button className="btn btn-ghost btn-sm" aria-pressed={raw} onClick={() => setRaw(!raw)}>{raw ? "Show formatted" : `Show raw ${message.encoding}`}</button><button className="btn btn-ghost btn-sm" disabled={!message.complete} onClick={() => { const bytes = messageBytes(message); if (bytes) download(new Blob([new Uint8Array(bytes)], { type: "application/octet-stream" }), "message-payload.bin"); }}>Download payload</button></div></div>
        <pre className="qf-code">{raw ? message.payload : payloadText(message)}</pre>
        <h3 className="font-semibold">Properties and headers</h3><pre className="qf-code">{message.propertiesAvailable ? JSON.stringify(message.properties, null, 2) : "The API did not return message properties."}</pre>
        <h3 className="font-semibold">Recorded failure history</h3>{history.length === 0 ? <p className="text-sm text-muted">No x-death records were returned. This does not prove the message has never failed.</p> : <ul className="space-y-2">{history.map((death, index) => <li key={index} className="rounded-lg border border-base-300 p-3 text-sm"><strong>{death.reason ?? "Unknown reason"}</strong> · {death.count ?? "Unknown"} occurrence(s)<p className="text-xs text-muted">Queue {death.queue ?? "unknown"} · exchange {death.exchange ?? "unknown"} · original keys {death.routingKeys?.join(", ") ?? "unknown"}</p></li>)}</ul>}
        <section className="qf-replay"><h3 className="font-semibold">Replay a copy</h3><p className="mt-2 text-xs leading-6 text-muted">{replay.reason}</p><div className="mt-3 grid gap-3 sm:grid-cols-2"><label className="qf-field"><span>Target exchange (empty = default)</span><input className="input input-bordered w-full" value={exchange} disabled={!replay.allowed || busy} onChange={(event) => { setExchange(event.target.value); setConfirm(false); }} /></label><label className="qf-field"><span>Routing key</span><input className="input input-bordered w-full" value={routingKey} disabled={!replay.allowed || busy} onChange={(event) => { setRoutingKey(event.target.value); setConfirm(false); }} /></label></div>{confirm ? <div className="mt-4"><p className="mb-3 text-sm text-warning">Publish one copy to {exchange || "the default exchange"} with key {routingKey || "(empty)"}? The original is retained, so duplicate processing is possible. Saved headers, including x-death and expiration, are preserved.</p><div className="flex gap-2"><button className="btn btn-primary" disabled={busy} onClick={() => void publishCopy()}>Confirm publish copy</button><button className="btn btn-ghost" disabled={busy} onClick={() => setConfirm(false)}>Cancel</button></div></div> : <button className="btn btn-primary mt-4" disabled={!replay.allowed || busy} onClick={() => setConfirm(true)}>Review replay copy</button>}</section>
      </>}
    </div>
  </Modal>;
}
