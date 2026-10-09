import { expect, test } from "bun:test";
import { readProps, writeProps } from "../src/amqp10/map.ts";
import type { Broker } from "../src/broker/index.ts";
import { get } from "../src/broker/delivery.ts";

const props = writeProps({ headers: [["tag", { t: "S", v: "held" }]], correlationId: "c-held", contentType: "text/plain", deliveryMode: 2 });
const encoded = Buffer.from(props).toString("base64");
const body = Buffer.from("held").toString("base64");

for (const queueType of ["quorum", "classic"]) {
  for (const stack of ["rust", "bun"]) {
    test(`remote ${queueType} get preserves ${stack} body and properties`, async () => {
      const q = { home: "remote", argsParsed: { queueType } };
      const calls: Array<{ peer: string; op: string; args: unknown }> = [];
      const raw = stack === "rust"
        ? { delivery_id: 7, message: { message_id: "qid", body_b64: body, propRaw: encoded, exchange: "", routing_key: "qq", persistent: true } }
        : { msg: { id: "qid", rowId: null, body, propRaw: encoded, exchange: "", routingKey: "qq", headers: [], persistent: true, priority: 0, expiresAt: null, redelivered: false } };
      const broker = {
        key: () => "queue", queues: new Map([["queue", q]]),
        waitQuorumLeader: async () => {}, promoteIfLeader: () => {},
        isQuorumLeader: () => false, quorumLeader: () => "remote", isLocalHome: () => false,
        dropLocal: () => {},
        cluster: { consensus: { node: {} }, call: async (peer: string, op: string, args: unknown) => {
          calls.push({ peer, op, args }); return raw;
        } },
      } as unknown as Broker;
      const message = await get.call(broker, "/", "qq", false);
      expect(Buffer.from(message!.body).toString()).toBe("held");
      expect(message!.propRaw).toEqual(props);
      expect(readProps(message!.propRaw)).toMatchObject({ correlationId: "c-held", contentType: "text/plain", deliveryMode: 2 });
      expect(readProps(message!.propRaw).headers).toEqual([["tag", { t: "S", v: "held" }]]);
      expect(calls).toEqual([{ peer: "remote", op: "get", args: { vhost: "/", queue: "qq", noAck: false, no_ack: false } }]);
    });
  }
}
