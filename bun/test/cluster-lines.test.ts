import { expect, test } from "bun:test";
import { takeLines } from "../src/cluster.ts";

test("one cluster chunk yields every line and keeps a partial tail", () => {
  const n = 1000;
  let body = "";
  for (let i = 0; i < n; i++) body += `{"op":"quorum_drop","id":${i}}\n`;
  const taken = takeLines("", body);
  expect(taken.lines).toHaveLength(n);
  expect(taken.rest).toBe("");
  expect(JSON.parse(taken.lines[0] ?? "").id).toBe(0);
  expect(JSON.parse(taken.lines[n - 1] ?? "").id).toBe(n - 1);

  const partial = takeLines("{\"op\":\"x\"}\n{\"op\":", "\"y\"}\n");
  expect(partial.lines).toEqual(['{"op":"x"}', '{"op":"y"}']);
  expect(partial.rest).toBe("");

  const split = takeLines("", "{\"op\":\"x\"}\n{\"op\":");
  expect(split.lines).toEqual(['{"op":"x"}']);
  expect(split.rest).toBe('{"op":');
  const next = takeLines(split.rest, "\"z\"}\n\n");
  expect(next.lines).toEqual(['{"op":"z"}']);
  expect(next.rest).toBe("");
});
