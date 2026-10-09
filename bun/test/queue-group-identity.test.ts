import { expect, test } from "bun:test";
import { queueGroup } from "../src/raft/node.ts";

test("queue group identity separates vhost and queue without legacy collisions", () => {
  expect(queueGroup("/a", "b/c")).not.toBe(queueGroup("/a/b", "c"));
  expect(queueGroup("/", "orders")).toBe("q:v2:2f:6f7264657273");
  expect(queueGroup("/á", "队列/📦")).toBe("q:v2:2fc3a1:e9989fe588972ff09f93a6");
  expect(queueGroup("", ":/")).not.toBe(queueGroup(":", "/"));
  // Every legacy identity contains '/', so this namespace cannot overlap it.
  expect(queueGroup("/", "orders")).not.toContain("/");
});
