import { test, expect } from "vitest";
import { randomUUID } from "node:crypto";
import jwt from "jsonwebtoken";
import { identity } from "../dist/security";
test("validates signature, issuer, audience and structured identity", () => {
  process.env.JWT_SECRET = "test-only-key-".repeat(6);
  const actor = { sub: randomUUID(), tenantId: randomUUID(), role: "member" };
  const token = jwt.sign(actor, process.env.JWT_SECRET, {
    issuer: "portfolio-auth",
    audience: "portfolio-api",
    expiresIn: 60,
  });
  expect(identity(token)).toEqual(actor);
  expect(() => identity("forged")).toThrow();
  expect(() =>
    identity(
      jwt.sign(actor, process.env.JWT_SECRET!, {
        issuer: "wrong",
        audience: "portfolio-api",
      }),
    ),
  ).toThrow();
});
