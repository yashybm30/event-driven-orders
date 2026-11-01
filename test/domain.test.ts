import { test, expect } from "vitest";
import { price } from "../dist/app";
test("order pricing is authoritative and rejects unknown SKUs and invalid quantities", () => {
  expect(
    price([
      { sku: "book", quantity: 2 },
      { sku: "keyboard", quantity: 1 },
    ]),
  ).toBe(10500);
  expect(() => price([{ sku: "forged", quantity: 1 }])).toThrow();
  expect(() => price([{ sku: "book", quantity: -1 }])).toThrow();
  expect(() => price([])).toThrow();
});
