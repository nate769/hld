import { test, expect } from "bun:test";
import { normalize, prefixesOf } from "../src/config";
import { fnv1a32, HashRing } from "../src/hash-ring";

test("normalize handles standard strings and Unicode NFC normalization", () => {
  expect(normalize("  Google  ")).toBe("google");
  expect(normalize("GOOGLE")).toBe("google");
  
  // NFC Normalization test: \u0065\u0301 (e + combining acute accent) normalizes to \u00e9 (é)
  const combined = "cafe\u0065\u0301";
  const normalized = normalize(combined);
  expect(normalized).toBe("cafe\u00e9");
  expect(normalized.length).toBe(5);
});

test("prefixesOf slices emojis and surrogate pairs safely without splitting them", () => {
  const emojiQuery = "ab😂cd";
  const prefixes = prefixesOf(emojiQuery);
  
  // Length should match character count (emoji 😂 is 1 char in code point array)
  expect(prefixes.length).toBe(5);
  expect(prefixes[0]).toBe("a");
  expect(prefixes[1]).toBe("ab");
  expect(prefixes[2]).toBe("ab😂"); // The emoji is kept whole!
  expect(prefixes[3]).toBe("ab😂c");
  expect(prefixes[4]).toBe("ab😂cd");
  
  // Verify that none of the prefixes have broken surrogate pairs
  for (const prefix of prefixes) {
    expect(prefix).not.toContain("\uD83D"); // should not contain incomplete surrogate parts
  }
});

test("fnv1a32 computes standard-compliant FNV-1a over UTF-8 bytes", () => {
  // Standard FNV-1a 32-bit values
  expect(fnv1a32("")).toBe(0x811c9dc5);
  expect(fnv1a32("a")).toBe(0xe40c59e6);
  expect(fnv1a32("google")).toBe(0xbe7f3f15);
  
  // Unicode string FNV-1a UTF-8 byte hashing test
  // "café" in UTF-8 bytes: [99, 97, 102, 195, 169]
  expect(fnv1a32("café")).toBe(0x56a4220b);
});

test("HashRing throws an error if initialized with an empty shards list", () => {
  expect(() => new HashRing([], 150)).toThrow("Cannot route key: HashRing has no active shards");
  
  // Verify route throws on empty ring
  const emptyRing = new HashRing([], 150);
  expect(() => emptyRing.route("key")).toThrow("Cannot route key: HashRing has no active shards");
});
