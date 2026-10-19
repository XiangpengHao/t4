# 01 — On-disk layout

A store is one file of 4 KiB pages. On disk there is a write-ahead log
(WAL) and value bytes, nothing else: no index, no free list, no superblock.
Everything else is rebuilt in memory by replaying the WAL at mount. All
integers are little-endian.

## The file

```text
offset  0K       4K       8K       12K      16K      20K      24K
        +--------+--------+--------+--------+--------+--------+--------+
        | WAL 0  | value a         | WAL 1  | (hole) | value b| ...
        +--------+--------+--------+--------+--------+--------+--------+
           |                          ^
           +---- next_page = 12K -----+
```

- **Page 0 is always the first WAL page.** A fresh file is exactly that one
  page, empty.
- **Everything after it comes from one allocator**: a bump pointer at the
  end of the file, plus a free list of holes (in memory only). WAL pages
  only ever come from the end of the file; values try the holes first. So
  WAL pages and values interleave.
- **Pages carry no type tag.** A WAL page is one reached through the
  `next_page` chain from page 0. A value page lies inside an extent that a
  live WAL entry points at. Any other page is a hole, or leaked space.

## WAL page

```text
 0       4     6     8               16        20        24             32
 +-------+-----+-----+---------------+---------+---------+--------------+
 | magic | ver | 0 0 | next_page     | count   | used    | lsn          |
 | BTF4  | u16 |     | u64           | u32     | u32     | u64, unused  |
 +-------+-----+-----+---------------+---------+---------+--------------+

 32                                                     used         4096
 +-------------+-------------+-----+-------------+------+--------------+
 | entry 0     | entry 1     | ... | entry n-1   |       zeros         |
 +-------------+-------------+-----+-------------+------+--------------+
```

| bytes  | field       | meaning                                              |
|--------|-------------|------------------------------------------------------|
| 0..4   | `magic`     | `BTF4` (`42 54 46 34`)                               |
| 4..6   | `version`   | format version, currently 3                          |
| 6..8   | —           | zero                                                 |
| 8..16  | `next_page` | offset of the next WAL page; 0 ends the chain        |
| 16..20 | `count`     | number of entries in the page                        |
| 20..24 | `used`      | bytes used, header included: 32 + the entries' bytes |
| 24..32 | `lsn`       | written as 0 and never read                          |

Entries are packed back to back from byte 32, with no alignment. The bytes
after `used` are zero.

## WAL entry

```text
 0         2       3       4                12          16              24
 +---------+-------+-------+----------------+-----------+---------------+
 | key_len | flags |   0   | offset         | length    | lsn           |
 | u16     | u8    |       | u64            | u32       | u64           |
 +---------+-------+-------+----------------+-----------+---------------+
 24                     24 + key_len
 +----------------------+
 | key bytes            |
 +----------------------+
```

| bytes   | field     | live entry                     | tombstone       |
|---------|-----------|--------------------------------|-----------------|
| 0..2    | `key_len` | key length, at most 255        | same            |
| 2       | `flags`   | 0                              | 1               |
| 3       | —         | zero                           | zero            |
| 4..12   | `offset`  | where the value starts         | 0               |
| 12..16  | `length`  | value length in bytes          | 0               |
| 16..24  | `lsn`     | log sequence number            | same            |
| 24..    | key       | `key_len` bytes                | same            |

- An entry takes 24 + `key_len` bytes, so 24 to 279: a page holds 169
  entries of empty keys, or 14 of 255-byte keys.
- LSNs strictly increase along the whole chain, across pages. Replay
  refuses a log where they don't.

## Value extent

```text
 offset: page aligned, at least 4K
 v
 +-------------------+-------------------+-------------------+
 | value bytes                                    |  zeros   |
 +-------------------+-------------------+-------------------+
 |<---------------------- length ---------------->|
 |<-------------- padded = round_up(length, 4K) ------------>|
```

- A value takes `ceil(length / 4K)` contiguous pages, zero-padded to the
  last page's end: what direct I/O needs.
- An empty value has `offset = 0` and `length = 0`, and takes no space.
- A value is under 4 GiB: `length` and the padded length are both `u32`.
- The extent holds bare bytes: no header, no key, no checksum. Only the
  WAL entry that points at it says what it is.

## How the WAL grows

Every append rewrites the whole tail page in place, with one more entry.
When the entry doesn't fit:

```text
 before                          after
 +--------------------+          +--------------------+    +---------------+
 | WAL k (tail, full) |          | WAL k              |--->| WAL k+1 (tail)|
 | next_page = 0      |          | next_page = P      |    | new entry     |
 +--------------------+          +--------------------+    +---------------+
                                  rewritten in place        page P, taken
                                                            from the file's end
```

The old tail (with its new `next_page`) and page P go out as one write
request. With io_uring, the two writes are linked: the old tail first, then
P. The fallback backend runs them in parallel.

## In memory only

| state         | what it is                                              | rebuilt by replay from                       |
|---------------|---------------------------------------------------------|----------------------------------------------|
| index         | `bbtree::BBTreeMap`: key → LSN and extent, or tombstone | the latest entry per key                     |
| holes         | free extents, by padded size                            | extents that later entries overwrote or removed |
| file tail     | the next free page at the end of the file               | page-aligned max of file length, highest value end, highest WAL page end |
| WAL tail page | a copy of the last WAL page                             | the last page on the chain                   |
| next LSN      | the LSN the next entry gets                             | the last entry's LSN + 1                     |

Tombstones stay in the live index (so an older concurrent put can't bring a
key back) but not in the replayed one: replay drops the key.

## A worked example

Starting from an empty file:

| op                  | entry (in WAL 0)                     | file space                          |
|---------------------|--------------------------------------|-------------------------------------|
| put `a`, 5000 bytes | live `a`, offset 4K, length 5000, lsn 0 | `[4K, 12K)` from the end of the file |
| put `b`, 100 bytes  | live `b`, offset 12K, length 100, lsn 1 | `[12K, 16K)` from the end of the file |
| remove `a`          | tombstone `a`, lsn 2                 | `[4K, 12K)` becomes an 8K hole      |
| put `c`, 3000 bytes | live `c`, offset 4K, length 3000, lsn 3 | splits the hole: `[4K, 8K)` for `c`, `[8K, 12K)` stays a hole |

Afterwards:

```text
 0K       4K       8K       12K      16K
 +--------+--------+--------+--------+
 | WAL 0  | c      | hole   | b      |
 +--------+--------+--------+--------+

 WAL 0: [live a 4K/5000 #0] [live b 12K/100 #1] [tomb a #2] [live c 4K/3000 #3]
```

Replaying WAL 0 gives back the same state:

1. Live `a`: index `a → [4K, 5000)`.
2. Live `b`: index `b → [12K, 100)`.
3. Tombstone `a`: drops `a`; its padded extent `[4K, 12K)` becomes a hole.
4. Live `c` at 4K, padded 4K: no hole of exactly 4K, but the 8K hole at 4K
   is within twice that, so replay splits it as the allocator did: `c`
   takes `[4K, 8K)`, and `[8K, 12K)` stays a hole.

Result: index `{b, c}`, holes `{[8K, 12K)}`, file tail 16K, next LSN 4.

## What the format leaves out

- **No checksums**, on WAL pages or values. Replay checks only that each
  page is well formed (magic, version, sizes, entries that decode) and that
  LSNs increase. Any page that fails stops the mount with an error.
- **No record of the free space.** Holes come from the log: an extent is
  free once a later entry overwrites or removes the key that pointed at it.
  Space a put took from the end of the file, but never logged (a crash
  between the value write and the WAL write), stays unused until a snapshot
  rewrites the store.
