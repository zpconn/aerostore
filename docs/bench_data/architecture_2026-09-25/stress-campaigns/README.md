# Preserved concentrated lifecycle stress campaigns

These are the original, pre-fleet workload campaigns. Their transaction histories and diagnostics remain useful stress evidence, but the sequential lifecycle corpus generally keeps at most one logical flight family live at a time. Configured identity count therefore does not establish a simultaneously populated fleet, and global background operations can be empty. These results do not qualify a HyperFeed replacement or establish a 10× performance advantage.

Each compressed bundle preserves regular JSON, JSONL, and log files at their original relative paths, including full history receipts where captured. The adjacent `*-campaign.json` files are exact, inspectable copies. Each `*-manifest.json` records the original source directory, every selected file's SHA-256 and byte length, and the compressed bundle hash. Every logical archive member was read back and checked. Identical files may be represented by ordinary tar hardlinks to an earlier member.

| Campaign | Trials recorded | Completed | Original campaign gate |
| --- | ---: | --- | --- |
| `full-final` | 48 | yes | failed |
| `metrics-final` | 48 | yes | failed |
| `retention-final` | 3 | yes | passed, limited exploratory scope |
| `interrupted-full` | 16 | no | failed / incomplete |

The interrupted campaign was stopped when independent review found that an open transaction's reply write could exceed its transaction deadline. It used the earlier service source and binary; it is not a completed campaign. The later three campaigns use the corrected service source. Their recorded source and binary hashes remain authoritative even though the working tree later acquired a separate fleet workload.

Build provenance is retained in the [main archive](../build-provenance.json), with the exact later stress sources in [stress-source.tar.gz](../stress-source.tar.gz). The interrupted campaign's [earlier build provenance](../pre-reply-deadline-build-provenance.json) and [earlier service evidence/source snapshot](../service-availability-pre-reply-deadline/README.md) preserve that earlier implementation. Later service regression evidence is [separate](../service-availability/README.md).

Private coordinator/worker request configurations, mappings, WAL files, sockets, binaries and temporary files are excluded. Exclusion does not modify any retained receipt. Absolute paths inside retained reports describe the original workspace; resolve archived case artifacts by their suffix relative to the manifest's `source_root`. The original reports remain subject to their own explicit contract and qualification limitations.
