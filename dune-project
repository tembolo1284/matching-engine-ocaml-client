(lang dune 2.9)
(name matching_engine_client)
(version 0.1.0)

(generate_opam_files true)

(source (github yourusername/matching-engine-ocaml-client))
(license MIT)
(authors "Paul Lopez")
(maintainers "nik.lopez381@gmail.com")

(package
 (name matching_engine_client)
 (synopsis "OCaml client for high-performance matching engine")
 (description "A client library and CLI for interacting with the matching engine. Supports TCP/UDP transport, CSV/Binary protocols, auto-discovery, and multicast market data subscription.")
 (depends
  (ocaml (>= 4.14.0))
  (dune (>= 3.0))))
