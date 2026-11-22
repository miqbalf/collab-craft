//! Reproducers the fuzz harness found, kept as regression tests: each is a base state an honest
//! history reached and an op (hex). Every one must now be refused by the gate, or applied without
//! a panic.
