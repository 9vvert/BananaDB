#!/bin/bash
cargo update indexmap@2.2.1 --precise 2.2.1
cargo +1.74.1 fetch --locked
cargo +1.74.1 vendor vendor --locked
