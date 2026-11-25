# fourmeme-token-sniper

Rust-based tooling for monitoring and trading tokens on BNB Chain, including four.meme workflows.

This is a clean source snapshot of the `smart-wallet-fast` working branch. Local configuration, Telegram session files, logs, build output, and prior Git history are not included.

## Configuration

Provide credentials and private keys through a local `.env` file or environment variables. Never commit session files or live credentials. Review the configuration names in `src/config.rs` for the settings required by the commands you run.

## Build

```sh
cargo build --release
```

This repository contains code derived from [0xRustPro/BSC-BNB-four.meme-fourmeme-token-sniper](https://github.com/0xRustPro/BSC-BNB-four.meme-fourmeme-token-sniper) with additional changes.
