# Build: docker build -t grimoire .
# Worker:  docker run -v ./grimw.toml:/etc/grimw.toml grimoire grimw --config /etc/grimw.toml
# Daemon:  docker run -v grim-state:/root/.grimoire -p 6660:6660 grimoire grim daemon
#
# The image ships only grim/grimw plus git (workspaces are git worktrees).
# Agent CLIs are deliberately NOT baked in — layer your own on top:
#
#   FROM grimoire
#   RUN npm install -g @anthropic-ai/claude-code

FROM rust:1.95-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --locked --bins

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates git && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/grim /src/target/release/grimw /usr/local/bin/
CMD ["grim", "daemon"]
