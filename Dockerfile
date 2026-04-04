FROM rust:latest AS builder
WORKDIR /build
COPY . .
RUN cargo build --release --bin fakecloud-server

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/fakecloud-server /usr/local/bin/
EXPOSE 4566
ENTRYPOINT ["fakecloud-server"]
