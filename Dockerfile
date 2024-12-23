FROM ubuntu:22.04 AS builder

RUN apt-get update && \
    apt-get install -y \
        cmake \
        g++ \
        make \
        libreadline-dev \
        libncurses-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN cmake -B build -S . \
    && cmake --build build --target Database

FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y \
        libreadline8 \
        libncurses5 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/build/Database /usr/local/bin/Database

CMD ["/usr/local/bin/Database"]
