# Mini Database Project

This is a simple toy database application that accepts SQL queries in an interactive console

## Prerequisites

- [Docker](https://www.docker.com/) installed on your machine.

## How to Build

Open a terminal in the project root directory and run:

```bash
docker build -t database .
```

This will create a Docker image named `database`

## How to Run

```bash
docker run -it --rm database
```

Once the container starts, you will see a prompt like:

```
Welcome to mini DB vovinquity!
Type EXIT or QUIT to stop.

sql>
```

From here, you can enter SQL-like commands. Type `EXIT` or `QUIT` to terminate the application (or press `Ctrl+C`)
