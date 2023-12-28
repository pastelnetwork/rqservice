# rqservice

`rqservice` is the service that performs raptorQ encoding and decoding of the files into and from rq-symbols

## Build

### Install Rust and Cargo

```shell
curl https://sh.rustup.rs -sSf | sh
```

### Clone and build rqservice repo

```shell
git clone https://github.com/pastelnetwork/rqservice.git
cd rqservice
cargo build
```

### Start servcie:

```
rq-service --grpc-service=127.0.0.1:5005
```

or create file `rqconfig.toml` with the following content:
```
grpc-service = "127.0.0.1:50051"
```

Config file can be palce at default working directory of Pastel or specified with command line parameter

## Test

```shell
cd rqservice
cargo test
```

## More info

[Pastel Network Docs](https://docs.pastel.network/introduction/pastel-overview)


## Command line options

```
rqservice v0.1.0
Pastel Network <pastel.network>
RaptorQ Service

USAGE:
    rq-service [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c, --config-file <FILE>        Set path to the config file. (default: /Users/alexeykireyev/Library/Application
                                    Support/Pastel/rqservice)
    -s, --grpc-service <IP:PORT>    Set IP address and PORT for gRPC server to listen on. (default: 127.0.0.1:50051)
```
