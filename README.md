# rqservice

To start:

```
rq-service --grpc-service=127.0.0.1:5005
```
or create file `rqconfig.toml` with the following content:

```
grpc-service = "127.0.0.1:50051"
```

Config file can be palce at default working directory of Pastel or specified with commnd line parameter. See bellow:


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
