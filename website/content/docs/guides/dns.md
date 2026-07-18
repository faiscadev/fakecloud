+++
title = "Real DNS: resolve Route 53 records for local containers"
description = "Run fakecloud as a DNS resolver so records created in Route 53 actually resolve for your containers, with no dnsmasq or /etc/hosts layer."
weight = 5
+++

A local multi-service setup often needs real DNS: service A reaching service B by hostname, or an app doing an `A`/`CNAME`/`MX`/`TXT` lookup for a record it expects. You can create the record in fakecloud's Route 53, but by default nothing answers the lookup, so people bolt on a separate dnsmasq or `/etc/hosts` layer -- a second source of truth beside the Route 53 records already declared.

Start fakecloud with `--dns` and it runs an actual DNS resolver (UDP + TCP) that answers straight from the Route 53 zones and records you created. Point a container's resolver at fakecloud and a normal lookup resolves to the local endpoint. Route 53 becomes the one source of truth -- used exactly like production. It is the companion to [instance/task credentials](/docs/guides/instance-credentials/): credentials and DNS are the two environmental surfaces that let a real app run locally, unmodified.

## Enable the resolver

```sh
# Port 53 needs root; use a high port for an unprivileged run.
fakecloud --dns --dns-addr 127.0.0.1:15353
```

Create a zone and a record as usual, then resolve it over real DNS:

```sh
ID=$(aws --endpoint-url http://localhost:4566 route53 create-hosted-zone \
  --name example.com --caller-reference $(date +%s) \
  --query 'HostedZone.Id' --output text)

aws --endpoint-url http://localhost:4566 route53 change-resource-record-sets \
  --hosted-zone-id "$ID" --change-batch '{
    "Changes":[{"Action":"CREATE","ResourceRecordSet":{
      "Name":"api.example.com","Type":"A","TTL":60,
      "ResourceRecords":[{"Value":"10.0.0.5"}]}}]}'

dig @127.0.0.1 -p 15353 api.example.com A     # -> 10.0.0.5
```

Answered types: `A`, `AAAA`, `CNAME`, `MX`, `TXT`, `NS`, `PTR`, `SPF`, `CAA`. `CNAME`s are chased into local zones, so a query for an alias returns the `CNAME` plus the records resolved at its target (address records are forward-resolved even when the target is an external name). Zone selection is a longest-suffix match across every account's hosted zones.

## Names outside Route 53 forward upstream

So a container can use fakecloud as its **sole** resolver, any name that falls in no Route 53 zone is forwarded to an upstream resolver:

```sh
dig @127.0.0.1 -p 15353 registry-1.docker.io A   # -> forwarded, real answer
```

The upstream defaults to the first non-loopback `nameserver` in `/etc/resolv.conf`, falling back to `8.8.8.8:53`. Override it with `--dns-upstream 1.1.1.1:53` (or `FAKECLOUD_DNS_UPSTREAM`).

## Point containers at fakecloud (docker-compose)

Run fakecloud where containers can reach it (here on a fixed address in the compose network) and set it as the `dns:` server for the services that should resolve your Route 53 records. Point `A` records at addresses reachable on the local network (container IPs or the host):

```yaml
services:
  fakecloud:
    image: faiscadev/fakecloud
    command: ["--dns", "--dns-addr", "0.0.0.0:53"]
    networks:
      appnet:
        ipv4_address: 172.28.0.53
    ports:
      - "4566:4566"

  app:
    image: my-app
    dns:
      - 172.28.0.53          # resolve via fakecloud's Route 53
    networks: [appnet]

networks:
  appnet:
    ipam:
      config:
        - subnet: 172.28.0.0/16
```

Create `api.example.com -> 172.28.0.x` (a container's address) in Route 53 and `app` reaches it by name. Non-Route 53 names (package registries, APIs) still resolve because fakecloud forwards them upstream.

Notes:

- Binding port 53 needs root (or `CAP_NET_BIND_SERVICE`); in a container fakecloud runs as its own process so `0.0.0.0:53` is fine. For a host run without root, use a high port and point clients at it.
- To resolve **container names** (rather than IPs you set as record values), keep Docker's embedded DNS in the loop: put a `CNAME` in Route 53 pointing at the container name and let the container's own resolver chain handle it, or set the record's value to the container's address.
- Wildcard record sets (`*.example.com`) are matched at a single label level (the common case). Weighted / latency / geo / failover routing policies are not evaluated: every matching record set is returned. Single-label `*.example.com` wildcards work; multi-level wildcards and `NAPTR`/`DS`/`ANY` queries are not served. Negative answers (`NXDOMAIN`/`NODATA`) do not carry the zone `SOA` in the authority section, so downstream resolvers cannot negatively-cache them. This is a local development resolver, not a full authoritative/recursive nameserver.

## Assert resolution from a test (no socket)

The same resolution logic is exposed over HTTP so a test can check what the resolver *would* answer without binding a UDP port (handy when `--dns` is off or port 53 is unavailable in CI):

```bash
curl 'http://localhost:4566/_fakecloud/dns/resolve?name=api.example.com&type=A'
# { "name": "api.example.com", "type": "A", "status": "ANSWERED",
#   "authoritative": true,
#   "records": [ { "name": "api.example.com", "type": "A", "ttl": 300, "value": "172.28.0.10" } ] }
```

`type` defaults to `A` and accepts `A`, `AAAA`, `CNAME`, `MX`, `TXT`, `NS`, `PTR`, `SPF`, `CAA`, `SRV`, `SOA`; an unsupported type returns `400`. `status` is one of `ANSWERED`, `NODATA` (name exists, no record of that type), `NXDOMAIN` (name not in any local zone), or `NOT_AUTHORITATIVE` (name is outside every Route 53 zone, so the `--dns` resolver would forward it upstream). Every [test-assertion SDK](/docs/sdks/) wraps this as `dnsResolve` / `dns_resolve`:

```python
res = fc.dns_resolve("api.example.com", "A")
assert res.status == "ANSWERED"
assert res.records[0].value == "172.28.0.10"
```
