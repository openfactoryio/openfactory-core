# Accessing OpenFactory Applications via Traefik

OpenFactory uses Traefik to expose applications via domain names instead of ports.

Each application is reachable through a hostname such as:
```
demo.openfactory.local  
scheduler.openfactory.local  
```

The suffix `openfactory.local` is the OpenFactory base domain, configured via the `OPENFACTORY_BASE_DOMAIN` setting in the `openfactory.yml` configuration file.

In the remainder of this document, it is assumed:
```
OPENFACTORY_BASE_DOMAIN=openfactory.local
```

This is compatible with the Docker Compose configurations provided in this folder to deploy Traefik.

All application hostnames are generated under this domain.


---

## 🧠 How it works

Two layers are involved:

1. DNS resolution  
   hostname → Traefik IP  

2. Traefik routing  
   hostname → application  

Both must be correctly configured.

---

## ⚙️ Step 1 — Deploy Traefik

### Docker (single-node)

```bash
docker compose -f docker-compose.traefik.yml up -d
```

### Docker Swarm (cluster)

```bash
docker stack deploy -c docker-compose.traefik.swarm.yml traefik
```

### Verify

```bash
curl http://<TRAEFIK-IP>
```

Expected:

* 404 response → Traefik is running

---

## ⚙️ Step 2 — Configure DNS

You must ensure that all `*.openfactory.local` hostnames resolve to: `<TRAEFIK-IP>`

---

## 🚀 Setup Options

### Option 1 — hosts file

Edit:

```bash
sudo nano /etc/hosts
```

Add:

```
<TRAEFIK-IP> traefik.openfactory.local
```
and for each application exposed via Traefik:
```
<TRAEFIK-IP> demo.openfactory.local
<TRAEFIK-IP> scheduler.openfactory.local
...
```

---

### Option 2 — local DNS server (recommended)

Use dnsmasq to resolve:

```
*.openfactory.local → <TRAEFIK-IP>
```

➡️ Full documentation: [dns.md](./dns.md)

Using a local DNS server will avoid the need to define entries for each new application in the `/etc/hosts` file. This mirrors how DNS is handled in production.

---

## 🌐 Access

```
http://demo.openfactory.local
http://scheduler.openfactory.local
```

Traefik dashboard:
```
http://traefik.openfactory.local/dashboard/
```

---

## 🧪 Troubleshooting

DNS:

```bash
getent hosts traefik.openfactory.local
```

Routing:

```bash
curl -H "Host: demo.openfactory.local" http://<TRAEFIK-IP>
```

---

## ⚠️ Notes

* Access via IP will not work
* Traefik routes only by hostname
* /etc/hosts does not support wildcards

---

## 🧭 Summary

DNS → hostname → IP
Traefik → hostname → service
