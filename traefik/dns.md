# 📄 Setup Local DNS Server for OpenFactory (dnsmasq)

This guide explains how to configure a local DNS server on the machine hosting Traefik so that all OpenFactory applications can be accessed via hostnames like:

```text
demo.factory.local
scheduler.factory.local
```

---

## 🎯 Goal

All hostnames matching:

```text
*.factory.local
```

should resolve to:

```text
<TRAefik-IP>
```

---

## 1. Install dnsmasq

```bash
sudo apt update
sudo apt install dnsmasq
```

---

## 2. Configure wildcard domain

Create a dedicated config file:

```bash
sudo nano /etc/dnsmasq.d/openfactory.conf
```

Add:

```text
address=/factory.local/<TRAefik-IP>
host-record=factory.local,<TRAefik-IP>
```

👉 Replace `<TRAefik-IP>` with the IP of the machine running Traefik

---

## 3. Configure systemd-resolved (Ubuntu)

Create:

```bash
sudo mkdir -p /etc/systemd/resolved.conf.d
sudo nano /etc/systemd/resolved.conf.d/openfactory.conf
```

Add:

```text
[Resolve]
DNSStubListener=no
DNS=127.0.0.1
Domains=~factory.local
```

> dnsmasq will resolve *.factory.local locally and forward all other DNS requests to the configured upstream DNS servers (typically corporate DNS servers provided by DHCP/VPN).

## 4. Restart services

Restart:

```bash
sudo systemctl restart systemd-resolved
sudo systemctl restart dnsmasq
```

Verify:

```bash
getent hosts demo.factory.local
```

---

## 5. Verify locally

```bash
nslookup demo.factory.local 127.0.0.1
```

Expected result:

```text
Name:    demo.factory.local
Address: <TRAefik-IP>
```

---

## 6. Open firewall (if needed)

```bash
sudo ufw allow 53
```

---

## 7. Test from another machine

From any machine on the network:

```bash
nslookup demo.factory.local <TRAefik-IP>
```

---

# 🧠 How it works

* Any request for `*.factory.local`
* is resolved to the Traefik node
* Traefik then routes based on hostname to the correct app

---

# ⚠️ Important notes

* This setup replaces the need for `/etc/hosts`
* Works for unlimited apps without extra configuration
* Requires clients to use this machine as DNS server
