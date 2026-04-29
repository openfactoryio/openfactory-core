# Configure Ubuntu Server to use OpenFactory DNS (DHCP-safe)

## 🎯 Goal

Resolve:

```text id="goal"
*.openfactory.local
```

using OpenFactory DNS while keeping all DHCP/corporate DNS untouched.

---

# 🧠 Design

We use only:

* `systemd-resolved` (already on Ubuntu)
* DHCP DNS (unchanged)
* per-domain DNS override (split DNS)

---

# 1. Create split-DNS configuration

```bash id="step1"
sudo mkdir -p /etc/systemd/resolved.conf.d
sudo nano /etc/systemd/resolved.conf.d/openfactory.conf
```

Add:

```ini id="cfg1"
[Resolve]
DNS=<DNS-IP>
Domains=~openfactory.local
```
where `<DNS-IP>` is the IP of your DNS server.

---

## 🧠 What this does

* `DNS=`: tells systemd-resolved which DNS to use for that domain
* `Domains=~openfactory.local`: applies ONLY to that domain
* everything else continues using DHCP DNS automatically

---

# 3. Restart system resolver

```bash id="step2"
sudo systemctl restart systemd-resolved
```

---

# 4. Verify configuration

```bash id="step3"
resolvectl status
```

You should see:

```text id="check"
DNS Domain: ~openfactory.local
```

And under your interface:

* DHCP DNS servers still listed (corporate DNS remains unchanged)

---

# 5. Test OpenFactory resolution

```bash id="test1"
resolvectl query traefik.openfactory.local
```

---

# 6. Test normal DNS still works

```bash id="test2"
resolvectl query google.com
```

---

# 🎯 Final result

| Query                 | Resolver             |
| --------------------- | -------------------- |
| `*.openfactory.local` | OpenFactory DNS      |
| everything else       | DHCP DNS (unchanged) |
