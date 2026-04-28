# Configure Windows to use OpenFactory DNS

## 🎯 Goal

Allow your Windows machine to resolve:

```text
*.openfactory.local
```

using your OpenFactory DNS server, while keeping normal internet access.

---

## 1. Open network settings

* Press **Win + R**
* Type:

```text
ncpa.cpl
```

---

## 2. Edit your active network adapter

* Right-click your active connection (**Wi-Fi** or **Ethernet**)
* Click **Properties**

---

## 3. Configure IPv4

* Select:

```text
Internet Protocol Version 4 (TCP/IPv4)
```

* Click **Properties**

---

## 4. Set DNS servers

Configure:

```text
Preferred DNS server:   <DNS-IP>
Alternate DNS server:   8.8.8.8
```

👉 Replace `<DNS-IP>` with the IP of your OpenFactory (dnsmasq) machine

---

## 5. Apply changes

* Click **OK**
* Close all windows

---

## 6. Verify

Open Command Prompt and run:

```bat
nslookup traefik.openfactory.local
```

---

## 🎯 Result

You can now access:

```text
http://traefik.openfactory.local/dashboard/
http://demo.openfactory.local
```
