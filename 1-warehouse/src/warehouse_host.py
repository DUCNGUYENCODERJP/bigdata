"""Suy ra IP dang chay cluster slice 1, de client khong phai nhap tay mot dia chi de chet.

Ly do so script truoc day co gia tri mac dinh cung loi: Docker Desktop tren Windows
cong bo 8020/9083 ra MOI interface, nen `--host` bat buoc la IP LAN *hien tai* cua
may. May nay dang ngoi tren Wi-Fi, IP doi moi lan chuyen mang (da ghi nhan
192.168.100.19 -> 192.168.36.64 -> 192.168.100.17). Mot dia chi cu nam trong
mac dinh khong fail ngay ma treo: Spark tim Thrift theo IP cu cho toi khi
timeout, nguoi dung ket luan oan "cluster chet".

Thu tu uu tien (luon in ra de mo i duoc):
    1. --host tren dong lenh
    2. bien moi truong WAREHOUSE_HOST
    3. IP cua interface dang co duong dan mac dinh (do duoi day)

Khong co goi mang ra ngoai: socket UDP khong gui packet nao, hat nhan chi dung de
chon tuyen duong (route) va tra ve dia chi cua chinh may.
"""

from __future__ import annotations

import os
import socket

DEFAULT_ROUTE_PROBE = ("10.255.255.255", 1)  # khong co dich thuc, khong luu hanh


def detect_host() -> str:
    """IP LAN cua may nay, hoac Exception neu khong co mang."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(DEFAULT_ROUTE_PROBE)
        ip = probe.getsockname()[0]
    except OSError as exc:  # may khong co route mac dinh (mang tat, may tam thoi)
        raise RuntimeError(
            "Khong phat hien duoc IP cua may. Ket noi mang roi chay lai, hoac dat "
            "bien WAREHOUSE_HOST / dung --host <IP>."
        ) from exc
    finally:
        probe.close()
    if ip.startswith("127."):
        raise RuntimeError(
            f"IP phat hien duoc la {ip} (loopback) - khong dung duoc cho client ben "
            "ngoai mang Docker. Dung --host <IP Wi-Fi>, chay Set-Host.ps1 de biet IP."
        )
    return ip


def resolve_host(cli_value: str | None) -> str:
    """Tra ve host se dung, kem in dong bao cho nguoi doc log biet no tu dau ma ra."""
    if cli_value:
        print(f"[host] --host = {cli_value} (chi dinh tay)")
        return cli_value
    env_value = os.environ.get("WAREHOUSE_HOST")
    if env_value:
        print(f"[host] WAREHOUSE_HOST = {env_value}")
        return env_value
    auto = detect_host()
    print(f"[host] tu phat hien IP may chay cluster = {auto} "
          "(ghi de bang --host <IP> neu kho nam o may khac)")
    return auto
