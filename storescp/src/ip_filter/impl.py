from ipaddress import IPv4Address
from typing import List
import ipaddress


class IPFilter:
    def __init__(self,
                 whitelist: List[str] | None = None,
                 blacklist: List[str] | None = None):
        self.blacklist = [ipaddress.IPv4Network(ip) for ip in blacklist] if blacklist else []
        self.whitelist = [ipaddress.IPv4Network(ip) for ip in whitelist] if whitelist else [ipaddress.IPv4Network("0.0.0.0/0")]

    def parse_ip(self, ip: str) -> ipaddress.IPv4Address:
        return ipaddress.IPv4Address(ip)

    def is_ip_in_network_list(self,
                              ip: str,
                              network_list: List[ipaddress.IPv4Network]) -> bool:
        ip = self.parse_ip(ip)
        for network in network_list:
            if ip in network:
                return True
        else:
            return False

    def is_ip_allowed(self, ip: str) -> bool:
        return self.is_ip_in_network_list(ip, self.whitelist) and not self.is_ip_in_network_list(ip, self.blacklist)
