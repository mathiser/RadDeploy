from ip_filter import IPFilter
import pytest


def test_ip_filter():
    av = IPFilter(["192.168.0.0/24", "192.168.1.255"],
                  ["19.16.1.0/24", "19.16.0.0"])

    assert av.is_ip_allowed("192.168.0.4")  # in whitelist
    assert not av.is_ip_allowed("192.1.1.1")  # in neither
    assert not av.is_ip_allowed("19.16.0.0")  # in blacklist


def test_ip_filter_no_blacklist():
    av = IPFilter(whitelist=["192.168.0.0/24", "192.168.1.255"])

    assert av.is_ip_allowed("192.168.0.4")  # in whitelist
    assert not av.is_ip_allowed("192.1.1.1")  # in neither
    assert not av.is_ip_allowed("19.16.0.0")  # in blacklist


def test_ip_filter_no_whitelist():
    av = IPFilter(blacklist=["192.168.0.0/24", "192.168.1.255"])

    assert not av.is_ip_allowed("192.168.0.4")  # in whitelist
    assert av.is_ip_allowed("192.1.1.1")  # in neither
    assert av.is_ip_allowed("19.16.0.0")  # in blacklist


if __name__ == '__main__':
    pytest.main()
