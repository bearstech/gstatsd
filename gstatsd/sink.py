
# standard
import cStringIO
import sys
import time

# vendor
from gevent import socket
from gevent import ssl

E_BADSPEC = "bad sink spec %r: %s"
E_SENDFAIL = 'failed to send stats to %s %s: %s'


class Sink(object):

    """
    A resource to which stats will be sent.
    """

    def error(self, msg):
        sys.stderr.write(msg + '\n')

    def _parse_hostport(self, spec):
        try:
            parts = spec.split(':')
            if len(parts) == 2:
                return (parts[0], int(parts[1]))
            if len(parts) == 1:
                return ('', int(parts[0]))
        except ValueError, ex:
            raise ValueError(E_BADSPEC % (spec, ex))
        raise ValueError("expected '[host]:port' but got %r" % spec)


class GraphiteSink(Sink):

    """
    Sends stats to one or more Graphite servers.
    """

    def __init__(self, ca=None, cert=None, key=None, hostname=None):
        self._hosts = []
        self._ca = ca
        self._cert = cert
        self._key = key
        if hostname is None:
            hostname = socket.gethostname()
        self._hostname = hostname

    def add(self, spec):
        self._hosts.append(self._parse_hostport(spec))

    def send(self, stats):
        "Format stats and send to one or more Graphite hosts"
        buf = cStringIO.StringIO()
        now = int(time.time())
        num_stats = 0

        # timer stats
        pct = stats.percent
        timers = stats.timers
        for key, vals in timers.iteritems():
            if not vals:
                continue

            # compute statistics
            num = len(vals)
            vals = sorted(vals)
            vmin = vals[0]
            vmax = vals[-1]
            mean = vmin
            max_at_thresh = vmax
            if num > 1:
                idx = round((pct / 100.0) * num)
                tmp = vals[:int(idx)]
                if tmp:
                    max_at_thresh = tmp[-1]
                    mean = sum(tmp) / idx

            key = 'servers.%s.stats.timers.%s' % (self._hostname, key)
            buf.write('%s.mean %f %d\n' % (key, mean, now))
            buf.write('%s.upper %f %d\n' % (key, vmax, now))
            buf.write('%s.upper_%d %f %d\n' % (key, pct, max_at_thresh, now))
            buf.write('%s.lower %f %d\n' % (key, vmin, now))
            buf.write('%s.count %d %d\n' % (key, num, now))
            num_stats += 1

        # counter stats
        counts = stats.counts
        for key, val in counts.iteritems():
            buf.write('servers.%s.stats.%s %f %d\n' % (self._hostname, key, val / stats.interval, now))
            buf.write('servers.%s.stats_counts.%s %f %d\n' % (self._hostname, key, val, now))
            num_stats += 1

        # counter stats
        gauges = stats.gauges
        for key, val in gauges.iteritems():
            buf.write('servers.%s.stats.%s %f %d\n' % (self._hostname, key, val, now))
            buf.write('servers.%s.stats_counts.%s %f %d\n' % (self._hostname, key, val, now))
            num_stats += 1

        buf.write('servers.%s.statsd.numStats %d %d\n' % (self._hostname, num_stats, now))

        # TODO: add support for N retries

        for host in self._hosts:
            # flush stats to graphite
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if self._ca is not None:
                    conn = ssl.SSLSocket(sock,
                                         ssl_version=ssl.PROTOCOL_SSLv3,
                                         keyfile=self._key,
                                         certfile=self._cert,
                                         ca_certs=self._ca,
                                         cert_reqs=ssl.CERT_REQUIRED)
                    #do_handshake_on_connect=False)
                    sock = conn
                sock.connect(host)
                sock.sendall(buf.getvalue())
                sock.close()
            except Exception, ex:
                self.error(E_SENDFAIL % ('graphite', host, ex))
