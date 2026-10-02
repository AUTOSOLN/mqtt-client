package mqttclient

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

// selfSigned returns a server certificate for 127.0.0.1 and a pool that
// trusts it.
func selfSigned(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test broker"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, pool
}

func newFakeTLSBroker(t *testing.T, cert tls.Certificate) *fakeBroker {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return &fakeBroker{t: t, ln: ln}
}

func TestTLS(t *testing.T) {
	cert, pool := selfSigned(t)
	b := newFakeTLSBroker(t, cert)
	url := strings.Replace(b.url(), "mqtt://", "mqtts://", 1)

	c, err := New(Options{Server: url, ClientID: "c", CleanStart: true, TLSConfig: &tls.Config{RootCAs: pool}}, Handlers{})
	if err != nil {
		t.Fatal(err)
	}
	res := connectAsync(c, 5*time.Second)
	fc := b.accept()
	fc.readConnect()
	fc.send(connackBytes(t, 4, 0, false, nil)...)
	if r := waitResult(t, res); r.err != nil {
		t.Fatal(r.err)
	}
	if err := c.Disconnect(t.Context(), 0, nil); err != nil {
		t.Fatal(err)
	}
	fc.expect(0xE0, 0x00)
}

func TestTLSUntrustedCertificate(t *testing.T) {
	cert, _ := selfSigned(t)
	b := newFakeTLSBroker(t, cert)
	url := strings.Replace(b.url(), "mqtt://", "mqtts://", 1)

	c, err := New(Options{Server: url, ClientID: "c", CleanStart: true}, Handlers{})
	if err != nil {
		t.Fatal(err)
	}
	go func() { // complete the server side of the handshake attempt
		if nc, err := b.ln.Accept(); err == nil {
			_ = nc.(*tls.Conn).Handshake()
			nc.Close()
		}
	}()
	_, err = c.Connect(t.Context())
	var unknown x509.UnknownAuthorityError
	if !errors.As(err, &unknown) {
		t.Fatalf("Connect err %v, want x509.UnknownAuthorityError", err)
	}
	if c.IsConnected() {
		t.Fatal("connected with an untrusted certificate")
	}
}
