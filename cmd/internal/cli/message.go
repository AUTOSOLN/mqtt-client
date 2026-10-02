package cli

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	mqttclient "github.com/AUTOSOLN/mqtt-client"
)

// UserProps collects repeated key=value flags into MQTT 5 user properties.
type UserProps []mqttclient.UserProperty

func (u *UserProps) String() string { return fmt.Sprint(*u) }

// Set implements flag.Value.
func (u *UserProps) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok {
		return errors.New("want key=value")
	}
	*u = append(*u, mqttclient.UserProperty{Key: k, Val: v})
	return nil
}

// Strings collects a repeated flag.
type Strings []string

func (s *Strings) String() string { return strings.Join(*s, ",") }

// Set implements flag.Value.
func (s *Strings) Set(v string) error {
	*s = append(*s, v)
	return nil
}

// DescribeMessage formats a received message for printing.
func DescribeMessage(m *mqttclient.Message) string {
	var b strings.Builder
	fmt.Fprintf(&b, "mid=%d qos=%d retain=%v topic=%q payload=%s", m.Mid, m.QoS, m.Retain, m.Topic, ShowPayload(m.Payload))
	p := m.Properties
	if p == nil {
		return b.String()
	}
	if p.PayloadFormatFlag {
		fmt.Fprintf(&b, " payload_format=%d", p.PayloadFormat)
	}
	if p.MessageExpiryInterval > 0 {
		fmt.Fprintf(&b, " expiry=%d", p.MessageExpiryInterval)
	}
	if p.ContentType != "" {
		fmt.Fprintf(&b, " content_type=%q", p.ContentType)
	}
	if p.ResponseTopic != "" {
		fmt.Fprintf(&b, " response_topic=%q", p.ResponseTopic)
	}
	if len(p.CorrelationData) > 0 {
		fmt.Fprintf(&b, " correlation=%s", ShowPayload(p.CorrelationData))
	}
	if len(p.SubscriptionIdentifier) > 0 {
		fmt.Fprintf(&b, " subscription_ids=%v", p.SubscriptionIdentifier)
	}
	DescribeUser(&b, p.User)
	return b.String()
}

// DescribeUser appends user properties to b.
func DescribeUser(b *strings.Builder, user []mqttclient.UserProperty) {
	for _, u := range user {
		fmt.Fprintf(b, " user[%q]=%q", u.Key, u.Val)
	}
}

// DescribeAckProps formats the reason string and user properties of an
// acknowledgement (MQTT 5), or returns "" if there are none.
func DescribeAckProps(p *mqttclient.Properties) string {
	if p == nil {
		return ""
	}
	var b strings.Builder
	if p.ReasonString != "" {
		fmt.Fprintf(&b, " reason_string=%q", p.ReasonString)
	}
	DescribeUser(&b, p.User)
	return b.String()
}

// ShowPayload prints text as a quoted string and anything else as hex,
// truncated to keep lines short.
func ShowPayload(b []byte) string {
	const max = 64
	more := ""
	if len(b) > max {
		b, more = b[:max], fmt.Sprintf("...(%d more bytes)", len(b)-max)
	}
	if utf8.Valid(b) {
		return strconv.Quote(string(b)) + more
	}
	return "0x" + hex.EncodeToString(b) + more
}
