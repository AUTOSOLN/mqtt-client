package capture

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// maxRecord bounds one captured frame, whatever snap length the file
// declares: 256 KiB is the largest snap length tcpdump and Wireshark use.
const maxRecord = 256 << 10

// File signatures.
var (
	magicGzip   = []byte{0x1f, 0x8b}
	magicZstd   = []byte{0x28, 0xb5, 0x2f, 0xfd}
	magicLZ4    = []byte{0x04, 0x22, 0x4d, 0x18}
	magicPcapng = []byte{0x0a, 0x0d, 0x0d, 0x0a}
)

// Read reads a capture file and returns its TCP connections. The file may
// be classic pcap or pcapng, and may be gzip-compressed.
//
// It returns an error wrapping ErrFormat if the file isn't a capture it can
// read, or ends in the middle of a record. Problems with one connection (a
// gap, a truncated frame) don't fail the whole file: they are in that
// connection's Err.
func Read(r io.Reader) (*Capture, error) {
	br := bufio.NewReader(r)
	head, err := br.Peek(4)
	if err != nil {
		return nil, fmt.Errorf("%w: file header: %v", ErrFormat, err)
	}
	switch {
	case bytes.HasPrefix(head, magicGzip):
		zr, err := gzip.NewReader(br)
		if err != nil {
			return nil, fmt.Errorf("%w: gzip: %v", ErrFormat, err)
		}
		defer zr.Close()
		zr.Multistream(false)
		return readUncompressed(bufio.NewReader(zr))
	case bytes.Equal(head, magicZstd):
		return nil, fmt.Errorf("%w: zstd-compressed captures are not supported; save it uncompressed or gzip-compressed", ErrFormat)
	case bytes.Equal(head, magicLZ4):
		return nil, fmt.Errorf("%w: lz4-compressed captures are not supported; save it uncompressed or gzip-compressed", ErrFormat)
	}
	return readUncompressed(br)
}

func readUncompressed(br *bufio.Reader) (*Capture, error) {
	head, err := br.Peek(4)
	if err != nil {
		return nil, fmt.Errorf("%w: file header: %v", ErrFormat, err)
	}
	a := newAssembler()
	if bytes.Equal(head, magicPcapng) {
		err = readPcapng(br, a)
	} else {
		err = readPcap(br, a)
	}
	if err != nil {
		return nil, err
	}
	return a.finish(), nil
}
