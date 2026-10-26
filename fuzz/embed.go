package fuzz

import "embed"

// Scenarios are the scenarios shipped with mqttfuzz: examples at
// scenarios/<name>.yaml, and a library of spec-level negative tests at
// scenarios/spec/<name>.yaml, one MQTT rule each. Load one with
// LoadFS(Scenarios, "scenarios/spec/keepalive-timeout.yaml", nil).
//
//go:embed scenarios/*.yaml scenarios/spec/*.yaml
var Scenarios embed.FS
