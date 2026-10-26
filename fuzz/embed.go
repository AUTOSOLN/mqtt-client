package fuzz

import "embed"

// Scenarios are the scenarios shipped with mqttfuzz, at scenarios/<name>.yaml.
// Load one with LoadFS(Scenarios, "scenarios/double-connect.yaml", nil).
//
//go:embed scenarios/*.yaml
var Scenarios embed.FS
