module github.com/AUTOSOLN/mqtt-client/integration

go 1.25.5

require github.com/AUTOSOLN/mqtt-client v0.0.0

require github.com/wind-c/comqtt/v2 v2.6.1 // indirect

// The client under test is this repository; the comqtt fork as in the root
// go.mod (replace only applies in the main module).
replace github.com/AUTOSOLN/mqtt-client => ../

replace github.com/wind-c/comqtt/v2 => github.com/AUTOSOLN/comqtt/v2 v2.6.2-0.20261002195201-5268a0644cd6
