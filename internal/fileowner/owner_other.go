//go:build !unix

// Package fileowner reads a file's owning uid where the platform has one.
package fileowner

import "os"

// UID reports no owner here, so every owner check refuses.
func UID(os.FileInfo) (uint32, bool) { return 0, false }
