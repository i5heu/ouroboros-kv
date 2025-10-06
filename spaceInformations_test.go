package ouroboroskv

import (
	"path/filepath"
	"testing"

	"github.com/shirou/gopsutil/disk"
)

func TestGetDeviceAndMountPoint_Success(t *testing.T) {
	partitions, err := disk.Partitions(true)
	if err != nil {
		t.Fatalf("disk.Partitions returned error: %v", err)
	}
	if len(partitions) == 0 {
		t.Skip("no partitions available on this system")
	}

	var chosen *disk.PartitionStat
	t.Log("Available partitions with mountpoints:")
	for i := range partitions {
		p := partitions[i]
		t.Log(p.Mountpoint)
		if p.Mountpoint != "" {
			chosen = &p
			break
		}
	}
	if chosen == nil {
		t.Skip("no partition with a mountpoint found")
	}

	// Construct a path that should be under the chosen mountpoint
	path := filepath.Join(chosen.Mountpoint, "some", "sub", "path")
	mountPoint, device, err := getDeviceAndMountPoint(path)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if mountPoint != chosen.Mountpoint {
		t.Fatalf("expected mount point %q, got %q", chosen.Mountpoint, mountPoint)
	}
	if device != chosen.Device {
		t.Fatalf("expected device %q, got %q", chosen.Device, device)
	}
}

func TestGetDeviceAndMountPoint_RelativePath_NotFound(t *testing.T) {
	// A relative path should not match any absolute mountpoint, so expect an error.
	path := "relative/path/that/does/not/start/with/slash"
	_, _, err := getDeviceAndMountPoint(path)
	if err == nil {
		t.Fatalf("expected error for relative path %q, got nil", path)
	}
}
