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

	temp := t.TempDir()
	absTemp, err := filepath.Abs(temp)
	if err != nil {
		t.Fatalf("failed to resolve temp dir: %v", err)
	}

	var chosen *disk.PartitionStat
	for i := range partitions {
		p := partitions[i]
		if p.Mountpoint != "" && contains(absTemp, p.Mountpoint) {
			chosen = &p
			break
		}
	}
	if chosen == nil {
		t.Skipf("no partition with a mountpoint covering temp dir %q", absTemp)
	}

	path := filepath.Join(temp, "some", "sub", "path")
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

func TestGetDeviceAndMountPoint_NotFound(t *testing.T) {
	path := "/a987wgf9a8wgf/path/that/does/not/exist"
	_, _, err := getDeviceAndMountPoint(path)
	if err == nil {
		t.Fatalf("expected error for path %q, got nil", path)
	}
}

func TestGetDeviceAndMountPoint_RootHome(t *testing.T) {
	path := "/home"
	_, _, err := getDeviceAndMountPoint(path)
	if err != nil {
		t.Fatalf("expected no error for path %q, got nil", path)
	}
}

func TestGetDeviceAndMountPoint_TempDir(t *testing.T) {
	temp := t.TempDir()

	partitions, err := disk.Partitions(true)
	if err != nil {
		t.Fatalf("disk.Partitions returned error: %v", err)
	}
	if len(partitions) == 0 {
		t.Skip("no partitions available on this system")
	}

	var expected *disk.PartitionStat
	for i := range partitions {
		p := partitions[i]
		if p.Mountpoint != "" && contains(temp, p.Mountpoint) {
			expected = &p
			break
		}
	}
	if expected == nil {
		t.Skipf("no partition found for temp dir %q", temp)
	}

	mountPoint, device, err := getDeviceAndMountPoint(temp)
	if err != nil {
		t.Fatalf("expected no error for temp dir %q, got: %v", temp, err)
	}
	if mountPoint != expected.Mountpoint {
		t.Fatalf("expected mount point %q, got %q", expected.Mountpoint, mountPoint)
	}
	if device != expected.Device {
		t.Fatalf("expected device %q, got %q", expected.Device, device)
	}
}

func TestGetDeviceAndMountPoint_TempDirNested(t *testing.T) {
	temp := t.TempDir()
	nested := filepath.Join(temp, "some", "nested", "path", "that", "does", "not", "exist")

	partitions, err := disk.Partitions(true)
	if err != nil {
		t.Fatalf("disk.Partitions returned error: %v", err)
	}
	if len(partitions) == 0 {
		t.Skip("no partitions available on this system")
	}

	var expected *disk.PartitionStat
	for i := range partitions {
		p := partitions[i]
		if p.Mountpoint != "" && contains(nested, p.Mountpoint) {
			expected = &p
			break
		}
	}
	if expected == nil {
		t.Skipf("no partition found for nested temp path %q", nested)
	}

	mountPoint, device, err := getDeviceAndMountPoint(nested)
	if err != nil {
		t.Fatalf("expected no error for nested temp path %q, got: %v", nested, err)
	}
	if mountPoint != expected.Mountpoint {
		t.Fatalf("expected mount point %q, got %q", expected.Mountpoint, mountPoint)
	}
	if device != expected.Device {
		t.Fatalf("expected device %q, got %q", expected.Device, device)
	}
}
