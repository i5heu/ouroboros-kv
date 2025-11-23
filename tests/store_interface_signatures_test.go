package ouroboroskv__test

import (
	"testing"

	ouroboroskv "github.com/i5heu/ouroboros-kv"
)

type mockStore struct{}

func (m *mockStore) WriteData(data ouroboroskv.Data) (ouroboroskv.Hash, error) {
	return ouroboroskv.Hash{}, nil
}
func (m *mockStore) BatchWriteData(dataList []ouroboroskv.Data) ([]ouroboroskv.Hash, error) {
	return nil, nil
}
func (m *mockStore) ReadData(key ouroboroskv.Hash) (ouroboroskv.Data, error) {
	return ouroboroskv.Data{}, nil
}
func (m *mockStore) BatchReadData(keys []ouroboroskv.Hash) ([]ouroboroskv.Data, error) {
	return nil, nil
}
func (m *mockStore) DeleteData(key ouroboroskv.Hash) error         { return nil }
func (m *mockStore) DataExists(key ouroboroskv.Hash) (bool, error) { return false, nil }
func (m *mockStore) GetChildren(parentKey ouroboroskv.Hash) ([]ouroboroskv.Hash, error) {
	return nil, nil
}
func (m *mockStore) GetAncestors(leafKey ouroboroskv.Hash) ([]ouroboroskv.Hash, error) {
	return nil, nil
}
func (m *mockStore) GetDescendants(rootKey ouroboroskv.Hash) ([]ouroboroskv.Hash, error) {
	return nil, nil
}
func (m *mockStore) GetParent(childKey ouroboroskv.Hash) (ouroboroskv.Hash, error) {
	return ouroboroskv.Hash{}, nil
}
func (m *mockStore) GetRoots() ([]ouroboroskv.Hash, error)     { return nil, nil }
func (m *mockStore) ListKeys() ([]ouroboroskv.Hash, error)     { return nil, nil }
func (m *mockStore) ListRootKeys() ([]ouroboroskv.Hash, error) { return nil, nil }
func (m *mockStore) GetDataInfo(key ouroboroskv.Hash) (ouroboroskv.DataInfo, error) {
	return ouroboroskv.DataInfo{}, nil
}
func (m *mockStore) ListStoredData() ([]ouroboroskv.DataInfo, error)      { return nil, nil }
func (m *mockStore) Validate(key ouroboroskv.Hash) error                  { return nil }
func (m *mockStore) ValidateAll() ([]ouroboroskv.ValidationResult, error) { return nil, nil }
func (m *mockStore) Close() error                                         { return nil }

var _ ouroboroskv.Store = (*mockStore)(nil)

func TestStoreInterfaceSignatures(t *testing.T) {}
