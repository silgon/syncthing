package files

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/calmh/syncthing/protocol"
	"github.com/calmh/syncthing/scanner"
)

func TestGlobalSet(t *testing.T) {
	m := NewSet()

	local := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1000},
		scanner.File{Name: "c", Version: 1000},
		scanner.File{Name: "d", Version: 1000},
	}

	remote := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1001},
		scanner.File{Name: "c", Version: 1002},
		scanner.File{Name: "e", Version: 1000},
	}

	expectedGlobal := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1001},
		scanner.File{Name: "c", Version: 1002},
		scanner.File{Name: "d", Version: 1000},
		scanner.File{Name: "e", Version: 1000},
	}

	m.SetLocal(local)
	m.SetRemote(1, remote)

	g := m.Global()
	if !reflect.DeepEqual(g, expectedGlobal) {
		t.Errorf("Global incorrect;\n%v !=\n%v", g, expectedGlobal)
	}

	if lb := len(m.files); lb != 7 {
		t.Errorf("Num files incorrect %d != 7\n%v", lb, m.files)
	}
}

func TestLocalDeleted(t *testing.T) {
	m := NewSet()

	local1 := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1000},
		scanner.File{Name: "c", Version: 1000},
		scanner.File{Name: "d", Version: 1000},
	}

	local2 := []scanner.File{
		local1[1],
		local1[3],
	}

	expectedGlobal := []scanner.File{
		local1[0],
		scanner.File{Name: "b", Version: 1001, Flags: protocol.FlagDeleted},
		local1[2],
		scanner.File{Name: "d", Version: 1001, Flags: protocol.FlagDeleted},
	}

	m.SetLocal(local1)
	m.SetLocal(local2)

	g := m.Global()
	if !reflect.DeepEqual(g, expectedGlobal) {
		t.Errorf("Global incorrect;\n%v !=\n%v", g, expectedGlobal)
	}
}

func BenchmarkSetLocal10k(b *testing.B) {
	m := NewSet()

	var local []scanner.File
	for i := 0; i < 10000; i++ {
		local = append(local, scanner.File{Name: fmt.Sprintf("file%d"), Version: 1000})
	}

	var remote []scanner.File
	for i := 0; i < 10000; i++ {
		remote = append(remote, scanner.File{Name: fmt.Sprintf("file%d"), Version: 1000})
	}

	m.SetRemote(1, remote)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.SetLocal(local)
	}
}

func BenchmarkSetLocal10(b *testing.B) {
	m := NewSet()

	var local []scanner.File
	for i := 0; i < 10; i++ {
		local = append(local, scanner.File{Name: fmt.Sprintf("file%d"), Version: 1000})
	}

	var remote []scanner.File
	for i := 0; i < 10000; i++ {
		remote = append(remote, scanner.File{Name: fmt.Sprintf("file%d"), Version: 1000})
	}

	m.SetRemote(1, remote)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.SetLocal(local)
	}
}

func BenchmarkAddLocal10k(b *testing.B) {
	m := NewSet()

	var local []scanner.File
	for i := 0; i < 10000; i++ {
		local = append(local, scanner.File{Name: fmt.Sprintf("file%d"), Version: 1000})
	}

	var remote []scanner.File
	for i := 0; i < 10000; i++ {
		remote = append(remote, scanner.File{Name: fmt.Sprintf("file%d"), Version: 1000})
	}

	m.SetRemote(1, remote)
	m.SetLocal(local)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for j := range local {
			local[j].Version++
		}
		b.StartTimer()
		m.AddLocal(local)
	}
}

func BenchmarkAddLocal10(b *testing.B) {
	m := NewSet()

	var local []scanner.File
	for i := 0; i < 10; i++ {
		local = append(local, scanner.File{Name: fmt.Sprintf("file%d"), Version: 1000})
	}

	var remote []scanner.File
	for i := 0; i < 10000; i++ {
		remote = append(remote, scanner.File{Name: fmt.Sprintf("file%d"), Version: 1000})
	}

	m.SetRemote(1, remote)
	m.SetLocal(local)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range local {
			local[j].Version++
		}
		m.AddLocal(local)
	}
}

func TestGlobalReset(t *testing.T) {
	m := NewSet()

	local := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1000},
		scanner.File{Name: "c", Version: 1000},
		scanner.File{Name: "d", Version: 1000},
	}

	remote := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1001},
		scanner.File{Name: "c", Version: 1002},
		scanner.File{Name: "e", Version: 1000},
	}

	expectedGlobalKey := map[string]key{
		"a": keyFor(local[0]),
		"b": keyFor(local[1]),
		"c": keyFor(local[2]),
		"d": keyFor(local[3]),
	}

	m.SetLocal(local)
	m.SetRemote(1, remote)
	m.SetRemote(1, nil)

	if !reflect.DeepEqual(m.globalKey, expectedGlobalKey) {
		t.Errorf("Global incorrect;\n%v !=\n%v", m.globalKey, expectedGlobalKey)
	}

	if lb := len(m.files); lb != 4 {
		t.Errorf("Num files incorrect %d != 4\n%v", lb, m.files)
	}
}

func TestNeed(t *testing.T) {
	m := NewSet()

	local := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1000},
		scanner.File{Name: "c", Version: 1000},
		scanner.File{Name: "d", Version: 1000},
	}

	remote := []scanner.File{
		scanner.File{Name: "a", Version: 1000},
		scanner.File{Name: "b", Version: 1001},
		scanner.File{Name: "c", Version: 1002},
		scanner.File{Name: "e", Version: 1000},
	}

	shouldNeed := []scanner.File{
		scanner.File{Name: "b", Version: 1001},
		scanner.File{Name: "c", Version: 1002},
		scanner.File{Name: "e", Version: 1000},
	}

	m.SetLocal(local)
	m.SetRemote(1, remote)

	need := m.Need(0)
	if !reflect.DeepEqual(need, shouldNeed) {
		t.Errorf("Need incorrect;\n%v !=\n%v", need, shouldNeed)
	}
}
