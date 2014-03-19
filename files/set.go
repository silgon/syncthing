// Package files provides a set type to track local/remote files with newness checks.
package files

import "github.com/calmh/syncthing/scanner"

type key struct {
	Name    string
	Version uint32
}

type fileRecord struct {
	Usage int
	File  scanner.File
}

type bitset uint64

func keyFor(f scanner.File) key {
	return key{
		Name:    f.Name,
		Version: f.Version,
	}
}

func (a key) newerThan(b key) bool {
	return a.Version > b.Version
}

type Set struct {
	files              map[key]fileRecord
	remoteKey          [64]map[string]key
	globalAvailability map[string]bitset
	globalKey          map[string]key
}

func NewSet() *Set {
	var m = Set{
		files:              make(map[key]fileRecord),
		globalAvailability: make(map[string]bitset),
		globalKey:          make(map[string]key),
	}
	return &m
}

func (m *Set) AddLocal(fs []scanner.File) {
	m.addRemote(0, fs)
}

func (m *Set) SetLocal(fs []scanner.File) {
	m.setRemote(0, fs)
}

func (m *Set) AddRemote(cid uint, fs []scanner.File) {
	if cid < 1 || cid > 63 {
		panic("Connection ID must be in the range 1 - 63 inclusive")
	}
	m.addRemote(cid, fs)
}

func (m *Set) SetRemote(cid uint, fs []scanner.File) {
	if cid < 1 || cid > 63 {
		panic("Connection ID must be in the range 1 - 63 inclusive")
	}
	m.setRemote(cid, fs)
}

func (m *Set) Need(cid uint) []scanner.File {
	var fs []scanner.File
	for name, gk := range m.globalKey {
		if gk.newerThan(m.remoteKey[cid][name]) {
			fs = append(fs, m.files[gk].File)
		}
	}
	return fs
}

func (m *Set) Have(cid uint) []scanner.File {
	var fs []scanner.File
	for _, rk := range m.remoteKey[cid] {
		fs = append(fs, m.files[rk].File)
	}
	return fs
}

func (m *Set) Global() []scanner.File {
	var fs []scanner.File
	for _, rk := range m.globalKey {
		fs = append(fs, m.files[rk].File)
	}
	return fs
}

func (m *Set) Get(cid uint, file string) scanner.File {
	return m.files[m.remoteKey[cid]].File
}

func (m *Set) Availability(name string) bitset {
	return m.globalAvailability[name]
}

func (m *Set) addRemote(cid uint, fs []scanner.File) {
	remFiles := m.remoteKey[cid]
	for _, f := range fs {
		n := f.Name
		fk := keyFor(f)

		if ck, ok := remFiles[n]; ok && ck == fk {
			// The remote already has exactly this file, skip it
			continue
		}

		remFiles[n] = fk

		// Keep the block list or increment the usage
		if br, ok := m.files[fk]; !ok {
			m.files[fk] = fileRecord{
				Usage: 1,
				File:  f,
			}
		} else {
			br.Usage++
			m.files[fk] = br
		}

		// Update global view
		gk, ok := m.globalKey[n]
		switch {
		case ok && fk == gk:
			av := m.globalAvailability[n]
			av |= 1 << cid
			m.globalAvailability[n] = av
		case fk.newerThan(gk):
			m.globalKey[n] = fk
			m.globalAvailability[n] = 1 << cid
		}
	}
}

func (m *Set) setRemote(cid uint, fs []scanner.File) {
	// Decrement usage for all files belonging to this remote, and remove
	// those that are no longer needed.
	for _, fk := range m.remoteKey[cid] {
		br, ok := m.files[fk]
		switch {
		case ok && br.Usage == 1:
			delete(m.files, fk)
		case ok && br.Usage > 1:
			br.Usage--
			m.files[fk] = br
		}
	}

	// Clear existing remote remoteKey
	m.remoteKey[cid] = make(map[string]key)

	// Recalculate global based on all remaining remoteKey
	for n := range m.globalKey {
		var nk key    // newest key
		var na bitset // newest availability

		for i, rem := range m.remoteKey {
			if rk, ok := rem[n]; ok {
				switch {
				case rk == nk:
					na |= 1 << uint(i)
				case rk.newerThan(nk):
					nk = rk
					na = 1 << uint(i)
				}
			}
		}

		if na != 0 {
			// Someone had the file
			m.globalKey[n] = nk
			m.globalAvailability[n] = na
		} else {
			// Noone had the file
			delete(m.globalKey, n)
			delete(m.globalAvailability, n)
		}
	}

	// Add new remote remoteKey to the mix
	m.addRemote(cid, fs)
}
