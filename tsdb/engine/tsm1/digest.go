package tsm1

import (
	"bytes"
	"io"
	"math"
	"os"
	"sort"
)

const (
	DigestFilename         = "digest.tsd"
	DigestManifestFilename = "digest.manifest"
)

type DigestOptions struct {
	MinTime, MaxTime int64
	MinKey, MaxKey   []byte
}

// DigestWithOptions writes a digest of dir to w using options to filter by
// time and key range.
func DigestWithOptions(dir string, files []string, opts DigestOptions, w io.WriteCloser) error {
	manifest, err := NewDigestManifest(dir, files)
	if err != nil {
		return err
	}

	tsmFiles := make([]TSMFile, 0, len(files))
	defer func() {
		for _, r := range tsmFiles {
			r.Close()
		}
	}()

	readers := make([]*TSMReader, 0, len(files))
	for _, fi := range files {
		f, err := os.Open(fi)
		if err != nil {
			return err
		}

		r, err := NewTSMReader(f)
		if err != nil {
			return err
		}
		readers = append(readers, r)
		tsmFiles = append(tsmFiles, r)
	}

	dw, err := NewDigestWriter(w)
	if err != nil {
		return err
	}
	defer dw.Close()

	// Write the manifest.
	if err := dw.WriteManifest(manifest); err != nil {
		return err
	}

	// Write the digest data.
	var n int
	ki := newMergeKeyIterator(tsmFiles, nil)
	for ki.Next() {
		key, _ := ki.Read()
		if len(opts.MinKey) > 0 && bytes.Compare(key, opts.MinKey) < 0 {
			continue
		}

		if len(opts.MaxKey) > 0 && bytes.Compare(key, opts.MaxKey) > 0 {
			continue
		}

		ts := &DigestTimeSpan{}
		n++
		kstr := string(key)

		for _, r := range readers {
			entries := r.Entries(key)
			for _, entry := range entries {
				crc, b, err := r.ReadBytes(&entry, nil)
				if err != nil {
					return err
				}

				// Filter blocks that are outside the time filter.  If they overlap, we
				// still include them.
				if entry.MaxTime < opts.MinTime || entry.MinTime > opts.MaxTime {
					continue
				}

				cnt := BlockCount(b)
				ts.Add(entry.MinTime, entry.MaxTime, cnt, crc)
			}
		}

		sort.Sort(ts)
		if err := dw.WriteTimeSpan(kstr, ts); err != nil {
			return err
		}
	}
	return dw.Close()
}

// Digest writes a digest of dir to w of a full shard dir.
func Digest(dir string, files []string, w io.WriteCloser) error {
	return DigestWithOptions(dir, files, DigestOptions{
		MinTime: math.MinInt64,
		MaxTime: math.MaxInt64,
	}, w)
}

// DigestManifest contains a list of tsm files used to generate a digest
// and information about those files which can be used to verify the
// associated digest file is still valid.
type DigestManifest struct {
	// Dir is the directory path this manifest describes.
	Dir string `json:"dir"`
	// Entries is a list of files used to generate a digest.
	Entries DigestManifestEntries `json:"entries"`
}

// NewDigestManifest creates a digest manifest for a shard directory and list
// of tsm files from that directory.
func NewDigestManifest(dir string, files []string) (*DigestManifest, error) {
	mfest := &DigestManifest{
		Dir:     dir,
		Entries: make([]*DigestManifestEntry, len(files)),
	}

	for i, name := range files {
		fi, err := os.Stat(name)
		if err != nil {
			return nil, err
		}
		mfest.Entries[i] = NewDigestManifestEntry(name, fi.Size())
	}

	sort.Sort(mfest.Entries)

	return mfest, nil
}

type DigestManifestEntry struct {
	// Filename is the name of one .tsm file used in digest generation.
	Filename string `json:"filename"`
	// Size is the size, in bytes, of the .tsm file.
	Size int64 `json:"size"`
}

// NewDigestManifestEntry creates a digest manifest entry initialized with a
// tsm filename and its size.
func NewDigestManifestEntry(filename string, size int64) *DigestManifestEntry {
	return &DigestManifestEntry{
		Filename: filename,
		Size:     size,
	}
}

// DigestManifestEntries is a list of entries in a manifest file, ordered by
// tsm filename.
type DigestManifestEntries []*DigestManifestEntry

func (a DigestManifestEntries) Len() int           { return len(a) }
func (a DigestManifestEntries) Less(i, j int) bool { return a[i].Filename < a[j].Filename }
func (a DigestManifestEntries) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
