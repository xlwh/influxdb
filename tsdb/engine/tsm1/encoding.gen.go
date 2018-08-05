// Generated by tmpl
// https://github.com/benbjohnson/tmpl
//
// DO NOT EDIT!
// Source: encoding.gen.go.tmpl

// 数据编码工具，可以吧数据进行编码

package tsm1

import (
	"fmt"
	"sort"

	"github.com/influxdata/influxdb/tsdb"
)

// Values represents a slice of  values.
type Values []Value  // 数据都转成二进制存储起来

func (a Values) MinTime() int64 {
	return a[0].UnixNano()
}

func (a Values) MaxTime() int64 {
	return a[len(a)-1].UnixNano()
}

func (a Values) Size() int {
	sz := 0
	for _, v := range a {
		sz += v.Size()
	}
	return sz
}

func (a Values) ordered() bool {
	if len(a) <= 1 {
		return true
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			return false
		}
	}
	return true
}

func (a Values) assertOrdered() {
	if len(a) <= 1 {
		return
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			panic(fmt.Sprintf("not ordered: %d %d >= %d", i, av, ab))
		}
	}
}

// Deduplicate returns a new slice with any values that have the same timestamp removed.
// The Value that appears last in the slice is the one that is kept.  The returned
// Values are sorted if necessary.
func (a Values) Deduplicate() Values {
	if len(a) <= 1 {
		return a
	}

	// See if we're already sorted and deduped
	var needSort bool
	for i := 1; i < len(a); i++ {
		if a[i-1].UnixNano() >= a[i].UnixNano() {
			needSort = true
			break
		}
	}

	if !needSort {
		return a
	}

	sort.Stable(a)
	var i int
	for j := 1; j < len(a); j++ {
		v := a[j]
		if v.UnixNano() != a[i].UnixNano() {
			i++
		}
		a[i] = v

	}
	return a[:i+1]
}

// Exclude returns the subset of values not in [min, max].  The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a Values) Exclude(min, max int64) Values {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return a
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) {
		if a[rmax].UnixNano() == max {
			rmax++
		}
		rest := len(a) - rmax
		if rest > 0 {
			b := a[:rmin+rest]
			copy(b[rmin:], a[rmax:])
			return b
		}
	}

	return a[:rmin]
}

// Include returns the subset values between min and max inclusive. The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a Values) Include(min, max int64) Values {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return nil
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) && a[rmax].UnixNano() == max {
		rmax++
	}

	if rmin > -1 {
		b := a[:rmax-rmin]
		copy(b, a[rmin:rmax])
		return b
	}

	return a[:rmax]
}

// search performs a binary search for UnixNano() v in a
// and returns the position, i, where v would be inserted.
// An additional check of a[i].UnixNano() == v is necessary
// to determine if the value v exists.
func (a Values) search(v int64) int {
	// Define: f(x) → a[x].UnixNano() < v
	// Define: f(-1) == true, f(n) == false
	// Invariant: f(lo-1) == true, f(hi) == false
	lo := 0
	hi := len(a)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if a[mid].UnixNano() < v {
			lo = mid + 1 // preserves f(lo-1) == true
		} else {
			hi = mid // preserves f(hi) == false
		}
	}

	// lo == hi
	return lo
}

// FindRange returns the positions where min and max would be
// inserted into the array. If a[0].UnixNano() > max or
// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
// indicating the array is outside the [min, max]. The values must
// be deduplicated and sorted before calling Exclude or the results
// are undefined.
func (a Values) FindRange(min, max int64) (int, int) {
	if len(a) == 0 || min > max {
		return -1, -1
	}

	minVal := a[0].UnixNano()
	maxVal := a[len(a)-1].UnixNano()

	if maxVal < min || minVal > max {
		return -1, -1
	}

	return a.search(min), a.search(max)
}

// Merge overlays b to top of a.  If two values conflict with
// the same timestamp, b is used.  Both a and b must be sorted
// in ascending order.
func (a Values) Merge(b Values) Values {
	if len(a) == 0 {
		return b
	}

	if len(b) == 0 {
		return a
	}

	// Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
	// possible stored blocks might contain duplicate values.  Remove them if they exists before
	// merging.
	a = a.Deduplicate()
	b = b.Deduplicate()

	if a[len(a)-1].UnixNano() < b[0].UnixNano() {
		return append(a, b...)
	}

	if b[len(b)-1].UnixNano() < a[0].UnixNano() {
		return append(b, a...)
	}

	out := make(Values, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		if a[0].UnixNano() < b[0].UnixNano() {
			out, a = append(out, a[0]), a[1:]
		} else if len(b) > 0 && a[0].UnixNano() == b[0].UnixNano() {
			a = a[1:]
		} else {
			out, b = append(out, b[0]), b[1:]
		}
	}
	if len(a) > 0 {
		return append(out, a...)
	}
	return append(out, b...)
}

// Sort methods
func (a Values) Len() int           { return len(a) }
func (a Values) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Values) Less(i, j int) bool { return a[i].UnixNano() < a[j].UnixNano() }

// FloatValues represents a slice of Float values.
type FloatValues []FloatValue

func NewFloatArrayFromValues(v FloatValues) *tsdb.FloatArray {
	a := tsdb.NewFloatArrayLen(len(v))
	for i, val := range v {
		a.Timestamps[i] = val.unixnano
		a.Values[i] = val.value
	}
	return a
}

func (a FloatValues) MinTime() int64 {
	return a[0].UnixNano()
}

func (a FloatValues) MaxTime() int64 {
	return a[len(a)-1].UnixNano()
}

func (a FloatValues) Size() int {
	sz := 0
	for _, v := range a {
		sz += v.Size()
	}
	return sz
}

func (a FloatValues) ordered() bool {
	if len(a) <= 1 {
		return true
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			return false
		}
	}
	return true
}

func (a FloatValues) assertOrdered() {
	if len(a) <= 1 {
		return
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			panic(fmt.Sprintf("not ordered: %d %d >= %d", i, av, ab))
		}
	}
}

// Deduplicate returns a new slice with any values that have the same timestamp removed.
// The Value that appears last in the slice is the one that is kept.  The returned
// Values are sorted if necessary.
func (a FloatValues) Deduplicate() FloatValues {
	if len(a) <= 1 {
		return a
	}

	// See if we're already sorted and deduped
	var needSort bool
	for i := 1; i < len(a); i++ {
		if a[i-1].UnixNano() >= a[i].UnixNano() {
			needSort = true
			break
		}
	}

	if !needSort {
		return a
	}

	sort.Stable(a)
	var i int
	for j := 1; j < len(a); j++ {
		v := a[j]
		if v.UnixNano() != a[i].UnixNano() {
			i++
		}
		a[i] = v

	}
	return a[:i+1]
}

// Exclude returns the subset of values not in [min, max].  The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a FloatValues) Exclude(min, max int64) FloatValues {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return a
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) {
		if a[rmax].UnixNano() == max {
			rmax++
		}
		rest := len(a) - rmax
		if rest > 0 {
			b := a[:rmin+rest]
			copy(b[rmin:], a[rmax:])
			return b
		}
	}

	return a[:rmin]
}

// Include returns the subset values between min and max inclusive. The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a FloatValues) Include(min, max int64) FloatValues {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return nil
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) && a[rmax].UnixNano() == max {
		rmax++
	}

	if rmin > -1 {
		b := a[:rmax-rmin]
		copy(b, a[rmin:rmax])
		return b
	}

	return a[:rmax]
}

// search performs a binary search for UnixNano() v in a
// and returns the position, i, where v would be inserted.
// An additional check of a[i].UnixNano() == v is necessary
// to determine if the value v exists.
func (a FloatValues) search(v int64) int {
	// Define: f(x) → a[x].UnixNano() < v
	// Define: f(-1) == true, f(n) == false
	// Invariant: f(lo-1) == true, f(hi) == false
	lo := 0
	hi := len(a)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if a[mid].UnixNano() < v {
			lo = mid + 1 // preserves f(lo-1) == true
		} else {
			hi = mid // preserves f(hi) == false
		}
	}

	// lo == hi
	return lo
}

// FindRange returns the positions where min and max would be
// inserted into the array. If a[0].UnixNano() > max or
// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
// indicating the array is outside the [min, max]. The values must
// be deduplicated and sorted before calling Exclude or the results
// are undefined.
func (a FloatValues) FindRange(min, max int64) (int, int) {
	if len(a) == 0 || min > max {
		return -1, -1
	}

	minVal := a[0].UnixNano()
	maxVal := a[len(a)-1].UnixNano()

	if maxVal < min || minVal > max {
		return -1, -1
	}

	return a.search(min), a.search(max)
}

// Merge overlays b to top of a.  If two values conflict with
// the same timestamp, b is used.  Both a and b must be sorted
// in ascending order.
func (a FloatValues) Merge(b FloatValues) FloatValues {
	if len(a) == 0 {
		return b
	}

	if len(b) == 0 {
		return a
	}

	// Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
	// possible stored blocks might contain duplicate values.  Remove them if they exists before
	// merging.
	a = a.Deduplicate()
	b = b.Deduplicate()

	if a[len(a)-1].UnixNano() < b[0].UnixNano() {
		return append(a, b...)
	}

	if b[len(b)-1].UnixNano() < a[0].UnixNano() {
		return append(b, a...)
	}

	out := make(FloatValues, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		if a[0].UnixNano() < b[0].UnixNano() {
			out, a = append(out, a[0]), a[1:]
		} else if len(b) > 0 && a[0].UnixNano() == b[0].UnixNano() {
			a = a[1:]
		} else {
			out, b = append(out, b[0]), b[1:]
		}
	}
	if len(a) > 0 {
		return append(out, a...)
	}
	return append(out, b...)
}

func (a FloatValues) Encode(buf []byte) ([]byte, error) {
	return encodeFloatValuesBlock(buf, a)
}

func encodeFloatValuesBlock(buf []byte, values []FloatValue) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	venc := getFloatEncoder(len(values))
	tsenc := getTimeEncoder(len(values))

	var b []byte
	err := func() error {
		for _, v := range values {
			tsenc.Write(v.unixnano)
			venc.Write(v.value)
		}
		venc.Flush()

		// Encoded timestamp values
		tb, err := tsenc.Bytes()
		if err != nil {
			return err
		}
		// Encoded values
		vb, err := venc.Bytes()
		if err != nil {
			return err
		}

		// Prepend the first timestamp of the block in the first 8 bytes and the block
		// in the next byte, followed by the block
		b = packBlock(buf, BlockFloat64, tb, vb)

		return nil
	}()

	putTimeEncoder(tsenc)
	putFloatEncoder(venc)

	return b, err
}

// Sort methods
func (a FloatValues) Len() int           { return len(a) }
func (a FloatValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a FloatValues) Less(i, j int) bool { return a[i].UnixNano() < a[j].UnixNano() }

// IntegerValues represents a slice of Integer values.
type IntegerValues []IntegerValue

func NewIntegerArrayFromValues(v IntegerValues) *tsdb.IntegerArray {
	a := tsdb.NewIntegerArrayLen(len(v))
	for i, val := range v {
		a.Timestamps[i] = val.unixnano
		a.Values[i] = val.value
	}
	return a
}

func (a IntegerValues) MinTime() int64 {
	return a[0].UnixNano()
}

func (a IntegerValues) MaxTime() int64 {
	return a[len(a)-1].UnixNano()
}

func (a IntegerValues) Size() int {
	sz := 0
	for _, v := range a {
		sz += v.Size()
	}
	return sz
}

func (a IntegerValues) ordered() bool {
	if len(a) <= 1 {
		return true
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			return false
		}
	}
	return true
}

func (a IntegerValues) assertOrdered() {
	if len(a) <= 1 {
		return
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			panic(fmt.Sprintf("not ordered: %d %d >= %d", i, av, ab))
		}
	}
}

// Deduplicate returns a new slice with any values that have the same timestamp removed.
// The Value that appears last in the slice is the one that is kept.  The returned
// Values are sorted if necessary.
func (a IntegerValues) Deduplicate() IntegerValues {
	if len(a) <= 1 {
		return a
	}

	// See if we're already sorted and deduped
	var needSort bool
	for i := 1; i < len(a); i++ {
		if a[i-1].UnixNano() >= a[i].UnixNano() {
			needSort = true
			break
		}
	}

	if !needSort {
		return a
	}

	sort.Stable(a)
	var i int
	for j := 1; j < len(a); j++ {
		v := a[j]
		if v.UnixNano() != a[i].UnixNano() {
			i++
		}
		a[i] = v

	}
	return a[:i+1]
}

// Exclude returns the subset of values not in [min, max].  The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a IntegerValues) Exclude(min, max int64) IntegerValues {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return a
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) {
		if a[rmax].UnixNano() == max {
			rmax++
		}
		rest := len(a) - rmax
		if rest > 0 {
			b := a[:rmin+rest]
			copy(b[rmin:], a[rmax:])
			return b
		}
	}

	return a[:rmin]
}

// Include returns the subset values between min and max inclusive. The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a IntegerValues) Include(min, max int64) IntegerValues {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return nil
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) && a[rmax].UnixNano() == max {
		rmax++
	}

	if rmin > -1 {
		b := a[:rmax-rmin]
		copy(b, a[rmin:rmax])
		return b
	}

	return a[:rmax]
}

// search performs a binary search for UnixNano() v in a
// and returns the position, i, where v would be inserted.
// An additional check of a[i].UnixNano() == v is necessary
// to determine if the value v exists.
func (a IntegerValues) search(v int64) int {
	// Define: f(x) → a[x].UnixNano() < v
	// Define: f(-1) == true, f(n) == false
	// Invariant: f(lo-1) == true, f(hi) == false
	lo := 0
	hi := len(a)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if a[mid].UnixNano() < v {
			lo = mid + 1 // preserves f(lo-1) == true
		} else {
			hi = mid // preserves f(hi) == false
		}
	}

	// lo == hi
	return lo
}

// FindRange returns the positions where min and max would be
// inserted into the array. If a[0].UnixNano() > max or
// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
// indicating the array is outside the [min, max]. The values must
// be deduplicated and sorted before calling Exclude or the results
// are undefined.
func (a IntegerValues) FindRange(min, max int64) (int, int) {
	if len(a) == 0 || min > max {
		return -1, -1
	}

	minVal := a[0].UnixNano()
	maxVal := a[len(a)-1].UnixNano()

	if maxVal < min || minVal > max {
		return -1, -1
	}

	return a.search(min), a.search(max)
}

// Merge overlays b to top of a.  If two values conflict with
// the same timestamp, b is used.  Both a and b must be sorted
// in ascending order.
func (a IntegerValues) Merge(b IntegerValues) IntegerValues {
	if len(a) == 0 {
		return b
	}

	if len(b) == 0 {
		return a
	}

	// Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
	// possible stored blocks might contain duplicate values.  Remove them if they exists before
	// merging.
	a = a.Deduplicate()
	b = b.Deduplicate()

	if a[len(a)-1].UnixNano() < b[0].UnixNano() {
		return append(a, b...)
	}

	if b[len(b)-1].UnixNano() < a[0].UnixNano() {
		return append(b, a...)
	}

	out := make(IntegerValues, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		if a[0].UnixNano() < b[0].UnixNano() {
			out, a = append(out, a[0]), a[1:]
		} else if len(b) > 0 && a[0].UnixNano() == b[0].UnixNano() {
			a = a[1:]
		} else {
			out, b = append(out, b[0]), b[1:]
		}
	}
	if len(a) > 0 {
		return append(out, a...)
	}
	return append(out, b...)
}

func (a IntegerValues) Encode(buf []byte) ([]byte, error) {
	return encodeIntegerValuesBlock(buf, a)
}

func encodeIntegerValuesBlock(buf []byte, values []IntegerValue) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	venc := getIntegerEncoder(len(values))
	tsenc := getTimeEncoder(len(values))

	var b []byte
	err := func() error {
		for _, v := range values {
			tsenc.Write(v.unixnano)
			venc.Write(v.value)
		}
		venc.Flush()

		// Encoded timestamp values
		tb, err := tsenc.Bytes()
		if err != nil {
			return err
		}
		// Encoded values
		vb, err := venc.Bytes()
		if err != nil {
			return err
		}

		// Prepend the first timestamp of the block in the first 8 bytes and the block
		// in the next byte, followed by the block
		b = packBlock(buf, BlockInteger, tb, vb)

		return nil
	}()

	putTimeEncoder(tsenc)
	putIntegerEncoder(venc)

	return b, err
}

// Sort methods
func (a IntegerValues) Len() int           { return len(a) }
func (a IntegerValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a IntegerValues) Less(i, j int) bool { return a[i].UnixNano() < a[j].UnixNano() }

// UnsignedValues represents a slice of Unsigned values.
type UnsignedValues []UnsignedValue

func NewUnsignedArrayFromValues(v UnsignedValues) *tsdb.UnsignedArray {
	a := tsdb.NewUnsignedArrayLen(len(v))
	for i, val := range v {
		a.Timestamps[i] = val.unixnano
		a.Values[i] = val.value
	}
	return a
}

func (a UnsignedValues) MinTime() int64 {
	return a[0].UnixNano()
}

func (a UnsignedValues) MaxTime() int64 {
	return a[len(a)-1].UnixNano()
}

func (a UnsignedValues) Size() int {
	sz := 0
	for _, v := range a {
		sz += v.Size()
	}
	return sz
}

func (a UnsignedValues) ordered() bool {
	if len(a) <= 1 {
		return true
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			return false
		}
	}
	return true
}

func (a UnsignedValues) assertOrdered() {
	if len(a) <= 1 {
		return
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			panic(fmt.Sprintf("not ordered: %d %d >= %d", i, av, ab))
		}
	}
}

// Deduplicate returns a new slice with any values that have the same timestamp removed.
// The Value that appears last in the slice is the one that is kept.  The returned
// Values are sorted if necessary.
func (a UnsignedValues) Deduplicate() UnsignedValues {
	if len(a) <= 1 {
		return a
	}

	// See if we're already sorted and deduped
	var needSort bool
	for i := 1; i < len(a); i++ {
		if a[i-1].UnixNano() >= a[i].UnixNano() {
			needSort = true
			break
		}
	}

	if !needSort {
		return a
	}

	sort.Stable(a)
	var i int
	for j := 1; j < len(a); j++ {
		v := a[j]
		if v.UnixNano() != a[i].UnixNano() {
			i++
		}
		a[i] = v

	}
	return a[:i+1]
}

// Exclude returns the subset of values not in [min, max].  The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a UnsignedValues) Exclude(min, max int64) UnsignedValues {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return a
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) {
		if a[rmax].UnixNano() == max {
			rmax++
		}
		rest := len(a) - rmax
		if rest > 0 {
			b := a[:rmin+rest]
			copy(b[rmin:], a[rmax:])
			return b
		}
	}

	return a[:rmin]
}

// Include returns the subset values between min and max inclusive. The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a UnsignedValues) Include(min, max int64) UnsignedValues {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return nil
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) && a[rmax].UnixNano() == max {
		rmax++
	}

	if rmin > -1 {
		b := a[:rmax-rmin]
		copy(b, a[rmin:rmax])
		return b
	}

	return a[:rmax]
}

// search performs a binary search for UnixNano() v in a
// and returns the position, i, where v would be inserted.
// An additional check of a[i].UnixNano() == v is necessary
// to determine if the value v exists.
func (a UnsignedValues) search(v int64) int {
	// Define: f(x) → a[x].UnixNano() < v
	// Define: f(-1) == true, f(n) == false
	// Invariant: f(lo-1) == true, f(hi) == false
	lo := 0
	hi := len(a)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if a[mid].UnixNano() < v {
			lo = mid + 1 // preserves f(lo-1) == true
		} else {
			hi = mid // preserves f(hi) == false
		}
	}

	// lo == hi
	return lo
}

// FindRange returns the positions where min and max would be
// inserted into the array. If a[0].UnixNano() > max or
// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
// indicating the array is outside the [min, max]. The values must
// be deduplicated and sorted before calling Exclude or the results
// are undefined.
func (a UnsignedValues) FindRange(min, max int64) (int, int) {
	if len(a) == 0 || min > max {
		return -1, -1
	}

	minVal := a[0].UnixNano()
	maxVal := a[len(a)-1].UnixNano()

	if maxVal < min || minVal > max {
		return -1, -1
	}

	return a.search(min), a.search(max)
}

// Merge overlays b to top of a.  If two values conflict with
// the same timestamp, b is used.  Both a and b must be sorted
// in ascending order.
func (a UnsignedValues) Merge(b UnsignedValues) UnsignedValues {
	if len(a) == 0 {
		return b
	}

	if len(b) == 0 {
		return a
	}

	// Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
	// possible stored blocks might contain duplicate values.  Remove them if they exists before
	// merging.
	a = a.Deduplicate()
	b = b.Deduplicate()

	if a[len(a)-1].UnixNano() < b[0].UnixNano() {
		return append(a, b...)
	}

	if b[len(b)-1].UnixNano() < a[0].UnixNano() {
		return append(b, a...)
	}

	out := make(UnsignedValues, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		if a[0].UnixNano() < b[0].UnixNano() {
			out, a = append(out, a[0]), a[1:]
		} else if len(b) > 0 && a[0].UnixNano() == b[0].UnixNano() {
			a = a[1:]
		} else {
			out, b = append(out, b[0]), b[1:]
		}
	}
	if len(a) > 0 {
		return append(out, a...)
	}
	return append(out, b...)
}

func (a UnsignedValues) Encode(buf []byte) ([]byte, error) {
	return encodeUnsignedValuesBlock(buf, a)
}

func encodeUnsignedValuesBlock(buf []byte, values []UnsignedValue) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	venc := getUnsignedEncoder(len(values))
	tsenc := getTimeEncoder(len(values))

	var b []byte
	err := func() error {
		for _, v := range values {
			tsenc.Write(v.unixnano)
			venc.Write(int64(v.value))
		}
		venc.Flush()

		// Encoded timestamp values
		tb, err := tsenc.Bytes()
		if err != nil {
			return err
		}
		// Encoded values
		vb, err := venc.Bytes()
		if err != nil {
			return err
		}

		// Prepend the first timestamp of the block in the first 8 bytes and the block
		// in the next byte, followed by the block
		b = packBlock(buf, BlockUnsigned, tb, vb)

		return nil
	}()

	putTimeEncoder(tsenc)
	putUnsignedEncoder(venc)

	return b, err
}

// Sort methods
func (a UnsignedValues) Len() int           { return len(a) }
func (a UnsignedValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a UnsignedValues) Less(i, j int) bool { return a[i].UnixNano() < a[j].UnixNano() }

// StringValues represents a slice of String values.
type StringValues []StringValue

func NewStringArrayFromValues(v StringValues) *tsdb.StringArray {
	a := tsdb.NewStringArrayLen(len(v))
	for i, val := range v {
		a.Timestamps[i] = val.unixnano
		a.Values[i] = val.value
	}
	return a
}

func (a StringValues) MinTime() int64 {
	return a[0].UnixNano()
}

func (a StringValues) MaxTime() int64 {
	return a[len(a)-1].UnixNano()
}

func (a StringValues) Size() int {
	sz := 0
	for _, v := range a {
		sz += v.Size()
	}
	return sz
}

func (a StringValues) ordered() bool {
	if len(a) <= 1 {
		return true
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			return false
		}
	}
	return true
}

func (a StringValues) assertOrdered() {
	if len(a) <= 1 {
		return
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			panic(fmt.Sprintf("not ordered: %d %d >= %d", i, av, ab))
		}
	}
}

// Deduplicate returns a new slice with any values that have the same timestamp removed.
// The Value that appears last in the slice is the one that is kept.  The returned
// Values are sorted if necessary.
func (a StringValues) Deduplicate() StringValues {
	if len(a) <= 1 {
		return a
	}

	// See if we're already sorted and deduped
	var needSort bool
	for i := 1; i < len(a); i++ {
		if a[i-1].UnixNano() >= a[i].UnixNano() {
			needSort = true
			break
		}
	}

	if !needSort {
		return a
	}

	sort.Stable(a)
	var i int
	for j := 1; j < len(a); j++ {
		v := a[j]
		if v.UnixNano() != a[i].UnixNano() {
			i++
		}
		a[i] = v

	}
	return a[:i+1]
}

// Exclude returns the subset of values not in [min, max].  The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a StringValues) Exclude(min, max int64) StringValues {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return a
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) {
		if a[rmax].UnixNano() == max {
			rmax++
		}
		rest := len(a) - rmax
		if rest > 0 {
			b := a[:rmin+rest]
			copy(b[rmin:], a[rmax:])
			return b
		}
	}

	return a[:rmin]
}

// Include returns the subset values between min and max inclusive. The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a StringValues) Include(min, max int64) StringValues {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return nil
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) && a[rmax].UnixNano() == max {
		rmax++
	}

	if rmin > -1 {
		b := a[:rmax-rmin]
		copy(b, a[rmin:rmax])
		return b
	}

	return a[:rmax]
}

// search performs a binary search for UnixNano() v in a
// and returns the position, i, where v would be inserted.
// An additional check of a[i].UnixNano() == v is necessary
// to determine if the value v exists.
func (a StringValues) search(v int64) int {
	// Define: f(x) → a[x].UnixNano() < v
	// Define: f(-1) == true, f(n) == false
	// Invariant: f(lo-1) == true, f(hi) == false
	lo := 0
	hi := len(a)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if a[mid].UnixNano() < v {
			lo = mid + 1 // preserves f(lo-1) == true
		} else {
			hi = mid // preserves f(hi) == false
		}
	}

	// lo == hi
	return lo
}

// FindRange returns the positions where min and max would be
// inserted into the array. If a[0].UnixNano() > max or
// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
// indicating the array is outside the [min, max]. The values must
// be deduplicated and sorted before calling Exclude or the results
// are undefined.
func (a StringValues) FindRange(min, max int64) (int, int) {
	if len(a) == 0 || min > max {
		return -1, -1
	}

	minVal := a[0].UnixNano()
	maxVal := a[len(a)-1].UnixNano()

	if maxVal < min || minVal > max {
		return -1, -1
	}

	return a.search(min), a.search(max)
}

// Merge overlays b to top of a.  If two values conflict with
// the same timestamp, b is used.  Both a and b must be sorted
// in ascending order.
func (a StringValues) Merge(b StringValues) StringValues {
	if len(a) == 0 {
		return b
	}

	if len(b) == 0 {
		return a
	}

	// Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
	// possible stored blocks might contain duplicate values.  Remove them if they exists before
	// merging.
	a = a.Deduplicate()
	b = b.Deduplicate()

	if a[len(a)-1].UnixNano() < b[0].UnixNano() {
		return append(a, b...)
	}

	if b[len(b)-1].UnixNano() < a[0].UnixNano() {
		return append(b, a...)
	}

	out := make(StringValues, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		if a[0].UnixNano() < b[0].UnixNano() {
			out, a = append(out, a[0]), a[1:]
		} else if len(b) > 0 && a[0].UnixNano() == b[0].UnixNano() {
			a = a[1:]
		} else {
			out, b = append(out, b[0]), b[1:]
		}
	}
	if len(a) > 0 {
		return append(out, a...)
	}
	return append(out, b...)
}

func (a StringValues) Encode(buf []byte) ([]byte, error) {
	return encodeStringValuesBlock(buf, a)
}

func encodeStringValuesBlock(buf []byte, values []StringValue) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	venc := getStringEncoder(len(values))
	tsenc := getTimeEncoder(len(values))

	var b []byte
	err := func() error {
		for _, v := range values {
			tsenc.Write(v.unixnano)
			venc.Write(v.value)
		}
		venc.Flush()

		// Encoded timestamp values
		tb, err := tsenc.Bytes()
		if err != nil {
			return err
		}
		// Encoded values
		vb, err := venc.Bytes()
		if err != nil {
			return err
		}

		// Prepend the first timestamp of the block in the first 8 bytes and the block
		// in the next byte, followed by the block
		b = packBlock(buf, BlockString, tb, vb)

		return nil
	}()

	putTimeEncoder(tsenc)
	putStringEncoder(venc)

	return b, err
}

// Sort methods
func (a StringValues) Len() int           { return len(a) }
func (a StringValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a StringValues) Less(i, j int) bool { return a[i].UnixNano() < a[j].UnixNano() }

// BooleanValues represents a slice of Boolean values.
type BooleanValues []BooleanValue

func NewBooleanArrayFromValues(v BooleanValues) *tsdb.BooleanArray {
	a := tsdb.NewBooleanArrayLen(len(v))
	for i, val := range v {
		a.Timestamps[i] = val.unixnano
		a.Values[i] = val.value
	}
	return a
}

func (a BooleanValues) MinTime() int64 {
	return a[0].UnixNano()
}

func (a BooleanValues) MaxTime() int64 {
	return a[len(a)-1].UnixNano()
}

func (a BooleanValues) Size() int {
	sz := 0
	for _, v := range a {
		sz += v.Size()
	}
	return sz
}

func (a BooleanValues) ordered() bool {
	if len(a) <= 1 {
		return true
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			return false
		}
	}
	return true
}

func (a BooleanValues) assertOrdered() {
	if len(a) <= 1 {
		return
	}
	for i := 1; i < len(a); i++ {
		if av, ab := a[i-1].UnixNano(), a[i].UnixNano(); av >= ab {
			panic(fmt.Sprintf("not ordered: %d %d >= %d", i, av, ab))
		}
	}
}

// Deduplicate returns a new slice with any values that have the same timestamp removed.
// The Value that appears last in the slice is the one that is kept.  The returned
// Values are sorted if necessary.
func (a BooleanValues) Deduplicate() BooleanValues {
	if len(a) <= 1 {
		return a
	}

	// See if we're already sorted and deduped
	var needSort bool
	for i := 1; i < len(a); i++ {
		if a[i-1].UnixNano() >= a[i].UnixNano() {
			needSort = true
			break
		}
	}

	if !needSort {
		return a
	}

	sort.Stable(a)
	var i int
	for j := 1; j < len(a); j++ {
		v := a[j]
		if v.UnixNano() != a[i].UnixNano() {
			i++
		}
		a[i] = v

	}
	return a[:i+1]
}

// Exclude returns the subset of values not in [min, max].  The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a BooleanValues) Exclude(min, max int64) BooleanValues {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return a
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) {
		if a[rmax].UnixNano() == max {
			rmax++
		}
		rest := len(a) - rmax
		if rest > 0 {
			b := a[:rmin+rest]
			copy(b[rmin:], a[rmax:])
			return b
		}
	}

	return a[:rmin]
}

// Include returns the subset values between min and max inclusive. The values must
// be deduplicated and sorted before calling Exclude or the results are undefined.
func (a BooleanValues) Include(min, max int64) BooleanValues {
	rmin, rmax := a.FindRange(min, max)
	if rmin == -1 && rmax == -1 {
		return nil
	}

	// a[rmin].UnixNano() ≥ min
	// a[rmax].UnixNano() ≥ max

	if rmax < len(a) && a[rmax].UnixNano() == max {
		rmax++
	}

	if rmin > -1 {
		b := a[:rmax-rmin]
		copy(b, a[rmin:rmax])
		return b
	}

	return a[:rmax]
}

// search performs a binary search for UnixNano() v in a
// and returns the position, i, where v would be inserted.
// An additional check of a[i].UnixNano() == v is necessary
// to determine if the value v exists.
func (a BooleanValues) search(v int64) int {
	// Define: f(x) → a[x].UnixNano() < v
	// Define: f(-1) == true, f(n) == false
	// Invariant: f(lo-1) == true, f(hi) == false
	lo := 0
	hi := len(a)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if a[mid].UnixNano() < v {
			lo = mid + 1 // preserves f(lo-1) == true
		} else {
			hi = mid // preserves f(hi) == false
		}
	}

	// lo == hi
	return lo
}

// FindRange returns the positions where min and max would be
// inserted into the array. If a[0].UnixNano() > max or
// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
// indicating the array is outside the [min, max]. The values must
// be deduplicated and sorted before calling Exclude or the results
// are undefined.
func (a BooleanValues) FindRange(min, max int64) (int, int) {
	if len(a) == 0 || min > max {
		return -1, -1
	}

	minVal := a[0].UnixNano()
	maxVal := a[len(a)-1].UnixNano()

	if maxVal < min || minVal > max {
		return -1, -1
	}

	return a.search(min), a.search(max)
}

// Merge overlays b to top of a.  If two values conflict with
// the same timestamp, b is used.  Both a and b must be sorted
// in ascending order.
func (a BooleanValues) Merge(b BooleanValues) BooleanValues {
	if len(a) == 0 {
		return b
	}

	if len(b) == 0 {
		return a
	}

	// Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
	// possible stored blocks might contain duplicate values.  Remove them if they exists before
	// merging.
	a = a.Deduplicate()
	b = b.Deduplicate()

	if a[len(a)-1].UnixNano() < b[0].UnixNano() {
		return append(a, b...)
	}

	if b[len(b)-1].UnixNano() < a[0].UnixNano() {
		return append(b, a...)
	}

	out := make(BooleanValues, 0, len(a)+len(b))
	for len(a) > 0 && len(b) > 0 {
		if a[0].UnixNano() < b[0].UnixNano() {
			out, a = append(out, a[0]), a[1:]
		} else if len(b) > 0 && a[0].UnixNano() == b[0].UnixNano() {
			a = a[1:]
		} else {
			out, b = append(out, b[0]), b[1:]
		}
	}
	if len(a) > 0 {
		return append(out, a...)
	}
	return append(out, b...)
}

func (a BooleanValues) Encode(buf []byte) ([]byte, error) {
	return encodeBooleanValuesBlock(buf, a)
}

func encodeBooleanValuesBlock(buf []byte, values []BooleanValue) ([]byte, error) {
	if len(values) == 0 {
		return nil, nil
	}

	venc := getBooleanEncoder(len(values))
	tsenc := getTimeEncoder(len(values))

	var b []byte
	err := func() error {
		for _, v := range values {
			tsenc.Write(v.unixnano)
			venc.Write(v.value)
		}
		venc.Flush()

		// Encoded timestamp values
		tb, err := tsenc.Bytes()
		if err != nil {
			return err
		}
		// Encoded values
		vb, err := venc.Bytes()
		if err != nil {
			return err
		}

		// Prepend the first timestamp of the block in the first 8 bytes and the block
		// in the next byte, followed by the block
		b = packBlock(buf, BlockBoolean, tb, vb)

		return nil
	}()

	putTimeEncoder(tsenc)
	putBooleanEncoder(venc)

	return b, err
}

// Sort methods
func (a BooleanValues) Len() int           { return len(a) }
func (a BooleanValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a BooleanValues) Less(i, j int) bool { return a[i].UnixNano() < a[j].UnixNano() }
