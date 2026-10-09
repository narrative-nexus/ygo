package crdt

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// OnUpdate payloads carry only the transaction's own deletions (Yjs
// writeUpdateMessageFromTransaction), never the document's whole delete set.

// captureUpdates subscribes to doc's OnUpdate and returns a pointer to the
// growing list of per-transaction payloads.
func captureUpdates(t *testing.T, doc *Doc) *[][]byte {
	t.Helper()
	var updates [][]byte
	unsub := doc.OnUpdate(func(u []byte, _ any) {
		cp := make([]byte, len(u))
		copy(cp, u)
		updates = append(updates, cp)
	})
	t.Cleanup(unsub)
	return &updates
}

// updateDeleteSet decodes the delete set an encoded V1 update carries.
func updateDeleteSet(t *testing.T, update []byte) DeleteSet {
	t.Helper()
	_, ds, err := decodeStructsV1(New(), update)
	require.NoError(t, err)
	return ds
}

func dsRanges(ds DeleteSet) map[ClientID][]DeleteRange {
	out := make(map[ClientID][]DeleteRange, len(ds.clients))
	for c, r := range ds.clients {
		if len(r) > 0 {
			out[c] = r
		}
	}
	return out
}

func TestUnit_TxnUpdate_NoDeletes_EmptyDeleteSet(t *testing.T) {
	for _, gc := range []bool{true, false} {
		doc := New(WithClientID(1), WithGC(gc))
		txt := doc.GetText("t")
		arr := doc.GetArray("a")
		doc.Transact(func(txn *Transaction) {
			txt.Insert(txn, 0, "Hello, world!", nil)
			arr.Push(txn, []any{1, 2, 3})
		})
		doc.Transact(func(txn *Transaction) {
			txt.Delete(txn, 5, 7)
			arr.Delete(txn, 0, 2)
		})
		require.NotEmpty(t, dsRanges(buildDeleteSet(doc.store)), "precondition: doc has prior deletes")

		updates := captureUpdates(t, doc)
		doc.Transact(func(txn *Transaction) { txt.Insert(txn, 6, "?", nil) })
		require.Len(t, *updates, 1)

		assert.Empty(t, dsRanges(updateDeleteSet(t, (*updates)[0])),
			"gc=%v: an insert-only transaction must not replay prior deletes", gc)

		// The full-state encoder keeps the whole delete set, as Yjs
		// encodeStateAsUpdate does.
		assert.NotEmpty(t, dsRanges(updateDeleteSet(t, EncodeStateAsUpdateV1(doc, nil))))
	}
}

func TestUnit_TxnUpdate_DeletesCarryOnlyOwn(t *testing.T) {
	doc := New(WithClientID(1))
	txt := doc.GetText("t")
	doc.Transact(func(txn *Transaction) { txt.Insert(txn, 0, "abcdefghij", nil) })
	doc.Transact(func(txn *Transaction) { txt.Delete(txn, 0, 2) }) // a,b at clocks 0-1

	updates := captureUpdates(t, doc)
	// Two non-adjacent deletes added in descending clock order: the payload
	// must be sorted and merged per client, as Yjs sortAndMergeDeleteSet does.
	doc.Transact(func(txn *Transaction) {
		txt.Delete(txn, 6, 2) // i,j at clocks 8-9
		txt.Delete(txn, 1, 2) // d,e at clocks 3-4
		txt.Delete(txn, 0, 1) // c at clock 2 (adjacent to d,e)
	})
	require.Equal(t, "fgh", txt.ToString())
	require.Len(t, *updates, 1)

	got := dsRanges(updateDeleteSet(t, (*updates)[0]))
	assert.Equal(t, map[ClientID][]DeleteRange{
		1: {{Clock: 2, Len: 3}, {Clock: 8, Len: 2}},
	}, got, "only this transaction's deletes, sorted and merged")
}

func TestUnit_TxnUpdate_RemoteApplyCarriesOnlyIncomingDeletes(t *testing.T) {
	src := New(WithClientID(1))
	txt := src.GetText("t")
	srcUpdates := captureUpdates(t, src)
	src.Transact(func(txn *Transaction) { txt.Insert(txn, 0, "abcdef", nil) })
	src.Transact(func(txn *Transaction) { txt.Delete(txn, 0, 1) })
	src.Transact(func(txn *Transaction) { txt.Delete(txn, 4, 1) }) // "f", clock 5

	dst := New(WithClientID(2))
	require.NoError(t, ApplyUpdateV1(dst, (*srcUpdates)[0], nil))
	require.NoError(t, ApplyUpdateV1(dst, (*srcUpdates)[1], nil))

	relayed := captureUpdates(t, dst)
	require.NoError(t, ApplyUpdateV1(dst, (*srcUpdates)[2], nil))
	require.Len(t, *relayed, 1)
	assert.Equal(t, map[ClientID][]DeleteRange{1: {{Clock: 5, Len: 1}}},
		dsRanges(updateDeleteSet(t, (*relayed)[0])))
}

// docSnapshot is the observable state compared for convergence.
type docSnapshot struct {
	text  string
	arr   []any
	m     map[string]any
	sv    StateVector
	ds    map[ClientID][]DeleteRange
	stats PendingStats
}

func snapshotDoc(d *Doc) docSnapshot {
	return docSnapshot{
		text:  d.GetText("t").ToString(),
		arr:   d.GetArray("a").ToSlice(),
		m:     d.GetMap("m").Entries(),
		sv:    d.StateVector(),
		ds:    dsRanges(buildDeleteSet(d.store)),
		stats: d.PendingStats(),
	}
}

// peerTypes holds a doc's root handles, resolved outside any transaction:
// Doc-level root accessors take the doc lock, so calling them inside
// Transact deadlocks (#138).
type peerTypes struct {
	txt *YText
	arr *YArray
	m   *YMap
}

func resolvePeer(d *Doc) peerTypes {
	return peerTypes{txt: d.GetText("t"), arr: d.GetArray("a"), m: d.GetMap("m")}
}

// randomEdit applies one random text/array/map edit, deletions included.
func randomEdit(rng *rand.Rand, txn *Transaction, p peerTypes) {
	txt, arr, m := p.txt, p.arr, p.m
	switch rng.Intn(6) {
	case 0, 1:
		txt.Insert(txn, rng.Intn(txt.Len()+1), string(rune('a'+rng.Intn(26))), nil)
	case 2:
		if n := txt.Len(); n > 0 {
			i := rng.Intn(n)
			txt.Delete(txn, i, 1+rng.Intn(min(3, n-i)))
		}
	case 3:
		arr.Insert(txn, rng.Intn(arr.Len()+1), []any{int64(rng.Intn(100))})
	case 4:
		if n := arr.Len(); n > 0 {
			arr.Delete(txn, rng.Intn(n), 1)
		}
	case 5:
		key := string(rune('k' + rng.Intn(3)))
		if rng.Intn(3) == 0 {
			m.Delete(txn, key)
		} else {
			m.Set(txn, key, int64(rng.Intn(100)))
		}
	}
}

// TestUnit_TxnUpdate_Convergence replays the per-transaction OnUpdate
// payloads of two concurrently editing peers into fresh docs, in causal
// order, shuffled and reversed, and replays everything one peer emits
// (relayed remote applies included), and requires each result to match the
// source state.
func TestUnit_TxnUpdate_Convergence(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		a := New(WithClientID(10))
		b := New(WithClientID(20))
		pa, pb := resolvePeer(a), resolvePeer(b)
		var log [][]byte
		var fromA, fromB [][]byte
		a.OnUpdate(func(u []byte, origin any) {
			if origin == nil {
				cp := append([]byte(nil), u...)
				log = append(log, cp)
				fromA = append(fromA, cp)
			}
		})
		b.OnUpdate(func(u []byte, origin any) {
			if origin == nil {
				cp := append([]byte(nil), u...)
				log = append(log, cp)
				fromB = append(fromB, cp)
			}
		})

		// relayed is everything B emits, its local transactions and the
		// transactions that apply A's updates alike: a server relaying a
		// peer's OnUpdate stream must lose no deletion along the way.
		var relayed [][]byte
		b.OnUpdate(func(u []byte, _ any) {
			relayed = append(relayed, append([]byte(nil), u...))
		})

		for round := 0; round < 12; round++ {
			for i := 0; i < 1+rng.Intn(3); i++ {
				a.Transact(func(txn *Transaction) {
					for j := 0; j < 1+rng.Intn(3); j++ {
						randomEdit(rng, txn, pa)
					}
				})
				b.Transact(func(txn *Transaction) {
					for j := 0; j < 1+rng.Intn(3); j++ {
						randomEdit(rng, txn, pb)
					}
				})
			}
			// Concurrent edits, then exchange only the new per-txn updates.
			for _, u := range fromA {
				require.NoError(t, ApplyUpdateV1(b, u, "remote"))
			}
			for _, u := range fromB {
				require.NoError(t, ApplyUpdateV1(a, u, "remote"))
			}
			fromA, fromB = nil, nil
		}

		require.NotEmpty(t, log, "seed %d: no local updates captured", seed)
		want := snapshotDoc(a)
		require.Equal(t, want, snapshotDoc(b), "seed %d: peers diverged", seed)
		require.Zero(t, want.stats, "seed %d: source left pending", seed)

		viaRelay := New(WithClientID(96))
		for _, u := range relayed {
			require.NoError(t, ApplyUpdateV1(viaRelay, u, nil))
		}
		assert.Equal(t, want, snapshotDoc(viaRelay), "seed %d: B's relayed stream", seed)

		inOrder := New(WithClientID(99))
		for _, u := range log {
			require.NoError(t, ApplyUpdateV1(inOrder, u, nil))
		}
		assert.Equal(t, want, snapshotDoc(inOrder), "seed %d: in-order replay", seed)

		shuffled := make([][]byte, len(log))
		copy(shuffled, log)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		outOfOrder := New(WithClientID(98))
		for _, u := range shuffled {
			require.NoError(t, ApplyUpdateV1(outOfOrder, u, nil))
		}
		assert.Equal(t, want, snapshotDoc(outOfOrder), "seed %d: shuffled replay", seed)

		reversed := New(WithClientID(97))
		for i := len(log) - 1; i >= 0; i-- {
			require.NoError(t, ApplyUpdateV1(reversed, log[i], nil))
		}
		assert.Equal(t, want, snapshotDoc(reversed), "seed %d: reversed replay", seed)
	}
}
