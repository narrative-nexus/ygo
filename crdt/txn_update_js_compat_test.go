package crdt_test

import (
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/crdt"
)

// TestCompat_TxnUpdate_MatchesYjs compares ygo's per-transaction OnUpdate
// payloads byte for byte with the 'update' events Yjs emits for the same
// transactions: structs since beforeState plus only the transaction's own
// deletions (writeUpdateMessageFromTransaction). Transactions that insert and
// delete the same content are left out: Yjs GCs that content before encoding,
// ygo after, so the struct bytes differ while the result converges.
func TestCompat_TxnUpdate_MatchesYjs(t *testing.T) {
	nodePath, _ := requireConformance(t, "yjs")
	yjsPath, err := filepath.Abs(filepath.Join("..", "testutil", "node_modules", "yjs"))
	require.NoError(t, err)

	doc := crdt.New(crdt.WithClientID(1))
	txt := doc.GetText("t")
	arr := doc.GetArray("a")
	m := doc.GetMap("m")
	var got []string
	doc.OnUpdate(func(u []byte, _ any) { got = append(got, hex.EncodeToString(u)) })

	doc.Transact(func(txn *crdt.Transaction) {
		txt.Insert(txn, 0, "Hello, world!", nil)
		arr.Push(txn, []any{"x", "y", "z"})
		m.Set(txn, "k", "v1")
	})
	doc.Transact(func(txn *crdt.Transaction) { txt.Delete(txn, 5, 7) })
	doc.Transact(func(txn *crdt.Transaction) { txt.Insert(txn, 6, "?", nil) })
	doc.Transact(func(txn *crdt.Transaction) {
		arr.Delete(txn, 2, 1)
		arr.Delete(txn, 0, 1)
	})
	doc.Transact(func(txn *crdt.Transaction) { m.Set(txn, "k", "v2") })
	doc.Transact(func(txn *crdt.Transaction) { arr.Push(txn, []any{"w"}) })

	script := `
const Y = require(process.argv[2]);
const doc = new Y.Doc();
doc.clientID = 1;
const out = [];
doc.on('update', u => out.push(Buffer.from(u).toString('hex')));
const t = doc.getText('t'), a = doc.getArray('a'), m = doc.getMap('m');
doc.transact(() => { t.insert(0, 'Hello, world!'); a.push(['x', 'y', 'z']); m.set('k', 'v1'); });
doc.transact(() => { t.delete(5, 7); });
doc.transact(() => { t.insert(6, '?'); });
doc.transact(() => { a.delete(2, 1); a.delete(0, 1); });
doc.transact(() => { m.set('k', 'v2'); });
doc.transact(() => { a.push(['w']); });
console.log(out.join('\n'));
`
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "txn_updates.js")
	require.NoError(t, os.WriteFile(scriptPath, []byte(script), 0o644))
	out, err := exec.Command(nodePath, scriptPath, yjsPath).CombinedOutput()
	require.NoError(t, err, "node: %s", out)

	want := strings.Split(strings.TrimSpace(string(out)), "\n")
	require.Equal(t, want, got, "per-transaction update bytes diverge from Yjs")
}
