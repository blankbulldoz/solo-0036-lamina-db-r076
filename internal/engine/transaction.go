package engine

import (
	"crypto/rand"
	"encoding/hex"
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"sort"
	"time"
)

// PatchIntent is a staged column-level write: Set expresses the columns the
// caller intends to change, Expect expresses read assumptions that must still
// hold at commit. The staged Mutation.Row carries the private preview (intent
// applied to the transaction snapshot); the durable final row is resolved at
// commit time by resolvePatch.
type PatchIntent struct {
	Set    datum.Row
	Expect datum.Row
}
type staged struct {
	Mutation   versions.Mutation
	InsertOnly bool
	Patch      *PatchIntent
}
type Transaction struct {
	ID       string `json:"id"`
	Snapshot uint64 `json:"snapshot"`
	ReadOnly bool   `json:"read_only"`
	Started  string `json:"started"`
	writes   map[string]map[string]staged
}
type Commit struct {
	Sequence uint64 `json:"sequence"`
	Writes   int    `json:"writes"`
	Merged   int    `json:"merged,omitempty"`
}

func (d *DB) Begin(readOnly bool) (Transaction, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Transaction{}, err
	}
	if len(d.transactions) >= 128 {
		return Transaction{}, fault("capacity", "128 active transactions reached")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Transaction{}, err
	}
	tx := &Transaction{ID: hex.EncodeToString(id[:]), Snapshot: d.sequence, ReadOnly: readOnly, Started: time.Now().UTC().Format(time.RFC3339Nano), writes: map[string]map[string]staged{}}
	d.transactions[tx.ID] = tx
	out := *tx
	out.writes = nil
	return out, nil
}
func (d *DB) tx(id string) (*Transaction, error) {
	tx, ok := d.transactions[id]
	if !ok {
		return nil, fault("not_found", "transaction not active")
	}
	return tx, nil
}
func (d *DB) Put(id, table string, row datum.Row, insertOnly bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	if r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	normalized, err := r.Normalize(row)
	if err != nil {
		return invalid(err)
	}
	key := r.Key(normalized)
	rows := d.store.Scan(table, tx.Snapshot)
	overlay(rows, tx.writes, table)
	if insertOnly {
		if _, ok := rows[key]; ok {
			return fault("conflict", "primary already exists")
		}
	}
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	tx.writes[table][key] = staged{Mutation: versions.Mutation{Relation: table, Key: key, Row: normalized}, InsertOnly: insertOnly}
	return nil
}
func (tx *Transaction) reserve(table, key string) error {
	if _, exists := tx.writes[table][key]; exists {
		return nil
	}
	count := 0
	for _, m := range tx.writes {
		count += len(m)
	}
	if count >= 1000 {
		return fault("capacity", "transaction mutation limit 1000")
	}
	return nil
}
func checkValue(c catalog.Column, v datum.Value) error {
	if v.Null() {
		if !c.Nullable {
			return fault("invalid", "column %s cannot be null", c.Name)
		}
		return nil
	}
	if v.Kind != c.Type {
		return fault("invalid", "column %s requires %s", c.Name, c.Type)
	}
	if v.Kind == "text" && len(v.Text) > 65536 {
		return fault("invalid", "column %s text exceeds 64KiB", c.Name)
	}
	return nil
}

// Patch stages a column-intent write. Set holds the columns to change (never
// the primary), Expect holds read assumptions that must survive until commit.
// The staged row is a private preview against the transaction snapshot; the
// durable row is resolved at commit.
func (d *DB) Patch(id, table string, key datum.Value, set, expect datum.Row) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	if r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	primary, _ := r.Column(r.Primary)
	if key.Null() || key.Kind != primary.Type {
		return fault("invalid", "invalid primary value")
	}
	if len(set) == 0 && len(expect) == 0 {
		return fault("invalid", "patch requires set or expect columns")
	}
	for name, v := range set {
		if name == r.Primary {
			return fault("invalid", "primary column cannot be patched")
		}
		c, ok := r.Column(name)
		if !ok {
			return fault("invalid", "unknown column %q", name)
		}
		if err := checkValue(c, v); err != nil {
			return err
		}
	}
	for name, v := range expect {
		if _, dup := set[name]; dup {
			return fault("invalid", "column %q in both set and expect", name)
		}
		c, ok := r.Column(name)
		if !ok {
			return fault("invalid", "unknown column %q", name)
		}
		if err := checkValue(c, v); err != nil {
			return err
		}
	}
	token := key.Key()
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if prior, exists := tx.writes[table][token]; exists {
		return d.restage(tx, r, table, token, prior, set, expect)
	}
	base, found := d.store.Read(table, token, tx.Snapshot)
	var preview datum.Row
	if found {
		for name, v := range expect {
			if !datum.Equal(base[name], v) {
				return fault("conflict", "expectation on column %s of %s does not hold at transaction snapshot", name, table)
			}
		}
		preview = datum.Clone(base)
	} else {
		if len(expect) > 0 {
			return fault("not_found", "row not found at transaction snapshot")
		}
		preview = datum.Row{r.Primary: key}
	}
	for name, v := range set {
		preview[name] = v
	}
	normalized, err := r.Normalize(preview)
	if err != nil {
		return invalid(err)
	}
	if err = tx.reserve(table, token); err != nil {
		return err
	}
	tx.writes[table][token] = staged{Mutation: versions.Mutation{Relation: table, Key: token, Row: normalized}, Patch: &PatchIntent{Set: set, Expect: expect}}
	return nil
}

// restage composes a patch with the entry already staged for the key: it
// applies onto a staged whole-row put, merges into a staged patch, and
// refuses to patch over a staged delete.
func (d *DB) restage(tx *Transaction, r catalog.Relation, table, token string, prior staged, set, expect datum.Row) error {
	if prior.Patch == nil {
		if prior.Mutation.Deleted {
			return fault("invalid", "cannot patch over a staged delete")
		}
		row := datum.Clone(prior.Mutation.Row)
		for name, v := range set {
			row[name] = v
		}
		normalized, err := r.Normalize(row)
		if err != nil {
			return invalid(err)
		}
		for name, v := range expect {
			if !datum.Equal(normalized[name], v) {
				return fault("conflict", "expectation on column %s fails against the staged row", name)
			}
		}
		tx.writes[table][token] = staged{Mutation: versions.Mutation{Relation: table, Key: token, Row: normalized}, InsertOnly: prior.InsertOnly}
		return nil
	}
	intent := prior.Patch
	for name, v := range set {
		intent.Set[name] = v
		delete(intent.Expect, name)
	}
	if len(expect) > 0 {
		base, found := d.store.Read(table, token, tx.Snapshot)
		if !found {
			return fault("not_found", "row not found at transaction snapshot")
		}
		for name, v := range expect {
			if _, ok := intent.Set[name]; ok {
				continue
			}
			if !datum.Equal(base[name], v) {
				return fault("conflict", "expectation on column %s of %s does not hold at transaction snapshot", name, table)
			}
			intent.Expect[name] = v
		}
	}
	preview := datum.Clone(prior.Mutation.Row)
	for name, v := range set {
		preview[name] = v
	}
	normalized, err := r.Normalize(preview)
	if err != nil {
		return invalid(err)
	}
	tx.writes[table][token] = staged{Mutation: versions.Mutation{Relation: table, Key: token, Row: normalized}, Patch: intent}
	return nil
}
func (d *DB) Delete(id, table string, value datum.Value) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	if r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	col, _ := r.Column(r.Primary)
	if value.Null() || value.Kind != col.Type {
		return fault("invalid", "invalid primary value")
	}
	key := value.Key()
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	tx.writes[table][key] = staged{Mutation: versions.Mutation{Relation: table, Key: key, Deleted: true}}
	return nil
}
func (d *DB) Rollback(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	if _, err := d.tx(id); err != nil {
		return err
	}
	delete(d.transactions, id)
	return nil
}
func (d *DB) Commit(id string) (Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Commit{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return Commit{}, err
	}
	defer delete(d.transactions, id)
	resolved := map[string]map[string]staged{}
	merged := 0
	for table, mutations := range tx.writes {
		for key, m := range mutations {
			if m.Patch != nil {
				final, emit, reconciled, err := d.resolvePatch(tx, table, key, m)
				if err != nil {
					d.conflicts++
					return Commit{}, err
				}
				if reconciled {
					merged++
				}
				if !emit {
					continue
				}
				m = staged{Mutation: versions.Mutation{Relation: table, Key: key, Row: final}}
			} else if d.store.Latest(table, key) > tx.Snapshot {
				d.conflicts++
				return Commit{}, fault("conflict", "write conflict on %s", table)
			}
			if resolved[table] == nil {
				resolved[table] = map[string]staged{}
			}
			resolved[table][key] = m
		}
	}
	for table := range resolved {
		if err = d.unique(table, resolved); err != nil {
			d.conflicts++
			return Commit{}, err
		}
	}
	writes := make([]versions.Mutation, 0)
	for _, mutations := range resolved {
		for _, m := range mutations {
			writes = append(writes, m.Mutation)
		}
	}
	d.merges += uint64(merged)
	if len(writes) == 0 {
		return Commit{Sequence: tx.Snapshot, Merged: merged}, nil
	}
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].Relation == writes[j].Relation {
			return writes[i].Key < writes[j].Key
		}
		return writes[i].Relation < writes[j].Relation
	})
	if err = d.publish(journal.Record{Kind: "commit", Transaction: id, Writes: writes}); err != nil {
		return Commit{}, err
	}
	d.commits++
	return Commit{Sequence: d.sequence, Writes: len(writes), Merged: merged}, nil
}

// resolvePatch turns a staged patch into the single final row that is
// validated, journaled, replayed and backed up. Whole-row put/delete keep
// first-committer-wins; a patch instead reconciles against the committed head:
// disjoint concurrent column changes merge, overlapping set columns or
// violated expectations reject with a comprehensible conflict.
func (d *DB) resolvePatch(tx *Transaction, table, key string, m staged) (final datum.Row, emit, merged bool, err error) {
	p := m.Patch
	if d.store.Latest(table, key) <= tx.Snapshot {
		return m.Mutation.Row, len(p.Set) > 0, false, nil
	}
	base, existed := d.store.Read(table, key, tx.Snapshot)
	if !existed {
		return nil, false, false, fault("conflict", "row %s in %s changed concurrently; re-read required", key, table)
	}
	current, exists := d.store.Read(table, key, d.sequence)
	if !exists {
		return nil, false, false, fault("conflict", "row %s in %s deleted concurrently", key, table)
	}
	changed := changedColumns(base, current)
	for column := range p.Set {
		if changed[column] {
			return nil, false, false, fault("conflict", "column %s of %s changed concurrently", column, table)
		}
	}
	for column := range p.Expect {
		if changed[column] {
			return nil, false, false, fault("conflict", "read assumption on column %s of %s invalidated; re-read required", column, table)
		}
	}
	final = datum.Clone(current)
	for column, v := range p.Set {
		final[column] = v
	}
	return final, len(p.Set) > 0, true, nil
}
func changedColumns(base, current datum.Row) map[string]bool {
	changed := map[string]bool{}
	for column, v := range base {
		if w, ok := current[column]; !ok || !datum.Equal(v, w) {
			changed[column] = true
		}
	}
	for column := range current {
		if _, ok := base[column]; !ok {
			changed[column] = true
		}
	}
	return changed
}
