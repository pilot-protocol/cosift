package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/pilot-protocol/cosift/internal/crawler"
	"github.com/pilot-protocol/cosift/internal/store"
)

var errCommunityReceiptConflict = errors.New("submission id belongs to another payload")

type communityReceiptRecord struct {
	PayloadHash         string                       `json:"payload_hash"`
	ApprovedContentHash string                       `json:"approved_content_hash,omitempty"`
	Eligible            bool                         `json:"eligible"`
	Receipt             *crawler.ContributionReceipt `json:"receipt,omitempty"`
}

// Journal intent before indexing and the receipt before returning HTTP success.
// A lost response, failed reward write, or restart must replay the same novelty
// decision. Keys live in Pebble's metadata family and are included in backups.
func (s *pebbleHTTP) fetchCommunityReceipt(ctx context.Context, id, raw string, artifact *crawler.LocalArtifact, approvedHash string) (crawler.ContributionReceipt, error) {
	if !crawler.ValidApprovedContentHash(approvedHash) {
		return crawler.ContributionReceipt{}, fmt.Errorf("%w: approved content hash is required", crawler.ErrContributionRejected)
	}
	if id == "" {
		return s.crawlCommunityFetch(ctx, raw, artifact, approvedHash)
	} // older trusted portals
	if s.store == nil {
		return crawler.ContributionReceipt{}, fmt.Errorf("receipt store unavailable")
	}
	s.communityReceiptMu.Lock()
	defer s.communityReceiptMu.Unlock()
	if err := ctx.Err(); err != nil {
		return crawler.ContributionReceipt{}, err
	}
	payload, _ := json.Marshal(struct {
		URL      string
		Artifact *crawler.LocalArtifact
	}{raw, artifact})
	hash := sha256.Sum256(payload)
	record := communityReceiptRecord{PayloadHash: hex.EncodeToString(hash[:]), ApprovedContentHash: approvedHash}
	key := []byte("mcommunity_receipt:" + id)
	db := s.store.DB()
	encoded, closer, err := db.Get(key)
	if err == nil {
		record = communityReceiptRecord{}
		err = json.Unmarshal(encoded, &record)
		closer.Close()
		if err != nil {
			return crawler.ContributionReceipt{}, err
		}
		if record.PayloadHash != hex.EncodeToString(hash[:]) {
			return crawler.ContributionReceipt{}, errCommunityReceiptConflict
		}
		if record.ApprovedContentHash == "" {
			// Upgrade a legacy receipt only when its indexed content is the
			// exact document approved now. Never grandfather unbound content.
			if record.Receipt != nil {
				canon, canonicalErr := crawler.ContributionURL(raw)
				if canonicalErr != nil {
					return crawler.ContributionReceipt{}, canonicalErr
				}
				doc, docErr := s.store.GetDocByURL(ctx, canon)
				if docErr != nil || doc == nil || crawler.ApprovedContentHash(doc.Title, doc.Text) != approvedHash {
					return crawler.ContributionReceipt{}, fmt.Errorf("%w: legacy receipt content is not approved", crawler.ErrContributionRejected)
				}
				textHash := sha256.Sum256([]byte(doc.Text))
				if record.Receipt.ContentHash != hex.EncodeToString(textHash[:]) {
					return crawler.ContributionReceipt{}, fmt.Errorf("%w: legacy receipt content changed", crawler.ErrContributionRejected)
				}
			}
			record.ApprovedContentHash = approvedHash
			data, _ := json.Marshal(record)
			if err := db.Set(key, data, pebble.Sync); err != nil {
				return crawler.ContributionReceipt{}, err
			}
		}
		if record.ApprovedContentHash != approvedHash {
			return crawler.ContributionReceipt{}, fmt.Errorf("%w: approval differs from recorded submission", crawler.ErrContributionRejected)
		}
		if record.Receipt != nil {
			return *record.Receipt, nil
		}
	} else if errors.Is(err, pebble.ErrNotFound) {
		canon, canonicalErr := crawler.ContributionURL(raw)
		if canonicalErr != nil {
			return crawler.ContributionReceipt{}, canonicalErr
		}
		_, priorErr := s.store.GetDocByURL(ctx, canon)
		if priorErr != nil && !errors.Is(priorErr, store.ErrNotFound) {
			return crawler.ContributionReceipt{}, priorErr
		}
		record.Eligible = errors.Is(priorErr, store.ErrNotFound)
		data, _ := json.Marshal(record)
		if err = db.Set(key, data, pebble.Sync); err != nil {
			return crawler.ContributionReceipt{}, err
		}
	} else {
		return crawler.ContributionReceipt{}, err
	}
	receipt, err := s.crawlCommunityFetch(ctx, raw, artifact, approvedHash)
	if err != nil {
		return crawler.ContributionReceipt{}, err
	}
	receipt.Novel = receipt.Indexed && record.Eligible
	record.Receipt = &receipt
	data, _ := json.Marshal(record)
	if err = db.Set(key, data, pebble.Sync); err != nil {
		return crawler.ContributionReceipt{}, err
	}
	return receipt, nil
}
