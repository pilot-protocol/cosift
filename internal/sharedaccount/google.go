package sharedaccount

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type googleStore struct{ db *firestore.Client }

func (s googleStore) Lookup(ctx context.Context, tid string) (tokenRecord, error) {
	it := s.db.CollectionGroup("tokens").Where("tid", "==", tid).Limit(2).Documents(ctx)
	defer it.Stop()
	doc, err := it.Next()
	if errors.Is(err, iterator.Done) {
		return tokenRecord{}, ErrUnauthorized
	}
	if err != nil {
		return tokenRecord{}, ErrUnavailable
	}
	// Refuse ambiguous or out-of-schema collection-group matches.
	if _, err = it.Next(); !errors.Is(err, iterator.Done) {
		return tokenRecord{}, ErrUnavailable
	}
	parent := doc.Ref.Parent.Parent
	if parent == nil || parent.Parent.ID != "accounts" || parent.Parent.Parent != nil || doc.Ref.ID != tid {
		return tokenRecord{}, ErrUnavailable
	}
	data := doc.Data()
	rec := tokenRecord{UID: parent.ID, Revoked: data["revoked_at"] != nil}
	if stamp, ok := data["last_used_at"].(time.Time); ok {
		rec.LastUsed = stamp
	}
	return rec, nil
}
func (s googleStore) Account(ctx context.Context, uid string) (Identity, error) {
	doc, err := s.db.Collection("accounts").Doc(uid).Get(ctx)
	if status.Code(err) == codes.NotFound {
		return Identity{}, ErrUnauthorized
	}
	if err != nil {
		return Identity{}, ErrUnavailable
	}
	data := doc.Data()
	if banned, exists := data["banned"]; exists && banned != nil {
		value, ok := banned.(bool)
		if !ok {
			return Identity{}, ErrUnavailable
		}
		if value {
			return Identity{}, ErrBanned
		}
	}
	if _, ok := data["verified_at"].(time.Time); !ok {
		return Identity{}, ErrUnauthorized
	}
	email, ok := data["email"].(string)
	if !ok || email != strings.ToLower(strings.TrimSpace(email)) {
		return Identity{}, ErrUnavailable
	}
	return Identity{UID: uid, Email: email}, nil
}
func (s googleStore) Touch(ctx context.Context, uid, tid string, now time.Time) error {
	_, err := s.db.Collection("accounts").Doc(uid).Collection("tokens").Doc(tid).Update(ctx, []firestore.Update{{Path: "last_used_at", Value: now}})
	return err
}

type Config struct {
	Project, Database, AuthURL, MCPURL string
	// For private Cloud Run staging, use the canonical run.app origins as audiences.
	// ADC supplies refreshing Google identity tokens; ck_ stays in Authorization.
	AuthAudience, MCPAudience string
}
type Client struct {
	cfg       Config
	verifier  *verifier
	db        *firestore.Client
	secrets   *secretmanager.Client
	auth, mcp *remote
}

func New(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Project == "" || cfg.Database == "" || strings.ContainsAny(cfg.Project+cfg.Database, "/\r\n") {
		return nil, fmt.Errorf("shared auth requires an explicit Google project and Firestore database")
	}
	auth, err := newRemote(ctx, cfg.AuthURL, cfg.AuthAudience, false)
	if err != nil {
		return nil, err
	}
	mcp, err := newRemote(ctx, cfg.MCPURL, cfg.MCPAudience, true)
	if err != nil {
		return nil, err
	}
	db, err := firestore.NewClientWithDatabase(ctx, cfg.Project, cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("initialize shared Firestore client: %w", err)
	}
	secrets, err := secretmanager.NewClient(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize token Secret Manager client: %w", err)
	}
	c := &Client{cfg: cfg, db: db, secrets: secrets, auth: auth, mcp: mcp}
	c.verifier = &verifier{store: googleStore{db: db}, pepper: func(ctx context.Context, id string) ([]byte, error) {
		res, err := secrets.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: fmt.Sprintf("projects/%s/secrets/cosift-token-pepper/versions/%s", cfg.Project, id)})
		if status.Code(err) == codes.NotFound {
			return nil, ErrUnauthorized
		}
		if err != nil || res.Payload == nil {
			return nil, ErrUnavailable
		}
		return res.Payload.Data, nil
	}}
	return c, nil
}
func (c *Client) Namespace() string { return c.cfg.Project + "/" + c.cfg.Database }
func (c *Client) Close() error      { return errors.Join(c.db.Close(), c.secrets.Close()) }
func (c *Client) Verify(ctx context.Context, token string) (Identity, error) {
	return c.verifier.verify(ctx, token)
}
