package sharedaccount

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func googleFixtureStore(t *testing.T, fixture *firestoreFixture) googleStore {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pb.RegisterFirestoreServer(server, fixture)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///fixture", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	db, err := firestore.NewClientWithDatabase(context.Background(), "fixture-project", "staging", option.WithGRPCConn(conn), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return googleStore{db: db}
}

func TestFirestoreRejectsMissingAndCorruptAccounts(t *testing.T) {
	const uid = "0123456789abcdef"
	for _, tc := range []struct {
		name            string
		mutate          func(map[string]*pb.Value)
		missing, denied bool
		want            error
	}{
		{name: "missing document", missing: true, want: ErrUnauthorized},
		{name: "IAM denied", denied: true, want: ErrUnavailable},
		{name: "unverified", mutate: func(fields map[string]*pb.Value) { delete(fields, "verified_at") }, want: ErrUnauthorized},
		{name: "wrong verification type", mutate: func(fields map[string]*pb.Value) {
			fields["verified_at"] = &pb.Value{ValueType: &pb.Value_StringValue{StringValue: "yesterday"}}
		}, want: ErrUnauthorized},
		{name: "wrong ban type", mutate: func(fields map[string]*pb.Value) {
			fields["banned"] = &pb.Value{ValueType: &pb.Value_StringValue{StringValue: "false"}}
		}, want: ErrUnavailable},
		{name: "unbanned", mutate: func(fields map[string]*pb.Value) {
			fields["banned"] = &pb.Value{ValueType: &pb.Value_BooleanValue{BooleanValue: false}}
		}},
		{name: "missing email", mutate: func(fields map[string]*pb.Value) { delete(fields, "email") }, want: ErrUnavailable},
		{name: "noncanonical email", mutate: func(fields map[string]*pb.Value) {
			fields["email"] = &pb.Value{ValueType: &pb.Value_StringValue{StringValue: " Fixture@example.com "}}
		}, want: ErrUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := map[string]*pb.Value{"email": {ValueType: &pb.Value_StringValue{StringValue: "fixture@example.com"}}, "verified_at": {ValueType: &pb.Value_TimestampValue{TimestampValue: timestamppb.Now()}}}
			if tc.mutate != nil {
				tc.mutate(fields)
			}
			fixture := &firestoreFixture{failure: tc.denied}
			if !tc.missing {
				fixture.account = &pb.Document{Name: "projects/fixture-project/databases/staging/documents/accounts/" + uid, CreateTime: timestamppb.Now(), UpdateTime: timestamppb.Now(), Fields: fields}
			}
			identity, err := googleFixtureStore(t, fixture).Account(context.Background(), uid)
			if !errors.Is(err, tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, err)
			}
			if tc.want == nil && (identity.UID != uid || identity.Email != "fixture@example.com") {
				t.Fatalf("wrong identity: %+v", identity)
			}
		})
	}
}

func TestFirestoreTokenCollectionGroupIsBoundToAccountSchema(t *testing.T) {
	const tid = "fixture-token-id"
	const uid = "0123456789abcdef"
	for _, path := range []string{"tokens/" + tid, "other/" + uid + "/tokens/" + tid, "tenants/nested/accounts/" + uid + "/tokens/" + tid, "accounts/" + uid + "/tokens/another-token-id"} {
		t.Run(path, func(t *testing.T) {
			doc := &pb.Document{Name: "projects/fixture-project/databases/staging/documents/" + path, CreateTime: timestamppb.Now(), UpdateTime: timestamppb.Now(), Fields: map[string]*pb.Value{"tid": {ValueType: &pb.Value_StringValue{StringValue: tid}}}}
			store := googleFixtureStore(t, &firestoreFixture{tokens: []*pb.Document{doc}})
			if _, err := store.Lookup(context.Background(), tid); !errors.Is(err, ErrUnavailable) {
				t.Fatal("out-of-schema token resolved to an account", err)
			}
		})
	}
	used := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	doc := &pb.Document{Name: "projects/fixture-project/databases/staging/documents/accounts/" + uid + "/tokens/" + tid, CreateTime: timestamppb.Now(), UpdateTime: timestamppb.Now(), Fields: map[string]*pb.Value{"tid": {ValueType: &pb.Value_StringValue{StringValue: tid}}, "last_used_at": {ValueType: &pb.Value_TimestampValue{TimestampValue: timestamppb.New(used)}}}}
	store := googleFixtureStore(t, &firestoreFixture{tokens: []*pb.Document{doc}})
	rec, err := store.Lookup(context.Background(), tid)
	if err != nil || !rec.LastUsed.Equal(used) {
		t.Fatal("last-used timestamp lost through SDK decoding", rec, err)
	}
}

func TestSharedGoogleConfigurationFailsBeforeUsingLocalCredentials(t *testing.T) {
	// An explicitly missing fixture credentials file prevents these tests from
	// consulting a developer's ADC or metadata service.
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("FIRESTORE_EMULATOR_HOST", "")
	valid := Config{Project: "fixture-project", Database: "staging", AuthURL: "https://auth.example.com", MCPURL: "https://mcp.example.com/v1/mcp"}
	for _, tc := range []struct {
		name   string
		change func(*Config)
		want   string
	}{
		{"missing project", func(c *Config) { c.Project = "" }, "explicit Google project"},
		{"missing database", func(c *Config) { c.Database = "" }, "explicit Google project"},
		{"path traversal", func(c *Config) { c.Database = "../staging" }, "explicit Google project"},
		{"invalid auth origin", func(c *Config) { c.AuthURL += "/auth/start" }, "auth URL must be an origin"},
		{"insecure MCP", func(c *Config) { c.MCPURL = "http://mcp.example.com/v1/mcp" }, "must use HTTPS"},
		{"missing ADC", func(c *Config) {}, "initialize shared Firestore client"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.change(&cfg)
			client, err := New(context.Background(), cfg)
			if client != nil || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected configuration error containing %q, got %v", tc.want, err)
			}
		})
	}
	if got := (&Client{cfg: valid}).Namespace(); got != "fixture-project/staging" {
		t.Fatalf("identity namespace not bound to both project and database: %q", got)
	}
}
