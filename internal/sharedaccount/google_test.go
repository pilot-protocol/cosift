package sharedaccount

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type firestoreFixture struct {
	pb.UnimplementedFirestoreServer
	tokens  []*pb.Document
	account *pb.Document
	failure bool
	updated string
}

func (f *firestoreFixture) RunQuery(r *pb.RunQueryRequest, s pb.Firestore_RunQueryServer) error {
	if f.failure {
		return status.Error(codes.PermissionDenied, "fixture denied")
	}
	for _, doc := range f.tokens {
		if err := s.Send(&pb.RunQueryResponse{Document: doc, ReadTime: timestamppb.Now()}); err != nil {
			return err
		}
	}
	return nil
}
func (f *firestoreFixture) BatchGetDocuments(r *pb.BatchGetDocumentsRequest, s pb.Firestore_BatchGetDocumentsServer) error {
	if f.failure {
		return status.Error(codes.PermissionDenied, "fixture denied")
	}
	response := &pb.BatchGetDocumentsResponse{ReadTime: timestamppb.Now()}
	if f.account == nil {
		response.Result = &pb.BatchGetDocumentsResponse_Missing{Missing: r.Documents[0]}
	} else {
		response.Result = &pb.BatchGetDocumentsResponse_Found{Found: f.account}
	}
	return s.Send(response)
}
func (f *firestoreFixture) Commit(_ context.Context, r *pb.CommitRequest) (*pb.CommitResponse, error) {
	f.updated = r.Writes[0].GetUpdate().Name
	return &pb.CommitResponse{CommitTime: timestamppb.Now(), WriteResults: []*pb.WriteResult{{UpdateTime: timestamppb.Now()}}}, nil
}
func TestFirestoreSDKAccountSchemaAndFailures(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	fixture := &firestoreFixture{}
	pb.RegisterFirestoreServer(server, fixture)
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///fixture", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	db, err := firestore.NewClientWithDatabase(ctx, "fixture-project", "staging", option.WithGRPCConn(conn), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := googleStore{db: db}
	uid := "0123456789abcdef"
	tid := "a601f3d8e1812947874ecae841ba000280de424bc6bdcf3e216409f2e511e909"
	root := "projects/fixture-project/databases/staging/documents/accounts/" + uid
	str := func(v string) *pb.Value { return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: v}} }
	token := &pb.Document{CreateTime: timestamppb.Now(), UpdateTime: timestamppb.Now(), Name: root + "/tokens/" + tid, Fields: map[string]*pb.Value{"tid": str(tid)}}
	fixture.tokens = []*pb.Document{token}
	fixture.account = &pb.Document{CreateTime: timestamppb.Now(), UpdateTime: timestamppb.Now(), Name: root, Fields: map[string]*pb.Value{"email": str("shared@example.com"), "verified_at": {ValueType: &pb.Value_TimestampValue{TimestampValue: timestamppb.Now()}}}}
	record, err := store.Lookup(ctx, tid)
	if err != nil || record.UID != uid || record.Revoked {
		t.Fatal("token schema", record, err)
	}
	identity, err := store.Account(ctx, uid)
	if err != nil || identity.Email != "shared@example.com" {
		t.Fatal("account schema", identity, err)
	}
	if err = store.Touch(ctx, uid, tid, time.Now()); err != nil || fixture.updated != token.Name {
		t.Fatal("last-used write", err)
	}
	token.Fields["revoked_at"] = &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: timestamppb.Now()}}
	record, err = store.Lookup(ctx, tid)
	if err != nil || !record.Revoked {
		t.Fatal("revoked token schema", err)
	}
	fixture.account.Fields["banned"] = &pb.Value{ValueType: &pb.Value_BooleanValue{BooleanValue: true}}
	if _, err = store.Account(ctx, uid); !errors.Is(err, ErrBanned) {
		t.Fatal("ban schema", err)
	}
	fixture.tokens = append(fixture.tokens, token)
	if _, err = store.Lookup(ctx, tid); !errors.Is(err, ErrUnavailable) {
		t.Fatal("ambiguous token accepted", err)
	}
	fixture.tokens = nil
	if _, err = store.Lookup(ctx, tid); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("unknown token", err)
	}
	fixture.failure = true
	if _, err = store.Lookup(ctx, tid); !errors.Is(err, ErrUnavailable) {
		t.Fatal("IAM denial must fail closed", err)
	}
}
