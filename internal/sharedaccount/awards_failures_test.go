package sharedaccount

import (
	"context"
	"errors"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type transactionFixture struct{ *firestoreFixture }

func (transactionFixture) BeginTransaction(context.Context, *pb.BeginTransactionRequest) (*pb.BeginTransactionResponse, error) {
	return &pb.BeginTransactionResponse{Transaction: []byte("fixture-transaction")}, nil
}

func (transactionFixture) Rollback(context.Context, *pb.RollbackRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func deniedAwardSource(t *testing.T) AwardSource {
	t.Helper()
	store := googleFixtureStore(t, transactionFixture{&firestoreFixture{failure: true}})
	return NewAwardSource(store.db)
}

func TestAwardReadsMapFirestoreFailuresToUnavailable(t *testing.T) {
	src := deniedAwardSource(t)
	ctx := context.Background()
	if page, err := src.PendingAwards(ctx, AwardPosition{}, 10); !errors.Is(err, ErrUnavailable) || page != nil {
		t.Fatalf("PendingAwards: %v %v", page, err)
	}
	if _, err := src.Contributor(ctx, "0f1e2d3c4b5a6978"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Contributor: %v", err)
	}
	if err := src.MarkApplied(ctx, "art:A", AwardResult{Status: "applied", AppliedDelta: 1000, LedgerID: "award:art:A"}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("MarkApplied must not report success or a verdict it could not read: %v", err)
	}
}

func TestContributorWithoutAStringUIDIsUnavailable(t *testing.T) {
	doc := &pb.Document{
		Name:       "projects/fixture-project/databases/staging/documents/contributors/0f1e2d3c4b5a6978",
		Fields:     map[string]*pb.Value{"uid": {ValueType: &pb.Value_IntegerValue{IntegerValue: 7}}},
		CreateTime: timestamppb.Now(),
		UpdateTime: timestamppb.Now(),
	}
	src := NewAwardSource(googleFixtureStore(t, transactionFixture{&firestoreFixture{account: doc}}).db)
	if got, err := src.Contributor(context.Background(), "0f1e2d3c4b5a6978"); !errors.Is(err, ErrUnavailable) || got.UID != "" {
		t.Fatalf("Contributor: %+v %v", got, err)
	}
}
