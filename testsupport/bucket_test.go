package testsupport

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
)

func TestBucket_Get_AnswersTheObject_NamesItAmongTheOpened_AndCountsItOpenUntilItIsClosed(t *testing.T) {
	bucket := NewBucket()
	bucket.Objects["a/1"] = "one"

	body, err := bucket.Get(context.Background(), "a/1")

	if err != nil {
		t.Fatal(err)
	}
	content, _ := io.ReadAll(body)
	if string(content) != "one" || !slices.Equal(bucket.Opened, []string{"a/1"}) || bucket.Open != 1 {
		t.Fatalf("read %q, opened %q, %d open", content, bucket.Opened, bucket.Open)
	}
	if err := body.Close(); err != nil || bucket.Open != 0 {
		t.Fatalf("after the close: err %v, %d open", err, bucket.Open)
	}
}

func TestBucket_Get_OfAnObjectThatIsNotThere_OrWithAnInjectedError_Fails_AndOpensNothing(t *testing.T) {
	unreachable := errors.New("bucket unreachable")
	for name, getErr := range map[string]error{"not there": nil, "injected": unreachable} {
		bucket := NewBucket()
		bucket.GetErr = getErr

		_, err := bucket.Get(context.Background(), "a/1")

		if err == nil || bucket.Open != 0 {
			t.Errorf("%s: err %v, %d open", name, err, bucket.Open)
		}
		if getErr != nil && !errors.Is(err, getErr) {
			t.Errorf("%s: err %v, want the injected one", name, err)
		}
	}
}

func TestBucket_List_AnswersTheKeysUnderThePrefix_InOrder(t *testing.T) {
	bucket := NewBucket()
	bucket.Objects["a/2"] = ""
	bucket.Objects["a/1"] = ""
	bucket.Objects["b/1"] = ""

	keys, err := bucket.List(context.Background(), "a/")

	if err != nil || !slices.Equal(keys, []string{"a/1", "a/2"}) {
		t.Fatalf("keys %q, err %v", keys, err)
	}
}
