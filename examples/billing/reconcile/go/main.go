package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}

	nc, _ := nats.Connect(url)
	defer nc.Drain()

	js, _ := jetstream.New(nc)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	kv, _ := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "billing", History: 10})

	t := time.Date(2006, 5, 4, 3, 2, 1, 0, time.UTC)
	fmt.Printf("%s Control plane created\n", t.Format(time.RFC3339))
	ctp := controlPlane{
		Name:              "ctp1",
		Namespace:         "group1",
		ID:                "abcd-1234",
		CreationTimestamp: t.Format(time.RFC3339),
		Size:              "small",
	}
	reconcile(ctx, kv, t, ctp)

	t = t.Add(5 * time.Minute)
	fmt.Printf("%s Control plane size updated\n", t.Format(time.RFC3339))
	ctp.Size = "large"
	reconcile(ctx, kv, t, ctp)

	t = t.Add(5 * time.Minute)
	fmt.Printf("%s Control plane synced with no changes\n", t.Format(time.RFC3339))
	reconcile(ctx, kv, t, ctp)

	t = t.Add(5 * time.Minute)
	fmt.Printf("%s Control plane deleted\n", t.Format(time.RFC3339))
	ctp.DeletionTimestamp = t.Format(time.RFC3339)
	reconcile(ctx, kv, t, ctp)

	t = t.Add(5 * time.Minute)
	fmt.Printf("%s Exporter deployed\n", t.Format(time.RFC3339))
	w, _ := kv.WatchAll(ctx, jetstream.IncludeHistory())
	caughtUp := make(chan bool)
	go func() {
		for kve := range w.Updates() {
			if kve == nil {
				fmt.Println("exporter: caught up")
				caughtUp <- true
				continue
			}
			fmt.Printf("exporter: watch: %s @ %d -> %q (op: %s)\n", kve.Key(), kve.Revision(), string(kve.Value()), kve.Operation())
		}
	}()

	<-caughtUp

	t = t.Add(5 * time.Minute)
	fmt.Printf("%s Control plane created\n", t.Format(time.RFC3339))
	ctp = controlPlane{
		Name:              "ctp2",
		Namespace:         "group1",
		ID:                "dcba-4321",
		CreationTimestamp: t.Format(time.RFC3339),
		Size:              "small",
	}
	reconcile(ctx, kv, t, ctp)

	// Give the exporter time to process remaining entries.
	time.Sleep(50 * time.Millisecond)
	w.Stop()
}

type controlPlane struct {
	Name              string
	Namespace         string
	ID                string
	CreationTimestamp string
	DeletionTimestamp string
	Size              string
}

func reconcile(ctx context.Context, kv jetstream.KeyValue, t time.Time, ctp controlPlane) {
	keyBase := fmt.Sprintf("controlplane.%s.%s.%s", ctp.Namespace, ctp.Name, ctp.ID)
	keyCreatedAt := keyBase + ".created_at"
	keyDeletedAt := keyBase + ".deleted_at"
	keySizeAt := keyBase + ".size_at"

	fmt.Println("reconciler: setting creation timestamp")
	fmt.Println("reconciler: create created_at")
	_, err := kv.Create(ctx, keyCreatedAt, []byte(ctp.CreationTimestamp))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			fmt.Println("reconciler: create: got ErrKeyExists")
		} else {
			log.Fatalf("reconciler: create: unexpected error: %s", err)
		}
	} else {
		fmt.Println("reconciler: create: succeeded")
	}

	fmt.Printf("reconciler: setting size=%s\n", ctp.Size)
	fmt.Println("reconciler: get size_at")
	kve, err := kv.Get(ctx, keySizeAt)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			fmt.Println("reconciler: get: got ErrKeyNotFound")
			fmt.Println("reconciler: create size_at")
			_, err = kv.Create(ctx, keySizeAt, []byte(strings.Join([]string{ctp.Size, t.Format(time.RFC3339)}, ",")))
			if err != nil {
				log.Fatalf("reconciler: create: unexpected error: %s", err)
			}
			fmt.Println("reconciler: create: succeeded")
		} else {
			log.Fatalf("reconciler: get: got unexpected error: %s\n", err)
		}
	} else {
		fmt.Println("reconciler: get: succeeded")
		storedSize := strings.Split(string(kve.Value()), ",")[0]
		if ctp.Size == storedSize {
			fmt.Println("reconciler: size is up to date")
		} else {
			fmt.Println("reconciler: update size_at")
			_, err = kv.Update(ctx, keySizeAt, []byte(strings.Join([]string{ctp.Size, t.Format(time.RFC3339)}, ",")), kve.Revision())
			if err != nil {
				log.Fatalf("reconciler: update: unexpected error: %s", err)
			}
			fmt.Println("reconciler: update: succeeded")
		}
	}

	if ctp.DeletionTimestamp == "" {
		return
	}
	fmt.Println("reconciler: setting deletion timestamp")
	fmt.Println("reconciler: create deleted_at")
	_, err = kv.Create(ctx, keyDeletedAt, []byte(ctp.DeletionTimestamp))
	if err != nil {
		log.Fatalf("reconciler: create: unexpected error: %s", err)
	}
	fmt.Println("reconciler: create: succeeded")
}
