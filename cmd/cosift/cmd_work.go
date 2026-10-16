package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
)

const workUsage = `usage: cosift work purge --url URL [-y] [-server URL] [-token TOKEN] [-json]
       cosift work void --worker ID [-y] [-server URL] [-token TOKEN] [-json]

purge removes what a forged URL indexed; void drops one worker's pending attestations.
Without -y nothing changes and the command prints what it would do.`

// runWork is `cosift work purge|void`: one call to a loopback /admin route of the running engine.
func runWork(ctx context.Context, cfg *config.Config, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || (args[0] != "purge" && args[0] != "void") {
		return &usageError{msg: workUsage}
	}
	op := args[0]
	fs := flag.NewFlagSet("work "+op, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	server := fs.String("server", strings.Replace("http://"+cfg.Server.Addr, "0.0.0.0", "127.0.0.1", 1), "cosift server URL")
	token := fs.String("token", "", "admin bearer token (defaults to COSIFT_ADMIN_TOKEN)")
	jsonOut := fs.Bool("json", false, "print the raw JSON answer")
	apply := fs.Bool("y", false, "apply the change")
	target := fs.String("url", "", "the forged URL (purge)")
	worker := fs.String("worker", "", "the worker id from the app's work_workers table (void)")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() > 0 {
		return &usageError{msg: workUsage}
	}
	path, body := "/admin/work-purge", map[string]any{"url": *target, "apply": *apply}
	if op == "void" {
		path, body = "/admin/work-void", map[string]any{"worker": *worker, "apply": *apply}
	}
	if (op == "purge" && *target == "") || (op == "void" && *worker == "") {
		return &usageError{msg: workUsage}
	}
	if *token == "" {
		*token = os.Getenv("COSIFT_ADMIN_TOKEN")
	}
	raw, _ := json.Marshal(body)
	req, err := adminRequest(ctx, http.MethodPost, *server, path, *token, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := (&http.Client{Timeout: 5 * time.Minute}).Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", *server, err)
	}
	defer res.Body.Close()
	out, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server %d: %s", res.StatusCode, bytes.TrimSpace(out))
	}
	if *jsonOut {
		fmt.Fprintln(stdout, strings.TrimSpace(string(out)))
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	mode := "applied"
	if !*apply {
		mode = "dry run, nothing changed; add -y to apply"
	}
	fmt.Fprintf(stdout, "%s (%s)\n", op, mode)
	for _, k := range workAnswerKeys[op] {
		fmt.Fprintf(stdout, "  %-22s %v\n", k, m[k])
	}
	return nil
}

var workAnswerKeys = map[string][]string{
	"purge": {"documents_deleted", "documents_kept", "records_deleted", "texts_deleted", "workers_settled", "parked_until"},
	"void":  {"records_changed", "attestations_dropped", "records_deleted", "urls_parked"},
}
