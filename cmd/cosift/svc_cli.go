package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/config"
	"github.com/pilot-protocol/cosift/internal/svcauth"
	v1 "github.com/pilot-protocol/cosift/internal/v1"
)

const defaultPepperFile = "/etc/cosift/cosift.env"

// svcAuthOwnerUID is the owner service-auth.json must have; a var for tests.
var svcAuthOwnerUID uint32

// exitError ends the process with a specific status.
type exitError struct {
	code int
	msg  string
}

func (e *exitError) Error() string { return e.msg }

const svcKeyUsage = "usage: cosift svc-key new --id <principal-id> --scopes <scope>[,<scope>...] --env prod|staging [--rpm <n>] [--burst <n>] [--counts-reads] [--key-only] [--pepper-file " + defaultPepperFile + "]"

func svcPepper(file string) []byte {
	if v := os.Getenv("COSIFT_SVC_PEPPER"); v != "" {
		return []byte(v)
	}
	v, _ := svcauth.ReadEnvFile(file, "COSIFT_SVC_PEPPER")
	return []byte(v)
}

func runSvcKey(args []string, stdout, stderr io.Writer, now time.Time) error {
	if len(args) == 0 || args[0] != "new" {
		return &usageError{msg: svcKeyUsage}
	}
	fs := flag.NewFlagSet("svc-key new", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	id := fs.String("id", "", "principal id")
	scopes := fs.String("scopes", "", "comma-separated scopes")
	env := fs.String("env", "", "prod or staging")
	rpm := fs.Int("rpm", 120, "requests per minute")
	burst := fs.Int("burst", 0, "request burst (default ceil(rpm/4))")
	countsReads := fs.Bool("counts-reads", false, "count distinct readers on match")
	keyOnly := fs.Bool("key-only", false, "print only a keys[] element")
	pepperFile := fs.String("pepper-file", defaultPepperFile, "EnvironmentFile holding COSIFT_SVC_PEPPER")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() > 0 {
		return &usageError{msg: svcKeyUsage}
	}
	if *id == "" || *scopes == "" || *env == "" {
		return &usageError{msg: svcKeyUsage}
	}
	kp := svcauth.KeyPrincipal{ID: *id, Env: *env, Scopes: strings.Split(*scopes, ","), RPM: *rpm, Burst: *burst, CountsReads: *countsReads}
	if _, _, err := kp.Entries(strings.Repeat("0", 16), [32]byte{}, ""); err != nil {
		return &usageError{msg: "svc-key: " + err.Error()}
	}
	pepper := svcPepper(*pepperFile)
	if !svcauth.PepperOK(pepper) {
		return &exitError{code: 3, msg: fmt.Sprintf("svc-key: COSIFT_SVC_PEPPER absent or shorter than %d bytes", svcauth.MinPepperLen)}
	}
	key, keyID, err := svcauth.NewKey()
	if err != nil {
		return fmt.Errorf("svc-key: %w", err)
	}
	principal, element, err := kp.Entries(keyID, svcauth.Digest(pepper, key), now.UTC().Format(time.DateOnly))
	if err != nil {
		return &usageError{msg: "svc-key: " + err.Error()}
	}
	entry, where := principal, "add the entry to "+svcauth.DefaultPath
	if *keyOnly {
		entry, where = element, "add the element to the principal's keys in "+svcauth.DefaultPath
	}
	fmt.Fprintf(stdout, "%s\n%s\n", key, entry)
	fmt.Fprintf(stderr, "svc-key: store the key now (it is not recoverable), %s,\nsvc-key: run `cosift svc-auth check`, then `systemctl reload cosift-serve`.\n", where)
	return nil
}

func runSvcAuth(cfg *config.Config, cfgPath string, args []string, stdout, stderr io.Writer) error {
	usage := &usageError{msg: "usage: cosift [-config cosift.json] svc-auth check [--file " + svcauth.DefaultPath + "] [--pepper-file " + defaultPepperFile + "] [--no-engine-config]"}
	if len(args) == 0 || args[0] != "check" {
		return usage
	}
	fs := flag.NewFlagSet("svc-auth check", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	file := fs.String("file", svcauth.DefaultPath, "service-auth.json")
	pepperFile := fs.String("pepper-file", defaultPepperFile, "EnvironmentFile holding COSIFT_SVC_PEPPER")
	noEngine := fs.Bool("no-engine-config", false, "skip the checks against cosift.json")
	if err := fs.Parse(args[1:]); err != nil || fs.NArg() > 0 {
		return usage
	}
	checks := svcauth.Checks{PeerAuthToken: cfg.Cluster.PeerAuthToken, AdminToken: cfg.Server.AdminToken, MainAddr: cfg.Server.Addr}
	if *noEngine {
		checks = svcauth.Checks{}
		fmt.Fprintln(stderr, "svc-auth: WARN --no-engine-config: the token collision and listen checks against cosift.json are skipped")
	} else if _, err := os.Stat(cfgPath); err != nil {
		return fmt.Errorf("svc-auth: engine config %s not found; pass -config <cosift.json> so the token collision check can run, or --no-engine-config to skip it", cfgPath)
	}
	pepper := svcPepper(*pepperFile)
	checks.Pepper = pepper
	b, err := svcauth.ReadFile(*file, svcAuthOwnerUID)
	if errors.Is(err, svcauth.ErrAbsent) {
		return fmt.Errorf("svc-auth: %s absent", *file)
	}
	if err != nil {
		return fmt.Errorf("svc-auth: %s refused: %v", *file, err)
	}
	c, err := svcauth.Parse(b, checks)
	if err != nil {
		return fmt.Errorf("svc-auth: %s refused: %v", *file, err)
	}
	keys := false
	for _, p := range c.Principals {
		scopes := make([]string, len(p.Scopes))
		for i, s := range p.Scopes {
			scopes[i] = string(s)
		}
		fmt.Fprintf(stdout, "%s %s %s %s\n", p.ID, p.Kind, p.Env, strings.Join(scopes, ","))
		keys = keys || p.Kind == v1.KindKey
	}
	if !svcauth.PepperOK(pepper) {
		note := "key authentication would be disabled"
		if keys {
			note = "key principals could not authenticate and the token collision check was skipped"
		}
		fmt.Fprintf(stderr, "svc-auth: WARN COSIFT_SVC_PEPPER absent or shorter than %d bytes — %s\n", svcauth.MinPepperLen, note)
	}
	return nil
}
