package community

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"github.com/pilot-protocol/cosift/internal/sharedaccount"
)

func (s *Server) bindSharedNamespace() error {
	var existing string
	err := s.db.QueryRow(`SELECT value FROM settings WHERE key='shared_namespace'`).Scan(&existing)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if s.cfg.Shared == nil {
		if existing != "" {
			return fmt.Errorf("this database uses shared authentication; configure its original provider")
		}
		return nil
	}
	namespace := s.cfg.Shared.Namespace()
	if namespace == "" || existing != "" && existing != namespace {
		return fmt.Errorf("shared account namespace differs from this database; use its original project/database")
	}
	_, err = s.db.Exec(`INSERT INTO settings(key,value) VALUES('shared_namespace',?) ON CONFLICT(key) DO NOTHING`, namespace)
	if err != nil {
		return err
	}
	if err = s.db.QueryRow(`SELECT value FROM settings WHERE key='shared_namespace'`).Scan(&existing); err != nil {
		return err
	}
	if existing != namespace {
		return fmt.Errorf("shared account namespace was initialized by another process")
	}
	return nil
}

// Email comes only from the verified upstream account. Linking preserves the
// existing ledger and saves, and invalidates every old password session.
func (s *Server) sharedUser(ctx context.Context, identity sharedaccount.Identity) (User, error) {
	if !sharedaccount.UIDPattern.MatchString(identity.UID) || identity.Email == "" {
		return User{}, sharedaccount.ErrUnavailable
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, err
	}
	defer tx.Rollback()
	var id string
	err = tx.QueryRowContext(ctx, `SELECT user_id FROM shared_identities WHERE uid=?`, identity.UID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, `SELECT id FROM users WHERE email=?`, identity.Email).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			id = randomID()
			_, err = tx.ExecContext(ctx, `INSERT INTO users(id,email,name,salt,password_hash,created_at) VALUES(?,?,?,?,?,?)`, id, identity.Email, strings.Split(identity.Email, "@")[0], randomID(), "", time.Now().Unix())
		}
		if err != nil {
			return User{}, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO shared_identities(uid,user_id) VALUES(?,?)`, identity.UID, id); err != nil {
			return User{}, err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM sessions WHERE user_id=?`, id); err != nil {
			return User{}, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE users SET password_hash='' WHERE id=?`, id); err != nil {
			return User{}, err
		}
	} else if err != nil {
		return User{}, err
	}
	u, err := scanUser(tx.QueryRowContext(ctx, `SELECT id,email,name,interests,onboarded FROM users WHERE id=?`, id))
	if err != nil {
		return User{}, err
	}
	if u.Email != identity.Email {
		return User{}, sharedaccount.ErrUnavailable
	}
	if err = tx.Commit(); err != nil {
		return User{}, err
	}
	return u, nil
}

func sharedToken(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		parts := strings.Fields(h)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			return parts[1]
		}
		return ""
	}
	if c, e := r.Cookie(cookieName); e == nil {
		return c.Value
	}
	return ""
}
func sharedProblem(w http.ResponseWriter, err error) {
	status := 503
	message := "shared Cosift services are unavailable; please retry"
	switch {
	case errors.Is(err, sharedaccount.ErrUnauthorized):
		status = 401
		message = "session invalid or revoked; sign in again"
	case errors.Is(err, sharedaccount.ErrBanned):
		status = 403
		message = "account suspended"
	case errors.Is(err, sharedaccount.ErrInvalid):
		status = 400
		message = "invalid shared account request"
	case errors.Is(err, sharedaccount.ErrLimited):
		status = 429
		message = "shared service limit reached; retry later"
	}
	problem(w, status, message)
}
func (s *Server) sharedAuth(w http.ResponseWriter, r *http.Request, next userHandler, optional bool) {
	_, cookieErr := r.Cookie(cookieName)
	if optional && cookieErr != nil && r.Header.Get("Authorization") == "" {
		next(w, r, User{})
		return
	}
	if s.cfg.Shared == nil {
		sharedProblem(w, sharedaccount.ErrUnauthorized)
		return
	}
	token := sharedToken(r)
	if _, err := sharedaccount.Parse(token); err != nil {
		sharedProblem(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	identity, err := s.cfg.Shared.Verify(ctx, token)
	if err != nil {
		if errors.Is(err, sharedaccount.ErrUnauthorized) && r.Header.Get("Authorization") == "" {
			http.SetCookie(w, s.sharedCookie("", -1))
		}
		sharedProblem(w, err)
		return
	}
	u, err := s.sharedUser(ctx, identity)
	if err != nil {
		sharedProblem(w, err)
		return
	}
	next(w, r, u)
}
func (s *Server) sharedCookie(token string, age int) *http.Cookie {
	return &http.Cookie{Name: cookieName, Value: token, Path: "/", HttpOnly: true, Secure: strings.HasPrefix(s.cfg.PublicURL, "https:"), SameSite: http.SameSiteLaxMode, MaxAge: age, Expires: time.Now().Add(time.Duration(age) * time.Second)}
}
func (s *Server) sharedStart(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Shared == nil {
		problem(w, 404, "shared login is not enabled")
		return
	}
	if !s.allow("shared-auth:"+s.clientIP(r), 10, time.Minute) {
		sharedProblem(w, sharedaccount.ErrLimited)
		return
	}
	var in struct {
		Email string `json:"email"`
	}
	if decode(r, &in) != nil {
		sharedProblem(w, sharedaccount.ErrInvalid)
		return
	}
	in.Email = strings.ToLower(strings.TrimSpace(in.Email))
	a, err := mail.ParseAddress(in.Email)
	if err != nil || a.Address != in.Email || len(in.Email) > 254 {
		sharedProblem(w, sharedaccount.ErrInvalid)
		return
	}
	result, err := s.cfg.Shared.Start(sharedaccount.WithClientIP(r.Context(), s.clientIP(r)), in.Email)
	if err != nil {
		sharedProblem(w, err)
		return
	}
	respond(w, 200, result)
}
func (s *Server) sharedFinish(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Shared == nil {
		problem(w, 404, "shared login is not enabled")
		return
	}
	if !s.allow("shared-verify:"+s.clientIP(r), 20, time.Minute) {
		sharedProblem(w, sharedaccount.ErrLimited)
		return
	}
	var in struct {
		RequestID string `json:"request_id"`
		Code      string `json:"code"`
	}
	if decode(r, &in) != nil || len(in.RequestID) != 26 || len(in.Code) != 6 || strings.Trim(in.Code, "0123456789") != "" {
		sharedProblem(w, sharedaccount.ErrInvalid)
		return
	}
	issued, err := s.cfg.Shared.Finish(sharedaccount.WithClientIP(r.Context(), s.clientIP(r)), in.RequestID, in.Code)
	if err != nil {
		sharedProblem(w, err)
		return
	}
	identity, err := s.cfg.Shared.Verify(r.Context(), issued.Token)
	if err == nil && identity.UID != issued.UID {
		err = sharedaccount.ErrUnauthorized
	}
	var u User
	if err == nil {
		u, err = s.sharedUser(r.Context(), identity)
	}
	if err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.cfg.Shared.Revoke(ctx, issued.Token)
		sharedProblem(w, err)
		return
	}
	http.SetCookie(w, s.sharedCookie(issued.Token, int(sessionAge.Seconds())))
	respond(w, 200, u)
}
func (s *Server) sharedTool(w http.ResponseWriter, r *http.Request, u User) {
	if s.cfg.Shared == nil {
		problem(w, 404, "shared topics are not enabled")
		return
	}
	if !s.allow("shared-tools:"+u.ID, 30, time.Minute) {
		sharedProblem(w, sharedaccount.ErrLimited)
		return
	}
	var in struct {
		Tool   string   `json:"tool"`
		Topic  string   `json:"topic"`
		Why    string   `json:"why"`
		Action string   `json:"action"`
		Topics []string `json:"topics"`
	}
	if decode(r, &in) != nil {
		sharedProblem(w, sharedaccount.ErrInvalid)
		return
	}
	args := map[string]any{}
	switch in.Tool {
	case "cosift_topics":
		if in.Action != "list" && in.Action != "add" && in.Action != "remove" {
			sharedProblem(w, sharedaccount.ErrInvalid)
			return
		}
		if len(in.Topics) > 20 || in.Action != "list" && len(in.Topics) == 0 {
			sharedProblem(w, sharedaccount.ErrInvalid)
			return
		}
		for _, v := range in.Topics {
			if strings.TrimSpace(v) == "" || len(v) > 200 {
				sharedProblem(w, sharedaccount.ErrInvalid)
				return
			}
		}
		args["action"] = in.Action
		if in.Action != "list" {
			args["topics"] = in.Topics
		}
	case "cosift_lookup", "cosift_request":
		if strings.TrimSpace(in.Topic) == "" || len(in.Topic) > 200 || len(in.Why) > 280 {
			sharedProblem(w, sharedaccount.ErrInvalid)
			return
		}
		args["topic"] = in.Topic
		if in.Tool == "cosift_request" && in.Why != "" {
			args["why"] = in.Why
		}
	default:
		sharedProblem(w, sharedaccount.ErrInvalid)
		return
	}
	result, err := s.cfg.Shared.Call(r.Context(), sharedToken(r), in.Tool, args)
	if err != nil {
		sharedProblem(w, err)
		return
	}
	respond(w, 200, result)
}
