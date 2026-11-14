// Command a2a-hub runs the hub server. "a2a-hub healthcheck" asks a running hub for /healthz.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/orswn/a2a-hub/internal/auth"
	"github.com/orswn/a2a-hub/internal/config"
	"github.com/orswn/a2a-hub/internal/engine"
	"github.com/orswn/a2a-hub/internal/pushguard"
	"github.com/orswn/a2a-hub/internal/server"
	"github.com/orswn/a2a-hub/internal/store"
)

// version is set at build time with -ldflags "-X main.version=<version>".
var version = "0.0.0-dev"

func main() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	os.Exit(run(os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr, signals))
}

func run(args []string, env config.Env, stdout, stderr io.Writer, signals <-chan os.Signal) int {
	if len(args) > 0 && args[0] == "healthcheck" {
		return healthcheck(args[1:], env, stderr)
	}
	log := func(line string) {
		_, _ = fmt.Fprintf(stderr, "%s %s\n", time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), line)
	}
	cfg, err := config.Load(args, env)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		var configErr *config.Error
		if errors.As(err, &configErr) {
			return 2
		}
		return 1
	}
	if err := serve(cfg, log, stdout, signals); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func serve(cfg config.Config, log func(string), stdout io.Writer, signals <-chan os.Signal) error {
	if cfg.TokensFile == "" && !cfg.TestAgents {
		log("warning: no token file; only /healthz and cards answer")
	}
	tokens, err := auth.LoadTokens(cfg.TokensFile)
	if err != nil {
		return err
	}
	db, err := store.Open(context.Background(), cfg.Data)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	guard, err := pushguard.New(cfg.PushAllow, pushguard.DefaultResolver)
	if err != nil {
		return err
	}
	hubEngine, err := engine.New(context.Background(), db, engine.Options{PushGuard: guard, Log: log})
	if err != nil {
		return err
	}
	defer func() { _ = hubEngine.Close() }()

	network := "tcp"
	if addr, err := netip.ParseAddr(cfg.Host); err == nil && addr.Is4() {
		network = "tcp4"
	}
	listener, err := net.Listen(network, net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
	if err != nil {
		return err
	}
	hub := server.New(server.Options{Version: version, Tokens: tokens, Agents: hubEngine, Log: log})
	httpServer := &http.Server{Handler: hub, ReadHeaderTimeout: 30 * time.Second, IdleTimeout: 5 * time.Second}
	served := make(chan error, 1)
	go func() { served <- httpServer.Serve(listener) }()
	_, _ = fmt.Fprintf(stdout, "listening %d\n", listener.Addr().(*net.TCPAddr).Port)

	for {
		select {
		case err := <-served:
			return err
		case sig := <-signals:
			if sig == syscall.SIGHUP {
				reloadTokens(cfg, hub, log)
				continue
			}
			log(signalName(sig) + ": stopping")
			if err := httpServer.Close(); err != nil {
				return fmt.Errorf("stop failed: %w", err)
			}
			if err := <-served; !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("stop failed: %w", err)
			}
			return nil
		}
	}
}

func reloadTokens(cfg config.Config, hub *server.Server, log func(string)) {
	tokens, err := auth.LoadTokens(cfg.TokensFile)
	if err != nil {
		log("SIGHUP: token reload failed, old tokens kept: " + err.Error())
	} else {
		hub.SetTokens(tokens)
		log("SIGHUP: tokens reloaded")
	}
	// FIXME(okka): reload the external agent file here once the gateway exists.
}

func signalName(sig os.Signal) string {
	switch sig {
	case syscall.SIGTERM:
		return "SIGTERM"
	case syscall.SIGINT:
		return "SIGINT"
	}
	return sig.String()
}

// healthcheck asks the hub on this host for /healthz. It reads the listen address from the same flags and
// environment as the server. 0 means healthy.
func healthcheck(args []string, env config.Env, stderr io.Writer) int {
	cfg, err := config.Load(args, env)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	client := &http.Client{Timeout: 5 * time.Second}
	res, err := client.Get("http://" + net.JoinHostPort(healthcheckHost(cfg.Host), strconv.Itoa(cfg.Port)) + "/healthz")
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		_, _ = fmt.Fprintf(stderr, "healthz: %s\n", res.Status)
		return 1
	}
	return 0
}

// healthcheckHost maps a wildcard listen host to the loopback address of the same family.
func healthcheckHost(host string) string {
	switch host {
	case "", "0.0.0.0":
		return "127.0.0.1"
	case "::":
		return "::1"
	}
	return host
}
