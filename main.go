package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/labstack/echo/v4"
	"github.com/urfave/cli/v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Mirage struct {
	client *http.Client
	server *http.Server
	echo   *echo.Echo
	r      *redis.Client
	db     *MirageDb
	logger *slog.Logger
	ctx    context.Context
	wg     sync.WaitGroup
}

type MirageDb struct {
	c  *gorm.DB
	mu sync.Mutex
}

type MirageArgs struct {
	PostgresHost string
	PostgresPort string
	PostgresDb   string
	PostgresUser string
	PostgresPass string
	RedisHost    string
	LogLevel     string
}

var (
	redisPrefix = "mirage/"
	plcRoot     = "https://plc.directory"
)

func NewMirage(ctx context.Context, args *MirageArgs) (*Mirage, error) {
	ll := slog.LevelInfo
	switch args.LogLevel {
	case "debug":
		ll = slog.LevelDebug
	case "info":
		ll = slog.LevelInfo
	case "warn":
		ll = slog.LevelWarn
	case "error":
		ll = slog.LevelError
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: ll,
	}))

	db, err := gorm.Open(postgres.Open(fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable", args.PostgresPass, args.PostgresUser, args.PostgresPass, args.PostgresDb, args.PostgresPort)))
	if err != nil {
		return nil, err
	}

	db.AutoMigrate(&PlcEntry{})

	return &Mirage{
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		db: &MirageDb{
			c:  db,
			mu: sync.Mutex{},
		},
		r: redis.NewClient(&redis.Options{
			Addr: args.RedisHost,
		}),
		logger: logger,
		ctx:    ctx,
		wg:     sync.WaitGroup{},
	}, nil
}

func main() {
	app := cli.App{
		Name: "mirage",
		Commands: []*cli.Command{
			run,
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "postgres-host",
				Usage:    "Postgres host",
				Required: true,
				EnvVars:  []string{"POSTGRES_HOST"},
			},
			&cli.StringFlag{
				Name:     "postgres-port",
				Usage:    "Postgres port",
				Required: true,
				EnvVars:  []string{"POSTGRES_PORT"},
			},
			&cli.StringFlag{
				Name:     "postgres-db",
				Usage:    "Postgres db",
				Required: true,
				EnvVars:  []string{"POSTGRES_DB"},
			},
			&cli.StringFlag{
				Name:     "postgres-user",
				Usage:    "Postgres user",
				Required: true,
				EnvVars:  []string{"POSTGRES_USER"},
			},
			&cli.StringFlag{
				Name:     "postgres-pass",
				Usage:    "Postgres pass",
				Required: true,
				EnvVars:  []string{"POSTGRES_PASS"},
			},
			&cli.StringFlag{
				Name:     "redis-host",
				Usage:    "Redis host",
				Required: true,
				EnvVars:  []string{"REDIS_HOST"},
			},
			&cli.StringFlag{
				Name:     "log-level",
				Usage:    "Log level",
				Required: false,
				EnvVars:  []string{"LOG_LEVEL"},
			},
			&cli.StringFlag{
				Name:     "server-port",
				Usage:    "Server port",
				Required: false,
				EnvVars:  []string{"SERVER_PORT"},
			},
		},
	}

	ctx := context.Background()

	app.RunContext(ctx, []string{})
}

func createMirage(c *cli.Context) (*Mirage, error) {
	args := &MirageArgs{
		PostgresHost: c.String("postgres-host"),
		PostgresPort: c.String("postgres-port"),
		PostgresDb:   c.String("postgres-db"),
		PostgresUser: c.String("postgres-user"),
		PostgresPass: c.String("postgres-pass"),
		RedisHost:    c.String("redis-host"),
		LogLevel:     c.String("log-level"),
	}

	return NewMirage(c.Context, args)
}

var run = &cli.Command{
	Name:  "run",
	Usage: "Run the mirage server",
	Action: func(c *cli.Context) error {
		m, err := createMirage(c)
		if err != nil {
			return err
		}

		m.RunServer(c.String("server-port"))

		return nil
	},
}

func (m *Mirage) RunServer(port string) {
	m.echo = echo.New()
	m.echo.GET("/:did", m.handleGetLastOp)

	m.server = &http.Server{
		Addr:    ":" + port,
		Handler: m.echo,
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.server.ListenAndServe()
	}()

	<-m.ctx.Done()
	m.server.Shutdown(m.ctx)

	m.wg.Wait()
}

func (m *Mirage) GetLastOp(did string) {

}

func (m *Mirage) GetPlcAuditLog(did string) {

}

func (m *Mirage) GetPlcOpLog(did string) {

}

func (m *Mirage) ResolveDid(did string) {

}

func (m *Mirage) GetPlcData(did string) {

}

func (m *Mirage) runExporter() {
	after, err := m.r.Get(redisPrefix + "after").Result()
	if err != nil && err != redis.Nil {
		m.logger.Error("failed to get after", "err", err)
		return
	}

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			u, _ := url.Parse(plcRoot)
			u.Path = "/export"
			if after != "" {
				u.RawQuery = "after=" + after
			}

			req, err := http.NewRequestWithContext(m.ctx, "GET", u.String(), nil)
			if err != nil {
				m.logger.Error("failed to create request", "err", err)
				continue
			}

			resp, err := m.client.Do(req)
			if err != nil {
				m.logger.Error("failed to get export", "err", err)
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				m.logger.Error("export returned non-200 status", "status", resp.StatusCode)
				continue
			}

			var entries []PlcEntry
			err = json.NewDecoder(resp.Body).Decode(&entries)
			if err != nil {
				m.logger.Error("failed to decode export", "err", err)
				continue
			}

			// We'll get back the previous entry every time, so we'll skip it
			if after != "" {
				entries = entries[1:]
			}

			for i, entry := range entries {
				m.db.mu.Lock()
				m.db.c.Create(&entry)
				m.db.mu.Unlock()

				if i == len(entries)-1 {
					m.r.Set(redisPrefix+"after", entry.CreatedAt, 0)
					after = entry.CreatedAt
				}
			}
		}
	}
}
