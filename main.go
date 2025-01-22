package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
	_ "github.com/joho/godotenv/autoload"
	"github.com/labstack/echo/v4"
	"github.com/urfave/cli/v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
	DID_KEY_PREFIX          = "did:key:"
	BASE58_MULTIBASE_PREFIX = "z"

	P256_DID_PREFIX      = []byte{0x80, 0x24}
	P256_JWT_ALG         = "ES256"
	SECP256K1_DID_PREFIX = []byte{0xe7, 0x01}
	SECP256K1_JWT_ALG    = "ES256K"
)

var (
	redisPrefix = "mirage/"
	plcRoot     = "https://plc.directory"
	respContext = []string{
		"https://www.w3.org/ns/did/v1",
		"https://w3id.org/security/multikey/v1",
	}
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

	db, err := gorm.Open(postgres.Open(fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable", args.PostgresHost, args.PostgresUser, args.PostgresPass, args.PostgresDb, args.PostgresPort)))
	if err != nil {
		return nil, err
	}

	logger.Info("migrating...")
	db.AutoMigrate(&PlcEntry{})
	db.AutoMigrate(&DidHandle{})

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
			runResolve,
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

	app.RunAndExitOnError()
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

	m.logger.Info("starting web server")
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.server.ListenAndServe()
	}()

	m.logger.Info("starting exporter")
	m.runExporter()

	<-m.ctx.Done()
	m.server.Shutdown(m.ctx)

	m.wg.Wait()
}

var runResolve = &cli.Command{
	Name:  "resolve",
	Usage: "Resolve a did",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "did",
			Required: true,
		},
	},
	Action: func(c *cli.Context) error {
		m, err := createMirage(c)
		if err != nil {
			return err
		}

		did := c.String("did")

		res, err := m.ResolveDid(did)
		if err != nil {
			return err
		}

		b, err := json.MarshalIndent(res, "", "  ")
		if err != nil {
			return err
		}

		fmt.Println(string(b))

		return nil
	},
}

func (m *Mirage) ResolveDid(did string) (*ResolveDidResponse, error) {
	var entry PlcEntry
	if err := m.db.c.Raw("SELECT * FROM plc_entries WHERE did = ? ORDER BY created_at DESC LIMIT 1", did).Scan(&entry).Error; err != nil {
		return nil, fmt.Errorf("failed to resolve did: %w", err)
	}

	aka := []string{}
	if entry.Operation.PlcOperation != nil {
		aka = entry.Operation.PlcOperation.AlsoKnownAs
	} else if entry.Operation.LegacyPlcOperation != nil {
		aka = []string{entry.Operation.LegacyPlcOperation.Handle}
	}

	ctxt := respContext

	vm := []DocVerificationMethod{}
	if entry.Operation.PlcOperation != nil {
		for kid, key := range entry.Operation.PlcOperation.VerificationMethods {
			kac, err := formatKeyAndContext(key)
			if err != nil {
				return nil, fmt.Errorf("failed to format key and context: %w", err)
			}

			includes := false
			for _, c := range ctxt {
				if c == kac.Context {
					includes = true
					break
				}
			}

			if !includes {
				ctxt = append(ctxt, kac.Context)
			}

			vm = append(vm, DocVerificationMethod{
				Id:                 fmt.Sprintf("%s#%s", entry.Did, kid),
				Type:               kac.Type,
				Controller:         entry.Did,
				PublicKeyMultibase: kac.PublicKeyMultibase,
			})
		}
	}

	svcs := []DocService{}
	for id, svc := range entry.Operation.PlcOperation.Services {
		svcs = append(svcs, DocService{
			Id:              id,
			Type:            svc.Type,
			ServiceEndpoint: svc.Endpoint,
		})
	}

	return &ResolveDidResponse{
		Context:            ctxt,
		Id:                 entry.Did,
		AlsoKnownAs:        aka,
		VerificationMethod: vm,
		Service:            svcs,
	}, nil
}

func (m *Mirage) GetLastOp(did string) {

}

func (m *Mirage) GetPlcAuditLog(did string) {

}

func (m *Mirage) GetPlcOpLog(did string) {

}

func (m *Mirage) GetPlcData(did string) {

}

func (m *Mirage) runExporter() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		after, err := m.r.Get(redisPrefix + "after").Result()
		if err != nil && err != redis.Nil {
			m.logger.Error("failed to get after", "err", err)
			return
		}

		for {
			select {
			case <-m.ctx.Done():
				return
			default:
				time.Sleep(1 * time.Second)

				ustr := plcRoot + "/export?limit=1000"

				if after != "" {
					ustr += "&after=" + after
				}

				req, err := http.NewRequestWithContext(m.ctx, "GET", ustr, nil)
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

				b, err := io.ReadAll(resp.Body)
				if err != nil {
					m.logger.Error("failed to read export", "err", err)
					continue
				}

				pts := strings.Split(string(b), "\n")

				if after != "" {
					pts = pts[1:]
				}

				for i, pt := range pts {
					func() {
						if pt == "" {
							return
						}

						var entry PlcEntry
						err = json.Unmarshal([]byte(pt), &entry)
						if err != nil {
							m.logger.Error("failed to unmarshal export", "err", err)
							return
						}

						if i == len(pts)-1 {
							m.r.Set(redisPrefix+"after", entry.CreatedAt, 0)
							after = entry.CreatedAt
						}

						m.db.mu.Lock()
						defer m.db.mu.Unlock()

						if err := m.db.c.Create(&entry).Error; err != nil {
							m.logger.Error("failed to create entry", "err", err)
							return
						}

						if entry.Operation.PlcTombstone != nil {
							if err := m.db.c.Exec("DELETE FROM did_handles WHERE did = ?", entry.Did).Error; err != nil {
								m.logger.Error("failed to delete did handles", "err", err)
								return
							}
						} else {
							handle := ""
							if entry.Operation.PlcOperation != nil {
								handle = entry.Operation.PlcOperation.AlsoKnownAs[0]
							} else if entry.Operation.LegacyPlcOperation != nil {
								handle = entry.Operation.LegacyPlcOperation.Handle
							}
							handle = strings.TrimPrefix(handle, "at://")

							t, err := time.Parse(time.RFC3339Nano, entry.CreatedAt)
							if err != nil {
								m.logger.Error("failed to parse created at", "err", err)
								return
							}

							if err := m.db.c.Clauses(clause.OnConflict{
								Columns:   []clause.Column{{Name: "did"}},
								DoUpdates: clause.AssignmentColumns([]string{"handle", "updated_at"}),
							}).Create(&DidHandle{
								Did:       entry.Did,
								Handle:    handle,
								UpdatedAt: t,
							}).Error; err != nil {
								m.logger.Error("failed to create did handle", "err", err)
								return
							}
						}
					}()
				}
			}
		}
	}()
}
