package mirage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/go-redis/redis"
	_ "github.com/joho/godotenv/autoload"
	"github.com/labstack/echo/v4"
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

type MirageServerArgs struct {
	ServerPort string
}

var (
	didKeyPrefix          = "did:key:"
	base58MultibasePrefix = "z"
	p256DidPrefix         = []byte{0x80, 0x24}
	p256JwtAlg            = "ES256"
	SECP256K1DidPrefix    = []byte{0xe7, 0x01}
	SECP256K1JwtAlg       = "ES256K"

	redisPrefix     = "mirage/"
	didHandlePrefix = "did_handle/"
	handleDidPrefix = "handle_did/"

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
			Timeout: 2 * time.Second,
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

func (m *Mirage) RunServer(args *MirageServerArgs) {
	m.echo = echo.New()
	m.echo.GET("/handle/:did", m.handleGetHandleFromDid)
	m.echo.GET("/did/:handle", m.handleGetDidFromHandle)

	dorhMw := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(e echo.Context) error {
			didOrHandle := e.Param("didOrHandle")
			did, found, err := m.getDidFromDidOrHandle(didOrHandle)
			if err != nil {
				return e.JSON(500, map[string]string{"error": err.Error()})
			}

			if !found {
				return e.JSON(404, map[string]string{"error": "did not found"})
			}

			e.SetParamValues(*did)

			if err := next(e); err != nil {
				e.Error(err)
			}

			return nil
		}
	}

	m.echo.GET("/service/:didOrHandle", m.handleGetService, dorhMw)
	m.echo.GET("/created/:didOrHandle", m.handleGetCreatedAt, dorhMw)
	m.echo.GET("/:didOrHandle", m.handleResolveDid, dorhMw)
	m.echo.GET("/:didOrHandle/log", m.handleGetPlcOpLog, dorhMw)
	m.echo.GET("/:didOrHandle/log/audit", m.handleGetAuditLog, dorhMw)
	m.echo.GET("/:didOrHandle/log/last", m.handleGetLastOp, dorhMw)
	m.echo.GET("/:didOrHandle/data", m.handleGetPlcData, dorhMw)
	m.echo.GET("/export", m.handleExport, dorhMw)

	m.server = &http.Server{
		Addr:    ":" + args.ServerPort,
		Handler: m.echo,
	}

	m.logger.Info("starting web server")
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.server.ListenAndServe()
	}()

	m.logger.Info("starting exporter")
	m.runExporter(args)

	<-m.ctx.Done()

	m.logger.Info("shutting down http server")
	m.server.Shutdown(m.ctx)

	m.wg.Wait()
}

func (m *Mirage) ResolveHandle(handle string) (*string, error) {
	res, err := net.LookupTXT("_atproto." + handle)
	if err == nil {
		for _, r := range res {
			if strings.HasPrefix(r, "did=") {
				return &r, nil
			}
		}
	}

	req, err := http.NewRequest("GET", "https://"+handle+"/.well-known/atproto-did", nil)
	if err != nil {
		return nil, err
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("non-200 status code")
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	mbDid := string(b)
	if _, err := syntax.ParseDID(mbDid); err != nil {
		return nil, err
	}

	return &mbDid, nil

}

func (m *Mirage) getDidFromDidOrHandle(didOrHandle string) (*string, bool, error) {
	if _, err := syntax.ParseDID(didOrHandle); err == nil {
		return &didOrHandle, true, nil
	}

	handle, found, err := m.GetDidFromHandle(didOrHandle)
	if err != nil {
		return nil, false, err
	}

	if !found {
		return nil, false, fmt.Errorf("did or handle not found")
	}

	return handle, true, nil
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
			Id:              "#" + id,
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

func (m *Mirage) GetPlcOpLog(did string) ([]PlcEntry, error) {
	var entries []PlcEntry
	if err := m.db.c.Raw("SELECT * FROM plc_entries WHERE did = ? ORDER BY created_at ASC", did).Scan(&entries).Error; err != nil {
		return nil, err
	}

	return entries, nil
}

func (m *Mirage) GetLastOp(did string) (*PlcEntry, error) {
	var entry PlcEntry
	if err := m.db.c.Raw("SELECT * FROM plc_entries WHERE did = ? ORDER BY created_at DESC LIMIT 1", did).Scan(&entry).Error; err != nil {
		return nil, err
	}

	return &entry, nil
}

func (m *Mirage) GetPlcData(did string) (*DataResponse, error) {
	op, err := m.GetLastOp(did)
	if err != nil {
		return nil, err
	}

	if op.Operation.PlcTombstone != nil {
		return nil, nil
	}

	if op.Operation.PlcOperation != nil {
		op := op.Operation.PlcOperation
		return &DataResponse{
			Did:                 did,
			VerificationMethods: op.VerificationMethods,
			RotationKeys:        op.RotationKeys,
			AlsoKnownAs:         op.AlsoKnownAs,
			Services:            op.Services,
		}, nil
	}

	// TODO figure out legacy ops later lol

	return nil, nil
}

func (m *Mirage) GetHandleFromDid(did string) (*string, bool, error) {
	cached, err := m.r.Get(redisPrefix + didHandlePrefix + did).Result()
	if err == nil {
		return &cached, true, nil
	} else if err != redis.Nil {
		return nil, false, err
	}

	var dh DidHandle
	if err := m.db.c.Raw("SELECT * FROM did_handles WHERE did = ?", did).Scan(&dh).Error; err != nil {
		return nil, false, err
	}

	if dh.Handle == "" {
		return nil, false, nil
	}

	m.r.Set(redisPrefix+didHandlePrefix+did, dh.Handle, 0)

	return &dh.Handle, true, nil
}

func (m *Mirage) GetService(did string) (*string, bool, error) {
	op, err := m.GetLastOp(did)
	if err != nil {
		return nil, false, err
	}

	if op == nil || op.Operation.PlcTombstone != nil {
		return nil, false, nil
	}

	if op.Operation.PlcOperation != nil {
		pds, found := op.Operation.PlcOperation.Services["atproto_pds"]
		if !found {
			return nil, false, nil
		}

		return &pds.Endpoint, true, nil
	} else if op.Operation.LegacyPlcOperation != nil {
		return &op.Operation.LegacyPlcOperation.Service, true, nil
	}

	return nil, false, nil
}

func (m *Mirage) GetDidFromHandle(handle string) (*string, bool, error) {
	cached, err := m.r.Get(redisPrefix + handleDidPrefix + handle).Result()
	if err == nil {
		return &cached, true, nil
	} else {
		return nil, false, errors.New("handle not found in cache. it may exist, but we are not tracking it")
	}
}

func (m *Mirage) GetCreatedAt(did string) (*string, bool, error) {
	var entries []PlcEntry
	if err := m.db.c.Raw("SELECT * FROM plc_entries WHERE did = ? ORDER BY created_at ASC LIMIT 1", did).Scan(&entries).Error; err != nil {
		return nil, false, err
	}

	if len(entries) == 0 {
		return nil, false, nil
	}

	return &entries[0].CreatedAt, true, nil
}

func (m *Mirage) runExporter(_ *MirageServerArgs) {
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
				m.logger.Info("exporting", "cursor", after)

				ustr := plcRoot + "/export?limit=1000"
				waitMs := 1000

				if after != "" {
					ustr += "&after=" + after

					t, _ := time.Parse(time.RFC3339Nano, after)
					if time.Since(t) > 1*time.Hour {
						waitMs = 600
					}
				}

				time.Sleep(time.Duration(waitMs) * time.Millisecond)

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

						if _, err := m.r.Get(redisPrefix + didHandlePrefix + entry.Did).Result(); err != redis.Nil {
							return
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
								if len(entry.Operation.PlcOperation.AlsoKnownAs) == 0 {
									m.logger.Info("encountered operation with no aka", "did", entry.Did)
									return
								}
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

							m.r.Set(redisPrefix+didHandlePrefix+entry.Did, handle, 0)

							curr, err := m.r.Get(redisPrefix + handleDidPrefix + handle).Result()
							if err == redis.Nil {
								m.r.Set(redisPrefix+handleDidPrefix+handle, entry.Did, 0)
							} else if err != nil {
								m.logger.Error("failed to get handle did", "err", err)
								return
							} else if curr != entry.Did {
								res, err := m.ResolveHandle(handle)
								if err != nil {
									m.logger.Error("failed to resolve handle", "err", err)
									return
								}

								if *res != entry.Did {
									m.logger.Error("handle did mismatch", "handle", handle, "did", entry.Did, "resolved", *res)
									return
								}
							}
						}
					}()
				}
			}
		}
	}()
}

func (m *Mirage) FillRedis(skip int) error {
	handleUsed := map[string]string{}

	m.logger.Info("fetching rows...")

	var dhs []DidHandle
	if err := m.db.c.Raw("SELECT * FROM did_handles").Scan(&dhs).Error; err != nil {
		return err
	}

	dhs = dhs[skip:]

	println("\n")
	for i, dh := range dhs {
		fmt.Printf("\r filling %d/%d", i, len(dhs))

		did, found := handleUsed[dh.Handle]
		if found && did != dh.Did {

			println("trying to verify dupe handle")
			did, err := m.ResolveHandle(dh.Handle)
			if err != nil {
				fmt.Printf("\nfailed to resolve handle: %v", err)
				println("\nfailed to resolve handle", dh.Handle)
				continue
			}

			if did == nil {
				println("\nfailed to resolve handle", dh.Handle)
				continue
			}

			if *did != dh.Did {
				println("\nhandle did mismatch", dh.Handle, dh.Did, *did)
				continue
			}

			println("verified dupe handle")
		}

		m.r.Set(redisPrefix+didHandlePrefix+dh.Did, dh.Handle, 0)
		m.r.Set(redisPrefix+handleDidPrefix+dh.Handle, dh.Did, 0)
		handleUsed[dh.Handle] = dh.Did
	}

	m.logger.Info("finished filling redis")

	return nil
}

func (m *Mirage) RunExporter(_ *MirageServerArgs) {
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
				m.logger.Info("exporting", "cursor", after)

				ustr := plcRoot + "/export?limit=1000"
				waitMs := 3000

				if after != "" {
					ustr += "&after=" + after

					t, _ := time.Parse(time.RFC3339Nano, after)
					if time.Since(t) > 1*time.Hour {
						waitMs = 600
					}
				}

				time.Sleep(time.Duration(waitMs) * time.Millisecond)

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

						if _, err := m.r.Get(redisPrefix + didHandlePrefix + entry.Did).Result(); err != redis.Nil {
							return
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
								if len(entry.Operation.PlcOperation.AlsoKnownAs) == 0 {
									m.logger.Info("encountered operation with no aka", "did", entry.Did)
									return
								}
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

							m.r.Set(redisPrefix+didHandlePrefix+entry.Did, handle, 0)

							curr, err := m.r.Get(redisPrefix + handleDidPrefix + handle).Result()
							if err == redis.Nil {
								m.r.Set(redisPrefix+handleDidPrefix+handle, entry.Did, 0)
							} else if err != nil {
								m.logger.Error("failed to get handle did", "err", err)
								return
							} else if curr != entry.Did {
								res, err := m.ResolveHandle(handle)
								if err != nil {
									m.logger.Error("failed to resolve handle", "err", err)
									return
								}

								if *res != entry.Did {
									m.logger.Error("handle did mismatch", "handle", handle, "did", entry.Did, "resolved", *res)
									return
								}
							}
						}
					}()
				}
			}
		}
	}()
}

func (m *Mirage) GetUpdatedInWindow(dur time.Duration) ([]DidHandle, error) {
	since := time.Now().Add(-dur)

	var dhs []DidHandle
	if err := m.db.c.Raw("SELECT * FROM did_handles WHERE updated_at >= ?", since).Scan(&dhs).Error; err != nil {
		return nil, err
	}

	return dhs, nil
}
