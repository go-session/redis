package redis

import (
	"context"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/json-iterator/go"
	"gopkg.in/session.v2"
)

var (
	_             session.ManagerStore = &managerStore{}
	_             session.Store        = &store{}
	jsonMarshal                        = jsoniter.Marshal
	jsonUnmarshal                      = jsoniter.Unmarshal
)

// NewRedisStore create an instance of a redis store
func NewRedisStore(opts *Options) session.ManagerStore {
	if opts == nil {
		panic("options cannot be nil")
	}
	return NewRedisStoreWithCli(redis.NewClient(opts.redisOptions()))
}

// NewRedisStoreWithCli create an instance of a redis store
func NewRedisStoreWithCli(cli *redis.Client) session.ManagerStore {
	return &managerStore{
		cli: cli,
		pool: sync.Pool{
			New: func() interface{} {
				return newStore(cli)
			},
		},
	}
}

// NewRedisClusterStore create an instance of a redis cluster store
func NewRedisClusterStore(opts *ClusterOptions) session.ManagerStore {
	if opts == nil {
		panic("options cannot be nil")
	}
	return NewRedisClusterStoreWithCli(redis.NewClusterClient(opts.redisClusterOptions()))
}

// NewRedisClusterStoreWithCli create an instance of a redis cluster store
func NewRedisClusterStoreWithCli(cli *redis.ClusterClient) session.ManagerStore {
	return &managerStore{
		cli: cli,
		pool: sync.Pool{
			New: func() interface{} {
				return newStore(cli)
			},
		},
	}
}

type clienter interface {
	Get(key string) *redis.StringCmd
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Expire(key string, expiration time.Duration) *redis.BoolCmd
	Exists(keys ...string) *redis.IntCmd
	TxPipeline() redis.Pipeliner
	Del(keys ...string) *redis.IntCmd
	Close() error
}

type managerStore struct {
	cli  clienter
	pool sync.Pool
}

func (s *managerStore) getValue(sid string) (string, error) {
	cmd := s.cli.Get(sid)
	if err := cmd.Err(); err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", err
	}

	return cmd.Val(), nil
}

func (s *managerStore) parseValue(value string) (map[string]interface{}, error) {
	var values map[string]interface{}
	if len(value) > 0 {
		err := jsonUnmarshal([]byte(value), &values)
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

func (s *managerStore) Create(ctx context.Context, sid string, expired int64) (session.Store, error) {
	store := s.pool.Get().(*store)
	store.reset(ctx, sid, expired, nil)
	return store, nil
}

func (s *managerStore) Update(ctx context.Context, sid string, expired int64) (session.Store, error) {
	store := s.pool.Get().(*store)

	value, err := s.getValue(sid)
	if err != nil {
		return nil, err
	} else if value == "" {
		store.reset(ctx, sid, expired, nil)
		return store, nil
	}

	cmd := s.cli.Expire(sid, time.Duration(expired)*time.Second)
	if err = cmd.Err(); err != nil {
		return nil, err
	}

	values, err := s.parseValue(value)
	if err != nil {
		return nil, err
	}
	store.reset(ctx, sid, expired, values)

	return store, nil
}

func (s *managerStore) Delete(_ context.Context, sid string) error {
	if ok, err := s.Check(nil, sid); err != nil {
		return err
	} else if !ok {
		return nil
	}

	cmd := s.cli.Del(sid)
	return cmd.Err()
}

func (s *managerStore) Check(_ context.Context, sid string) (bool, error) {
	cmd := s.cli.Exists(sid)
	if err := cmd.Err(); err != nil {
		return false, err
	}
	return cmd.Val() > 0, nil
}

func (s *managerStore) Refresh(ctx context.Context, oldsid, sid string, expired int64) (session.Store, error) {
	store := s.pool.Get().(*store)

	value, err := s.getValue(oldsid)
	if err != nil {
		return nil, err
	} else if value == "" {
		store.reset(ctx, sid, expired, nil)
		return store, nil
	}

	pipe := s.cli.TxPipeline()
	pipe.Set(sid, value, time.Duration(expired)*time.Second)
	pipe.Del(oldsid)
	_, err = pipe.Exec()
	if err != nil {
		return nil, err
	}

	values, err := s.parseValue(value)
	if err != nil {
		return nil, err
	}
	store.reset(ctx, sid, expired, values)

	return store, nil
}

func (s *managerStore) Close() error {
	return s.cli.Close()
}

func newStore(cli clienter) *store {
	return &store{
		cli: cli,
	}
}

type store struct {
	sync.RWMutex
	ctx     context.Context
	sid     string
	expired int64
	values  map[string]interface{}
	cli     clienter
}

func (s *store) reset(ctx context.Context, sid string, expired int64, values map[string]interface{}) {
	if values == nil {
		values = make(map[string]interface{})
	}
	s.ctx = ctx
	s.sid = sid
	s.expired = expired
	s.values = values
}

func (s *store) Context() context.Context {
	return s.ctx
}

func (s *store) SessionID() string {
	return s.sid
}

func (s *store) Set(key string, value interface{}) {
	s.Lock()
	s.values[key] = value
	s.Unlock()
}

func (s *store) Get(key string) (interface{}, bool) {
	s.RLock()
	defer s.RUnlock()
	val, ok := s.values[key]
	return val, ok
}

func (s *store) Delete(key string) interface{} {
	s.RLock()
	v, ok := s.values[key]
	s.RUnlock()
	if ok {
		s.Lock()
		delete(s.values, key)
		s.Unlock()
	}
	return v
}

func (s *store) Flush() error {
	s.Lock()
	s.values = make(map[string]interface{})
	s.Unlock()
	return s.Save()
}

func (s *store) Save() error {
	var value string

	s.RLock()
	if len(s.values) > 0 {
		buf, err := jsonMarshal(s.values)
		if err != nil {
			s.RUnlock()
			return err
		}
		value = string(buf)
	}
	s.RUnlock()

	cmd := s.cli.Set(s.sid, value, time.Duration(s.expired)*time.Second)
	return cmd.Err()
}
