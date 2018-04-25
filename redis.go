package redis

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"gopkg.in/session.v2"
)

var (
	_ session.ManagerStore = &managerStore{}
	_ session.Store        = &store{}
)

// NewRedisStore Create an instance of a redis store
func NewRedisStore(opt *Options) session.ManagerStore {
	if opt == nil {
		panic("Option cannot be nil")
	}
	return &managerStore{cli: redis.NewClient(opt.redisOptions())}
}

type managerStore struct {
	cli *redis.Client
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

func (s *managerStore) parseValue(value string) (map[string]string, error) {
	var values map[string]string

	if len(value) > 0 {
		err := json.Unmarshal([]byte(value), &values)
		if err != nil {
			return nil, err
		}
	}

	if values == nil {
		values = make(map[string]string)
	}
	return values, nil
}

func (s *managerStore) Create(ctx context.Context, sid string, expired int64) (session.Store, error) {
	values := make(map[string]string)
	return &store{ctx: ctx, sid: sid, cli: s.cli, expired: expired, values: values}, nil
}

func (s *managerStore) Update(ctx context.Context, sid string, expired int64) (session.Store, error) {
	value, err := s.getValue(sid)
	if err != nil {
		return nil, err
	} else if value == "" {
		return s.Create(ctx, sid, expired)
	}

	cmd := s.cli.Set(sid, value, time.Duration(expired)*time.Second)
	if err = cmd.Err(); err != nil {
		return nil, err
	}

	values, err := s.parseValue(value)
	if err != nil {
		return nil, err
	}

	return &store{ctx: ctx, sid: sid, cli: s.cli, expired: expired, values: values}, nil
}

func (s *managerStore) Delete(_ context.Context, sid string) error {
	cmd := s.cli.Del(sid)
	return cmd.Err()
}

func (s *managerStore) Check(_ context.Context, sid string) (bool, error) {
	cmd := s.cli.Get(sid)
	if err := cmd.Err(); err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *managerStore) Close() error {
	return s.cli.Close()
}

type store struct {
	sid     string
	cli     *redis.Client
	expired int64
	values  map[string]string
	sync.RWMutex
	ctx context.Context
}

func (s *store) Context() context.Context {
	return s.ctx
}

func (s *store) SessionID() string {
	return s.sid
}

func (s *store) Set(key, value string) {
	s.Lock()
	s.values[key] = value
	s.Unlock()
}

func (s *store) Get(key string) (string, bool) {
	s.RLock()
	defer s.RUnlock()
	val, ok := s.values[key]
	return val, ok
}

func (s *store) Delete(key string) string {
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
	s.values = make(map[string]string)
	s.Unlock()
	return s.Save()
}

func (s *store) Save() error {
	var value string

	s.RLock()
	if len(s.values) > 0 {
		buf, _ := json.Marshal(s.values)
		value = string(buf)
	}
	s.RUnlock()

	cmd := s.cli.Set(s.sid, value, time.Duration(s.expired)*time.Second)
	return cmd.Err()
}
