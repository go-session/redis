package redis

import (
	"testing"
	"time"
)

const (
	addr = "127.0.0.1:6379"
	db   = 15
)

func TestStore(t *testing.T) {
	mstore := NewRedisStore(&Options{
		Addr: addr,
		DB:   db,
	})
	defer mstore.Close()

	store, err := mstore.Create(nil, "store", 2)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if store.SessionID() != "store" {
		t.Error("Wrong value obtained")
		return
	}

	store.Set("foo", "bar")
	store.Set("user", "bar")
	err = store.Save()
	if err != nil {
		t.Error(err.Error())
		return
	}

	foo, ok := store.Get("foo")
	if !ok || foo != "bar" {
		t.Error("Wrong value obtained")
		return
	}

	foo = store.Delete("foo")
	if foo != "bar" {
		t.Error("Wrong value obtained")
		return
	}

	err = store.Save()
	if err != nil {
		t.Error(err.Error())
		return
	}

	_, ok = store.Get("foo")
	if ok {
		t.Error("Expected value is false")
		return
	}

	user, ok := store.Get("user")
	if !ok || user != "bar" {
		t.Error("Wrong value obtained")
		return
	}

	err = store.Flush()
	if err != nil {
		t.Error(err.Error())
		return
	}

	_, ok = store.Get("user")
	if ok {
		t.Error("Expected value is false")
		return
	}
}

func TestManagerStore(t *testing.T) {
	mstore := NewRedisStore(&Options{
		Addr: addr,
		DB:   db,
	})
	defer mstore.Close()

	sid := "manager"
	store, err := mstore.Create(nil, sid, 2)
	if err != nil {
		t.Error(err.Error())
		return
	}

	store.Set("foo", "bar")
	err = store.Save()
	if err != nil {
		t.Error(err.Error())
		return
	}

	store, err = mstore.Update(nil, sid, 2)
	if err != nil {
		t.Error(err.Error())
		return
	}

	foo, ok := store.Get("foo")
	if !ok || foo != "bar" {
		t.Error("Wrong value obtained")
		return
	}

	err = mstore.Delete(nil, sid)
	if err != nil {
		t.Error(err.Error())
		return
	}

	exists, err := mstore.Check(nil, sid)
	if err != nil {
		t.Error(err.Error())
		return
	} else if exists {
		t.Error("Expected value is false")
	}
}

func TestStoreWithExpired(t *testing.T) {
	mstore := NewRedisStore(&Options{
		Addr: addr,
		DB:   db,
	})
	defer mstore.Close()

	sid := "test_store_expired"
	store, err := mstore.Create(nil, sid, 1)
	if err != nil {
		t.Error(err.Error())
		return
	}

	store.Set("foo", "bar")
	err = store.Save()
	if err != nil {
		t.Error(err.Error())
		return
	}

	foo, ok := store.Get("foo")
	if !ok || foo != "bar" {
		t.Error("Wrong value obtained")
		return
	}

	time.Sleep(time.Second * 2)

	exists, err := mstore.Check(nil, sid)
	if err != nil {
		t.Error(err.Error())
		return
	} else if exists {
		t.Error("Expected value is false")
	}
}
