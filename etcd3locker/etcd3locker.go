// Requires etcd 3.2+
package etcd3locker

import (
	"context"
	"sync"
	"time"

	etcd3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/tus/tusd"
)

type Etcd3Locker struct {
	// etcd3 client session
	Session *concurrency.Session

	// locks is used for storing etcd3 concurrency.Mutexes before they are
	// unlocked. If you want to release a lock, you need the same locker
	// instance and therefore we need to save them temporarily.
	locks map[string]*concurrency.Mutex
	mutex sync.Mutex
}

// New constructs a new locker using the provided client.
func New(client *etcd3.Client) (*Etcd3Locker, error) {
	session, err := concurrency.NewSession(client)

	if err != nil {
		return nil, err
	}

	locksMap := map[string]*concurrency.Mutex{}
	return &Etcd3Locker{Session: session, locks: locksMap, mutex: sync.Mutex{}}, nil
}

// UseIn adds this locker to the passed composer.
func (locker *Etcd3Locker) UseIn(composer *tusd.StoreComposer) {
	composer.UseLocker(locker)
}

// LockUpload tries to obtain the exclusive lock.
func (locker *Etcd3Locker) LockUpload(id string) error {
	lock := concurrency.NewMutex(locker.Session, "/tusd/"+id)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// this is a blocking call; if we receive DeadlineExceeded
	// the lock is most likely already taken
	if err := lock.Lock(ctx); err != nil {
		if err == context.DeadlineExceeded {
			return tusd.ErrFileLocked
		} else {
			return err
		}
	}

	locker.mutex.Lock()
	defer locker.mutex.Unlock()
	// Only add the lock to our list if the acquire was successful and no error appeared.
	locker.locks[id] = lock

	return nil
}

// UnlockUpload releases a lock. If no such lock exists, no error will be returned.
func (locker *Etcd3Locker) UnlockUpload(id string) error {
	locker.mutex.Lock()
	defer locker.mutex.Unlock()

	// Complain if no lock has been found. This can only happen if LockUpload
	// has not been invoked before or UnlockUpload multiple times.
	lock, ok := locker.locks[id]
	if !ok {
		return nil
	}

	defer delete(locker.locks, id)

	return lock.Unlock(context.Background())
}
