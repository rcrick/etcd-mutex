package elock

import (
	"context"
	"errors"
	"fmt"
	"log"

	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
)

type Mutex struct {
	client  *v3.Client
	pfx     string
	myKey   string
	myRev   int64
	ttl     int64
	leaseId v3.LeaseID
	// Hdr   *pb.ResponseHeader
}

// // var client *v3.Client
// func Init() {
// 	client, err := v3.New(v3.Config{Endpoints: []string{"127.0.0.1:2379"}})
// 	if err != nil {
// 		return
// 	}
// }

type Err string

func (e Err) Error() string {
	return string(e)
}
func NewMutex(client *v3.Client, pfx string, ttl int64) *Mutex {
	return &Mutex{
		client: client,
		pfx:    pfx + "/",
		myKey:  "",
		myRev:  -1,
		ttl:    ttl,
	}
}

func (m *Mutex) AcquireOnce(ctx context.Context) error {
	// 3f357a41d952f076
	resp, err := m.client.Grant(ctx, int64(m.ttl))
	if err != nil {
		return err
	}
	m.leaseId = resp.ID
	m.myKey = fmt.Sprintf("%s%x", m.pfx, m.leaseId)

	// 比较当前m.myKey的CreateRevision是否为0，0代表目前不存在该key，执行put操作
	// 非0表示key已经被创建，需要执行get操作
	cmp := v3.Compare(v3.CreateRevision(m.myKey), "=", 0)
	// put the kv to etcd, and attach the kv with the lease
	put := v3.OpPut(m.myKey, "", v3.WithLease(m.leaseId))
	// 获取key是否已设置成锁
	get := v3.OpGet(m.myKey)
	// 获取当前锁真正的持有者
	getOwner := v3.OpGet(m.pfx, v3.WithFirstCreate()...)
	// cmp条件成立，则执行then，否则执行else
	txnResp, err := m.client.Txn(ctx).If(cmp).Then(put /*resp.Responses[0]*/, getOwner /*resp.Responses[1]*/).Else(get, getOwner).Commit()

	if err != nil {
		return err
	}
	m.myRev = txnResp.Header.Revision
	if txnResp.Succeeded != true {
		m.myRev = txnResp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
	}
	//
	ownerKey := txnResp.Responses[1].GetResponseRange().Kvs
	log.Printf("key: %s id: %d acquire once", m.myKey, m.myRev)
	if len(ownerKey) == 0 || ownerKey[0].CreateRevision == m.myRev {
		return nil
	}
	return Err("try to lock failed")

}

func (m *Mutex) Lock(ctx context.Context) error {
	err := m.AcquireOnce(ctx)
	if err != nil && !errors.Is(err, Err("try to lock failed")) {
		return err
	}
	err = m.waitRelease(ctx, m.client, m.pfx, m.myRev-1)

	if err != nil {
		return err
	}
	log.Printf("key: %s id: %d lock  success", m.myKey, m.myRev)
	return nil
}

func (m *Mutex) waitRelease(ctx context.Context, client *v3.Client, prefix string, rev int64) error {
	getOpts := append(v3.WithLastCreate(), v3.WithMaxCreateRev(rev))
	for {
		resp, err := m.client.Get(ctx, m.pfx, v3.WithLastCreate()...)
		resp, err = m.client.Get(ctx, m.pfx, getOpts...)
		if err != nil {
			return err
		}
		if len(resp.Kvs) == 0 {
			return nil
		}
		lastKey := string(resp.Kvs[0].Key)
		// lastKey is the key's rev less than current rev, resp.Header.Revision means in the newest snapshot to query
		if err = waitDelete(ctx, m.client, lastKey, resp.Header.Revision); err != nil {
			return err
		}
	}
}

func waitDelete(ctx context.Context, client *v3.Client, key string, rev int64) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wr v3.WatchResponse
	wch := client.Watch(cctx, key, v3.WithRev(rev))
	for wr = range wch {
		for _, ev := range wr.Events {
			if ev.Type == mvccpb.DELETE {
				log.Printf("wait key: %s on rev: %d deleted", key, rev)
				return nil
			}
		}
	}
	if err := wr.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("lost watcher waiting for delete")
}

func (m *Mutex) Unlock(ctx context.Context) error {
	_, err := m.client.Revoke(ctx, m.leaseId)
	if err != nil {
		return err
	}
	_, err = m.client.Delete(ctx, m.myKey)
	if err != nil {
		return err
	}
	return nil
}
