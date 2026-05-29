package coordinator

import (
	"context"
	"fmt"
	"strconv"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type ShardAssignment struct {
	ShardID int
	Start   int
	End     int
	mutex   *concurrency.Mutex
}

type Coordinator struct {
	client  *clientv3.Client
	session *concurrency.Session
	prefix  string
}

func NewCoordinator(endpoints []string, prefix string) (*Coordinator, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	session, err := concurrency.NewSession(client, concurrency.WithTTL(10))
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create session: %w", err)
	}

	return &Coordinator{
		client:  client,
		session: session,
		prefix:  prefix,
	}, nil
}

func (c *Coordinator) AcquireShard(ctx context.Context, totalMeters, numShards int) (*ShardAssignment, error) {
	metersPerShard := totalMeters / numShards
	if totalMeters%numShards != 0 {
		metersPerShard++
	}

	for shardID := 0; shardID < numShards; shardID++ {
		lockKey := fmt.Sprintf("%s/shard/%d", c.prefix, shardID)
		mutex := concurrency.NewMutex(c.session, lockKey)

		if err := mutex.TryLock(ctx); err != nil {
			continue
		}

		start := shardID * metersPerShard
		end := start + metersPerShard
		if end > totalMeters {
			end = totalMeters
		}

		if start >= totalMeters {
			mutex.Unlock(ctx)
			continue
		}

		return &ShardAssignment{
			ShardID: shardID,
			Start:   start,
			End:     end,
			mutex:   mutex,
		}, nil
	}

	return nil, fmt.Errorf("no available shards")
}

func (sa *ShardAssignment) Release(ctx context.Context) error {
	if sa.mutex != nil {
		return sa.mutex.Unlock(ctx)
	}
	return nil
}

func (c *Coordinator) RegisterCollector(ctx context.Context, collectorID string, shardID int) error {
	key := fmt.Sprintf("%s/collectors/%s", c.prefix, collectorID)
	lease, err := c.client.Grant(ctx, 10)
	if err != nil {
		return fmt.Errorf("failed to create lease: %w", err)
	}

	_, err = c.client.Put(ctx, key, strconv.Itoa(shardID), clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("failed to register collector: %w", err)
	}

	ch, err := c.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return fmt.Errorf("failed to keep alive: %w", err)
	}

	go func() {
		for range ch {
		}
	}()

	return nil
}

func (c *Coordinator) GetActiveCollectors(ctx context.Context) (map[string]int, error) {
	key := fmt.Sprintf("%s/collectors/", c.prefix)
	resp, err := c.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to get collectors: %w", err)
	}

	collectors := make(map[string]int)
	for _, kv := range resp.Kvs {
		collectorID := string(kv.Key)[len(key):]
		shardID, _ := strconv.Atoi(string(kv.Value))
		collectors[collectorID] = shardID
	}

	return collectors, nil
}

func (c *Coordinator) Close() error {
	if c.session != nil {
		c.session.Close()
	}
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}
