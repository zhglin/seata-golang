package server

import (
	"github.com/opentrx/seata-golang/v2/pkg/apis"
	"sync"
)

// CallbackMessageQueue 回调分支事务客户端的消息队列
type CallbackMessageQueue struct {
	lock *sync.Mutex

	queue []*apis.BranchMessage
}

// NewCallbackMessageQueue 每个分支事务客户端address对应一个
func NewCallbackMessageQueue() *CallbackMessageQueue {
	return &CallbackMessageQueue{
		queue: make([]*apis.BranchMessage, 0),
		lock:  &sync.Mutex{},
	}
}

// Enqueue 添加回调消息
func (p *CallbackMessageQueue) Enqueue(msg *apis.BranchMessage) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.queue = append(p.queue, msg)
}

// Dequeue 获取回调消息
func (p *CallbackMessageQueue) Dequeue() *apis.BranchMessage {
	p.lock.Lock()
	defer p.lock.Unlock()
	if len(p.queue) == 0 {
		return nil
	}
	var msg *apis.BranchMessage
	msg, p.queue = p.queue[0], p.queue[1:]
	return msg
}
