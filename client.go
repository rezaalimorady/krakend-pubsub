package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gocloud.dev/pubsub"

	"github.com/luraproject/lura/v2/config"
	"github.com/luraproject/lura/v2/logging"
	"github.com/luraproject/lura/v2/proxy"
)

var OpenCensusViews = pubsub.OpenCensusViews
var errNoBackendHostDefined = fmt.Errorf("no host backend defined")

const (
	publisherNamespace  = "github.com/devopsfaith/krakend-pubsub/publisher"
	subscriberNamespace = "github.com/devopsfaith/krakend-pubsub/subscriber"
)

func NewBackendFactory(ctx context.Context, logger logging.Logger, bf proxy.BackendFactory) *BackendFactory {
	return &BackendFactory{
		logger: logger,
		bf:     bf,
		ctx:    ctx,
	}
}

type BackendFactory struct {
	ctx    context.Context
	logger logging.Logger
	bf     proxy.BackendFactory
}

func (f *BackendFactory) New(remote *config.Backend) proxy.Proxy {
	if prxy, err := f.initSubscriber(f.ctx, remote); err == nil {
		return prxy
	}

	if prxy, err := f.initPublisher(f.ctx, remote); err == nil {
		return prxy
	}

	return f.bf(remote)
}

func (f *BackendFactory) initPublisher(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {
	cfg := &publisherCfg{}
	if err := getConfig(remote, publisherNamespace, cfg); err != nil {
		if _, ok := err.(*NamespaceNotFoundErr); !ok {
			f.logger.Error(fmt.Sprintf("[BACKEND][PubSub] Error initializing publisher: %s", err.Error()))
		}
		return proxy.NoopProxy, err
	}

	logPrefix := "[BACKEND: " + cfg.Topic_url + "][PubSub]"
	kafka_brokers := os.Getenv("KAFKA_BROKERS")
	if kafka_brokers == "" {
		f.logger.Error(fmt.Sprintf("[BACKEND][PubSub] Error initializing publisher: the KAFKA_BROKERS environment variable dose not exist"))
		return proxy.NoopProxy, &KafkaBrokerEmpyErr{}
	}

	p, err0 := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafka_brokers, "delivery.timeout.ms": 3000})
	if err0 != nil {
		f.logger.Error(fmt.Sprintf("[BACKEND][PubSub] Error initializing publisher: %s", err0.Error()))
		return proxy.NoopProxy, err0
	}

	f.logger.Debug(logPrefix, "Publisher initialized sucessfully")

	go func() {
		<-ctx.Done()
		p.Close()
	}()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					f.logger.Error(fmt.Sprintf("[BACKEND][PubSub] Failed to deliver message: %v\n", ev.TopicPartition.Error))
				}
			}
		}
	}()

	return func(ctx context.Context, r *proxy.Request) (*proxy.Response, error) {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		headers := []kafka.Header{}

		for k, vs := range r.Headers {
			kh := kafka.Header{}
			kh.Key = k
			kh.Value = []byte(vs[0])
			headers = append(headers, kh)
		}
		topic := cfg.Topic_url
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(body),
			Headers:        headers,
		}, nil)

		return &proxy.Response{IsComplete: true}, nil
	}, nil
}

func (f *BackendFactory) initSubscriber(ctx context.Context, remote *config.Backend) (proxy.Proxy, error) {

	cfg := &subscriberCfg{}

	if err := getConfig(remote, subscriberNamespace, cfg); err != nil {
		if _, ok := err.(*NamespaceNotFoundErr); !ok {
			f.logger.Error(fmt.Sprintf("[BACKEND][PubSub] Error initializing subscriber: %s", err.Error()))
		}
		return proxy.NoopProxy, err
	}

	kafka_brokers := os.Getenv("KAFKA_BROKERS")
	if kafka_brokers == "" {
		f.logger.Error(fmt.Sprintf("[BACKEND][PubSub] Error initializing subscriber: the KAFKA_BROKERS environment variable dose not exist"))
		return proxy.NoopProxy, &KafkaBrokerEmpyErr{}
	}

	logPrefix := "[BACKEND: " + cfg.Subscription_url + "][PubSub]"

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": kafka_brokers,
		"group.id":          cfg.Group_id,
	})

	if err != nil {
		f.logger.Error(fmt.Sprintf(logPrefix, "Error while opening subscription: %s", err.Error()))
		return proxy.NoopProxy, err
	}

	f.logger.Debug(logPrefix, "Subscriber initialized sucessfully")

	go func() {
		<-ctx.Done()
		c.Close()
	}()

	ef := proxy.NewEntityFormatter(remote)

	return func(ctx context.Context, _ *proxy.Request) (*proxy.Response, error) {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			return nil, err
		}

		var data map[string]interface{}
		if err := remote.Decoder(bytes.NewBuffer(msg.Value), &data); err != nil && err != io.EOF {
			// TODO: figure out how to Nack if possible
			return nil, err
		}

		newResponse := proxy.Response{Data: data, IsComplete: true}
		newResponse = ef.Format(newResponse)
		return &newResponse, nil
	}, nil
}

type publisherCfg struct {
	Topic_url string
	// Addresses string
}

type subscriberCfg struct {
	Subscription_url string
	// Addresses        string
	Group_id string
}

func getConfig(remote *config.Backend, namespace string, v interface{}) error {
	data, ok := remote.ExtraConfig[namespace]
	if !ok {
		return &NamespaceNotFoundErr{
			Namespace: namespace,
		}
	}

	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return json.Unmarshal(raw, &v)
}

type NamespaceNotFoundErr struct {
	Namespace string
}

func (n *NamespaceNotFoundErr) Error() string {
	return n.Namespace + " not found in the extra config"
}

type KafkaBrokerEmpyErr struct {
}

func (n *KafkaBrokerEmpyErr) Error() string {
	return "The 'KAFKA_BROKERS' environment variable dose not exist!"
}
