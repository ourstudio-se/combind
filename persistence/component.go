package persistence

import (
	"context"
	"fmt"
	"reflect"

	"github.com/olivere/elastic/v7"
	log "github.com/sirupsen/logrus"
)

type elasticComponentStorage struct {
	client         *elastic.Client
	componentIndex string
}

func NewElasticComponentStorage(client *elastic.Client, componentIndex string) ComponentStorage {
	return &elasticComponentStorage{
		client:         client,
		componentIndex: componentIndex,
	}
}

func (s *elasticComponentStorage) Find(ctx context.Context, componentType string) ([]Component, error) {

	result, err := s.client.Search(s.componentIndex).Query(elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("type.keyword", componentType),
	)).Size(10000).Do(ctx)

	if err != nil {
		return nil, err
	}

	values := []Component{}

	var typ Component
	for _, sh := range result.Each(reflect.TypeOf(typ)) {
		if v, ok := sh.(Component); ok {
			values = append(values, v)
		}
	}

	return values, nil

}

func (s *elasticComponentStorage) Save(ctx context.Context, c ...*Component) error {
	bp, err := elastic.NewBulkProcessorService(s.client).Do(ctx)
	defer bp.Close()
	if err != nil {
		log.Error("Failed to create bulkprocessor")
	}

	for _, d := range c {
		req := elastic.NewBulkIndexRequest().Index(s.componentIndex).Id(fmt.Sprintf("%s_%s", d.Type, d.Code)).Doc(d)
		bp.Add(req)
	}

	return bp.Flush()
}
