package persistence

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/ourstudio-se/combind/utils/keyutils"
	log "github.com/sirupsen/logrus"
)

type elasticSearchBoxStorage struct {
	client      *elastic.Client
	searchIndex string
}

func NewElasticSearchBoxStorage(client *elastic.Client, metaIndex string) SearchBoxStorage {
	return &elasticSearchBoxStorage{
		client:      client,
		searchIndex: metaIndex,
	}
}

func (s *elasticSearchBoxStorage) Init(ctx context.Context) error {
	r, err := elastic.NewCatAliasesService(s.client).Do(ctx)
	if err != nil {
		return err
	}

	isAlias := false
	for _, r := range r {
		if r.Alias == s.searchIndex {
			isAlias = true
			break
		}
	}

	originIdx := fmt.Sprintf("%s-%d", s.searchIndex, time.Now().UTC().Unix())
	if !isAlias {
		//aliases is not yet setup
		if _, err := s.client.CreateIndex(originIdx).Do(ctx); err != nil {
			return err
		}

		if _, err := s.client.GetMapping().Index(s.searchIndex).Do(ctx); err != nil {
			return err
		}

		_, err := elastic.NewReindexService(s.client).SourceIndex(s.searchIndex).DestinationIndex(originIdx).Do(ctx)
		if err != nil {
			return err
		}

		if _, err := s.client.DeleteIndex(s.searchIndex).Do(ctx); err != nil {
			//this is not good
			return err
		}

		if _, err := elastic.NewAliasService(s.client).Add(originIdx, s.searchIndex).Do(ctx); err != nil {
			// not so good either...
			return err
		}
	}

	return nil
}

func (s *elasticSearchBoxStorage) Find(ctx context.Context, boxType string) ([]SearchBox, error) {
	result, err := s.client.Search(s.searchIndex).Query(elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("type.keyword", boxType),
	)).Size(1000).Do(ctx)

	if err != nil {
		return nil, err
	}

	values := []SearchBox{}

	var typ Component
	for _, sh := range result.Each(reflect.TypeOf(typ)) {
		if v, ok := sh.(SearchBox); ok {
			values = append(values, v)
		}
	}

	return values, nil
}

func (s *elasticSearchBoxStorage) Save(ctx context.Context, sb ...*SearchBox) error {
	start := time.Now()
	defer func() {
		log.Debugf("indexing took %s ", time.Since(start))
	}()
	originIdx := fmt.Sprintf("%s-%d", s.searchIndex, time.Now().UTC().Unix())

	exists, err := s.client.IndexExists(originIdx).Do(ctx)
	if err != nil {
		return err
	}

	if !exists {
		if _, err := s.client.CreateIndex(originIdx).Do(ctx); err != nil {
			return err
		}
	}

	bp, err := elastic.NewBulkProcessorService(s.client).Stats(true).Do(ctx)
	defer bp.Close()
	if err != nil {
		log.Error("Failed to create bulkprocessor")
	}
	log.Debugf("Start indexing %d items", len(sb))

	indexed := int64(0)
	for _, d := range sb {
		for _, key := range d.Matches {
			dCopy := *d
			dCopy.HashMatch = keyutils.Hash(key)
			dCopy.Match = key
			dCopy.Matches = []Key{}
			req := elastic.NewBulkIndexRequest().Index(originIdx).Id(fmt.Sprintf("%s_%s_%s", d.Type, d.Key, dCopy.HashMatch)).Doc(dCopy)
			bp.Add(req)
			indexed++
			if indexed%1000 == 0 {
				log.Debugf("Indexed done for %d documents", indexed)
			}
		}
	}

	if err := bp.Flush(); err != nil {
		return err
	}

	count, err := s.client.Count(originIdx).Do(ctx)
	if err != nil {
		log.Warn(err)
	}

	statsIndexed := bp.Stats().Indexed
	if statsIndexed != int64(indexed) {
		log.Errorf("Expected %d documents, but count returned %d", count, bp.Stats().Indexed)
		s.client.DeleteIndex(originIdx).Do(ctx)
		return fmt.Errorf("Wrong number of documents indexed")
	}
	// roll alias
	as := elastic.NewCatAliasesService(s.client)
	r, err := as.Do(ctx)
	if err != nil {
		return err
	}

	als := elastic.NewAliasService(s.client)

	idxToDelete := []string{}

	for _, r := range r {
		if r.Alias == s.searchIndex {
			als = als.Remove(r.Index, r.Alias)
			idxToDelete = append(idxToDelete, r.Index)
		}
	}

	if _, err := als.Add(originIdx, s.searchIndex).Do(ctx); err != nil {
		return err
	}

	if _, err := elastic.NewIndicesDeleteService(s.client).Index(idxToDelete).Do(ctx); err != nil {
		log.Warn(err)
	}

	return nil
}
