package combind

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/olivere/elastic/v7"

	log "github.com/sirupsen/logrus"
)

type elasticSearchBoxStorage struct {
	client      *elastic.Client
	searchIndex string
}
type scrollResults struct {
	data *elastic.SearchResult
	err  error
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

	results := []SearchBox{}
	for s := range scroll(s.client, ctx, boxType, s.searchIndex) {
		if s.err != nil {
			return nil, s.err
		}
		var typ Component
		for _, h := range s.data.Each(reflect.TypeOf(typ)) {
			results = append(results, h.(SearchBox))
		}
	}

	return results, nil
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
	if err != nil {
		log.Error("Failed to create bulkprocessor")
	}
	defer bp.Close()
	log.Debugf("Start indexing %d items", len(sb))

	indexed := int64(0)
	for _, d := range sb {
		for _, key := range d.Matches {
			dCopy := *d
			dCopy.HashMatch = Hash(key)
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

	statsIndexed := bp.Stats().Indexed
	if statsIndexed != indexed {
		log.Errorf("Expected %d documents, but count returned %d", indexed, statsIndexed)
		if _, err := s.client.DeleteIndex(originIdx).Do(ctx); err != nil {
			log.Errorf("Error while deleting new index %s", originIdx)
			return err
		}
		return fmt.Errorf("wrong number of documents indexed")
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

func (s *elasticComponentStorage) Find(ctx context.Context, componentType string) ([]BackendComponent, error) {

	results := []BackendComponent{}
	for s := range scroll(s.client, ctx, componentType, s.componentIndex) {
		var typ BackendComponent
		if s.err != nil {
			return nil, s.err
		}
		for _, h := range s.data.Each(reflect.TypeOf(typ)) {
			results = append(results, h.(BackendComponent))
		}
	}

	return results, nil
}

func (s *elasticComponentStorage) Save(ctx context.Context, c ...*BackendComponent) error {
	bp, err := elastic.NewBulkProcessorService(s.client).Do(ctx)
	if err != nil {
		return err
	}
	defer bp.Close()

	for _, d := range c {
		req := elastic.NewBulkIndexRequest().Index(s.componentIndex).Id(fmt.Sprintf("%s_%s", d.Type, d.Code)).Doc(d)
		bp.Add(req)
	}

	return bp.Flush()
}

func (s *elasticComponentStorage) Delete(ctx context.Context, c ...*BackendComponent) error {
	bp, err := elastic.NewBulkProcessorService(s.client).Do(ctx)
	if err != nil {
		return err
	}
	defer bp.Close()

	for _, d := range c {
		bp.Add(elastic.NewBulkDeleteRequest().Index(s.componentIndex).Id(fmt.Sprintf("%s_%s", d.Type, d.Code)))
	}

	return bp.Flush()
}

func scroll(client *elastic.Client, ctx context.Context, typ, index string) chan *scrollResults {
	scroller := client.Scroll(index).Size(10000).Query(elastic.NewBoolQuery().Must(
		elastic.NewMatchQuery("type.keyword", typ),
	))

	results := make(chan *scrollResults, 3)
	go func() {
		defer close(results)
		for {
			r, err := scroller.Do(ctx)
			if err == io.EOF {
				//done
				return
			}
			if err != nil {
				log.Warnf("Error while scrolling %v", err)
				results <- &scrollResults{
					err: err,
				}
				return
			}
			results <- &scrollResults{
				data: r,
			}
		}
	}()
	return results
}
