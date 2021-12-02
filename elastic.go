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
	indexPrefix string
}
type scrollResults struct {
	data *elastic.SearchResult
	err  error
}

func NewElasticSearchBoxStorage(client *elastic.Client, alias string, indexPrefix string) SearchBoxStorage {

	//alias: rule-set-resolver-search-{{specmarket}}
	//index: rule-set-resolver-{{spec-market}}-{{time}}

	return &elasticSearchBoxStorage{
		client:      client,
		searchIndex: alias,
		indexPrefix: indexPrefix,
	}
}

func (s *elasticSearchBoxStorage) Find(ctx context.Context, boxType string) ([]SearchBox, error) {

	bq := elastic.NewBoolQuery()

	bq.Must(
		elastic.NewTermQuery("type.keyword", boxType),
	)

	results := []SearchBox{}
	for s := range scroll(s.client, ctx, s.searchIndex, bq) {
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
	originIdx := fmt.Sprintf("%s-%d", s.indexPrefix, time.Now().UTC().Unix())

	// Check if index exists. If not, create it
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

	for _, row := range r {
		if row.Alias == s.searchIndex {
			als = als.Remove(row.Index, row.Alias)
			idxToDelete = append(idxToDelete, row.Index)
		}
	}

	if _, err := als.Add(originIdx, s.searchIndex).Do(ctx); err != nil {
		return err
	}

	if _, err := s.client.DeleteIndex(idxToDelete...).Do(ctx); err != nil {
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

	bq := elastic.NewBoolQuery()

	bq.Must(
		elastic.NewTermQuery("type.keyword", componentType),
	)

	results := []BackendComponent{}
	for s := range scroll(s.client, ctx, s.componentIndex, bq) {
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

func (s *elasticComponentStorage) Search(ctx context.Context, componentType string, searchFilter SearchFilter) ([]BackendComponent, error) {

	bq := elastic.NewBoolQuery()

	bq.Must(
		elastic.NewTermQuery("type.keyword", componentType),
	)

	for k, v := range searchFilter {
		bq.Must(
			elastic.NewTermQuery(fmt.Sprintf("props.%s.keyword", k), v),
		)
	}

	results := []BackendComponent{}
	for s := range scroll(s.client, ctx, s.componentIndex, bq) {
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

func (s *elasticComponentStorage) FilteredDelete(ctx context.Context, componentType string, searchFilter SearchFilter) (int, error) {

	bq := elastic.NewBoolQuery()

	bq.Must(
		elastic.NewTermQuery("type.keyword", componentType),
	)

	for k, v := range searchFilter {
		bq.Must(
			elastic.NewTermQuery(fmt.Sprintf("props.%s.keyword", k), v),
		)
	}

	resp, err := elastic.NewDeleteByQueryService(s.client).Index(s.componentIndex).Query(bq).Do(ctx)

	if err != nil {
		return 0, err
	}

	return int(resp.Deleted), nil

}

func scroll(client *elastic.Client, ctx context.Context, index string, query elastic.Query) chan *scrollResults {
	scroller := client.Scroll(index).Size(10000).Query(query)

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
