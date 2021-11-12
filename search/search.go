package search

import (
	"cloud.google.com/go/datastore"
	"cloud.google.com/go/storage"
	"fmt"
	"github.com/rismaster/allris-common/application"
	"github.com/rismaster/allris-common/common/ocr"
	"github.com/rismaster/allris-common/common/slog"
	"github.com/rismaster/allris-db/db"
	"google.golang.org/api/iterator"
	"log"
	"path/filepath"
	"strings"
)

type SearchParent struct {
	Pages []SearchPage
}

type SearchPage struct {
	Seite int
	Text  string
}

type SearchEntity struct {
	Title    string
	Datum    int64
	SubTitle string
	KeyEnc   string
	Kind     string
	Name     string
}

type SearchDocument struct {
	Title    string
	Datum    int64
	KeyEnc   string
	Kind     string
	Name     string
	Filename string
}

type SearchBeratung struct {
	Status        string
	Typ           string
	BSVV          string
	Datum         int64
	SILFDNR       int
	Beschlussart  string
	Gremium       string
	TOLFDNR       int
	Betreff       string
	Bearbeiter    string
	Federfuehrend string
	VOLFDNR       int
	Oeff          string
}

type SearchElem struct {
	Pages      []SearchPage
	Document   SearchDocument
	Parent     SearchEntity
	Beratungen []SearchBeratung
	TotalPages int
}

type SearchContext struct {
	appContext   *application.AppContext
	SearchIndex  Index
	SearchClient Client
}

type Index interface {
	DeleteBy(option string) (interface{}, error)
	SaveObjects(elems []SearchElem, exist bool) (interface{}, error)
}

type Client interface {
	InitIndex(index string) *Index
}

func (sctx *SearchContext) UpdateSearchForDocument(documentName string) error {

	err := sctx.deleteOldEntitiesInSearch(documentName)
	if err != nil {
		slog.Error("error deleting search index for %s:  %v", documentName, err)
		return err
	}

	err = sctx.createEntitiesInSearch(documentName)
	if err != nil {
		slog.Error("error creating search index for %s:  %v", documentName, err)
		return err
	}

	return nil
}

func (sctx *SearchContext) deleteOldEntitiesInSearch(documentName string) error {
	slog.Info("Delete old Document %s", documentName)
	_, err := sctx.SearchIndex.DeleteBy(fmt.Sprintf("Document.Name:\"%s\"", documentName))
	return err
}

func (sctx *SearchContext) createEntitiesInSearch(documentName string) error {

	totalPages, pageelems, err := sctx.processPage(documentName)
	if err != nil {
		slog.Error("error processing document %s: %v", documentName, err)
		return err
	}
	var searchElems []SearchElem
	for _, pe := range pageelems {
		result, err := sctx.prepareSearchElem(pe, documentName, totalPages)
		if err != nil {
			slog.Error("error preparing search element for document %s, %v", documentName, err)
			return err
		}
		searchElems = append(searchElems, result)
		log.Printf("%s (%s) => %s", result.Parent.Kind, result.Parent.Name, result.Document.Name)
	}
	_, err = sctx.SearchIndex.SaveObjects(searchElems, true)
	if err != nil {
		slog.Error("error inexing document %s - %v", documentName, err)
		return err
	}
	slog.Info("Saved Search for %s", documentName)
	return nil
}

func (sctx *SearchContext) prepareSearchElem(elem *SearchParent, documentName string, totalPages int) (res SearchElem, err error) {
	documentKey := sctx.createDocumentKey(filepath.Base(documentName), nil)
	entity, beratungen, err := sctx.getEntityBeratungen(documentKey.Parent)
	if err != nil {
		slog.Error("error getting datum for document %s - %v", documentName, err)
		return res, err
	}

	var anlage db.Anlage
	err = sctx.appContext.Db().Get(sctx.appContext.Ctx(), documentKey, &anlage)
	if err != nil {
		slog.Error("error getting sitzung for document %s - %v", documentName, err)
		return res, err
	}

	result := SearchElem{

		Pages:      elem.Pages,
		TotalPages: totalPages,
		Parent:     entity,

		Document: SearchDocument{
			Title:    anlage.Title,
			KeyEnc:   documentKey.Encode(),
			Kind:     documentKey.Kind,
			Name:     documentKey.Name,
			Filename: documentName,
		},
		Beratungen: beratungen,
	}
	return result, nil
}

func (sctx *SearchContext) countBytesSearchParent(p *SearchParent) int {
	if p == nil {
		return 0
	}
	count := 0
	for _, p := range p.Pages {
		count = count + len(p.Text)
	}
	return count
}

func (sctx *SearchContext) processPage(documentName string) (totalPages int, elems []*SearchParent, err error) {

	var lastElem = &SearchParent{
		Pages: []SearchPage{},
	}
	elems = append(elems, lastElem)
	query := &storage.Query{Prefix: documentName}
	it := sctx.appContext.Store().Bucket(sctx.appContext.Config.GetBucketOcr()).Objects(sctx.appContext.Ctx(), query)
	var lastPCount = sctx.countBytesSearchParent(lastElem)
	for {

		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			slog.Error("Error on iterating files for document %s - %v", documentName, err)
			return 0, nil, err
		}

		jsonOcr, err := ocr.ReadOcrFromFile(sctx.appContext, attrs.Name, sctx.appContext.Config.GetBucketOcr())
		if err != nil {
			slog.Error("Error on Reading Ocr-File %s - %v", attrs.Name, err)
		}

		for _, resp := range jsonOcr.Responses {

			countBytes := len([]byte(resp.FullTextAnnotation.Text))
			lastPCount = lastPCount + countBytes
			if lastPCount > 10500 {
				lastElem = &SearchParent{
					Pages: []SearchPage{},
				}
				lastPCount = 0
				elems = append(elems, lastElem)
			}
			lastElem.Pages = append(lastElem.Pages, SearchPage{
				Text:  resp.FullTextAnnotation.Text,
				Seite: resp.Context.PageNumber,
			})
		}
		totalPages = totalPages + len(jsonOcr.Responses)
	}
	return totalPages, elems, nil
}

func (sctx *SearchContext) getEntityBeratungen(parentKey *datastore.Key) (entity SearchEntity, results []SearchBeratung, err error) {

	var query *datastore.Query
	if parentKey.Kind == sctx.appContext.Config.GetEntitySitzung() {
		var sitzung db.Sitzung
		err = sctx.appContext.Db().Get(sctx.appContext.Ctx(), parentKey, &sitzung)
		if err != nil {
			slog.Error("error getting sitzung from datastore parentKey %v - %v", parentKey, err)
			return entity, nil, err
		}

		query = datastore.NewQuery(sctx.appContext.Config.GetEntityTop()).Filter("SILFDNR =", sitzung.SILFDNR)
		entity = SearchEntity{
			Title:    sitzung.Title,
			Datum:    sitzung.Datum.Unix(),
			Name:     fmt.Sprintf("%d", sitzung.SILFDNR),
			Kind:     sctx.appContext.Config.GetEntitySitzung(),
			KeyEnc:   parentKey.Encode(),
			SubTitle: sitzung.Gremium,
		}
	} else if parentKey.Kind == sctx.appContext.Config.GetEntityVorlage() {
		var vorlage db.Vorlage
		err = sctx.appContext.Db().Get(sctx.appContext.Ctx(), parentKey, &vorlage)
		if err != nil {
			slog.Error("error getting vorlage from datastore parentKey %v - %v", parentKey, err)
			return entity, nil, err
		}

		query = datastore.NewQuery(sctx.appContext.Config.GetEntityTop()).Filter("VOLFDNR =", vorlage.VOLFDNR)
		entity = SearchEntity{
			Title:    vorlage.Betreff,
			Datum:    vorlage.DatumAngelegt.Unix(),
			Name:     fmt.Sprintf("%d", vorlage.VOLFDNR),
			Kind:     sctx.appContext.Config.GetEntityVorlage(),
			KeyEnc:   parentKey.Encode(),
			SubTitle: vorlage.Federfuehrend,
		}
	} else if parentKey.Kind == sctx.appContext.Config.GetEntityTop() {

		var top db.Top
		err = sctx.appContext.Db().Get(sctx.appContext.Ctx(), parentKey, &top)
		if err != nil {
			slog.Error("error getting top from datastore parentKey %v - %v", parentKey, err)
			return entity, nil, err
		}

		var sitzung db.Sitzung
		err = sctx.appContext.Db().Get(sctx.appContext.Ctx(), parentKey.Parent, &sitzung)
		if err != nil {
			return entity, nil, err
		}

		query = datastore.NewQuery(sctx.appContext.Config.GetEntityTop()).Filter("TOLFDNR =", top.TOLFDNR)

		entity = SearchEntity{
			Title:    fmt.Sprintf("%s (%s: %s)", top.Betreff, top.Nr, sitzung.Title),
			Datum:    top.Datum.Unix(),
			Name:     fmt.Sprintf("%d", top.TOLFDNR),
			Kind:     sctx.appContext.Config.GetEntityTop(),
			KeyEnc:   parentKey.Encode(),
			SubTitle: fmt.Sprintf("%s | %s", top.Federfuehrend, sitzung.Gremium),
		}
	}

	var beratungen []db.Top
	_, err = sctx.appContext.Db().GetAll(sctx.appContext.Ctx(), query, &beratungen)
	if err != nil {
		slog.Error("error getting from datastore parentKey %v - %v", parentKey, err)
		return entity, nil, err
	}
	for _, beratung := range beratungen {
		results = append(results, SearchBeratung{
			VOLFDNR:       beratung.VOLFDNR,
			Federfuehrend: beratung.Federfuehrend,
			Datum:         beratung.Datum.Unix(),
			Beschlussart:  beratung.Beschlussart,
			Typ:           beratung.Typ,
			BSVV:          beratung.BSVV,
			TOLFDNR:       beratung.TOLFDNR,
			Gremium:       beratung.Gremium,
			SILFDNR:       beratung.SILFDNR,
			Status:        beratung.Status,
		})
	}
	return entity, results, nil
}

func (sctx *SearchContext) createDocumentKey(name string, parentKey *datastore.Key) *datastore.Key {

	var restpath string
	var key *datastore.Key
	if strings.HasPrefix(name, sctx.appContext.Config.GetSitzungType()) {
		restpath, key = sctx.createKey(name, "Sitzung", sctx.appContext.Config.GetSitzungType(), parentKey)
	} else if strings.HasPrefix(name, sctx.appContext.Config.GetVorlageType()) {
		restpath, key = sctx.createKey(name, "Vorlage", sctx.appContext.Config.GetVorlageType(), parentKey)
	} else if strings.HasPrefix(name, sctx.appContext.Config.GetTopType()) {
		restpath, key = sctx.createKey(name, "Top", sctx.appContext.Config.GetTopType(), parentKey)
	} else if strings.HasPrefix(name, sctx.appContext.Config.GetAnlageType()) {
		trimmed := strings.TrimPrefix(name, sctx.appContext.Config.GetAnlageType()+"-")
		return datastore.NameKey("Anlage", trimmed, parentKey)
	} else if strings.HasPrefix(name, sctx.appContext.Config.GetAnlageDocumentType()) {
		restpath, key = sctx.createKey(name, "BasisAnlage", sctx.appContext.Config.GetAnlageDocumentType(), parentKey)
		return key
	} else {
		slog.Error("ERROR createDocumentKey: %s", name)
	}

	return sctx.createDocumentKey(restpath, key)
}

func (sctx *SearchContext) createKey(name string, entity string, prefix string, parentKey *datastore.Key) (string, *datastore.Key) {
	trimmed := strings.TrimPrefix(name, prefix+"-")
	splitted := strings.SplitN(trimmed, "-", 2)
	return splitted[1], datastore.NameKey(entity, splitted[0], parentKey)
}
