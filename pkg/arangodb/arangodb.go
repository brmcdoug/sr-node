package arangodb

import (
	"context"
	"encoding/json"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/jalapeno/topology/pkg/dbclient"
	notifier "github.com/jalapeno/topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/message"
	"github.com/sbezverk/gobmp/pkg/tools"
)

type arangoDB struct {
	dbclient.DB
	*ArangoConn
	stop      chan struct{}
	lsprefix  driver.Collection
	lssrv6sid driver.Collection
	lsnode    driver.Collection
	srnode    driver.Collection
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname, lsprefix, lssrv6sid, lsnode, srnode string) (dbclient.Srv, error) {
	if err := tools.URLAddrValidation(arangoSrv); err != nil {
		return nil, err
	}
	arangoConn, err := NewArango(ArangoConfig{
		URL:      arangoSrv,
		User:     user,
		Password: pass,
		Database: dbname,
	})
	if err != nil {
		return nil, err
	}
	arango := &arangoDB{
		stop: make(chan struct{}),
	}
	arango.DB = arango
	arango.ArangoConn = arangoConn

	// Check if vertex collections exist if not, fail with error
	arango.lsprefix, err = arango.db.Collection(context.TODO(), lsprefix)
	if err != nil {
		return nil, fmt.Errorf("ls_prefix collection not found")
	}
	arango.lssrv6sid, err = arango.db.Collection(context.TODO(), lssrv6sid)
	if err != nil {
		return nil, fmt.Errorf("ls_prefix collection not found")
	}
	arango.lsnode, err = arango.db.Collection(context.TODO(), lsnode)
	if err != nil {
		return nil, fmt.Errorf("ls_prefix collection not found")
	}

	// check for sr_node collection, if it doesn't exist, create it
	found, err := arango.db.CollectionExists(context.TODO(), srnode)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), srnode)
		if err != nil {
			return nil, err
		}
		glog.Infof("sr_node collection found, proceed to processing data")

		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}
	// create sr node collection
	var srnode_options = &driver.CreateCollectionOptions{ /* ... */ }
	glog.Infof("sr_node collection not found, creating collection")
	arango.srnode, err = arango.db.CreateCollection(context.TODO(), "sr_node", srnode_options)
	if err != nil {
		return nil, err
	}

	// check if collection exists, if not fail as processor has failed to create collection
	arango.srnode, err = arango.db.Collection(context.TODO(), srnode)
	if err != nil {
		return nil, fmt.Errorf("failed to create sr_node collection")
	}

	return arango, nil
}

func (a *arangoDB) Start() error {
	if err := a.loadCollection(); err != nil {
		return err
	}
	glog.Infof("Connected to arango database, starting monitor")
	go a.monitor()

	return nil
}

func (a *arangoDB) Stop() error {
	close(a.stop)

	return nil
}

func (a *arangoDB) GetInterface() dbclient.DB {
	return a.DB
}

func (a *arangoDB) GetArangoDBInterface() *ArangoConn {
	return a.ArangoConn
}

func (a *arangoDB) StoreMessage(msgType dbclient.CollectionType, msg []byte) error {
	event := &notifier.EventMessage{}
	if err := json.Unmarshal(msg, event); err != nil {
		return err
	}
	event.TopicType = msgType
	switch msgType {
	case bmp.LSSRv6SIDMsg:
		return a.lsSRv6SIDHandler(event)
	case bmp.LSNodeMsg:
		return a.lsNodeHandler(event)
	case bmp.LSPrefixMsg:
		return a.lsPrefixHandler(event)
	}

	return nil
}

func (a *arangoDB) monitor() {
	for {
		select {
		case <-a.stop:
			// TODO Add clean up of connection with Arango DB
			return
		}
	}
}

func (a *arangoDB) loadCollection() error {
	ctx := context.TODO()
	// copy ls_node data into new sr_node collection
	glog.Infof("copy ls_node into sr_node")
	lsn_query := "for l in " + a.lsnode.Name() + " insert l in " + a.srnode.Name() + ""
	cursor, err := a.db.Query(ctx, lsn_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	// query ls_prefix collection and pass data to prefixSID processor
	glog.Infof("processing ls prefix")
	sr_query := "for p in  " + a.lsprefix.Name() + " return p "
	cursor, err = a.db.Query(ctx, sr_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.LSPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		if err := a.processPrefixSID(ctx, meta.Key, meta.ID.String(), &p); err != nil {
			glog.Errorf("Failed to process ls_prefix_sid %s with error: %+v", p.ID, err)
		}
	}

	// query ls_srv6_sid collection and pass data to SRv6 SID processor
	glog.Infof("processing srv6")
	srv6_query := "for s in  " + a.lssrv6sid.Name() + " return s "
	cursor, err = a.db.Query(ctx, srv6_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.LSSRv6SID
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		if err := a.processLSSRv6SID(ctx, meta.Key, meta.ID.String(), &p); err != nil {
			glog.Errorf("Failed to process ls_srv6_sid %s with error: %+v", p.ID, err)
		}
	}

	return nil
}
