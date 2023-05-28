package arangodb

import (
	"context"
	"fmt"
	"strings"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	notifier "github.com/jalapeno/topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) lsNodeHandler(obj *notifier.EventMessage) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// Check if Collection encoded in ID exists
	c := strings.Split(obj.ID, "/")[0]
	if strings.Compare(c, a.lsnode.Name()) != 0 {
		return fmt.Errorf("configured collection name %s and received in event collection name %s do not match", a.lsnode.Name(), c)
	}
	glog.V(6).Infof("Processing action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.LSNode
	_, err := a.lsnode.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a LSNode removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		glog.V(6).Infof("SRv6 SID deleted: %s for srnode key: %s ", obj.Action, obj.Key)
		return a.processSRNodeRemoval(ctx, obj.Key)
	}
	switch obj.Action {
	case "add":
		if err := a.processSRNode(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for vertex %s with error: %+v", obj.Action, obj.Key, err)
		}
	default:
		// NOOP for update
	}

	return nil
}

func (a *arangoDB) lsPrefixHandler(obj *notifier.EventMessage) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// Check if Collection encoded in ID exists
	c := strings.Split(obj.ID, "/")[0]
	if strings.Compare(c, a.lsprefix.Name()) != 0 {
		return fmt.Errorf("configured collection name %s and received in event collection name %s do not match", a.lsprefix.Name(), c)
	}
	glog.V(6).Infof("Processing action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.LSPrefix
	_, err := a.lsprefix.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a LSNode removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		glog.V(6).Infof("Prefix SID deleted: %s for srnode key: %s ", obj.Action, obj.Key)
		return nil
	}
	switch obj.Action {
	case "add":
		if err := a.processPrefixSID(ctx, obj.Key, obj.ID, &o); err != nil {
			return fmt.Errorf("failed to process action %s for vertex %s with error: %+v", obj.Action, obj.Key, err)
		}
	default:
		// NOOP for update
	}

	return nil
}

func (a *arangoDB) lsSRv6SIDHandler(obj *notifier.EventMessage) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// Check if Collection encoded in ID exists
	c := strings.Split(obj.ID, "/")[0]
	if strings.Compare(c, a.lssrv6sid.Name()) != 0 {
		return fmt.Errorf("configured collection name %s and received in event collection name %s do not match", a.lssrv6sid.Name(), c)
	}
	glog.V(6).Infof("Processing action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.LSSRv6SID
	_, err := a.lssrv6sid.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a ls_srv6_sid removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		glog.V(6).Infof("SRv6 SID deleted: %s for srnode key: %s ", obj.Action, obj.Key)
		return nil
	}
	switch obj.Action {
	case "add":
		if err := a.processLSSRv6SID(ctx, obj.Key, obj.ID, &o); err != nil {
			return fmt.Errorf("failed to process action %s for edge %s with error: %+v", obj.Action, obj.Key, err)
		}
	default:
		// NOOP
	}

	return nil
}
