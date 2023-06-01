package arangodb

import (
	"context"
	"strings"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/base"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) processPrefixSID(ctx context.Context, key, id string, e *message.LSPrefix) error {
	if strings.Contains(e.Key, ":") {
		// we're looking for v4 prefixes
		return nil
	}
	if e.PrefixAttrTLVs == nil {
		// we're looking for SR prefix SIDs
		glog.V(5).Infof("ls prefix does not come with ISIS extended prefix attributes: %s ", e.Key)
		return nil
	}
	if e.PrefixAttrTLVs.LSPrefixSID == nil {
		// we're looking for SR prefix SIDs
		return nil
	}
	glog.V(5).Infof("correlate sr_node with ls_prefix: %s ", e.Key)
	query := "for l in  " + a.srnode.Name() +
		" filter l.igp_router_id == " + "\"" + e.IGPRouterID + "\""
	query += " return l"
	pcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var ln SRNode
		nl, err := pcursor.ReadDocument(ctx, &ln)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		glog.V(5).Infof("sr node: %s + prefix sid %v +  ", ln.Key, e.PrefixAttrTLVs.LSPrefixSID)

		obj := srObject{
			PrefixAttrTLVs: e.PrefixAttrTLVs,
		}

		if _, err := a.srnode.UpdateDocument(ctx, nl.Key, &obj); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
		}
	}

	return nil
}

func (a *arangoDB) processLSSRv6SID(ctx context.Context, key, id string, e *message.LSSRv6SID) error {
	//glog.Infof("processing SRv6SID %s", e.SRv6SID)
	glog.V(5).Infof("query to correlate srv6 sid %s", e.Key)
	query := "for l in " + a.srnode.Name() +
		" filter l.igp_router_id == " + "\"" + e.IGPRouterID + "\""
	query += " return l"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()
	var sn SRNode
	ns, err := ncursor.ReadDocument(ctx, &sn)
	//glog.Infof("read document %v, with sids %v", sn.Key, sn.SIDs)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}
	sid := SID{
		SRv6SID:              e.SRv6SID,
		SRv6EndpointBehavior: e.SRv6EndpointBehavior,
		SRv6BGPPeerNodeSID:   e.SRv6BGPPeerNodeSID,
		SRv6SIDStructure:     e.SRv6SIDStructure,
	}
	// so sorry, this is a placeholder hack until I figure out how to range over an existing array of SIDs looking for duplicates
	if len(sn.SIDs) < 4 {
		//if len(sn.SIDs) == 0 {
		glog.Infof("adding sid, %v", sid)
		sn.SIDs = append(sn.SIDs, sid)
		srn := SRNode{
			SIDs: sn.SIDs,
		}
		if _, err := a.srnode.UpdateDocument(ctx, ns.Key, &srn); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
		}
	} else {
		glog.Infof("max sids reached: %v", sid)
	}
	// } else if len(sn.SIDs) > 0 {

	// 	for x := range sn.SIDs {
	// 		if sn.SIDs[x].SRv6SID == e.SRv6SID {
	// 			// found
	// 			glog.Infof("sid exists, skipping, %v", sid)
	// 		} else {
	// 			sn.SIDs = append(sn.SIDs, sid)
	// 			srn := SRNode{
	// 				SIDs: sn.SIDs,
	// 			}
	// 			glog.Infof("appending sid: %v", sid)
	// 			if _, err := a.srnode.UpdateDocument(ctx, ns.Key, &srn); err != nil {
	// 				if !driver.IsConflict(err) {
	// 					return err
	// 				}
	// 			}
	// 		}
	// 	}
	// }
	return nil
}

func (a *arangoDB) processSRNode(ctx context.Context, key string, e *message.LSNode) error {
	if e.ProtocolID == base.BGP {
		// EPE Case cannot be processed because LS Node collection does not have BGP routers
		return nil
	}
	query := "for l in " + a.lsnode.Name() +
		" filter l._key == " + "\"" + e.Key + "\""
	query += " return l"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()
	var sn SRNode
	ns, err := ncursor.ReadDocument(ctx, &sn)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}

	if _, err := a.srnode.CreateDocument(ctx, &sn); err != nil {
		glog.Infof("adding sr node: %s ", sn.Key)
		if !driver.IsConflict(err) {
			return err
		}
		if err := a.findPrefixSID(ctx, sn.Key, e); err != nil {
			//glog.Infof("finding prefix SID for node: %s ", sn.Key)
			if err != nil {
				return err
			}
		}
		// The document already exists, updating it with the latest info
		if _, err := a.srnode.UpdateDocument(ctx, ns.Key, e); err != nil {
			return err
		}
		return nil
	}

	if err := a.processSRNode(ctx, ns.Key, e); err != nil {
		glog.Errorf("Failed to process sr_node %s with error: %+v", e, err)
	}
	return nil
}

func (a *arangoDB) findPrefixSID(ctx context.Context, key string, e *message.LSNode) error {
	//glog.Infof("finding prefix sid for node: %s ", e.Key)
	query := "for l in " + a.lsprefix.Name() +
		" filter l.igp_router_id == " + "\"" + e.IGPRouterID + "\"" +
		" filter l.prefix_attr_tlvs.lsprefix_sid != null"
	query += " return l"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()
	var lp message.LSPrefix
	pl, err := ncursor.ReadDocument(ctx, &lp)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}
	obj := srObject{
		PrefixAttrTLVs: lp.PrefixAttrTLVs,
	}
	if _, err := a.srnode.UpdateDocument(ctx, e.Key, &obj); err != nil {
		glog.V(5).Infof("adding prefix sid: %s ", pl.Key)
		return err
	}
	return nil
}

// processSRNodeRemoval removes records from the sn_node collection which are referring to deleted LSNode
func (a *arangoDB) processSRNodeRemoval(ctx context.Context, key string) error {
	query := "FOR d IN " + a.srnode.Name() +
		" filter d._key == " + "\"" + key + "\""
	query += " return d"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()

	for {
		var nm SRNode
		m, err := ncursor.ReadDocument(ctx, &nm)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		if _, err := a.srnode.RemoveDocument(ctx, m.ID.Key()); err != nil {
			if !driver.IsNotFound(err) {
				return err
			}
		}
	}

	return nil
}
