ruleset driver_endpoint {
  meta {
    use module io.picolabs.subscription alias subs
    shares __testing, shop_map
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" },
        { "name": "shop_map" }
      ] , "events":
      [ //{ "domain": "d1", "type": "t1" }
      //, { "domain": "d2", "type": "t2", "attrs": [ "a1", "a2" ] }
        { "domain": "test", "type": "event", "attrs": [ "shop_id"] }
      ]
    }
    shop_map = function() { ent:shop_map }
  }
  
  rule initialize {
    select when wrangler ruleset_added where rids >< meta:rid
    always {
      ent:shop_map := {}
    }
  }
  
  // Incoming
  rule auto_accept {
    select when wrangler inbound_pending_subscription_added
    pre {
      tx = event:attr("Tx")
      // tx = event:attr("wellKnown_Tx").klog("WELL_KOWNN_TX")
      shop_id = event:attr("shop_id")
    }
    always {
      ent:shop_map{shop_id} := tx;
    }
  }
  
  rule update_orders {
    select when shop orders_updated
    pre {
      // map of current existing and available orders keyed by order_id
      // order objects contain shop_id, address, order_id
      orders = event:attr("open_orders")
    }
  }
  
  rule bid_accepted {
    select when shop bid_accepted
    pre {
      // contains shop_id, order_id, location
      order = event:attr("order")
      // contains driver_id, bid_id, bid_amount
      bid = event:attr("bid")
    }
  }
  
  // Outgoing
  rule delivery_complete {
    select when delivery complete
      pre {
        order_id = event:attr("order_id")
      }
    event:send(
      { "eci": ent:shop_map{shop_id}, 
        "eid": "Order Complete",
        "domain": "driver", 
        "type": "delivery_complete",
        "attrs": { "driver_id": meta:picoId, "order_id": order_id }
      }
    )
  }
}
