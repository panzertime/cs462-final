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
      [ 
        { "domain": "test", "type": "event", "attrs": [ "shop_id"] },
        { "domain": "internal", "type": "get_directions_to_shop", 
            "attrs": [ "driver_location", "shop_id"] }
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
      shop_id = event:attr("shop_id")
    }
    fired {
      ent:shop_map{shop_id} := tx;
      raise wrangler event "pending_subscription_approval" attributes event:attrs
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
  
  rule google_response_to_shop {
    select when shop google_response_to_shop
    pre {
      directions = event:attr("directions");
    }
  }
  
  rule google_response_to_delivery {
    select when shop google_response_to_delivery
    pre {
      directions = event:attr("directions");
    }
  }
  
  // Outgoing
  rule delivery_complete {
    select when internal delivery_complete
      pre {
        order_id = event:attr("order_id")
      }
    event:send(
      { "eci": ent:shop_map{shop_id}, 
        "eid": "Order Complete",
        "domain": "driver", 
        "type": "delivery_complete",
        "attrs": { "driver_id": meta:picoId, "order_id": order_id }
      })
  }
  
  rule get_directions_to_shop {
    select when internal get_directions_to_shop
    pre {
      driver_location = event:attr("driver_location")
      shop_id = event:attr("shop_id")
    }
    event:send(
      { "eci": ent:shop_map{shop_id}, 
        "eid": "Directions To Shop",
        "domain": "driver", 
        "type": "get_directions_to_shop",
        "attrs": { "driver_id": meta:picoId, "driver_location": driver_location }
      })
  }
  
  rule get_directions_to_delivery {
    select when internal get_directions_to_delivery
    pre {
      driver_location = event:attr("driver_location")
      shop_id = event:attr("shop_id")
      order_id = event:attr("order_id")
    }
    event:send(
      { "eci": ent:shop_map{shop_id}, 
        "eid": "Directions To Shop",
        "domain": "driver", 
        "type": "get_directions_to_delivery",
        "attrs": { "driver_id": meta:picoId, "driver_location": driver_location, "order_id": order_id }
      })
  }
}
