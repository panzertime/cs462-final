ruleset flower_shop_endpoint {
  meta {
    use module io.picolabs.subscription alias subs
    use module flower_shop_driver_manager
    use module flower_shop_order_manager
    use module flower_shop_profile
    
    shares __testing, drivers, bids, orders, address, name, id
  }
  global {
    __testing = { "queries":
      [ { "name": "drivers" },
        { "name": "bids" },
        { "name": "orders" },
        { "name": "address" },
        { "name": "name" },
        { "name": "id" },
        { "name": "__testing" }
      ] , "events":
      [ 
        { "domain": "driver", "type": "subscribe",
            "attrs": [ "id", "eci" ]},
        { "domain": "driver", "type": "submit_bid",
            "attrs": [ "driver_id", "order_id", "bid_amount" ]},
        { "domain": "driver", "type": "delivery_complete",
            "attrs": [ "driver_id", "order_id" ]}
      ]
    }
    drivers = function() { flower_shop_driver_manager:drivers() }
    bids = function() { flower_shop_order_manager:bids() }
    orders = function() { flower_shop_order_manager:orders() }
    address = function() { flower_shop_profile:address() }
    name = function() { flower_shop_profile:name() }
    id = function() { flower_shop_profile:id() }
  }
  
  rule initialize {
    select when wrangler ruleset_added where rids >< meta:rid
    always {
      
    }
  }
  
  // Incoming 
  rule driver_register {
    select when driver subscribe 
    pre {
      driver_id = event:attr("id");
      driver_eci = event:attr("eci");
    }
    if (flower_shop_driver_manager:drivers() >< driver_id) then 
      send_directive("driver already subscribed")
    notfired {
    raise internal event "new_driver"
      attributes { "id": driver_id, "eci": driver_eci }
    }
  }
  
  rule driver_submit_bid {
    select when driver submit_bid 
    pre {
      driver_id = event:attr("driver_id")
      order_id = event:attr("order_id");
      bid_amount = event:attr("bid_amount");
      bid_id = random:uuid();
    }
    if not (orders() >< order_id) then
      send_directive("That order has been assigned")
    notfired {
      // send_directive("adding bid: " + bid_id);
      raise internal event "new_bid"
        attributes { 
          "driver_id": driver_id,
          "bid_id": bid_id, 
          "order_id": order_id, 
          "bid_amount": bid_amount }
    }
  }
  
  rule driver_delivery_complete {
    select when driver delivery_complete 
    pre {
      driver_id = event:attr("driver_id")
      order_id = event:attr("order_id");
    }
    if not (flower_shop_order_manager:out_for_delivery(){order_id}{"driver_id"} == driver_id) then
      send_directive("That order belongs to another driver")
    notfired {
      raise internal event "delivery_complete"
        attributes {
          "driver_id": driver_id,
          "order_id": order_id
        }
    }
  }
  
  // { "domain": "internal", "type": "get_directions_to_shop",
  //           "attrs": [ "driver_id", "driver_location"]},
  //       { "domain": "internal", "type": "get_directions_to_delivery",
  //           "attrs": [ "driver_id", "driver_location", "order_id"]}
  rule get_directions_to_shop {
    select when driver get_directions_to_shop
    pre {
      driver_id = event:attr("driver_id");
      driver_location = event:attr("driver_location")
    }
    if (driver_id.isnull() || driver_location.isnull()) then
      send_directive("Must inclued driver_id and driver_location")
    notfired {
      raise internal event "get_directions_to_shop"
        attributes event:attrs
    }
  }
  
  rule get_directions_to_delivery {
    select when driver get_directions_to_delivery
    pre {
      driver_id = event:attr("driver_id");
      driver_location = event:attr("driver_location")
      order_id = event:attr("order_id")
    }
    if (driver_id.isnull() || driver_location.isnull() || order_id.isnull()) then
      send_directive("Must inclued driver_id, driver_location, and order_id")
    notfired {
      raise internal event "get_directions_to_delivery"
        attributes event:attrs
    }
  }
  
  // Outgoing 
  rule update_sequence {
    select when internal orders_updated
    always { 
      ent:sequence := ent:sequence + 1; 
      raise internal event "orders_updated_ns"
        attributes event:attrs
    }
  }
  rule update_orders {
    select when internal orders_updated_ns
    foreach subs:established("Tx_role","driver") setting (subscription)
    pre {
      orders = {}.put("sequence", ent:sequence)
                  .put("open_orders", flower_shop_order_manager:orders()); 
    }
    event:send(
      { "eci": subscription{"Tx"}, 
        "eid": "orders_updated",
        "domain": "shop", 
        "type": "orders_updated",
        "attrs": {"open_orders": orders}
      }
    )
  }
  
  rule order_assigned {
    select when internal order_assigned
    pre {
      order = event:attr("order")
      bid = event:attr("bid")
      driver_eci = drivers(){bid{"driver_id"}};
    }
    event:send(
      { "eci": driver_eci, 
        "eid": "Order Assigned",
        "domain": "shop", 
        "type": "bid_accepted",
        "attrs": {"order": order, "bid": bid}
      })
  }
  
  rule google_response {
    select when internal google_response
    pre {
      label = event:attr("label").split(re#:#);
      driver_id = label[1];
      type = label[0];
      directions = event:attr("directions")
      driver_eci = drivers(){driver_id};
    }
    event:send(
      { "eci": driver_eci, 
        "eid": "Google Resoponse",
        "domain": "shop", 
        "type": "google_response" + type,
        "attrs": {"directions": directions}
      })
  }
}
