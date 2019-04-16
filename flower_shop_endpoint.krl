ruleset flower_shop_endpoint {
  meta {
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
}
