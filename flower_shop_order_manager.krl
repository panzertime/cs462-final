ruleset flower_shop_order_manager {
  meta {
    use module io.picolabs.subscription alias subs
    shares __testing, orders, bids, out_for_delivery, auto_assign_driver
    provides bids, orders, auto_assign_driver, out_for_delivery
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" },
        { "name": "out_for_delivery"},
        { "name": "bids"},
        { "name": "orders"},
        { "name": "auto_assign_driver"}
      //, { "name": "entry", "args": [ "key" ] }
      ] , "events":
      [ //{ "domain": "d1", "type": "t1" }
      //, { "domain": "d2", "type": "t2", "attrs": [ "a1", "a2" ] }
        { "domain": "internal", "type": "new_order", 
            "attrs": [ "order_id", "address" ]},
        { "domain": "internal", "type": "clear_orders" },
        { "domain": "internal", "type": "assign_order",
            "attrs": [ "order_id", "driver_id" ]},
        { "domain": "internal", "type": "delivery_complete",
            "attrs": [ "order_id", "driver_id" ]}
      ]
    }
    orders = function() { ent:orders }
    bids = function() { ent:bids }
    out_for_delivery = function() { ent:out_for_delivery }
    auto_assign_driver = function() { ent:auto_assign_driver }
  }
  rule initialize {
    select when wrangler ruleset_added where rids >< meta:rid
    always {
      ent:orders := {};
      ent:bids := {};
      ent:out_for_delivery := {};
      ent:auto_assign_driver := true;
    }
  }
  
  rule update_orders {
    select when internal orders_updated
    foreach subs:established("Tx_role","driver") setting (subscription)
    pre {
      orders = ent:orders
    }
    event:send(
      { "eci": subscription{"Tx"}, 
        "eid": "orders_updated",
        "domain": "orders", 
        "type": "updated",
        "attrs": {"open_orders": orders}
      }
    )
  }
  
  rule new_bid {
    select when internal new_bid
    pre {
      order_id = event:attr("order_id");
      bid_id = event:attr("bid_id");
      driver_id = event:attr("driver_id");
      bid = {
        "driver_id": driver_id,
        "bid_id": bid_id,
        "bid_amount": event:attr("bid_amount") }.klog("BID");
    }
    always {
      ent:bids{order_id} := ent:bids{order_id}.defaultsTo({}).put([driver_id], bid);
    }
  }
  
  rule assign_order {
    select when internal assign_order
    pre {
      order_id = event:attr("order_id");
      driver_id = event:attr("driver_id");
      out_for_delivery = {"driver_id": driver_id, "timestamp": time:now()};
    }
    if not (ent:bids >< order_id && ent:bids{order_id} >< driver_id) then
      send_directive("That order or bid is now unavailable")
    notfired {
      ent:bids := ent:bids.delete(order_id);
      ent:orders := ent:orders.delete(order_id);
      ent:out_for_delivery{order_id} := out_for_delivery;
      raise internal event "orders_updated"
    }
  }
  
  rule new_order {
    select when internal new_order 
    pre {
      order_id = event:attr("order_id").isnull() => random:uuid() |
        event:attr("order_id");
      address = event:attr("address");
    }
    always {  
      ent:orders := ent:orders.put(order_id, address);
      raise internal event "orders_updated"
    }
  }
  
  rule delivery_complete {
    select when internal delivery_complete 
    pre {
      driver_id = event:attr("driver_id");
      order_id = event:attr("order_id");
      order = ent:out_for_delivery{order_id}
      delivery_time = order{"timestamp"};
    }
    if not (ent:out_for_delivery{order_id}{"driver_id"} == driver_id) then
      send_directive("That order belongs to another driver")
    notfired {
      ent:out_for_delivery := ent:out_for_delivery.delete(order_id).klog("OFD");
      raise twitter event delivery_complete
        attributes {
          "driver_id": driver_id,
          "order_id": order_id,
          "delivery_time": delivery_time
        }
    }
  }
  
  rule clear_orders {
    select when internal clear_orders 
    always {
      ent:orders := {};
      ent:bids := {};
      raise internal event "orders_updated"
    }
  }
}
