ruleset flower_shop_order_manager {
  meta {
    use module io.picolabs.subscription alias subs
    shares __testing, orders, bids, out_for_delivery, deliveries, auto_assign_driver
    provides bids, orders, auto_assign_driver, deliveries, out_for_delivery
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" },
        { "name": "out_for_delivery"},
        { "name": "deliveries"},
        { "name": "bids"},
        { "name": "orders"},
        { "name": "auto_assign_driver"}
      ] , "events":
      [ 
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
    deliveries = function() { ent:deliveries }
    auto_assign_driver = function() { ent:auto_assign_driver }
  }
  rule initialize {
    select when wrangler ruleset_added where rids >< meta:rid
    always {
      ent:orders := {};
      ent:bids := {};
      ent:out_for_delivery := {};
      ent:deliveries := [];
      ent:auto_assign_driver := true;
    }
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
        "bid_amount": event:attr("bid_amount") };
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
      order = ent:orders{order_id}
      bid = ent:bids{order_id}{driver_id}
      address = order{"address"};
    }
    if not (ent:bids >< order_id && ent:bids{order_id} >< driver_id) then
      send_directive("That order or bid is now unavailable")
    notfired {
      ent:bids := ent:bids.delete(order_id);
      ent:orders := ent:orders.delete(order_id);
      ent:out_for_delivery{order_id} := out_for_delivery;
      raise internal event "order_assigned"
        attributes {"order": order, "bid": bid, "address": address};
      raise internal event "orders_updated"
    }
  }
  
  rule new_order {
    select when internal new_order 
    pre {
      order_id = event:attr("order_id").isnull() => random:uuid() |
        event:attr("order_id");
      address = event:attr("address");
      order = {}.put("address", address).put("shop_id", meta:picoId).put("order_id", order_id);
    }
    always {  
      ent:orders := ent:orders.put(order_id, order);
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
      new_delivery = {}.put("driver_id", driver_id)
                        .put("order_id", order_id)
                        .put("timestamp", delivery_time)
                        .put("address", order{"address"});
      old_deliveries = ent:deliveries;
    }
    if not (ent:out_for_delivery{order_id}{"driver_id"} == driver_id) then
      send_directive("That order belongs to another driver")
    notfired {
      ent:deliveries := old_deliveries.append(new_delivery);
      ent:out_for_delivery := ent:out_for_delivery.delete(order_id);
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
