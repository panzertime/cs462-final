ruleset driver {
  meta {
    shares __testing
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" }
      //, { "name": "entry", "args": [ "key" ] }
      ] , "events":
      [ //{ "domain": "d1", "type": "t1" }
      //, { "domain": "d2", "type": "t2", "attrs": [ "a1", "a2" ] }
      ]
    }
  }
  rule auto_accept {
    select when wrangler inbound_pending_subscription_added
    fired {
      raise wrangler event "pending_subscription_approval" attributes event:attrs
    }
  }
  
  rule update_orders {
    select when orders updated
    pre {
      orders = event:attr("open_orders").klog("NEW_ORDERS")
    }
  }
}
