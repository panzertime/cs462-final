ruleset flower_shop_driver_manager {
  meta {
    shares __testing, drivers
    provides drivers
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" }
      ] , "events":
      [ 
        { "domain": "drivers", "type": "clear" }
      ]
    }
    drivers = function() { ent:drivers }
  }
  
  rule initialize {
    select when wrangler ruleset_added where rids >< meta:rid
    always {
      ent:drivers := {};
      ent:auto_assign_driver := true;
    }
  }
  
  rule clear_drivers {
    select when drivers clear
    always {
      ent:drivers := {}
    }
  }
  
  rule subscribe_new_driver {
    select when internal new_driver
    pre {
      driver_id = event:attr("id");
      driver_eci = event:attr("eci");
    }
    always {
     raise wrangler event "subscription" 
        attributes {
          "Rx_role": "shop",
          "Tx_role": "driver",
          "channel_type": "subscription",
          "wellKnown_Tx": driver_eci,
          "id": driver_id,
          "shop_id": meta:picoId
        }
    }
  }
  rule add_subscription {
    select when wrangler subscription_added
    pre {
      driver_id = event:attr("id");
      driver_eci = event:attr("Rx");
    }
    fired {
      ent:drivers{driver_id} := driver_eci;
    }
  }
}
