ruleset flower_shop_google {
  meta {
    use module flower_shop_profile
    use module flower_shop_order_manager
    use module google_keys
    shares __testing
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" }
    
      ] , "events":
      [ 
        { "domain": "internal", "type": "get_directions_to_shop",
            "attrs": [ "driver_id", "driver_location"]},
        { "domain": "internal", "type": "get_directions_to_delivery",
            "attrs": [ "driver_id", "driver_location", "order_id"]}
      ]
    }
    get_directions_html = function(legs, label) {
      // title = label.split(re#:#)[0].as("Number");
      l = legs[0];
      title = label.split(re#:#)[0] == "to_shop" => 
      "DIRECTIONS TO SHOP" | "DIRECTIONS TO DELIVERY DROP OFF";
      distance = l{"distance"}{"text"};
      duration = l{"duration"}{"text"};
      start_address = l{"start_address"};
      end_address = l{"end_address"};
      steps = l{"steps"};
      "<b>" + title + "</b></br>" + 
      "Distance: " + distance + "</br>" +
      "Duration: " + duration + "</br>" +
      "Start: " + start_address + "</br>" +
      "End: " + end_address + "</br>" + "</br>" +
      steps.map(function(s) {
        s{"html_instructions"} + "</br>"
      }).reduce(function(a, b) {a + b})
    }
  }
  
  rule initialize {
    select when wrangler ruleset_added where rids >< meta:rid
    always {
      ent:google_directions_path := "https://maps.googleapis.com/maps/api/directions/json?origin=";
    }
  }
  
  rule get_directions_to_shop {
    select when internal get_directions_to_shop 
    pre {
      driver_id = event:attr("driver_id")
      driver_location = event:attr("driver_location")
      shop_location = flower_shop_profile:address();
      url = ent:google_directions_path 
            + driver_location + "&destination=" + shop_location + "&key=" + keys:google{"api_key"}
    }
    http:get(url, autoraise = "to_shop:" + driver_id, parseJSON = true);
  }
  
  rule get_directions_to_delivery {
    select when internal get_directions_to_delivery
    pre {
      driver_id = event:attr("driver_id")
      driver_location = event:attr("driver_location")
      order_id = event:attr("order_id")
      order_location = flower_shop_order_manager:orders(){order_id}{"address"}
      url = ent:google_directions_path  
            + driver_location + "&destination=" + order_location + "&key=" + keys:google{"api_key"}
    }
    http:get(url, autoraise = "to_delivery:" + driver_id, parseJSON = true);
  }
  
  rule google_response {
    select when http get
    pre {
      content = event:attr("content")
      routes = content{"routes"}
      route = routes[0]
      legs = route{"legs"}
      label = event:attr("label")
      html_directions = get_directions_html(legs, label)
    }
    send_directive(html_directions);
    always {
      raise internal event "google_response"  
        attributes {"directions": html_directions, "label": label}
    }
  }
}
