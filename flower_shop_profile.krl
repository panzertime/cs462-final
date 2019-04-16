ruleset flower_shop_profile {
  meta {
    shares __testing, name, address, id
    provides id, name, address
  }
  global {
    __testing = { "queries":
      [ 
        { "name": "name" },
        { "name": "address" }
      ] , "events":
      [ 
        { "domain": "flower_shop_profile", "type": "update_address", 
          "attrs": [ "city", "state", "street_address" ]}
      ]
    }
    id = function() { meta:picoId }
    name = function() { ent:name }
    address = function() {
      ent:street_address + "," + ent:city + "," + ent:state;
    }
  }
  rule initialize {
    select when wrangler ruleset_added where rids >< meta:rid
    always {
      ent:name := "Generic Name";
      ent:street_address := null;
      ent:city := "Salt Lake City";
      ent:state := "UT";
      ent:country := "USA";
    }
  }
  rule update_address {
    select when flower_shop_profile update_address
    pre {
      city = event:attr("city");
      state = event:attr("state");
      street_address = event:attr("street_address");
    }
    if (not city.isnull()) then noop()
    fired {
      ent:city := city;
      ent:state := state;
      ent:street_address := street_address;
    }
  }
  rule update_name {
    select when flower_shop_profile update_name
    pre {
      name = event:attr("name")
    }
    if (not name.isnull()) then noop()
    fired {
      ent:name := name;
    }
  }
}
