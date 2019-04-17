ruleset gossip {
  meta {
    name "Flower Driver Gossip Ruleset"
    description <<
      Ruleset
    for Gossiping With Flower Drivers
      >>
      author "RT Hatfield"
    logging on
    
    use module io.picolabs.wrangler alias wrangler
    use module io.picolabs.subscription alias subscription
    
    provides 
      orders,
      peers,
      seen,
      peerseen
    shares
      orders,
      peers,
      seen,
      peerseen
  }

  global {
    orders = function() {
      get_rumors()
    }
    peers = function() {
      get_peers()
    }
    seen = function() {
      // {store_id: sequence}
      get_seen()
    }
    peerseen = function() {
      // {peer_id: {store_id: sequence}}
      get_peer_seen()
    }
    get_peers = function() {
      // {peer_id : subscription_id}
      ent:peers.isnull() => {} | ent:peers
    }
    
    get_rumors = function() {
      /*
          {
            shop_id: {
              sequence: 7,
              orders: [...]
            },
            shop_id2: { ... }
          }
      */
      ent:rumors.isnull() => {} | ent:rumors
    }
    
    get_peer_seen = function() {
      // {peer_id: {store_id: sequence}}
      ent:peer_seen.isnull() => {} | ent:peer_seen
    }
    
    get_seen = function() {
      // {store_id: sequence}
      ent:seen.isnull() => {} | ent:seen
    }
    
    process = function() {
      ent:process.isnull() => true | ent:process
    }
    
    prepareMessage = function(node) {
      rumor = pick_rumor(node{"peer_id"});

      // if we can't get a fresh rumor for that peer (i.e. everyone is caught up)
      // we default to sending a seen message. if we have peers that need rumors,
      // they should show up here first and have a 50/50 chance of getting one

      type = rumor.isnull() => "seen" | ["rumor", "seen"][random:integer(1)];
      body = (type == "seen") =>  get_seen() | rumor{"body"}; 
      attrs = (type == "seen") => 
          {
              "node": meta:picoId,
              "body": body
          } |
          {
            "node": meta:picoId,
            "store_id": rumor{"store_id"},
            "body": body
          }

      {
        "eci": node{"Tx"},
        "domain": "gossip",
        "type": type,
        "attrs": attrs
      }
    }
    
    pick_rumor = function(peer_id) {
      // given this peer ID, pick something they haven't seen and send it
      // {store_id: 0, body:{seq, orders}}
      p_seen = get_peer_seen().get(peer_id);
      // let's recycle the select_unseen from earlier, but just pick one of them
      unseen = select_unseen(p_seen);
      unseen.head()
    }
    
    select_unseen = function(seen) {
      messages = [];
      get_rumors().map(function(k, v) {
        // if v{sequence} > seen{key}, 
        //  add {key: v}
        msg = {"store_id" : k, "body" : v}
        messages = v{"sequence"} > seen("key") => messages.append(msg) | messages
      });
      messages
    }
    
    store_rumor = function(store_id, rumor) {
      get_rumors().put(store_id, rumor)
    }
      
      
    heartbeat_update = function(event, peer_id) {
      attrs = event{"attrs"};
      body = attrs{"body"}:
      event{"type"} == "rumor" => 
          update_peer_seen(peer_id, attrs{"store_id"}, body{"sequence"}) |
          get_peer_seen()
    }
    
    update_peer_seen = function(peer_id, store_id, sequence) {
      new_seen = get_peer_seen().get(peer_id);
      new_seen = new_seen.set(store_id, sequence);
      get_peer_seen().put(peer_id, new_seen)
    }
    
    update_rcv_from_peer = function(peer_id, seen) {
      get_peer_seen().set(peer_id, seen)
    }
    
    update_sent_to_peer = function(peer_id, messages) {
      // msg = {"store_id" : k, "body" : {"sequence" : 0, "orders" = []}
      
      peer_seen = get_peer_seen()
      messages.reduce(function(acc, msg) {
        store_id = msg{"store_id"};
        rumor = msg{"body"};
        sequence = rumor{"sequence"};
        p_seen = acc.get(peer_id); // should never be null
        new_seen = p_seen.put(store_id, sequence)
        acc.put(peer_id, new_seen)
      }, peer_seen)
    }

    update_seen = function(shop_id, sequence) {
      get_seen().put(shop_id, new_seq)
    }

    getPeer = function() {
      // okay. easy way:
      // peers = peer_seen.map({k = k, v = score(v)})
      // score(v){union the keys, then reduce to total ahead/behind}
      // sort those keys by score (lowest = most behind)
      // pick from top of list
      score = function(seen_list){
        stores = get_seen().keys().union(seen_list.keys());
        stores.reduce(function(score, store) {
          // subtract our seen for that store_id from the one in the seen_list
          score + (seen_list.get(store).defaultsTo(-1) - get_seen().get(store).defaultsTo(-1))
        }, 0)
      }

      peer_scores = get_peer_seen().map(function(k, v) {
        {k: score(v)};
      })

      peer_id = peer_scores.keys().sort(function(a, b){
        score_a = peer_scores.get(a);
        score_b = peer_scores.get(b);
        score_a < score_b  => -1 |
            score_a == score_b =>  0 |
            1
      }).head();
      
      sub = subscription:established("Id", get_peers(){"peer_id"})[0];
      {
        "peer_id": peer_id,
        "Tx": sub{"Tx"},
        "Rx": sub{"Rx"},
        "Tx_host": sub{"Tx_host"}
      }
    }
    
    pump_velocity = function() {
      n = 15;
      time:add(time:now(), {"seconds": n})
    }
    
    respondToSeen = defaction(messages, Tx, host) {
      msg = messages.head()
      if not msg.isnull() then
      every {
        event:send({
          "eci": Tx,
          "domain": "gossip",
          "type": "rumor",
          "attrs": {
            "node": meta:picoId,
            "store_id": msg{"store_id"}
            "body": msg{"body"}
          }
        }, host);
        respondToSeen(messages.tail(), Tx, host)
      }
    }
  }

  rule process_heartbeat {
    select when gossip heartbeat
    pre {
      peer = getPeer()
      ev = prepareMessage(peer)
    }
    if not ev{"eci"}.isnull() && process() then
      event:send(ev, host=peer{"Tx_host"});
    fired {
      p = peer.klog("PEER BEFORE HEARTBEAT UPDATE");
      ev.klog("Event in heartbeat");
      // next line will probably be broken by store_ids
      ent:peer_seen := heartbeat_update(ev, peer{"peer_id"}); 
    } finally {
      schedule gossip event "heartbeat" at pump_velocity()
    }
  }
  
  rule autostart {
    select when wrangler ruleset_added where rids >< meta:rid
    always {
      schedule gossip event "heartbeat" at pump_velocity()
    }
  }
  
  rule pump_switch {
    select when gossip process
    pre {
      status = event:attr("status").as("Boolean").defaultsTo(process())
    }
    if status && not process() then
    noop();
    fired {
      schedule gossip event "heartbeat" at pump_velocity()
    } finally {
      ent:process := status
    }
  }
  
  rule process_rumor {
    select when gossip rumor
    pre {
      rumor = event:attr("body")
      peer_id = event:attr("node")
      store_id = event:attr("store_id")
      sequence = rumor{"sequence"}
      max_seen = get_seen().get(store_id).defaultsTo(-1)
    }
    if (sequence > max_seen && not store_id.isnull()) then
      noop();
    fired {
      p = peer_id.klog("PEER ID BEFORE RUMOR UPDATE");
      ent:rumors := store_rumor(store_id, rumor);
      ent:seen := update_seen(store_id, sequence);
      ent:peer_seen := update_peer_seen(peer_id, store_id, sequence);
    }
  }
  
  rule process_seen {
    select when gossip seen
    pre {
      seen = event:attr("body")
      peer_id = event:attr("node")
      sub = subscription:established("Id", get_peers().get(peer_id))[0]
      messages = select_unseen(seen).defaultsTo([])
    }
    
    respondToSeen(messages, sub{"Tx"}, sub{"Tx_host"})
    
    fired{
      p = peer_id.klog("PEER ID BEFORE SEEN UPDATE");
      ent:peer_seen := update_rcv_from_peer(peer_id, seen);
      ent:peer_seen := update_sent_to_peer(peer_id, messages)
    }
  }
  
  rule increment_sequence {
    select when shop orders_updated
    pre {
      shop_id = event:attr("shop_id")
      sequence = event:attr("sequence")
      curr_seq = get_seen().get(shop_id).defaultsTo(-1)
      orders = event:attr("open_orders")
      rumor = {"sequence": sequence, "orders": orders}
    }
    if sequence > curr_seq then
      noop();
    fired {
      ent:rumors := store_rumor(shop_id, rumor);
      ent:seen := update_seen(shop_id, sequence);
    }
  }

  rule auto_accept {
    select when wrangler inbound_pending_subscription_added
    if event:attr("Rx_role") == "gossip_node" then
      noop()
    fired {
      raise wrangler event "pending_subscription_approval"
        attributes event:attrs
    }
  }
  
  rule request_peer {
    select when gossip peer
    pre {
      new_peer_eci = event:attr("eci")
      new_peer_host = event:attr("host")
      name = sensor_profile:get_profile(){"name"}.isnull() => wrangler:randomPicoName() | sensor_profile:get_profile(){"name"}
    }
    event:send(
      {
        "eci": new_peer_eci,
        "domain": "gossip",
        "type": "hello",
        "attrs" : {
          "wellKnown" : subscription:wellKnown_Rx(){"id"},
          "host" : meta:host,
          "ids" : [meta:picoId],
          "name" : name
        }
      }, host=new_zone_host)
  }

  rule accept_peer {
    select when gossip hello
    pre {
        wellKnown = event:attr("wellKnown")
        name = event:attr("name")
        host = event:attr("host").isnull() => meta:host | event:attr("host")
        ids = event:attr("ids").append(meta:picoId)
      }
      noop();
      fired {
        raise wrangler event "subscription" attributes
             { "name" : name,
               "peer_ids" : ids,
               "Rx_role": "gossip_node",
               "Tx_role": "gossip_node",
               "channel_type": "subscription",
               "wellKnown_Tx": wellKnown,
               "Tx_host": host
             };
      }
  }

  rule save_new_subscription {
    select when wrangler subscription_added
    pre {
      remoteHost = event:attr("Tx_host").klog("Remote host: ")
      name = event:attr("name")
      id  = event:attr("Id")
      peer_id = event:attr("peer_ids").difference(meta:picoId)[0]
    }
    if not (get_peers() >< peer_id) then
      send_directive("sensor_subscribed", {
          "remoteHost" : remoteHost
        })
    fired {
      ent:peer_seen := get_peer_seen().put([peer_id], {});
      ent:peers := get_peers().put([peer_id], id)
    }
  }


}