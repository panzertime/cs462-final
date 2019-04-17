ruleset flower_shop_microsoft {
  meta {
    use module flower_shop_order_manager
    use module microsoft_config
    shares __testing
  }
  global {
    __testing = { "queries":
      [ { "name": "__testing" }
      //, { "name": "entry", "args": [ "key" ] }
      ] , "events":
      [ //{ "domain": "d1", "type": "t1" }
        { "domain": "internal", "type": "send_order_report" }
      ]
    }
    create_request = function(deliveries) {
      deliveries;
      content = 
      "<b>Your shop's deliveries in the last " + ent:order_report_gap + " hours</b></br></br>" +
      deliveries.map(function(d) {
        "Order Id: " + d{"order_id"} + 
        ", Address: " + d{"address"} +
        ", Driver Id: " + d{"driver_id"} +
        ", Delivery Time: " + d{"timestamp"} + "</br>"
      }).reduce(function(a,b) {a + b});
      
      {
        "message": {
          "subject": "New Delivery Report",
          "body": {
            "contentType": "HTML",
            "content": content
          },
          "toRecipients": [
            {
              "emailAddress": {
                "address": keys:microsoft{"email_address"}
              }
            }
          ]
        }
      }
    }
  }
  
   rule initialize {
    select when wrangler ruleset_added where rids >< meta:rid
    always {
      ent:microsoft_send_mail_uri := "https://graph.microsoft.com/v1.0/me/sendMail";
      ent:order_report_gap := 6;
      ent:sent_orders := [];
      schedule internal event "send_order_report" at time:add(time:now(), {"seconds": 5});
    }
  }

  
  rule send_order_report {
    select when internal send_order_report
    pre {
      new_deliveries = flower_shop_order_manager:deliveries().difference(ent:sent_deliveries)
      email_request = create_request(new_deliveries)
    }
    http:post(ent:microsoft_send_mail_uri,
      autoraise = "email_sent", 
      json = email_request,
      headers = {"Authorization": "Bearer " + keys:microsoft{"token"}},
      parseJSON = true);
      always {
        ent:sent_deliveries := ent:sent_deliveries.append(new_deliveries);
      }
  }
}
