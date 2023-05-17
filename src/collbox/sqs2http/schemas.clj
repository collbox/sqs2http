(ns collbox.sqs2http.schemas)

(def http-url [:re #"^https?://"])

(def config
  [:map
   [:delete-batch-size [:and pos-int? [:<= 10]]]  ; AWS limit
   [:delete-wait-msecs nat-int?]
   [:fetch-max-messages [:and pos-int? [:<= 10]]] ; AWS limit
   [:fetch-wait-secs nat-int?]
   [:fetch-visibility-timeout-secs
    [:maybe [:and pos-int? [:<= 43200]]]]         ; AWS limit
   [:post-content-type :string]
   [:post-max-connections pos-int?]
   [:post-timeout-msecs pos-int?]
   [:post-url http-url]
   [:queue-url http-url]])
