(ns collbox.sqs2http.core
  (:require
   [clj-http.client :as http]
   [clojure.core.async :as a]
   [clojure.tools.logging :as log]
   [cognitect.aws.client.api :as aws]
   [collbox.sqs2http.schemas :as schemas]
   [malli.core :as m]
   [malli.error :as me]))

(def default-config
  {:delete-batch-size             10
   :delete-wait-msecs             1000
   :fetch-max-messages            10
   :fetch-wait-secs               20
   :fetch-visibility-timeout-secs nil
   :post-content-type             "application/json"
   :post-max-connections          4
   :post-timeout-msecs            180000})

(def fatal-anomaly-category?
  #{:cognitect.anomalies/conflict
    :cognitect.anomalies/fault
    :cognitect.anomalies/forbidden
    :cognitect.anomalies/incorrect
    :cognitect.anomalies/not-found
    :cognitect.anomalies/unsupported})

(defn- fetch-messages
  "Fetch messages from SQS queue in a loop, adding them to channel
  `fetched-c`.  Halts and closes `fetched-c` when channel `halt-c`
  closed."
  [{:keys [fetch-max-messages fetch-wait-secs fetch-visibility-timeout-secs queue-url]}
   sqs halt-c fetched-c]
  (loop []
    (let [{anom-cat :cognitect.anomalies/category
           :as      resp}
          (aws/invoke sqs {:op      :ReceiveMessage
                           :request (cond-> {:QueueUrl            queue-url
                                             :AttributeNames      ["All"]
                                             :MaxNumberOfMessages fetch-max-messages
                                             :WaitTimeSeconds     fetch-wait-secs}
                                      fetch-visibility-timeout-secs
                                      (assoc :VisibilityTimeout fetch-visibility-timeout-secs))})
          ok?
          (cond
            (nil? anom-cat)
            (let [{:keys [Messages]} resp]
              (log/info "Fetched messages"
                        {:count (count Messages)
                         :wait  fetch-wait-secs
                         :ids   (map :MessageId Messages)})
              (when (seq Messages)
                (a/onto-chan! fetched-c Messages false))
              true)

            (fatal-anomaly-category? anom-cat)
            (do (log/fatal "Fatal anomaly while fetching" resp)
                false)

            :else
            (do (log/error "Anomaly while fetching" resp)
                true))

          halt? (nil? (first (a/alts!! [halt-c] :default true)))]
      (if (or (not ok?) halt?)
        (do (log/info "Shutting down fetcher" {:ok? ok?})
            (a/close! fetched-c)
            ok?)
        (recur)))))

(defn- post-message
  "HTTP POST `msg` to `post-url`, add message to channel `result-c` if
  POST succeeds and returns status 200 (OK).

  Closes `result-c` in all cases."
  [{:keys [post-content-type post-timeout-msecs post-url]} msg result-c]
  (try
    (log/info "Posting message" {:url post-url :id (:MessageId msg)})
    (http/post post-url
               {:async              true
                :body               (:Body msg)
                :content-type       post-content-type
                :connection-timeout post-timeout-msecs
                :socket-timeout     post-timeout-msecs}
               (fn on-post-success [{:keys [status] :as resp}]
                 (a/go
                   (if (= 200 status)
                     (do (log/info "Post succeeded" {:id (:MessageId msg)})
                         (a/>! result-c msg))
                     (log/error "Post failed" {:id (:MessageId msg) :resp resp}))
                   (a/close! result-c)))
               (fn on-post-failure [ex]
                 (a/go
                   (log/error ex "Exception while posting")
                   (a/close! result-c))))
    (catch Exception ex
      ;; This shouldn't really happen, but I've seen clj-http throw on
      ;; bad URLs and such, so better safe than sorry.
      (log/error ex "Exception while initializing post")
      (a/close! result-c))))

(defn- delete-messages
  "Delete SQS messages in channel `posted-c` from SQS queue.

  Batches delete requests to reduce AWS costs.  Waits until either
  `delete-batch-size` messages have accumulated, or a message has been
  in queue for `delete-wait-msecs`.  Makes final request immediately
  if `posted-c` channel closes."
  [{:keys [delete-batch-size delete-wait-msecs queue-url]} sqs posted-c]
  (a/go-loop []
    (when-let [msg (a/<! posted-c)]
      (loop [msgs    [msg]
             timeout (a/timeout delete-wait-msecs)]
        (if-let [msg' (and (< (count msgs) delete-batch-size)
                           (first (a/alts! [posted-c timeout])))]
          (recur (conj msgs msg') timeout)
          (a/<!
           (a/thread
             (log/info "Deleting messages" {:ids (map :MessageId msgs)})
             (let [resp (aws/invoke
                         sqs
                         {:op      :DeleteMessageBatch
                          :request {:QueueUrl queue-url
                                    :Entries  (for [{:keys [MessageId ReceiptHandle]} msgs]
                                                {:Id            MessageId
                                                 :ReceiptHandle ReceiptHandle})}})]
               (if (:cognitect.anomalies/category resp)
                 (log/error "Anomaly while deleting" resp)
                 (log/info "Deleted messages" {:result resp})))))))
      (recur))))

(defn run
  "Run main system.  Asynchronous.  Returns a vector of `[halt-c
  error-c]` where `halt-c` is control channel which can be closed to
  shut the system down, and `error-c` is a channel that will return a
  truthy value if a fatal error occurred."
  [{:keys [post-max-connections] :as config}]
  (if-let [config-errs (m/explain schemas/config config)]
    (do (log/fatal "Config is invalid" (me/humanize config-errs))
        (throw (ex-info "Config is invalid" config-errs)))
    (let [sqs       (aws/client
                     {:api :sqs})
          halt-c    (a/chan 1)
          fetched-c (a/chan 64)
          posted-c  (a/chan 64)
          _         (log/info "Starting..." (select-keys config [:post-url :queue-url]))
          error-c   (a/thread (not (fetch-messages config sqs halt-c fetched-c)))]
      (a/pipeline-async post-max-connections posted-c (partial post-message config) fetched-c)
      (delete-messages config sqs posted-c)
      [halt-c error-c])))
