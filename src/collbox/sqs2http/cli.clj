(ns collbox.sqs2http.cli
  (:require
   [clojure.core.async :as a]
   [clojure.tools.logging :as log]
   [collbox.sqs2http.core :as sqs2http]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Public

(defn run [config]
  (let [[halt-c error-c]
        (->> config
             (merge sqs2http/default-config)
             sqs2http/run)]
    (. (Runtime/getRuntime)
       (addShutdownHook
        (Thread.
         #(do (log/error "Interrupt received, stopping system.")
              (a/close! halt-c)
              (shutdown-agents)))))
    (when (a/<!! error-c)
      (System/exit 1))))
