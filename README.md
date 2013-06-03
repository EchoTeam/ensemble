# ensemble

Cluster jobs distribution and orchestration library for Clojure

    :dependencies [
      [com.aboutecho.ensemble/ensemble "0.1.0"]
    ]

## Jobs distribution

Ensemble uses ZooKeeper to coordinate, so you'll need one installed.

    (require '[com.aboutecho.ensemble :as ensemble])

    (declare stop-all start-all)

    (let [cluster (ensemble/join-cluster {
                    :url   "zk://zookeeper.local:2181"
                    :ns    "/echo-ensemble"
                    :name  "node32" })
          jobs    (ensemble/peer-jobs cluster)]
      (add-watch jobs :state-change
        (fn [_ _ old new]
          (stop-all old)
          (start-all new)))
      (start-all @jobs)
      ...
      (ensemble/leave-cluster cluster))


## Jobs execution

Ensemble can execute long-running tasks in separate Thread and supervise them
to stay alive and not stall.

    (require '[com.aboutecho.ensemble.process :as process])

    (declare on-data)

    (process/supervise
      (fn []
        (let [stream (clj-http/get "https://stream.twitter.com" :as stream)]
          (process/add-finalizer #(.close stream))
          (loop []
            (on-data (.readLine stream))
            (process/heertbeat)
            (recur))))
      { :name "twitter-stream" 
        :threshold  40000 })
    ...
    (process/stop-supervisor sup)

## 
Copyright © 2013 JackNyfe, Inc. (dba Echo) [http://aboutecho.com/](http://aboutecho.com/).
All rights reserved.