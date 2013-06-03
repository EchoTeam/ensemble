(ns com.aboutecho.ensemble.test.zookeeper
  (:require
    [com.aboutecho.ensemble.util :as util]
    [com.aboutecho.ensemble.zookeeper :as zookeeper])
  (:use clojure.test))

(deftest test-zookeeper []
  (with-open [server (zookeeper/server)
              zk     (zookeeper/connect :url (:url server))]
    (testing "basic operations"
      (zookeeper/create zk "/a" :persistent? true)
      (zookeeper/create zk "/a/b")
      (zookeeper/create zk "/a/c")
      (is (= true (zookeeper/exists? zk "/a/b")))
      (is (= #{"b" "c"} (set (zookeeper/children zk "/a"))))
      (zookeeper/delete zk "/a/c")
      (is (= false (zookeeper/exists? zk "/a/c")))
      (is (= #{"b"} (set (zookeeper/children zk "/a")))))

    (testing "create-all"
      (zookeeper/create-all zk "/b")
      (is (= true (zookeeper/exists? zk "/b")))
      (zookeeper/create-all zk "/c/d/e")
      (zookeeper/create-all zk "/c/d/f")
      (is (= true (zookeeper/exists? zk "/c/d/e")))
      (is (= #{"e" "f"} (set (zookeeper/children zk "/c/d")))))

    (testing "data"
      (zookeeper/create zk "/d" :persistent? true :data (.getBytes "123"))
      (is (= (String. (zookeeper/data zk "/d")) "123")))

    (testing "children-data"
      (zookeeper/create-all zk "/e/f" :persistent? true :data (.getBytes "data-f"))
      (zookeeper/create-all zk "/e/g" :persistent? true :data (.getBytes "data-g"))
      (is (= (util/map-vals #(String. %) (zookeeper/children-data zk "/e")) {"f" "data-f" "g" "data-g"})))

    (testing "watcher"
      (let [last-event (atom {:n 0 :e nil})
            watcher    (fn [event] (swap! last-event #(-> % (update-in [:n] inc) (assoc :e event))))]
        (zookeeper/create zk "/f" :persistent? true)

        (testing "on create"
          (zookeeper/children zk "/f" :watcher watcher)
          (zookeeper/create zk "/f/f")
          (util/wait 5000 (= @last-event {:n 1 :e {:event :node-children-changed, :state :sync-connected, :path "/f"}})))

        (testing "is one-time"
          (zookeeper/create zk "/f/g")
          (is (= (:n @last-event) 1)))

        (testing "on delete"
          (zookeeper/children zk "/f" :watcher watcher)
          (zookeeper/delete zk "/f/g")
          (util/wait 5000 (= @last-event {:n 2 :e {:event :node-children-changed, :state :sync-connected, :path "/f"}})))

        (testing "on exists"
          (zookeeper/exists? zk "/f/h" :watcher watcher)
          (zookeeper/create zk "/f/h")
          (util/wait 5000 (= @last-event {:n 3 :e {:event :node-created, :state :sync-connected, :path "/f/h"}}))
          (zookeeper/exists? zk "/f/h" :watcher watcher)
          (zookeeper/delete zk "/f/h")
          (util/wait 5000 (= @last-event {:n 4 :e {:event :node-deleted, :state :sync-connected, :path "/f/h"}})))

        (testing "on data"
          (zookeeper/create zk "/f/i" :data (.getBytes "data-1"))
          (zookeeper/data zk "/f/i" :watcher watcher) ;; set up two waches actually, exists and data
          (zookeeper/delete zk "/f/i")
          (util/wait 5000 (= @last-event {:n 6 :e {:event :node-deleted, :state :sync-connected, :path "/f/i"}})))))))
