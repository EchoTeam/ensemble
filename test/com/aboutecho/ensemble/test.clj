(ns com.aboutecho.ensemble.test
  (:require
    [clojure.string :as string]
    [com.aboutecho.ensemble :as ensemble]
    [com.aboutecho.ensemble.zookeeper :as zookeeper]
    [com.aboutecho.ensemble.util :as util])
  (:use clojure.test))

(defmacro with-cluster [[sym opts] & body]
 `(let [~sym (ensemble/join-cluster ~opts)]
    (try
      ~@body
      (finally
        (ensemble/leave-cluster ~sym)))))

(deftest test-sync-nodes
  (with-open [server (zookeeper/server)]
    (with-open [zk (zookeeper/connect :url (:url server))]
      (ensemble/sync-nodes zk "/some/path" {"ch1" {:id 1} "ch2" {:id 2} "ch3" {:id 3} "ch4" nil})
      (is (= (ensemble/children-data zk "/some/path")
             {"ch1" {:id 1} "ch2" {:id 2} "ch3" {:id 3} "ch4" nil}))

      (ensemble/sync-nodes zk "/some/path" {"ch1" {:id 1} "ch2" {:id 22} "ch4" {:id 4} "ch5" {:id 5}})
      (is (= (ensemble/children-data zk "/some/path")
             {"ch1" {:id 1} "ch2" {:id 22} "ch4" {:id 4} "ch5" {:id 5}}))

      (ensemble/sync-nodes zk "/some/path" {})
      (is (= (zookeeper/children zk "/some/path") nil)))))

(deftest test-closest-before
  (is (= -1 (ensemble/closest-before [10 20 30 40] 5)))
  (is (= 10 (ensemble/closest-before [10 20 30 40] 10)))
  (is (= 10 (ensemble/closest-before [10 20 30 40] 15)))
  (is (= 20 (ensemble/closest-before [10 20 30 40] 20)))
  (is (= 20 (ensemble/closest-before [10 20 30 40] 25)))
  (is (= 40 (ensemble/closest-before [10 20 30 40] 40)))
  (is (= 40 (ensemble/closest-before [10 20 30 40] 45))))

(deftest test-short-hash
  (is (not= (@#'ensemble/short-hash "abc") (@#'ensemble/short-hash "def"))))

(deftest test-responsible
  (let [ring [[100 200 300]
              [110 210 310]
              [120 220 320]]]
    (testing "0..first_region always for last peer"
      (= (ensemble/responsible 50 ring) 2)
      (= (ensemble/responsible 50 (reverse ring)) 2))
    (are [hash res] (= (ensemble/responsible hash ring) res)
      100 0
      105 0
      110 1
      115 1
      120 2
      200 0
      205 0
      210 1
      215 1
      300 0
      305 0
      310 1
      315 1
      320 2
      330 2))

  (let [ring [[100 200]
              [110 190]
              [120 180]]]
    (testing "0..first_region always for last peer"
      (= (ensemble/responsible 50 ring) 2)
      (= (ensemble/responsible 50 (reverse ring)) 2))

    (are [hash res] (= (ensemble/responsible hash ring) res)
      100 0
      105 0
      110 1
      115 1
      120 2
      150 2
      180 2
      185 2
      190 1
      195 1
      200 0
      205 0))

  (testing "single peer takes everything"
    (let [ring [[100 200 300]]]
      (are [hash res] (= (ensemble/responsible hash ring) res)
        50 0
        100 0
        150 0
        200 0
        250 0
        300 0
        350 0)))

  (testing "vnodes collisions"
    (let [ring [[100         140 150 160             200]
                [    110     140 150     170     190]
                [        120 140     160 170 180]]]
      (are [hash res] (= (ensemble/responsible hash ring) res)
        105 0
        115 1
        125 2
        140 2 ;; last one wins
        145 2 ;; ...
        150 1
        155 1
        160 2
        165 2
        170 2
        175 2
        180 2
        185 2
        190 1
        200 0
        205 0))))

(deftest test-join
  (with-open [server (zookeeper/server)]
    (let [peers (fn [cluster]
                  (->> (:peers @(:tree cluster))
                    keys
                    (map #(first (string/split % #"#")))))]
      (with-cluster [n2 {:name "n2" :url (:url server)}]
        (util/wait 5000 (= (peers n2) ["n2"]))

        (with-cluster [n1 {:name "n1" :url (:url server)}]
          (util/wait 5000 (= (peers n1) ["n1" "n2"]))
          (util/wait 5000 (= (peers n2) ["n1" "n2"]))

          (with-cluster [n3 {:name "n3" :url (:url server)}]
            (util/wait 5000 (= (peers n1) ["n1" "n2" "n3"]))
            (util/wait 5000 (= (peers n2) ["n1" "n2" "n3"]))
            (util/wait 5000 (= (peers n3) ["n1" "n2" "n3"]))

            (ensemble/leave-cluster n2)
            (util/wait 5000 (= (peers n1) ["n1" "n3"]))
            (util/wait 5000 (= (peers n2) []))
            (util/wait 5000 (= (peers n3) ["n1" "n3"]))

            (with-cluster [n4 {:name "n4" :url (:url server)}]
              (util/wait 5000 (= (peers n1) ["n1" "n3" "n4"]))
              (util/wait 5000 (= (peers n3) ["n1" "n3" "n4"]))
              (util/wait 5000 (= (peers n4) ["n1" "n3" "n4"]))

              (ensemble/leave-cluster n1)
              (ensemble/leave-cluster n4)
              (util/wait 5000 (= (peers n3) ["n3"])))))))))

(deftest test-players
  (let [peers {"n1" {:role #".*"}
               "n2" {:role #"twitter\..*"}
               "n3" {:role #".*\.m2.large"}
               "n4" {:role #""}}]
    (are [job res] (= (ensemble/players job peers) res)
      "test"              ["n1"]
      "twitter.firehose"  ["n1" "n2"]
      "pipeline.m2.large" ["n1" "n3"]
      "twitter.m2.large"  ["n1" "n2" "n3"])))

(deftest test-subscribe
  (let [zk (zookeeper/connect :url "zk://localhost:2382" :ns "/testsubscribe")] ;; connect to inactive server
    (try
      (let [tree (ensemble/subscribe zk)]
        (util/wait 5000 (= @tree {}))
        (with-open [server (zookeeper/server :port 2382)]
          (zookeeper/create-all zk "/nodes" :persistent? true)
          (util/wait 5000 (= @tree {:nodes {}}))

          (zookeeper/create zk "/nodes/n1")
          (util/wait 5000 (= @tree {:nodes {"n1" nil}}))

          (zookeeper/create zk "/nodes/n2" :persistent? true)
          (util/wait 5000 (= @tree {:nodes {"n1" nil "n2" nil}}))

          (zookeeper/delete zk "/nodes/n1")
          (util/wait 5000 (= @tree {:nodes {"n2" nil}}))

          (zookeeper/delete zk "/nodes/n2")
          (util/wait 5000 (= @tree {:nodes {}}))

          (zookeeper/delete zk "/nodes")
          (util/wait 5000 (= @tree {}))

          (zookeeper/create-all zk "/nodes/n3" :data (@#'ensemble/serialize {:x 1 :y "2"}))
          (util/wait 5000 (= @tree {:nodes {"n3" {:x 1 :y "2"}}}))

          ;; close before server to avoid long stacktrace
          (zookeeper/close zk)))
      (finally
        (zookeeper/close zk)))))

(deftest test-peer-jobs
  (with-open [server (zookeeper/server)]
    (with-redefs [ensemble/short-hash (fn [s] (Integer/parseInt (second (re-matches #"job-(\d+)" s))))] 
      (let [genjobs (fn [& hashes] (into {} (map (fn [hash] [(str "job-" hash) (str "data-" hash)]) hashes)))]
        (with-cluster [n1 {:name "n1" :vnodes [50 100] :url (:url server)}]
          (let [n1-jobs (ensemble/peer-jobs n1)]
            (testing "Single node"
              (ensemble/sync-jobs n1 (genjobs 45 55 65 75))
              (util/wait 5000 (= @n1-jobs (genjobs 45 55 65 75)))

              (testing "remove job"
                (ensemble/sync-jobs n1 (genjobs 45 55 75))
                (util/wait 5000 (= @n1-jobs (genjobs 45 55 75))))

              (testing "add job"
                (ensemble/sync-jobs n1 (genjobs 45 55 75 105 115))
                (util/wait 5000 (= @n1-jobs (genjobs 45 55 75 105 115)))))

            (testing "Nodes join/leave"
              (with-cluster [n2 {:name "n2" :vnodes [60 110] :url (:url server)}]
                (let [n2-jobs (ensemble/peer-jobs n2)]
                  (util/wait 5000 (= @n1-jobs (genjobs 55 105)))
                  (util/wait 5000 (= @n2-jobs (genjobs 45 75 115)))

                  (with-cluster [n3 {:name "n3" :vnodes [70 120] :url (:url server)}]
                    (let [n3-jobs (ensemble/peer-jobs n3)]
                      (util/wait 5000 (= @n1-jobs (genjobs 55 105)))
                      (util/wait 5000 (= @n2-jobs (genjobs 115)))
                      (util/wait 5000 (= @n3-jobs (genjobs 45 75)))

                      (ensemble/leave-cluster n2)
                      (util/wait 5000 (= @n1-jobs (genjobs 55 105 115)))
                      (util/wait 5000 (= @n2-jobs nil))
                      (util/wait 5000 (= @n3-jobs (genjobs 45 75))))

                    (testing "Jobs reconfig"
                      (let [n3-jobs (ensemble/peer-jobs n3)]  ;; second watcher
                        (util/wait 5000 (= @n3-jobs (genjobs 45 75)))

                        (ensemble/sync-jobs n1 (genjobs 55 65 75))
                        (util/wait 5000 (= @n1-jobs (genjobs 55 65)))
                        (util/wait 5000 (= @n3-jobs (genjobs 75)))

                        (ensemble/sync-jobs n1 (genjobs 105 115 125))
                        (util/wait 5000 (= @n1-jobs (genjobs 105 115)))
                        (util/wait 5000 (= @n3-jobs (genjobs 125)))))))))))))))
