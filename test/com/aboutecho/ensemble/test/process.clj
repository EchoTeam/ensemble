(ns com.aboutecho.ensemble.test.process
  (:require
    [com.aboutecho.ensemble.util :as util]
    [com.aboutecho.ensemble.process :as process])
  (:use clojure.test))

(defn- resource []
  { :data  (atom 1)
    :state (atom []) })

(defn- finalize [res]
  (fn []
    (swap! (:state res) conj :closed)))

(defn- infiloop []
  (Thread/sleep 1)
  (recur))

(defn- fail []
  (throw (ex-info "" {:log? false} )))

(deftest test-process
  (testing "Exits gracefully"
    (let [res     (resource)
          process (process/spawn
                    (fn []
                      (swap! (:data res) inc))
                    { :name "test-process-exit"
                      :finalizers [(finalize res)] })]
      (util/wait 5000 (= @(:state res) [:closed]))
      (is (= @(:data  res) 2))))

  (testing "Stopping"
    (let [res     (resource)
          process (process/spawn
                    (fn []
                      (loop []
                        (swap! (:data res) inc)
                        (Thread/sleep 1)
                        (recur))
                      (reset! (:data res) -1))
                    { :name "test-process-stop"
                      :finalizers [(finalize res)] })]
      (Thread/sleep 10)
      (process/stop-process process)
      (util/wait 5000 (= @(:state res) [:closed]))
      (is (> @(:data  res) 3))))

  (testing "Error handling"
    (let [res     (resource)
          process (process/spawn
                    (fn []
                      (swap! (:data res) inc)
                      (fail)
                      (reset! (:data res) -1))
                    { :name "test-process-error"
                      :finalizers [(finalize res)] })]
      (util/wait 5000 (= @(:state res) [:closed]))
      (is (= @(:data  res) 2))))

  (testing "Adding a finalizer"
    (let [res1  (resource)
          res2  (resource)
          latch (promise)
          process (process/spawn
                    (fn []
                      (process/add-finalizers [(finalize res2)])
                      (deliver latch :ok)
                      (infiloop))
                    { :name "test-process-reg"
                      :finalizers [(finalize res1)] })]
      @latch
      (process/stop-process process)
      (util/wait 5000 (= @(:state res1) [:closed]))
      (util/wait 5000 (= @(:state res2) [:closed])))))

(deftest test-supervisor
  (testing "Supervisor stops process"
    (let [res-proc (resource)
          res-sup  (resource)
          latch    (promise)
          sup (process/supervise
                (fn []
                  (process/add-finalizers [(finalize res-proc)])
                  (deliver latch :ok)
                  (infiloop)
                  (reset! (:data res-proc) -1)
                  (reset! (:data res-sup) -1))
                { :name "test-sup-stop"
                  :finalizers [(finalize res-sup)] })]
      @latch
      (process/stop-supervisor sup)
      (util/wait 5000 (= @(:state res-proc) [:closed]))
      (util/wait 5000 (= @(:state res-sup)  [:closed]))
      (is (= @(:data  res-proc) 1))
      (is (= @(:data  res-sup)  1))))

  (testing "Supervisor restarts process"
    (let [res (resource)
          sup (process/supervise
                (fn []
                  (swap! (:data res) inc)
                  (fail))
                { :name "test-sup-restart"
                  :finalizers [(finalize res)]
                  :threshold 10 })]
      (Thread/sleep 100)
      (process/stop-supervisor sup)
      (util/wait 5000 (= @(:state res) [:closed]))
      (is (> @(:data  res) 5))))

  (testing "Supervisor restarts stalled process"
    (let [res (resource)
          sup (process/supervise
                (fn []
                  (reset! (:data res) 1)
                  (loop []
                    (swap! (:data res) inc)
                    (Thread/sleep 10)
                    (process/heartbeat)
                    (recur)))
                { :name "test-sup-stalled"
                  :finalizers [(finalize res)]
                  :threshold 25 })]
      (Thread/sleep 100)
      (process/stop-supervisor sup)
      (util/wait 5000 (= @(:state res) [:closed]))
      (is (> @(:data  res) 5)))

    (let [res (resource)
          sup (process/supervise
                (fn []
                  (swap! (:data res) inc)
                  (Thread/sleep 100)
                  (reset! (:data res) -1))
                { :name "test-sup-stalled"
                  :finalizers [(finalize res)]
                  :threshold 25 })]
      (Thread/sleep 100)
      (process/stop-supervisor sup)
      (util/wait 5000 (= @(:state res) [:closed]))
      (is (> @(:data  res) 3)))))

(defn fn-with-subprocess []
  (Thread/sleep 10)
  (reduce +
    (pmap (fn [i]
            (Thread/sleep 10)
            (process/heartbeat)
            i)
      (range 1 5))
    ))

(deftest test-subprocess-heartbeat
  (let [res (resource)
        started (System/currentTimeMillis)
        prcss (process/spawn
                fn-with-subprocess {:name "fn-with-subprocess" :finalizers [(finalize res)]})]
    (Thread/sleep 100)
    (util/wait 5000 (= @(:state res) [:closed]))
    (let [meta (meta prcss)
          last-beat (:heartbeat meta)]
      (is (> @last-beat started)))))
