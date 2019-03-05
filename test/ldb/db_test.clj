(ns ldb.db-test
  (:require [ldb.db :refer :all]
            [clojure.test :refer :all]
            [clojure.java.io :as jio]))

(def ^:dynamic conn)
(defn setup [f]
  (binding [conn (connect "/tmp")]
    (f)
    (jio/delete-file "/tmp/data.mdb")
    (jio/delete-file "/tmp/lock.mdb")))

(use-fixtures :each setup)

(deftest insert
  (testing "schema insertion"
    (let [{:keys [tx-data tempids]} (transact conn {:tx-data [{:db/ident       :name
                                                               :db/valueType   :db.type/string
                                                               :db/cardinality :db.cardinality/one}]})]
      (is (= {"1" 0, "ldb.tx" 1} tempids))))

  (testing "data insertion"
    (let [{:keys [tx-data tempids]} (transact conn {:tx-data [{:name "Gabriele"}]})]
      (is (= {"1" 2, "ldb.tx" 3} tempids))))

  (testing "data extraction"
    (let [res (q conn {:find  '[?eid]
                       :where '[[?eid :name "Gabriele"]]})]
      (is (= res [(list 2)])))))
