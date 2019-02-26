(ns core
  (:require [ldb.db :as d]))

(def db ".")
(def conn (d/connect db))

(comment
  (def records (mapv (fn [i]
                       {:name    (str "Gabriele" i)
                        :surname (str "Carrettoni" i)}) (range 100000)))

  (time (def result (d/transact conn {:tx-data records})))

  (time (d/entities-by-id conn (d/q conn {:find  '[?eid]
                                          :where '[[?eid :name ?name]
                                                   [?eid :surname "Carrettoni"]]})))

  (d/transact conn {:tx-data [{:db/ident       :name
                               :db/valueType   :db.type/string
                               :db/cardinality :db.cardinality/one}

                              {:db/ident       :surname
                               :db/valueType   :db.type/string
                               :db/cardinality :db.cardinality/one}

                              {:db/ident       :address
                               :db/valueType   :db.type/ref
                               :db/cardinality :db.cardinality/many}]})

  (d/transact conn {:tx-data [{                             ;:db/id   3
                               :name    "Kek"
                               :surname "Carrettoni"
                               :address [{:name "kek"}]
                               }]})



  (do (d/transact conn {:tx-data [{:name    ["Gabriele"]
                                   :surname "Carrettoni"}]})
      (d/show-db conn))

  (d/entities-by-id conn (d/q conn {:find  '[?eid]
                                    :where '[[?eid :name "Kek"]
                                             [?eid :surname "Carrettoni"]]}))


  (d/transact conn {:tx-data [{:db/id   (:db/id (first entities)),
                               :name    "Giuseppe",
                               :surname "Cafarelli"}]})

  (d/entities-by-id conn (d/q conn {:find  '[?eid]
                                    :where '[[?eid :name ?name]
                                             [?eid :surname "Cafarelli"]]}))

  (d/show-db conn))
