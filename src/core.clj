(ns core
  (:require [ldb.db :as d]))

(def db ".")
(def conn (d/connect db))

(def records (mapv (fn [i]
                     {:name    (str "Gabriele" i)
                      :surname (str "Carrettoni" i)}) (range 100000)))

(time (d/transaction conn {:tx-data records}))

(time (d/entities-by-id conn (d/q conn {:find  '[?eid]
                                        :where '[[?eid :name ?name]
                                                 [?eid :surname "Carrettoni20"]]})))

(comment
  (d/transaction conn {:tx-data [{:name    "Gabriele"
                                  :surname "Carrettoni"}

                                 {:name    "Gabriele"
                                  :surname "Cafarelli"}

                                 {:name    "Gabriele"
                                  :surname "Cafarelli"}]})

  (def entities (d/entities-by-id conn (d/q conn {:find  '[?eid]
                                                  :where '[[?eid :name "Gabriele"]]})))

  (d/transaction conn {:tx-data [{:db/id   (:db/id (first entities)),
                                  :name    "Giuseppe",
                                  :surname "Cafarelli"}]})

  (d/entities-by-id conn (d/q conn {:find  '[?eid]
                                    :where '[[?eid :name ?name]
                                             [?eid :surname "Cafarelli"]]}))

  (d/show-db conn))
