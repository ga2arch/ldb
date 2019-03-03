(ns core
  (:require [ldb.db :as d]
            [clj-http.client :as client]))

(def db ".")
(def conn (d/connect db))

(comment
  (def records (mapv (fn [i]
                       {:name    (str "Gabriele" i)
                        :surname (str "Carrettoni" i)}) (range 100000)))

  (time (def result (d/transact conn {:tx-data records})))

  (time (d/entities-by-ids conn (d/q conn {:find  '[?eid]
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

  (d/transact conn {:tx-data [{:db/ident       :paper/title
                               :db/valueType   :db.type/string
                               :db/cardinality :db.cardinality/one}
                              {:db/ident       :paper/link
                               :db/valueType   :db.type/string
                               :db/cardinality :db.cardinality/one}
                              {:db/ident       :paper/data
                               :db/valueType   :db.type/bytes
                               :db/cardinality :db.cardinality/one}]})

  (def data (bytes (:body (client/get
                            "https://www.ndss-symposium.org/wp-content/uploads/2019/02/ndss2019_02B-5_Wampler_paper.pdf"
                            {:as :byte-array}))))

  (d/transact conn {:tx-data [{:paper/title "Ex Spectre"
                               :paper/link  "https://www.ndss-symposium.org/wp-content/uploads/2019/02/ndss2019_02B-5_Wampler_paper.pdf"
                               :paper/data  (.getBytes "kek")}]})

  (d/transact conn {:tx-data [{                             ;:db/id   3
                               :name    "Kek"
                               :surname "Carrettoni"
                               :address [{                  ;:db/id 4
                                          :name  "Via lol"}
                                         {
                                          :name  "dub"}]}]})

  (do (d/transact conn {:tx-data [{:name    ["Gabriele"]
                                   :surname "Carrettoni"}]})
      (d/show-db conn))

  (d/entities-by-ids conn (d/q conn {:find  '[?eid]
                                     :where '[[?eid :name "Kek"]
                                              [?eid :surname "Carrettoni"]]}))

  (d/transact conn {:tx-data [{:db/id   (:db/id (first entities)),
                               :name    "Giuseppe",
                               :surname "Cafarelli"}]})

  (d/entities-by-ids conn (d/q conn {:find  '[?eid]
                                     :where '[[?eid :name ?name]
                                              [?eid :surname "Cafarelli"]]}))

  (d/show-db conn))
